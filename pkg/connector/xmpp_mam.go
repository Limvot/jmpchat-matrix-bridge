// MAM (XEP-0313 Message Archive Management) support for backfill and catch-up.
// Used for: (1) backfilling history when the bridge requests it; (2) ensuring
// messages sent from other Jabber clients appear in the Matrix room (catch-up).

package connector

import (
	"context"
	"encoding/xml"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
	"mellium.im/xmpp/stanza"
	"mellium.im/xmlstream"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
)

const (
	mamNS     = "urn:xmpp:mam:2"
	forwardNS = "urn:xmpp:forward:0"
	delayNS   = "urn:xmpp:delay"
	rsmNS     = "http://jabber.org/protocol/rsm"
	xdataNS   = "jabber:x:data"
)

// mamPendingEntry holds the channel and metadata for an in-flight MAM query.
type mamPendingEntry struct {
	ch      chan mamResultItem
	portal  networkid.PortalKey
	forward bool
}

type mamResultItem struct {
	From      string
	To        string
	Type      string
	StanzaID  string
	Body      string
	Timestamp time.Time
	QueryID   string
}

// mamState holds MAM-related state for the client.
type mamState struct {
	mu      sync.Mutex
	pending map[string]*mamPendingEntry
}

func (m *mamState) register(queryID string, portal networkid.PortalKey, forward bool) chan mamResultItem {
	ch := make(chan mamResultItem, 50)
	m.mu.Lock()
	if m.pending == nil {
		m.pending = make(map[string]*mamPendingEntry)
	}
	m.pending[queryID] = &mamPendingEntry{ch: ch, portal: portal, forward: forward}
	m.mu.Unlock()
	return ch
}

func (m *mamState) deliverMessage(queryID string, item mamResultItem) bool {
	m.mu.Lock()
	entry := m.pending[queryID]
	m.mu.Unlock()
	if entry == nil {
		return false
	}
	select {
	case entry.ch <- item:
		return true
	default:
		return false
	}
}

func (m *mamState) closeAndRemove(queryID string) {
	m.mu.Lock()
	entry := m.pending[queryID]
	if entry != nil {
		delete(m.pending, queryID)
		close(entry.ch)
	}
	m.mu.Unlock()
}

// mamFinHandler adapts xmppClient for mux.IQ so MAM <fin> results close the pending channel.
type mamFinHandler struct{ x *xmppClient }

func (h mamFinHandler) HandleIQ(iq stanza.IQ, _ xmlstream.TokenReadEncoder, _ *xml.StartElement) error {
	id := string(iq.ID)
	if strings.HasPrefix(id, "mam-") {
		queryID := strings.TrimPrefix(id, "mam-")
		h.x.mam.closeAndRemove(queryID)
	}
	return nil
}

// handleMAMResult is called when the message handler sees a MAM result wrapper.
// It parses the forwarded message and delivers it to the pending MAM collector.
func (x *xmppClient) handleMAMResult(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	queryID := attrValue(start.Attr, "queryid")
	if queryID == "" {
		return nil
	}
	// Parse <forwarded xmlns='urn:xmpp:forward:0'><message .../><delay .../></forwarded>
	var from, to, msgType, stanzaID, body string
	var timestamp time.Time
	depth := 1
	for depth > 0 {
		tok, err := t.Token()
		if err != nil {
			return err
		}
		switch v := tok.(type) {
		case xml.StartElement:
			depth++
			// Inner message may have xmlns="jabber:client" (XEP-0297) or inherit forwardNS; accept either.
			if v.Name.Local == "message" && from == "" {
				from = attrValue(v.Attr, "from")
				to = attrValue(v.Attr, "to")
				msgType = attrValue(v.Attr, "type")
				stanzaID = attrValue(v.Attr, "id")
			}
			if v.Name.Local == "body" {
				// Next token(s) are character data
				inner, ierr := t.Token()
				if ierr != nil {
					continue
				}
				if cd, ok := inner.(xml.CharData); ok {
					body = strings.TrimSpace(string(cd))
				}
			}
			if v.Name.Space == delayNS && v.Name.Local == "delay" {
				if stamp := attrValue(v.Attr, "stamp"); stamp != "" {
					// XEP-0082: CCYY-MM-DDThh:mm:ss[.sss]Z
					if t, err := time.Parse(time.RFC3339, stamp); err == nil {
						timestamp = t
					}
				}
			}
		case xml.EndElement:
			depth--
		}
	}
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	if stanzaID == "" {
		stanzaID = "xmp:" + time.Now().Format("20060102150405.000")
	}
	x.mam.deliverMessage(queryID, mamResultItem{
		From: from, To: to, Type: msgType, StanzaID: stanzaID, Body: body, Timestamp: timestamp, QueryID: queryID,
	})
	return nil
}

// mamQueryPayload returns a TokenReader for the MAM query element (XEP-0313).
func mamQueryPayload(queryID, withJID string, start, end *time.Time, max int) xml.TokenReader {
	qStart := xml.StartElement{
		Name: xml.Name{Space: mamNS, Local: "query"},
		Attr: []xml.Attr{{Name: xml.Name{Local: "queryid"}, Value: queryID}},
	}
	var formTokens []xml.Token
	formFields := []string{"FORM_TYPE", "urn:xmpp:mam:2"}
	if start != nil {
		formFields = append(formFields, "start", start.UTC().Format(time.RFC3339))
	}
	if end != nil {
		formFields = append(formFields, "end", end.UTC().Format(time.RFC3339))
	}
	if withJID != "" {
		formFields = append(formFields, "with", withJID)
	}
	xStart := xml.StartElement{Name: xml.Name{Space: xdataNS, Local: "x"}, Attr: []xml.Attr{{Name: xml.Name{Local: "type"}, Value: "submit"}}}
	formTokens = append(formTokens, xStart)
	for i := 0; i < len(formFields); i += 2 {
		varName := formFields[i]
		varValue := ""
		if i+1 < len(formFields) {
			varValue = formFields[i+1]
		}
		fieldStart := xml.StartElement{Name: xml.Name{Space: xdataNS, Local: "field"}, Attr: []xml.Attr{{Name: xml.Name{Local: "var"}, Value: varName}}}
		valueStart := xml.StartElement{Name: xml.Name{Space: xdataNS, Local: "value"}}
		formTokens = append(formTokens, fieldStart, valueStart, xml.CharData(varValue), valueStart.End(), fieldStart.End())
	}
	formTokens = append(formTokens, xStart.End())

	var rsmTokens []xml.Token
	if max > 0 {
		setStart := xml.StartElement{Name: xml.Name{Space: rsmNS, Local: "set"}}
		maxStart := xml.StartElement{Name: xml.Name{Space: rsmNS, Local: "max"}}
		rsmTokens = append(rsmTokens, setStart, maxStart, xml.CharData(fmt.Sprintf("%d", max)), maxStart.End(), setStart.End())
	}

	readers := make([]xml.TokenReader, 0, 3)
	readers = append(readers, xmlstream.Token(qStart))
	for _, t := range formTokens {
		readers = append(readers, xmlstream.Token(t))
	}
	for _, t := range rsmTokens {
		readers = append(readers, xmlstream.Token(t))
	}
	readers = append(readers, xmlstream.Token(qStart.End()))
	return xmlstream.MultiReader(readers...)
}

// fetchMAMItems runs a MAM query and returns the collected items. Used by FetchMessages and by periodic catch-up.
func (x *xmppClient) fetchMAMItems(ctx context.Context, portalKey networkid.PortalKey, withJID string, start, end time.Time, max int) ([]mamResultItem, error) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, bridgev2.ErrNotLoggedIn
	}
	local := sess.LocalAddr()
	if local.String() == "" {
		return nil, fmt.Errorf("no local JID for MAM query")
	}
	// XEP-0313: user archive is queried at the account's bare JID.
	toJID := local.Bare()
	queryID := xid.New().String()
	ch := x.mam.register(queryID, portalKey, true)
	defer x.mam.closeAndRemove(queryID)
	iqID := "mam-" + queryID
	payload := mamQueryPayload(queryID, withJID, &start, &end, max)
	iq := stanza.IQ{ID: iqID, Type: stanza.SetIQ, To: toJID}
	if err := sess.Send(ctx, iq.Wrap(payload)); err != nil {
		return nil, err
	}
	timeout := time.NewTimer(25 * time.Second)
	defer timeout.Stop()
	var items []mamResultItem
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout.C:
			return items, nil
		case item, ok := <-ch:
			if !ok {
				return items, nil
			}
			items = append(items, item)
		}
	}
}

// FetchMessages implements BackfillingNetworkAPI. It uses MAM (XEP-0313) when the server
// supports it to backfill history and to catch up messages sent from other Jabber clients.
func (x *xmppClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if !x.IsLoggedIn() {
		return nil, bridgev2.ErrNotLoggedIn
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, bridgev2.ErrNotLoggedIn
	}

	portalID := string(params.Portal.ID)
	if portalID == "" {
		return nil, nil
	}
	x.log.Info().
		Str("portal_id", portalID).
		Bool("forward", params.Forward).
		Int("count", params.Count).
		Msg("Backfill FetchMessages called by bridge")
	x.addPortalIDForMAMCatchUp(portalID)

	// Only support DM for now; MUC MAM may use different with JID (room@service).
	withJID := portalID
	var anchorTime time.Time
	if params.AnchorMessage != nil {
		anchorTime = params.AnchorMessage.Timestamp
	}
	count := params.Count
	if count <= 0 {
		count = 50
	}
	var start, end time.Time
	if params.Forward {
		start = anchorTime
		end = time.Now()
	} else {
		end = anchorTime
		if anchorTime.IsZero() {
			end = time.Now()
		}
		start = end.Add(-365 * 24 * time.Hour)
	}

	items, err := x.fetchMAMItems(ctx, params.Portal.PortalKey, withJID, start, end, count+1)
	if err != nil {
		x.log.Warn().Err(err).Str("portal_id", portalID).Msg("MAM query failed")
		return nil, err
	}
	x.log.Info().Str("portal_id", portalID).Int("mam_items", len(items)).Msg("MAM backfill query returned")

	// Convert to BackfillMessage; respect count (we may have requested count+1 to detect hasMore).
	hasMore := len(items) > count
	if len(items) > count {
		items = items[:count]
	}
	// MAM server order is implementation-defined; sort by timestamp so Matrix always gets oldest-first.
	if len(items) > 1 {
		sort.Slice(items, func(i, j int) bool { return items[i].Timestamp.Before(items[j].Timestamp) })
	}

	messages := make([]*bridgev2.BackfillMessage, 0, len(items))
	for _, item := range items {
		fromBare := item.From
		if idx := strings.Index(item.From, "/"); idx >= 0 {
			fromBare = item.From[:idx]
		}
		// Skip groupchat for now if we ever get MUC MAM
		if item.Type == "groupchat" {
			continue
		}
		sender := x.makeEventSender(fromBare)
		msgID := networkid.MessageID(item.StanzaID)
		if msgID == "" {
			msgID = networkid.MessageID("xmp:" + item.Timestamp.Format("20060102150405.000"))
		}
		cm := &bridgev2.ConvertedMessage{
			Parts: []*bridgev2.ConvertedMessagePart{{
				Type:    event.EventMessage,
				Content: &event.MessageEventContent{MsgType: event.MsgText, Body: item.Body},
			}},
		}
		messages = append(messages, &bridgev2.BackfillMessage{
			ConvertedMessage: cm,
			Sender:           sender,
			ID:               msgID,
			TxnID:            networkid.TransactionID(msgID),
			Timestamp:        item.Timestamp,
			StreamOrder:      item.Timestamp.UnixMilli(),
		})
	}

	return &bridgev2.FetchMessagesResponse{
		Messages:  messages,
		HasMore:   hasMore,
		Forward:   params.Forward,
		MarkRead:  false,
	}, nil
}

const mamCatchUpInterval = 45 * time.Second
const mamCatchUpWindow = 2 * time.Minute // first run: messages from last 2 minutes

// runMAMCatchUpLoop runs periodic MAM catch-up so messages sent from other Jabber clients
// appear in the Matrix room within roughly one interval (e.g. 45s) when carbons are not used.
func (x *xmppClient) runMAMCatchUpLoop(ctx context.Context) {
	defer func() { x.log.Debug().Msg("MAM catch-up loop exited") }()
	ticker := time.NewTicker(mamCatchUpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !x.IsLoggedIn() || x.login == nil {
				continue
			}
			x.runMAMCatchUpOnce(ctx)
		}
	}
}

// addPortalIDForMAMCatchUp records a portal ID so we run MAM catch-up for it even if not in roster
// (e.g. Cheogram group MMS). Call when queueing any inbound or carbon message.
func (x *xmppClient) addPortalIDForMAMCatchUp(portalID string) {
	if portalID == "" {
		return
	}
	key := strings.ToLower(portalID)
	x.knownPortalIDsForMAMMu.Lock()
	if x.knownPortalIDsForMAM == nil {
		x.knownPortalIDsForMAM = make(map[string]struct{})
	}
	x.knownPortalIDsForMAM[key] = struct{}{}
	x.knownPortalIDsForMAMMu.Unlock()
}

// runMAMCatchUpOnce runs MAM catch-up for roster contacts and any portal we've seen (e.g. Cheogram groups).
func (x *xmppClient) runMAMCatchUpOnce(ctx context.Context) {
	jidSet := make(map[string]struct{})
	x.rosterMu.RLock()
	for j := range x.roster {
		jidSet[strings.ToLower(j)] = struct{}{}
	}
	x.rosterMu.RUnlock()
	x.knownPortalIDsForMAMMu.Lock()
	for j := range x.knownPortalIDsForMAM {
		jidSet[j] = struct{}{}
	}
	x.knownPortalIDsForMAMMu.Unlock()
	if len(jidSet) == 0 {
		return
	}
	jids := make([]string, 0, len(jidSet))
	for j := range jidSet {
		jids = append(jids, j)
	}
	for _, bareJID := range jids {
		x.mamCatchUpForPortal(ctx, bareJID)
		// Brief yield so we don't hammer the server
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// runFullBackfill runs MAM backfill (mamBackfillOnView) for every roster DM JID, skipping MUCs.
// Used after login (when full_backfill_on_login is set) and when the user runs sync-roster (when backfill is enabled),
// so portals are created and history filled before new messages arrive (avoids newest message appearing above history).
// Respects backfill.enabled; no-ops if disabled. Throttles 200ms between JIDs.
func (x *xmppClient) runFullBackfill(ctx context.Context) {
	if x.login == nil || x.conn.backfillConfig == nil || !x.conn.backfillConfig.Enabled {
		return
	}
	x.rosterMu.RLock()
	jids := make([]string, 0, len(x.roster))
	for j := range x.roster {
		jids = append(jids, strings.ToLower(j))
	}
	x.rosterMu.RUnlock()
	if len(jids) == 0 {
		x.log.Debug().Msg("Full backfill: roster empty, nothing to do")
		return
	}
	x.log.Info().Int("roster_count", len(jids)).Msg("Full backfill: starting MAM backfill for roster DMs")
	done := 0
	for _, bareJID := range jids {
		if ctx.Err() != nil {
			break
		}
		if x.isMUC(ctx, bareJID) {
			continue
		}
		x.mamBackfillOnView(ctx, bareJID)
		done++
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
	x.log.Info().Int("backfilled_count", done).Msg("Full backfill: finished")
}

// mamCatchUpForPortal queries MAM for messages since last sync and queues new ones as remote events.
func (x *xmppClient) mamCatchUpForPortal(ctx context.Context, bareJID string) {
	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(bareJID),
		Receiver: x.loginID,
	}
	x.lastMAMSyncMu.Lock()
	lastSync := x.lastMAMSync[bareJID]
	x.lastMAMSyncMu.Unlock()
	if lastSync.IsZero() {
		lastSync = time.Now().Add(-mamCatchUpWindow)
	}
	end := time.Now()
	items, err := x.fetchMAMItems(ctx, portalKey, bareJID, lastSync, end, 50)
	if err != nil {
		x.log.Debug().Err(err).Str("portal_id", bareJID).Msg("MAM catch-up query failed")
		return
	}
	if len(items) == 0 {
		x.lastMAMSyncMu.Lock()
		if x.lastMAMSync == nil {
			x.lastMAMSync = make(map[string]time.Time)
		}
		x.lastMAMSync[bareJID] = end
		x.lastMAMSyncMu.Unlock()
		return
	}
	queued := 0
	for _, item := range items {
		if item.Type == "groupchat" {
			continue
		}
		msgID := networkid.MessageID(item.StanzaID)
		if msgID == "" {
			continue
		}
		existing, _ := x.conn.br.DB.Message.GetFirstPartByID(ctx, x.loginID, msgID)
		if existing != nil {
			continue
		}
		// GetFirstPartByID finds XMPP-originated messages (ID = stanza ID). Matrix-originated messages
		// use ID = matrix:$eventId and store stanza ID in metadata. Search portal for metadata match.
		searchStart := item.Timestamp.Add(-2 * time.Minute)
		if searchStart.After(end) {
			searchStart = end.Add(-5 * time.Minute)
		}
		dbMessages, err := x.conn.br.DB.Message.GetMessagesBetweenTimeQuery(ctx, portalKey, searchStart, end.Add(time.Minute))
		if err == nil {
			for _, msg := range dbMessages {
				if msg.Metadata != nil {
					if meta, ok := msg.Metadata.(*MessageMetadata); ok && meta != nil && meta.XMPPMessageID == item.StanzaID {
						existing = msg
						break
					}
				}
				if string(msg.ID) == item.StanzaID {
					existing = msg
					break
				}
			}
		}
		if existing != nil {
			continue
		}
		// Skip ephemeral/chat state notifications (no body).
		if item.Body == "" {
			continue
		}
		senderJID, bodyOut := ResolveCheogramGroupMMSSenderForMAM(item.From, item.Body)
		sender := x.makeEventSender(senderJID)
		ts := item.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}
		x.login.QueueRemoteEvent(&jabberRemoteMessage{
			portalKey: portalKey,
			sender:    sender,
			id:        msgID,
			body:      bodyOut,
			timestamp: ts,
			x:         x,
		})
		queued++
	}
	if queued > 0 {
		x.log.Debug().Str("portal_id", bareJID).Int("queued", queued).Msg("MAM catch-up: queued new messages")
	}
	x.lastMAMSyncMu.Lock()
	if x.lastMAMSync == nil {
		x.lastMAMSync = make(map[string]time.Time)
	}
	x.lastMAMSync[bareJID] = end
	x.lastMAMSyncMu.Unlock()
}

const mamBackfillWindow = 365 * 24 * time.Hour
const mamBackfillMaxMessagesDefault = 50

// maybeTriggerBackfillOnNewPortal is called when queueing an inbound remote event. If the portal
// has no Matrix room yet (MXID empty), the bridge will create one. Trigger MAM backfill in a
// goroutine—must be async because the XMPP handler must return for the mellium Serve loop to
// read IQ results; the portal lookup (DB) and portalIsMUC (disco#info IQ) would otherwise block
// the loop that has to deliver their responses. Only the cheap config guard runs synchronously.
// Respects backfill.enabled and skips MUC. ctx is reserved as the trace link origin; it is not
// used for cancellation. Several messages arriving before the room exists may each trigger a
// backfill (same race as before).
func (x *xmppClient) maybeTriggerBackfillOnNewPortal(ctx context.Context, portalKey networkid.PortalKey) {
	if x.login == nil || x.conn.backfillConfig == nil || !x.conn.backfillConfig.Enabled {
		return
	}
	go func() {
		lctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		portal, err := x.conn.br.GetPortalByKey(lctx, portalKey)
		if err != nil || portal == nil || portal.MXID != "" || x.portalIsMUC(lctx, portal) {
			return
		}
		x.mamBackfillOnView(x.log.WithContext(context.Background()), string(portalKey.ID))
	}()
}

// mamBackfillOnView fetches historical MAM messages for a DM portal and queues them as remote events.
// Called from HandleMatrixViewingChat when the framework never triggers FetchMessages (e.g. appservice
// flow, sync-portal, pm command). Only runs for DMs; MUC uses different MAM semantics.
// Respects config.backfill.enabled and config.backfill.max_initial_messages.
func (x *xmppClient) mamBackfillOnView(ctx context.Context, portalID string) {
	if portalID == "" || x.login == nil {
		return
	}
	bf := x.conn.backfillConfig
	if bf == nil || !bf.Enabled {
		return
	}
	maxMsgs := bf.MaxInitialMessages
	if maxMsgs <= 0 {
		maxMsgs = mamBackfillMaxMessagesDefault
	}
	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(portalID),
		Receiver: x.loginID,
	}
	start := time.Now().Add(-mamBackfillWindow)
	end := time.Now()
	items, err := x.fetchMAMItems(ctx, portalKey, portalID, start, end, maxMsgs+1)
	if err != nil {
		x.log.Debug().Err(err).Str("portal_id", portalID).Msg("MAM backfill on view: query failed")
		return
	}
	if len(items) == 0 {
		return
	}
	// MAM server order is implementation-defined; sort by timestamp so we queue oldest-first for Matrix.
	if len(items) > 1 {
		sort.Slice(items, func(i, j int) bool { return items[i].Timestamp.Before(items[j].Timestamp) })
	}
	if len(items) > maxMsgs {
		items = items[:maxMsgs]
	}
	queued := 0
	for _, item := range items {
		if item.Type == "groupchat" {
			continue
		}
		if item.Body == "" {
			continue
		}
		msgID := networkid.MessageID(item.StanzaID)
		if msgID == "" {
			continue
		}
		existing, _ := x.conn.br.DB.Message.GetFirstPartByID(ctx, x.loginID, msgID)
		if existing != nil {
			continue
		}
		searchStart := item.Timestamp.Add(-2 * time.Minute)
		if searchStart.After(end) {
			searchStart = end.Add(-5 * time.Minute)
		}
		dbMessages, err := x.conn.br.DB.Message.GetMessagesBetweenTimeQuery(ctx, portalKey, searchStart, end.Add(time.Minute))
		if err == nil {
			for _, msg := range dbMessages {
				if msg.Metadata != nil {
					if meta, ok := msg.Metadata.(*MessageMetadata); ok && meta != nil && meta.XMPPMessageID == item.StanzaID {
						existing = msg
						break
					}
				}
				if string(msg.ID) == item.StanzaID {
					existing = msg
					break
				}
			}
		}
		if existing != nil {
			continue
		}
		senderJID, bodyOut := ResolveCheogramGroupMMSSenderForMAM(item.From, item.Body)
		sender := x.makeEventSender(senderJID)
		ts := item.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}
		x.login.QueueRemoteEvent(&jabberRemoteMessage{
			portalKey: portalKey,
			sender:    sender,
			id:        msgID,
			body:      bodyOut,
			timestamp: ts,
			x:         x,
		})
		queued++
	}
	if queued > 0 {
		x.log.Info().Str("portal_id", portalID).Int("queued", queued).Msg("MAM backfill on view: queued historical messages")
	}
}
