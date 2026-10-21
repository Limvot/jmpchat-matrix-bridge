package connector

import (
	"context"
	"encoding/xml"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Rejected outbound messages. A <message type='error'> that comes back for a message the bridge sent
// (a MUC refusing it, the server refusing it) is parsed on the Serve goroutine from the token stream
// (memory only), logged, and handed to a goroutine that correlates it with the Matrix-originated row,
// marks that event failed and tells the user in the portal room. The original message payload that
// RFC 6120 allows inside an error stanza is never read, so no message body can reach logs or traces.

const (
	// stanzaErrorNS is the namespace of RFC 6120 defined conditions.
	stanzaErrorNS = "urn:ietf:params:xml:ns:xmpp-stanzas"
	// maxErrorTextLen caps the server-provided <text> (runes).
	maxErrorTextLen = 200
	// maxErrorFieldLen caps the other server-provided strings (bytes).
	maxErrorFieldLen = 256
	// messageErrorTimeout bounds the DB/Matrix work of one rejection.
	messageErrorTimeout = 30 * time.Second
	// rejectedDedupeTTL is how long a reported rejection suppresses further reports for the same event.
	rejectedDedupeTTL = 10 * time.Minute
)

// rejectedReports remembers which Matrix events were already reported as rejected, so repeated error
// stanzas for the same message (server retries, error per resource) produce one status and one notice.
type rejectedReports struct {
	mu   sync.Mutex
	seen map[id.EventID]time.Time
}

// first reports whether evt was not reported within rejectedDedupeTTL, and records it.
func (r *rejectedReports) first(evt id.EventID, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen == nil {
		r.seen = make(map[id.EventID]time.Time)
	}
	for k, t := range r.seen {
		if now.Sub(t) > rejectedDedupeTTL {
			delete(r.seen, k)
		}
	}
	if t, ok := r.seen[evt]; ok && now.Sub(t) <= rejectedDedupeTTL {
		return false
	}
	r.seen[evt] = now
	return true
}

// messageError is the parsed content of an error message stanza.
type messageError struct {
	ID        string
	From      string
	To        string
	ErrorType string // <error type=>: auth, cancel, continue, modify, wait
	By        string // <error by=>
	Condition string // local name of the RFC 6120 defined-condition element
	Text      string // <text>, sanitized and capped
}

// sanitizeErrorField drops control characters and caps the length (in runes when maxRunes > 0, else bytes).
func sanitizeErrorField(s string, maxRunes int) string {
	s = strings.TrimSpace(strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s))
	if maxRunes > 0 {
		if utf8.RuneCountInString(s) > maxRunes {
			s = string([]rune(s)[:maxRunes])
		}
		return s
	}
	if len(s) > maxErrorFieldLen {
		s = strings.ToValidUTF8(s[:maxErrorFieldLen], "")
	}
	return s
}

// parseMessageError reads the children of an error <message> from t (positioned after the start
// element) and returns the parsed error. Only the direct <error> child is inspected; everything else
// (including any echoed original payload) is skipped without being retained. A read error returns what
// was found so far.
func parseMessageError(t xml.TokenReader, start *xml.StartElement) (messageError, error) {
	me := messageError{
		ID:   sanitizeErrorField(attrValue(start.Attr, "id"), 0),
		From: sanitizeErrorField(attrValue(start.Attr, "from"), 0),
		To:   sanitizeErrorField(attrValue(start.Attr, "to"), 0),
	}
	var text strings.Builder
	depth := 1
	inError, inText := false, false
	finish := func() messageError {
		me.Text = sanitizeErrorField(text.String(), maxErrorTextLen)
		return me
	}
	for depth > 0 {
		tok, err := t.Token()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return finish(), err
		}
		switch v := tok.(type) {
		case xml.StartElement:
			depth++
			switch {
			case depth == 2 && v.Name.Local == "error" && !inError && me.ErrorType == "" && me.Condition == "":
				inError = true
				me.ErrorType = sanitizeErrorField(attrValue(v.Attr, "type"), 0)
				me.By = sanitizeErrorField(attrValue(v.Attr, "by"), 0)
			case depth == 3 && inError && v.Name.Space == stanzaErrorNS:
				if v.Name.Local == "text" {
					inText = true
				} else if me.Condition == "" {
					me.Condition = sanitizeErrorField(v.Name.Local, 0)
				}
			}
		case xml.EndElement:
			if depth == 3 {
				inText = false
			}
			if depth == 2 {
				inError = false
			}
			depth--
		case xml.CharData:
			if inText && text.Len() < 4*maxErrorTextLen {
				text.Write(v)
			}
		}
	}
	return finish(), nil
}

// noticeText is the portal notice for a rejected message.
func (me messageError) noticeText() string {
	cond := me.Condition
	if cond == "" {
		cond = "unknown error"
	}
	s := "Your message was not delivered: " + cond
	if me.Text != "" {
		s += " – " + me.Text
	}
	return s
}

// status is the Matrix message status for the rejection: retriable for temporary errors (type wait),
// permanent otherwise.
func (me messageError) status() event.MessageStatus {
	if me.ErrorType == "wait" {
		return event.MessageStatusRetriable
	}
	return event.MessageStatusFail
}

// handleMessageError runs on the Serve goroutine (memory only): it logs the rejection, drops the
// pending MUC echo entry and hands the rest to a goroutine.
func (x *xmppClient) handleMessageError(me messageError) {
	x.log.Warn().
		Str("from", me.From).
		Str("to", me.To).
		Str("id", me.ID).
		Str("error_type", me.ErrorType).
		Str("condition", me.Condition).
		Str("by", me.By).
		Str("text", me.Text).
		Msg("XMPP message error stanza received (server or room rejected a message)")
	x.healMUCAfterError(me)
	if me.ID == "" {
		return
	}
	room := bareJID(me.From)
	if room == "" {
		return
	}
	// Rejections of groupchat messages come from the room's bare JID. An error from an occupant
	// (room/nick) is forwarded by the room on that occupant's behalf and must not touch our echo state.
	if !strings.Contains(me.From, "/") {
		x.mucEcho.discard(room, me.ID)
	}
	if x.conn == nil || x.conn.br == nil || x.login == nil {
		return
	}
	go x.reportRejectedMessageAsync(me)
}

func (x *xmppClient) reportRejectedMessageAsync(me messageError) {
	ctx, cancel := context.WithTimeout(x.log.WithContext(context.Background()), messageErrorTimeout)
	defer cancel()
	ctx, span := telemetry.Start(ctx, "xmpp.message_error", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("xmpp.peer.jid", bareJID(me.From)),
			attribute.String("xmpp.error.type", me.ErrorType),
			attribute.String("xmpp.error.condition", me.Condition),
		)
	}
	x.reportRejectedMessage(ctx, x.conn.br.DB, me)
}

// findRejectedMessage returns the Matrix-originated message of this login in portal whose XMPP id or
// origin-id is ref (room/portal-scoped), or nil.
func findRejectedMessage(ctx context.Context, db *database.Database, portal networkid.PortalKey, self networkid.UserID, ref string) (*database.Message, error) {
	if db == nil || ref == "" {
		return nil, nil
	}
	ids, err := aliasMessageIDs(ctx, db, portal, []string{metaKeyXMPPID, metaKeyOriginID}, ref)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if !strings.HasPrefix(string(id), "matrix:") {
			continue
		}
		parts, err := db.Message.GetAllPartsByID(ctx, portal.Receiver, id)
		if err != nil {
			return nil, err
		}
		for _, p := range parts {
			if p.Room == portal && p.SenderID == self && p.MXID != "" {
				return p, nil
			}
		}
	}
	return nil, nil
}

// reportRejectedMessage correlates the rejection with one of our outbound messages and, when found,
// fails its Matrix event and posts a notice in the portal room. Not found: nothing more than the log
// line already written.
func (x *xmppClient) reportRejectedMessage(ctx context.Context, db *database.Database, me messageError) bool {
	log := x.logFor(ctx)
	portalKey := networkid.PortalKey{ID: networkid.PortalID(bareJID(me.From)), Receiver: x.loginID}
	row, err := findRejectedMessage(ctx, db, portalKey, networkid.UserID(x.loginID), me.ID)
	if err != nil {
		log.Warn().Err(err).Str("id", me.ID).Msg("Rejected message lookup failed")
		return false
	}
	if row == nil {
		log.Debug().Str("id", me.ID).Msg("Message error stanza does not match a Matrix-originated message")
		return false
	}
	roomKey := row.Room
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: roomKey}}
	if db != nil {
		if p, err := db.Portal.GetByKey(ctx, portalKey); err == nil && p != nil {
			portal = &bridgev2.Portal{Portal: p}
		}
	}
	// Spoofing: in a MUC every occupant sees our message ids (the room reflects them) and can send a
	// type=error private message that the room forwards from room/nick. Only the room itself (bare
	// JID) may reject a groupchat message.
	if strings.Contains(me.From, "/") && portalLooksMUC(portal) {
		log.Debug().Str("id", me.ID).Str("from", me.From).Msg("Ignoring message error from a MUC occupant")
		return false
	}
	if !x.rejected.first(row.MXID, time.Now()) {
		log.Debug().Str("id", me.ID).Msg("Rejection already reported for this message")
		return false
	}
	text := me.noticeText()
	ms := &bridgev2.MessageStatus{
		Step:        status.MsgStepRemote,
		Status:      me.status(),
		ErrorReason: event.MessageStatusNetworkError,
		Message:     text,
		IsCertain:   true,
	}
	info := &bridgev2.MessageStatusEventInfo{
		RoomID:        portal.MXID,
		SourceEventID: row.MXID,
		EventType:     event.EventMessage,
	}
	if x.login != nil && x.login.UserLogin != nil {
		info.Sender = x.login.UserMXID
	}
	x.sendMessageStatus(ctx, ms, info)
	x.sendPortalNotice(ctx, portal, text)
	return true
}

// sendMessageStatus reports ms for an existing Matrix event through the bridgev2 Matrix connector
// (Connector.SendMessageStatus, which sends the checkpoint and MSS event).
func (x *xmppClient) sendMessageStatus(ctx context.Context, ms *bridgev2.MessageStatus, info *bridgev2.MessageStatusEventInfo) {
	if x.statusHook != nil {
		x.statusHook(ms, info)
		return
	}
	if x.conn == nil || x.conn.br == nil || x.conn.br.Matrix == nil || info.RoomID == "" {
		return
	}
	// Connector.SendMessageStatus sends asynchronously; the caller's ctx is canceled on return.
	x.conn.br.Matrix.SendMessageStatus(context.WithoutCancel(ctx), ms, info)
}

// portalLooksMUC decides from the stored room type (no IQ) whether portal is a group chat; legacy
// portals without a type fall back to the JID heuristic.
func portalLooksMUC(portal *bridgev2.Portal) bool {
	if portal == nil || portal.Portal == nil {
		return false
	}
	switch portal.RoomType {
	case database.RoomTypeGroupDM:
		return true
	case database.RoomTypeDM:
		return false
	}
	return isMUCJID(string(portal.ID))
}

// mucHealInterval is the minimum time between two self-heal re-joins of the same room.
const mucHealInterval = 5 * time.Minute

// mucHealCondition reports whether an error condition from a room's bare JID means the room does not
// consider us an occupant (XEP-0045: not-acceptable for a non-occupant sender).
func mucHealCondition(cond string) bool {
	switch cond {
	case "not-acceptable", "item-not-found", "recipient-unavailable":
		return true
	}
	return false
}

// mucHealAllowed records a heal attempt for room and reports whether the last one is older than
// mucHealInterval.
func (x *xmppClient) mucHealAllowed(room string, now time.Time) bool {
	x.mucHealMu.Lock()
	defer x.mucHealMu.Unlock()
	if x.mucHealAt == nil {
		x.mucHealAt = make(map[string]time.Time)
	}
	if t, ok := x.mucHealAt[room]; ok && now.Sub(t) < mucHealInterval {
		return false
	}
	x.mucHealAt[room] = now
	return true
}

// healMUCAfterError runs on the Serve goroutine (memory only): when a MUC room's bare JID reports that
// we are not an occupant, it hands a single rate-limited re-join to a goroutine. Errors forwarded from
// an occupant (room/nick) never trigger it.
func (x *xmppClient) healMUCAfterError(me messageError) {
	if me.ErrorType == "" || !mucHealCondition(me.Condition) || strings.Contains(me.From, "/") {
		return
	}
	room := bareJID(me.From)
	if room == "" {
		return
	}
	go x.healMUC(room)
}

func (x *xmppClient) healMUC(room string) {
	ctx, cancel := context.WithTimeout(x.log.WithContext(context.Background()), messageErrorTimeout)
	defer cancel()
	ctx, span := telemetry.Start(ctx, "xmpp.muc.heal", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	log := x.logFor(ctx)
	portal, err := x.lookupMUCPortal(ctx, room)
	if err != nil {
		log.Debug().Err(err).Str("room", room).Msg("MUC heal: portal lookup failed")
		return
	}
	isMUC := isMUCJID(room)
	if portal != nil {
		isMUC = portalLooksMUC(&bridgev2.Portal{Portal: portal})
		if meta, _ := portal.Metadata.(*PortalMetadata); meta != nil && meta.MUCLeft {
			return // left on purpose: only the join command returns
		}
	}
	if !isMUC || !x.mucHealAllowed(room, time.Now()) {
		return
	}
	x.mucOurOccupantJIDMu.Lock()
	delete(x.mucOurOccupantJID, room)
	x.mucOurOccupantJIDMu.Unlock()
	if err := x.joinMUC(ctx, room, nil); err != nil {
		telemetry.Fail(span, err)
		log.Warn().Err(err).Str("room", room).Msg("MUC heal: re-join after not-an-occupant error failed")
		return
	}
	log.Info().Str("room", room).Msg("MUC heal: re-joined after the room rejected a message as not-an-occupant")
}
