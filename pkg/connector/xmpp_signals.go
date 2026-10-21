package connector

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Chat signals: XEP-0085 chat states (typing), XEP-0333 chat markers (displayed) and XEP-0184 delivery
// receipts, in both directions. This file holds the session state, the config switches and the inbound
// side; the outbound side is in xmpp_signals_out.go.

// signalPrefs are the config switches (network.send_typing etc.). The privacy cost of each is described
// in the example config.
type signalPrefs struct {
	SendTyping           bool
	SendReadMarkers      bool // 1:1 chats
	SendDeliveryReceipts bool
	MUCReadMarkers       bool // tells the room's occupants what the user read
}

// signalPrefs resolves the config; a nil config or a missing key means the default (everything on).
func (c *JabberConnector) signalPrefs() signalPrefs {
	p := signalPrefs{SendTyping: true, SendReadMarkers: true, SendDeliveryReceipts: true, MUCReadMarkers: true}
	if c == nil || c.networkConfig == nil {
		return p
	}
	cfg := c.networkConfig
	if cfg.SendTyping != nil {
		p.SendTyping = *cfg.SendTyping
	}
	if cfg.SendReadMarkers != nil {
		p.SendReadMarkers = *cfg.SendReadMarkers
	}
	if cfg.SendDeliveryReceipts != nil {
		p.SendDeliveryReceipts = *cfg.SendDeliveryReceipts
	}
	if cfg.MUCReadMarkers != nil {
		p.MUCReadMarkers = *cfg.MUCReadMarkers
	}
	return p
}

func (x *xmppClient) signalPrefs() signalPrefs { return x.conn.signalPrefs() }

const (
	// signalMapCap bounds every per-session map below (a hostile or busy account cannot grow them).
	signalMapCap = 4096
	// typingResendAfter: the same chat state is not re-sent to a portal sooner than this.
	typingResendAfter = 10 * time.Second
	// inboundTypingTimeout is how long a composing indication shows in Matrix without a refresh.
	inboundTypingTimeout = 30 * time.Second
)

type typingRecord struct {
	state string
	at    time.Time
}

// signalState is per session (reset in Connect).
type signalState struct {
	mu             sync.Mutex
	chatStatePeers map[string]struct{}     // lower bare JIDs that sent us any XEP-0085 state this session
	typingSent     map[string]typingRecord // portal ID -> last state sent
	displayedSent  map[string]string       // portal ID -> last displayed ref sent
}

func (s *signalState) reset() {
	s.mu.Lock()
	s.chatStatePeers, s.typingSent, s.displayedSent = nil, nil, nil
	s.mu.Unlock()
}

func (s *signalState) notePeer(bare string) {
	bare = strings.ToLower(bare)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.chatStatePeers[bare]; ok {
		return
	}
	if s.chatStatePeers == nil || len(s.chatStatePeers) >= signalMapCap {
		s.chatStatePeers = map[string]struct{}{}
	}
	s.chatStatePeers[bare] = struct{}{}
}

func (s *signalState) isPeer(bare string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.chatStatePeers[strings.ToLower(bare)]
	return ok
}

// shouldSendTyping reports whether state is worth sending to the portal now and records it when so.
func (s *signalState) shouldSendTyping(portal, state string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.typingSent[portal]; ok && rec.state == state && now.Sub(rec.at) < typingResendAfter {
		return false
	}
	if s.typingSent == nil || len(s.typingSent) >= signalMapCap {
		s.typingSent = map[string]typingRecord{}
	}
	s.typingSent[portal] = typingRecord{state: state, at: now}
	return true
}

// forgetTyping drops the record after a failed send so the next attempt is not suppressed.
func (s *signalState) forgetTyping(portal string) {
	s.mu.Lock()
	delete(s.typingSent, portal)
	s.mu.Unlock()
}

func (s *signalState) lastDisplayed(portal string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.displayedSent[portal]
}

func (s *signalState) setDisplayed(portal, ref string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.displayedSent == nil || len(s.displayedSent) >= signalMapCap {
		s.displayedSent = map[string]string{}
	}
	s.displayedSent[portal] = ref
}

// hasInboundSignal reports whether the stanza carries a chat state, displayed marker or receipt.
func (e messageExtras) hasInboundSignal() bool {
	return e.ChatState != "" || e.Displayed != "" || e.ReceiptReceived != ""
}

// inboundSignal is one stanza's signals plus where and from whom it came.
type inboundSignal struct {
	key       networkid.PortalKey
	channel   string // retractChanLive / retractChanCarbonSent / retractChanCarbonReceived
	groupchat bool
	fromFull  string
	sender    bridgev2.EventSender
	fromMe    bool
	hasBody   bool
	extras    messageExtras
}

// handleInboundSignals runs on the Serve goroutine: memory and queueing only (no DB, no IQ).
// MAM never reaches it (archived signals are stale).
func (x *xmppClient) handleInboundSignals(s inboundSignal) {
	e := s.extras
	if !e.hasInboundSignal() {
		return
	}
	// Cheogram group MMS: the sender of a group stanza is synthesized and cannot be attributed.
	if _, groupMMS := ParseCheogramGroupMMSJID(string(s.key.ID)); groupMMS && !s.groupchat {
		return
	}
	live := s.channel == retractChanLive
	dm := !s.groupchat

	// Chat states: live 1:1 only (carbons, MAM and MUC are noise).
	if e.ChatState != "" && live && dm && !s.fromMe {
		x.sig.notePeer(bareJID(s.fromFull))
		if !s.hasBody {
			x.queueTyping(s)
		}
	}

	// Displayed markers.
	if e.Displayed != "" {
		switch {
		case live && dm && !s.fromMe:
			x.queueMarkerReceipt(s, e.Displayed, refReplyDM, false, true)
		case s.channel == retractChanCarbonSent && dm:
			x.queueMarkerReceipt(s, e.Displayed, refReplyDM, false, false)
		case s.channel == retractChanCarbonReceived && dm:
			x.queueMarkerReceipt(s, e.Displayed, refReplyDM, false, true)
		case live && s.groupchat && s.fromMe:
			x.queueMarkerReceipt(s, e.Displayed, refReplyMUC, false, false)
		case live && s.groupchat && x.mucOccupantSeen(s.fromFull):
			x.queueMarkerReceipt(s, e.Displayed, refReplyMUC, false, true)
		}
	}

	// XEP-0184 receipt from the contact: delivered (1:1, live only).
	if e.ReceiptReceived != "" && live && dm && !s.fromMe {
		x.queueMarkerReceipt(s, e.ReceiptReceived, refReplyDM, true, true)
	}
}

func (x *xmppClient) queueTyping(s inboundSignal) {
	ctx, span := startReceiveSpan("chat_state", "", s.fromFull, string(s.key.ID), "", false, false, false)
	defer span.End()
	timeout := time.Duration(0)
	if s.extras.ChatState == "composing" {
		timeout = inboundTypingTimeout
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.chat_state", s.extras.ChatState))
	}
	x.queueRemoteEvent(ctx, &simplevent.Typing{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventTyping,
			PortalKey:    s.key,
			Sender:       s.sender,
			Timestamp:    time.Now(),
			CreatePortal: false,
		},
		Timeout: timeout,
		Type:    bridgev2.TypingTypeText,
	})
}

func (x *xmppClient) queueMarkerReceipt(s inboundSignal, ref string, kind refKind, delivery, wantOwn bool) {
	name := "displayed"
	if delivery {
		name = "receipt"
	}
	ctx, span := startReceiveSpan(name, "", s.fromFull, string(s.key.ID), "", false, false, false)
	defer span.End()
	x.logFor(ctx).Debug().Str("channel", s.channel).Bool("muc", s.groupchat).Bool("from_me", s.sender.IsFromMe).Msg("Inbound " + name + " signal")
	x.queueRemoteEvent(ctx, newMarkerReceipt(xmppRetractionEnv{x}, x.loginID, s.key, s.sender, ref, kind, delivery, wantOwn))
}

// receiptEnv is the seam between the receipt pre-handler and the bridge database.
type receiptEnv interface {
	findByRef(ctx context.Context, key networkid.PortalKey, ref string, kind refKind) ([]*database.Message, error)
	log(ctx context.Context) *zerolog.Logger
}

// newMarkerReceipt builds a read (delivery=false) or delivery receipt event. ref is the raw XMPP reference;
// the pre-handler resolves it room-scoped to the bridgev2 message ID (our sent rows are `matrix:$event`,
// found only through their stored XMPP id, and bridgev2's own receipt lookups are not room-scoped).
// wantOwn: the target must be a message of ours (a contact read or received it) or must not be (we read it).
func newMarkerReceipt(env receiptEnv, loginID networkid.UserLoginID, key networkid.PortalKey, sender bridgev2.EventSender, ref string, kind refKind, delivery, wantOwn bool) *simplevent.Receipt {
	typ := bridgev2.RemoteEventReadReceipt
	if delivery {
		typ = bridgev2.RemoteEventDeliveryReceipt
	}
	var hctx context.Context
	evt := &simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type:         typ,
			PortalKey:    key,
			Sender:       sender,
			Timestamp:    time.Now(),
			CreatePortal: false,
		},
	}
	evt.PreHandleWithResultFunc = func(ctx context.Context, portal *bridgev2.Portal) *bridgev2.EventHandlingResult {
		ctx = startLazyRemoteEvent(ctx)
		hctx = ctx
		done := func(res bridgev2.EventHandlingResult) *bridgev2.EventHandlingResult {
			telemetry.EndRemoteEvent(ctx)
			return &res
		}
		rctx, span := telemetry.StartChild(ctx, "bridge.resolve_receipt_target")
		defer span.End()
		rows, err := env.findByRef(rctx, key, ref, kind)
		if err != nil {
			telemetry.Fail(span, err)
			return done(bridgev2.EventHandlingResultFailed.WithError(err))
		}
		if len(rows) == 0 || rows[0].Room != key || isOwnMessage(rows[0], loginID) != wantOwn {
			span.SetAttributes(attribute.Bool("bridge.found", false))
			env.log(rctx).Debug().Msg("Ignoring receipt: target not found or not applicable")
			return done(bridgev2.EventHandlingResultIgnored)
		}
		span.SetAttributes(attribute.Bool("bridge.found", true))
		if delivery {
			evt.Targets = []networkid.MessageID{rows[0].ID}
		} else {
			evt.LastTarget = rows[0].ID
		}
		return nil
	}
	evt.PostHandleFunc = func(ctx context.Context, _ *bridgev2.Portal) {
		if hctx != nil {
			ctx = hctx
		}
		telemetry.EndRemoteEvent(ctx)
	}
	return evt
}

// ---- answering XEP-0184 requests ----

// receiptRequest is a XEP-0184 request that arrived on the live 1:1 path.
type receiptRequest struct {
	to      string             // full JID of the sender
	origID  string             // id attribute of the requesting message
	msgType stanza.MessageType // chat or normal
}

// receiptFor returns the receipt to send for a live stanza, or nil. XEP-0184 section 5.3 forbids
// answering groupchat; the callers only ask on the live path (never carbons or archive copies).
func (x *xmppClient) receiptFor(extras messageExtras, msgTypeAttr, idAttr, from string) *receiptRequest {
	if !extras.ReceiptRequest || idAttr == "" {
		return nil
	}
	var typ stanza.MessageType
	switch msgTypeAttr {
	case "chat":
		typ = stanza.ChatMessage
	case "", "normal":
		typ = stanza.NormalMessage
	default:
		return nil
	}
	fromBare := bareJID(from)
	if strings.EqualFold(fromBare, bareJID(string(x.loginID))) {
		return nil
	}
	if _, groupMMS := ParseCheogramGroupMMSJID(fromBare); groupMMS {
		return nil
	}
	if !x.signalPrefs().SendDeliveryReceipts {
		return nil
	}
	return &receiptRequest{to: from, origID: idAttr, msgType: typ}
}

// answerReceiptRequest runs from the portal event loop after the message was handled: the receipt is only
// sent when the row exists in this portal (so a message that was ignored or failed is not acknowledged).
func (x *xmppClient) answerReceiptRequest(ctx context.Context, m *jabberRemoteMessage) {
	r := m.receipt
	if r == nil || !x.messageBridged(ctx, m.portalKey, m.id) {
		return
	}
	to, err := jid.Parse(r.to)
	if err != nil {
		return
	}
	if err := x.sendSignal(ctx, outSignal{To: to, Type: r.msgType, Kind: sigReceived, Value: r.origID}); err != nil {
		x.logFor(ctx).Debug().Err(err).Msg("Failed to send delivery receipt")
	}
}

// messageBridged reports whether a row for id exists in the portal.
func (x *xmppClient) messageBridged(ctx context.Context, key networkid.PortalKey, id networkid.MessageID) bool {
	if x.bridgedHook != nil {
		return x.bridgedHook(key, id)
	}
	if x.conn == nil || x.conn.br == nil || x.conn.br.DB == nil {
		return false
	}
	row, err := x.conn.br.DB.Message.GetFirstPartByID(ctx, key.Receiver, id)
	return err == nil && row != nil && row.Room == key
}
