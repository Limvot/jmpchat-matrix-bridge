package connector

import (
	"context"
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Outbound chat signals (see xmpp_signals.go): typing (XEP-0085), displayed markers (XEP-0333) and
// delivery receipts (XEP-0184), plus the flags every outbound message carries.

type sigKind int

const (
	sigChatState sigKind = iota // Value: composing | active
	sigDisplayed                // Value: the referenced id
	sigReceived                 // Value: the id of the message being acknowledged
)

// markerWindow bounds how far back a read receipt looks for the newest markable message.
const markerWindow = 48 * time.Hour

// outSignal is one body-less signalling message.
type outSignal struct {
	To    jid.JID
	Type  stanza.MessageType // "" -> normal
	ID    string             // "" -> generate (MUC sends pass an id registered in the echo table)
	Kind  sigKind
	Value string
}

// emptyElement is <local xmlns='space' attrs.../>.
func emptyElement(space, local string, attrs ...xml.Attr) xml.TokenReader {
	start := xml.StartElement{
		Name: xml.Name{Space: space, Local: local},
		Attr: append([]xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: space}}, attrs...),
	}
	return xmlstream.MultiReader(xmlstream.Token(start), xmlstream.Token(xml.EndElement{Name: start.Name}))
}

func idAttr(v string) xml.Attr { return xml.Attr{Name: xml.Name{Local: "id"}, Value: v} }

// signalPayload is the child content of s.
func signalPayload(s outSignal) xml.TokenReader {
	switch s.Kind {
	case sigChatState:
		return xmlstream.MultiReader(emptyElement(chatStatesNS, s.Value), emptyElement(hintsNS, "no-store"))
	case sigDisplayed:
		if s.Type == stanza.MessageType("groupchat") {
			return emptyElement(markersNS, "displayed", idAttr(s.Value)) // no <store/>: the room would archive every read marker
		}
		return xmlstream.MultiReader(emptyElement(markersNS, "displayed", idAttr(s.Value)), storeHintReader())
	default:
		return emptyElement(receiptsNS, "received", idAttr(s.Value))
	}
}

// sendSignal sends s on the current session (or hands it to the test hook).
func (x *xmppClient) sendSignal(ctx context.Context, s outSignal) error {
	if x.signalHook != nil {
		return x.signalHook(ctx, s)
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return errRetractNotConnected
	}
	return sendXMPPSignal(ctx, sess, s)
}

// sendXMPPSignal builds and sends s. Span and logs carry kinds and ids only.
func sendXMPPSignal(ctx context.Context, sess *xmpp.Session, s outSignal) error {
	ctx, span := telemetry.StartChild(ctx, "xmpp.send_signal", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	id := s.ID
	if id == "" {
		var err error
		if id, err = generateXMPPMessageID(); err != nil {
			telemetry.Fail(span, err)
			return err
		}
	}
	typ := s.Type
	if typ == "" {
		typ = stanza.NormalMessage
	}
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("xmpp.peer.jid", s.To.String()),
			attribute.String("xmpp.message.type", string(typ)),
			attribute.Int("xmpp.signal.kind", int(s.Kind)),
			attribute.String("xmpp.stanza.id", id),
		)
	}
	msg := stanza.Message{To: s.To, Type: typ, ID: id}
	if err := sess.Send(ctx, msg.Wrap(signalPayload(s))); err != nil {
		telemetry.Fail(span, err)
		return err
	}
	return nil
}

// withSignalFlags adds the signalling elements every outbound message with a body carries: <markable/>
// always (lets peers send us displayed markers), a receipt request in 1:1 chats (never groupchat, XEP-0184
// section 5.3) and <active/> in 1:1 chats when typing is enabled and the peer is not a Cheogram JID (the
// gateway cannot convey it to a phone).
func (x *xmppClient) withSignalFlags(m outMessage) outMessage {
	m.Markable = true
	if m.Type == stanza.ChatMessage {
		m.RequestReceipt = true
		if x.signalPrefs().SendTyping && !IsCheogramDMJID(m.To.Bare().String()) {
			m.Active = true
		}
	}
	return m
}

// HandleMatrixTyping implements bridgev2.TypingHandlingNetworkAPI. Best effort: never an error.
func (x *xmppClient) HandleMatrixTyping(ctx context.Context, msg *bridgev2.MatrixTyping) error {
	if msg == nil || msg.Portal == nil || !x.signalPrefs().SendTyping {
		return nil
	}
	portalID := string(msg.Portal.ID)
	// RoomType only: portalIsMUC can do a disco IQ and typing is frequent.
	if msg.Portal.RoomType != database.RoomTypeDM || IsCheogramDMJID(portalID) {
		return nil
	}
	// XEP-0085 section 5.1: only to peers that showed chat state support this session.
	if !x.sig.isPeer(portalID) {
		return nil
	}
	state := "active"
	if msg.IsTyping {
		state = "composing"
	}
	to, err := jid.Parse(portalID)
	if err != nil {
		return nil
	}
	if !x.sig.shouldSendTyping(portalID, state, time.Now()) {
		return nil
	}
	ctx, span := telemetry.StartChild(ctx, "matrix.handle_typing", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	if err := x.sendSignal(ctx, outSignal{To: to, Type: stanza.ChatMessage, Kind: sigChatState, Value: state}); err != nil {
		x.sig.forgetTyping(portalID)
		x.logFor(ctx).Debug().Err(err).Msg("Failed to send chat state")
		return nil
	}
	x.logFor(ctx).Debug().Str("state", state).Msg("Sent chat state")
	return nil
}

// pickDisplayedTarget picks the message a read receipt acknowledges: the exact message when it is
// somebody else's and markable, else the newest such message of window (the rows between the previous and
// the new read position). Own messages are never marked displayed and XEP-0333 only allows markers for
// messages that carried <markable/>.
func pickDisplayedTarget(exact *database.Message, window []*database.Message, loginID networkid.UserLoginID) *database.Message {
	ok := func(m *database.Message) bool {
		if m == nil || isOwnMessage(m, loginID) {
			return false
		}
		meta := targetMeta(m)
		return meta != nil && meta.Markable
	}
	if ok(exact) {
		return exact
	}
	var best *database.Message
	for _, m := range window {
		if ok(m) && (best == nil || m.Timestamp.After(best.Timestamp)) {
			best = m
		}
	}
	return best
}

// messagesBetween returns the portal rows in [start, end] (test hook or the bridge database).
func (x *xmppClient) messagesBetween(ctx context.Context, key networkid.PortalKey, start, end time.Time) ([]*database.Message, error) {
	if x.betweenHook != nil {
		return x.betweenHook(key, start, end)
	}
	if x.conn == nil || x.conn.br == nil || x.conn.br.DB == nil {
		return nil, nil
	}
	return x.conn.br.DB.Message.GetMessagesBetweenTimeQuery(ctx, key, start, end)
}

// HandleMatrixReadReceipt implements bridgev2.ReadReceiptHandlingNetworkAPI (explicit and implicit
// receipts alike): the user read a Matrix room, so mark the newest markable message displayed.
func (x *xmppClient) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error {
	if msg == nil || msg.Portal == nil {
		return nil
	}
	portal := msg.Portal
	portalID := string(portal.ID)
	if IsCheogramDMJID(portalID) {
		return nil
	}
	prefs := x.signalPrefs()
	isMUC := refIsMUCFor(false, portal, portal.PortalKey)
	switch {
	case portal.RoomType == database.RoomTypeDM && prefs.SendReadMarkers:
	case portal.RoomType == database.RoomTypeGroupDM && prefs.MUCReadMarkers:
	default:
		return nil
	}
	if isMUC && x.getMUCOurOccupantJID(portalID) == "" {
		return nil // not in the room this session; a receipt must not trigger a join
	}
	ctx, span := telemetry.StartChild(ctx, "matrix.handle_read_receipt", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	log := x.logFor(ctx)

	var window []*database.Message
	if msg.ExactMessage == nil || isOwnMessage(msg.ExactMessage, x.loginID) || func() bool { m := targetMeta(msg.ExactMessage); return m == nil || !m.Markable }() {
		start := msg.LastRead
		if floor := msg.ReadUpTo.Add(-markerWindow); start.Before(floor) {
			start = floor
		}
		var err error
		if window, err = x.messagesBetween(ctx, portal.PortalKey, start, msg.ReadUpTo); err != nil {
			telemetry.Fail(span, err)
			log.Debug().Err(err).Msg("Failed to look up messages for the read marker")
			return nil
		}
	}
	target := pickDisplayedTarget(msg.ExactMessage, window, x.loginID)
	if target == nil {
		return nil
	}
	ref, roomRef, ok := replyRefForMessage(target, isMUC)
	if !ok || (isMUC && !roomRef) {
		log.Debug().Bool("muc", isMUC).Msg("Read marker target has no usable XMPP id; skipped")
		return nil
	}
	if x.sig.lastDisplayed(portalID) == ref {
		return nil
	}
	to, err := jid.Parse(portalID)
	if err != nil {
		return nil
	}
	if isMUC {
		// Trailing debounce: the newest ref goes out after a quiet period (never a live span into the timer).
		x.sig.scheduleMUCMarker(portalID, ref, mucMarkerDebounce, func(ref string) { x.sendMUCDisplayed(portalID, ref) })
		return nil
	}
	sig := outSignal{To: to, Type: stanza.ChatMessage, Kind: sigDisplayed, Value: ref}
	if err := x.sendSignal(ctx, sig); err != nil {
		log.Debug().Err(err).Msg("Failed to send displayed marker")
		return nil
	}
	x.sig.setDisplayed(portalID, ref)
	log.Debug().Bool("muc", false).Msg(fmt.Sprintf("Sent displayed marker (ref length %d)", len(ref)))
	return nil
}

// sendMUCDisplayed sends the debounced groupchat displayed marker (timer goroutine: no inherited span).
func (x *xmppClient) sendMUCDisplayed(portalID, ref string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log := x.logFor(ctx)
	if x.getMUCOurOccupantJID(portalID) == "" || x.sig.lastDisplayed(portalID) == ref {
		return
	}
	to, err := jid.Parse(portalID)
	if err != nil {
		return
	}
	id, err := generateXMPPMessageID()
	if err != nil {
		return
	}
	// Register before sending so the room's reflection of the marker is dropped as our own echo.
	x.mucEcho.register(portalID, id)
	if err := x.sendSignal(ctx, outSignal{To: to, Type: stanza.MessageType("groupchat"), ID: id, Kind: sigDisplayed, Value: ref}); err != nil {
		x.mucEcho.discard(portalID, id)
		log.Debug().Err(err).Msg("Failed to send displayed marker")
		return
	}
	x.sig.setDisplayed(portalID, ref)
	log.Debug().Bool("muc", true).Msg(fmt.Sprintf("Sent displayed marker (ref length %d)", len(ref)))
}

// noteOutboundSent drops the typing record after a message carrying <active/> went out: the stanza told
// the peer we stopped typing, so a later composing state must not be suppressed as a duplicate.
func (x *xmppClient) noteOutboundSent(m outMessage) {
	if m.Active {
		x.sig.forgetTyping(strings.ToLower(m.To.Bare().String()))
	}
}

var (
	_ bridgev2.TypingHandlingNetworkAPI      = (*xmppClient)(nil)
	_ bridgev2.ReadReceiptHandlingNetworkAPI = (*xmppClient)(nil)
)
