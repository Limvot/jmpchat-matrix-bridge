package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Outbound retraction (Matrix redaction -> XEP-0424 retraction). bridgev2 calls
// HandleMatrixMessageRemove only for redactions that originate in Matrix; remote retractions never
// come back through here, so there is no loop.

const smsCannotUnsendNotice = "SMS/MMS messages cannot be unsent: the message was removed here, but it stays on the recipient's phone."

var (
	errRetractNotOwn       = errors.New("Only your own messages can be retracted on XMPP.")
	errRetractNoMUCRef     = errors.New("Cannot retract: the room has not confirmed this message yet (or does not support stable ids).")
	errRetractNoXMPPRef    = errors.New("Cannot retract: this message has no XMPP id.")
	errRetractNotConnected = errors.New("not connected to XMPP")
)

// isOwnMessage reports whether the bridged row was sent by the logged-in user (Matrix-originated or
// our own carbon). Only these can be retracted.
func isOwnMessage(m *database.Message, loginID networkid.UserLoginID) bool {
	return m != nil && m.SenderID != "" && strings.EqualFold(bareJID(string(m.SenderID)), bareJID(string(loginID)))
}

// xmppIDForMessage returns the XMPP reference used to address a 1:1 message: for Matrix-originated
// rows (matrix:$event) the id we generated (== origin-id); otherwise the stored origin-id when the
// sender set one (XEP-0424 prefers it), else the row ID (the `id` attribute).
func xmppIDForMessage(m *database.Message) (string, bool) {
	meta := targetMeta(m)
	if strings.HasPrefix(string(m.ID), "matrix:") {
		if meta != nil && meta.XMPPMessageID != "" {
			return meta.XMPPMessageID, true
		}
		return "", false
	}
	if meta != nil && meta.OriginID != "" {
		return meta.OriginID, true
	}
	if m.ID == "" {
		return "", false
	}
	return string(m.ID), true
}

// resolveRetractRef returns the id the retraction must carry. DM: xmppIDForMessage. MUC: the
// room-assigned stanza-id from the row, or from the echo table when the echo has just arrived or is
// still pending (waits up to timeout). Never falls back to origin-id: groupchat receivers match on
// the stanza-id, so a fallback would report success while nothing is retracted.
func resolveRetractRef(ctx context.Context, table *mucEchoTable, room string, isMUC bool, target *database.Message, timeout time.Duration) (string, error) {
	if !isMUC {
		ref, ok := xmppIDForMessage(target)
		if !ok {
			return "", errRetractNoXMPPRef
		}
		return ref, nil
	}
	meta := targetMeta(target)
	if meta != nil && meta.StanzaID != "" {
		return meta.StanzaID, nil
	}
	if meta != nil && meta.XMPPMessageID != "" {
		if stanzaID, found, _ := table.wait(ctx, room, meta.XMPPMessageID, timeout); found && stanzaID != "" {
			return stanzaID, nil
		}
	}
	return "", errRetractNoMUCRef
}

// withStanzaID returns a copy of the row whose metadata carries stanzaID (for the tombstone, the row
// handed over by bridgev2 may predate the echo).
func withStanzaID(m *database.Message, stanzaID string) *database.Message {
	cp := *m
	var meta MessageMetadata
	if old := targetMeta(m); old != nil {
		meta = *old
	}
	if meta.StanzaID == "" {
		meta.StanzaID = stanzaID
	}
	cp.Metadata = &meta
	return &cp
}

// sendPortalNotice posts an m.notice from the bridge bot into the portal room.
func (x *xmppClient) sendPortalNotice(ctx context.Context, portal *bridgev2.Portal, text string) {
	if x.noticeHook != nil {
		x.noticeHook(portal, text)
		return
	}
	if portal == nil || portal.MXID == "" || x.conn == nil || x.conn.br == nil || x.conn.br.Bot == nil {
		return
	}
	content := &event.MessageEventContent{MsgType: event.MsgNotice, Body: text}
	if _, err := x.conn.br.Bot.SendMessage(ctx, portal.MXID, event.EventMessage, &event.Content{Parsed: content}, nil); err != nil {
		x.logFor(ctx).Warn().Err(err).Msg("Failed to send notice")
	}
}

// HandleMatrixMessageRemove implements bridgev2.RedactionHandlingNetworkAPI.
func (x *xmppClient) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	ctx, span := telemetry.Start(ctx, "matrix.handle_redaction", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.peer.jid", string(msg.Portal.ID)))
		setPortalTypeAttr(span, msg.Portal)
		if msg.Portal.MXID != "" {
			span.SetAttributes(attribute.String("matrix.room_id", msg.Portal.MXID.String()))
		}
		if msg.Event != nil {
			span.SetAttributes(attribute.String("matrix.event_id", string(msg.Event.ID)))
		}
	}
	err := x.handleMatrixMessageRemove(ctx, msg)
	telemetry.Fail(span, err)
	return err
}

func (x *xmppClient) handleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	log := x.logFor(ctx)
	target := msg.TargetMessage
	portal := msg.Portal
	portalID := string(portal.ID)

	if !isOwnMessage(target, x.loginID) {
		return bridgev2.WrapErrorInStatus(errRetractNotOwn).WithSendNotice(true)
	}
	// Relay mode: every relayed message is stored under the relay login, so a relayed Matrix user may
	// only retract a message that they sent themselves.
	if msg.OrigSender != nil && (msg.Event == nil || target.SenderMXID == "" || target.SenderMXID != msg.Event.Sender) {
		return bridgev2.WrapErrorInStatus(errRetractNotOwn).WithSendNotice(true)
	}
	// SMS/MMS cannot be unsent. Not an error: the Matrix redaction stands (bridgev2 then deletes the
	// row and reports success), the notice explains that the recipient still has it. Nothing is sent
	// to the gateway. The refs are tombstoned so MAM catch-up cannot re-bridge the deleted row.
	if IsCheogramDMJID(portalID) {
		log.Debug().Msg("Not retracting a Cheogram SMS/MMS message; notifying the room")
		if kv := x.kv(); kv != nil {
			markRetractedRefs(ctx, kv, x.loginID, portal.ID, retractedRefsOf([]*database.Message{target}))
		}
		x.sendPortalNotice(ctx, portal, smsCannotUnsendNotice)
		return nil
	}

	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return bridgev2.WrapErrorInStatus(errRetractNotConnected).WithSendNotice(true)
	}
	toJID, err := jid.Parse(portalID)
	if err != nil {
		return bridgev2.WrapErrorInStatus(fmt.Errorf("invalid portal JID format")).WithSendNotice(true)
	}
	isMUC := x.portalIsMUC(ctx, portal)
	msgType := stanza.ChatMessage
	if isMUC {
		msgType = stanza.MessageType("groupchat")
		// Be in the room before retracting (same as sending).
		if x.getMUCOurOccupantJID(portalID) == "" {
			if joinErr := x.JoinMUC(ctx, portalID); joinErr != nil {
				log.Warn().Err(joinErr).Msg("Failed to join MUC before retraction; it may be rejected")
			} else {
				select {
				case <-ctx.Done():
					return bridgev2.WrapErrorInStatus(ctx.Err()).WithSendNotice(true)
				case <-time.After(1500 * time.Millisecond):
				}
			}
		}
	}

	ref, err := resolveRetractRef(ctx, &x.mucEcho, portalID, isMUC, target, mucEchoWait)
	if err != nil {
		return bridgev2.WrapErrorInStatus(err).WithSendNotice(true)
	}
	// MUC: register the stanza id before sending (any echo of this stanza is ours).
	var preID string
	if isMUC {
		var idErr error
		preID, idErr = generateXMPPMessageID()
		if idErr != nil {
			return bridgev2.WrapErrorInStatus(idErr).WithSendNotice(true)
		}
		x.mucEcho.register(portalID, preID)
	}
	if _, err := sendXMPPRetractionID(ctx, sess, toJID, ref, msgType, preID); err != nil {
		if preID != "" {
			x.mucEcho.discard(portalID, preID)
		}
		return bridgev2.WrapErrorInStatus(fmt.Errorf("failed to send XMPP retraction: %w", err)).WithSendNotice(true)
	}
	log.Debug().Bool("muc", isMUC).Str("retract_target", ref).Msg("Sent XMPP retraction")

	// Tombstone every reference of the row so MAM/MUC history replay cannot resurrect it (bridgev2
	// deletes the row after we return).
	row := target
	if isMUC {
		row = withStanzaID(target, ref)
	}
	if kv := x.kv(); kv != nil {
		markRetractedRefs(ctx, kv, x.loginID, portal.ID, retractedRefsOf([]*database.Message{row}))
	}
	return nil
}
