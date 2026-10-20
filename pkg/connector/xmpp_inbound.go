package connector

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/mux"
	"mellium.im/xmpp/roster"
	"mellium.im/xmpp/stanza"
	"mellium.im/xmlstream"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

const jabberClientNS = "jabber:client"

// ContentKeyStanzaID is the Matrix message content key for the XMPP stanza ID (inbound only).
// Bridge ConvertedMessagePart.Content is *event.MessageEventContent so we cannot add this key in content;
// the stanza ID is persisted as MessageMetadata.XMPPMessageID via ConvertedMessagePart.DBMetadata.
const ContentKeyStanzaID = "me.mssj.mautrixjabber.stanza_id"

// messageLoggingHandler wraps a mux to log every incoming <message> stanza (before dispatch)
// and intercepts messages to parse body + XEP-0066 OOB so media URLs are captured.
type messageLoggingHandler struct {
	inner xmpp.Handler
	log   *zerolog.Logger
	x     *xmppClient
}

func (h *messageLoggingHandler) HandleXMPP(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	switch start.Name.Local {
	case "message":
		from := attrValue(start.Attr, "from")
		to := attrValue(start.Attr, "to")
		typeVal := attrValue(start.Attr, "type")
		h.log.Debug().Str("type", typeVal).Msg("XMPP stanza received: <message> (before mux dispatch)")
		warnIfDebugIdentifiers(h.log)
		h.log.Debug().
			Str("from", from).
			Str("to", to).
			Str("type", typeVal).
			Msg("XMPP stanza received: <message> (before mux dispatch)")
		if typeVal == "error" {
			// Log error stanzas (e.g. MUC "not in room") for debugging.
			h.log.Warn().
				Str("from", from).
				Str("to", to).
				Msg("XMPP message error stanza received (server rejected our message or sent an error)")
		}
		// Handle normal/chat ourselves so we can parse body + OOB (XEP-0066).
		if typeVal == "" || typeVal == "normal" || typeVal == "chat" {
			return h.x.handleMessageWithOOB(t, start)
		}
		// Handle groupchat (MUC) ourselves so we parse body + XEP-0444 reactions (inner mux only parses first body).
		if typeVal == "groupchat" {
			h.log.Warn().
				Str("from", from).
				Str("to", to).
				Msg("GROUPCHAT stanza received (will parse and queue); if you sent a reaction and see this, the bridge received it")
			return h.x.handleGroupchatWithOOB(t, start)
		}
	case "presence":
		return h.x.handlePresence(t, start)
	case "iq":
		typeVal := attrValue(start.Attr, "type")
		idVal := attrValue(start.Attr, "id")
		h.log.Debug().
			Str("type", typeVal).
			Str("id", idVal).
			Msg("XMPP stanza received: <iq> (before mux dispatch)")
		// Intercept carbons enable response so we can detect server rejection.
		// #region agent log
		matched := h.x.tryHandleCarbonsEnableResponse(t, start, typeVal, idVal)
		h.log.Debug().Str("hypothesis", "H2").Str("idVal", idVal).Str("typeVal", typeVal).Bool("matched", matched).Msg("DEBUG: IQ tryHandleCarbonsEnableResponse")
		// #endregion
		if matched {
			return nil
		}
	}
	return h.inner.HandleXMPP(t, start)
}

func attrValue(attrs []xml.Attr, name string) string {
	for _, a := range attrs {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// parseMessageBodyAndOOB reads the message stanza children from t (positioned after <message>) and returns body text, OOB URL (XEP-0066), reply ID (XEP-0461), and reaction data (XEP-0444).
func parseMessageBodyAndOOB(t xml.TokenReader) (body, oobURL, replyID string, reactionTarget string, reactionEmojis []string, jmi *jingleMessageInfo, err error) {
	body, oobURL, replyID, reactionTarget, reactionEmojis, jmi, _, err = parseMessageBodyOOBIDs(t)
	return
}

// parseMessageBodyOOBIDs is parseMessageBodyAndOOB plus the XEP-0359/0421 identifiers found as
// direct children of the message (untrusted; see messageIDs).
func parseMessageBodyOOBIDs(t xml.TokenReader) (body, oobURL, replyID string, reactionTarget string, reactionEmojis []string, jmi *jingleMessageInfo, ids messageIDs, err error) {
	var bodyBuf, urlBuf, reactionEmojiBuf strings.Builder
	depth := 1
	inBody, inOOB, inURL, inReactions, inReaction := false, false, false, false, false
	reactionEmojis = []string{}
	for depth > 0 {
		tok, err := t.Token()
		if err != nil {
			return strings.TrimSpace(bodyBuf.String()), strings.TrimSpace(urlBuf.String()), replyID, reactionTarget, reactionEmojis, jmi, ids, err
		}
		switch v := tok.(type) {
		case xml.StartElement:
			depth++
			if depth == 2 {
				ids.observe(v)
			}
			jmi = detectJingleMessage(jmi, v)
			if v.Name.Local == "body" {
				inBody = true
			}
			if v.Name.Local == "x" && attrValue(v.Attr, "xmlns") == oobNS {
				inOOB = true
			}
			if v.Name.Local == "url" && inOOB {
				inURL = true
			}
			if v.Name.Local == "reply" && attrValue(v.Attr, "xmlns") == replyNS {
				// Extract the 'id' attribute from <reply> element (required per XEP-0461)
				replyID = attrValue(v.Attr, "id")
			}
			if v.Name.Local == "reactions" && attrValue(v.Attr, "xmlns") == reactionsNS {
				// Extract the 'id' attribute from <reactions> element (XEP-0444)
				reactionTarget = attrValue(v.Attr, "id")
				inReactions = true
			}
			if v.Name.Local == "reaction" && inReactions {
				inReaction = true
				reactionEmojiBuf.Reset()
			}
		case xml.CharData:
			if inBody {
				bodyBuf.Write([]byte(v))
			}
			if inURL {
				urlBuf.Write([]byte(v))
			}
			if inReaction {
				reactionEmojiBuf.Write([]byte(v))
			}
		case xml.EndElement:
			depth--
			if v.Name.Local == "body" {
				inBody = false
			}
			if v.Name.Local == "url" {
				inURL = false
			}
			if v.Name.Local == "x" {
				inOOB = false
			}
			if v.Name.Local == "reactions" {
				inReactions = false
			}
			if v.Name.Local == "reaction" {
				if inReaction {
					emoji := strings.TrimSpace(reactionEmojiBuf.String())
					if emoji != "" {
						reactionEmojis = append(reactionEmojis, emoji)
					}
				}
				inReaction = false
			}
		}
	}
	return strings.TrimSpace(bodyBuf.String()), strings.TrimSpace(urlBuf.String()), replyID, reactionTarget, reactionEmojis, jmi, ids, nil
}

// tokenReaderWithPrefix implements xml.TokenReader by yielding one token then delegating to r.
// Used to "unread" a token when we peek and then hand the stream to another parser.
type tokenReaderWithPrefix struct {
	first xml.Token
	r     xml.TokenReader
}

func (p *tokenReaderWithPrefix) Token() (xml.Token, error) {
	if p.first != nil {
		t := p.first
		p.first = nil
		return t, nil
	}
	return p.r.Token()
}

// startReceiveSpan starts the root span for an inbound message stanza. The handlers run on the XMPP
// Serve goroutine and have no ctx parameter, so the span is rooted on context.Background(). Only
// JIDs, ids and flags are recorded, never bodies, emoji or URLs. peer is the bare peer JID (the
// room JID for groupchat); msgType and peer may be empty.
func startReceiveSpan(kind, msgType, from, peer, stanzaID string, hasOOB, hasReply, reaction bool) (context.Context, trace.Span) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.message.receive", trace.WithSpanKind(trace.SpanKindConsumer))
	if span.IsRecording() {
		attrs := []attribute.KeyValue{
			attribute.String("xmpp.message.kind", kind),
			attribute.String("xmpp.from", from),
			attribute.String("xmpp.stanza.id", stanzaID),
			attribute.Bool("xmpp.has_oob", hasOOB),
			attribute.Bool("xmpp.has_reply", hasReply),
			attribute.Bool("xmpp.reaction", reaction),
		}
		if msgType != "" {
			attrs = append(attrs, attribute.String("xmpp.message.type", msgType))
		}
		if peer != "" {
			key := "xmpp.peer.jid"
			if msgType == "groupchat" {
				key = "xmpp.room.jid"
			}
			attrs = append(attrs, attribute.String(key, peer))
		}
		span.SetAttributes(attrs...)
	}
	return ctx, span
}

// ensureMUCGhostTraced runs ensureMUCGhostDisplayName under a 5s timeout derived from ctx, in a
// bridge.ensure_muc_ghost span when ctx carries a trace.
func (x *xmppClient) ensureMUCGhostTraced(ctx context.Context, occupantFull string) {
	ctx, span := telemetry.StartChild(ctx, "bridge.ensure_muc_ghost")
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	x.ensureMUCGhostDisplayName(ctx, occupantFull)
}

// handleMessageWithOOB parses a full message stanza (body + XEP-0066 OOB + XEP-0461 reply + XEP-0444 reactions) and queues it for the bridge.
// If the first child is a MAM result (urn:xmpp:mam:2), it is handed to handleMAMResult for backfill/catch-up.
func (x *xmppClient) handleMessageWithOOB(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	tok, err := t.Token()
	if err != nil {
		return err
	}
	if se, ok := tok.(xml.StartElement); ok && se.Name.Space == mamNS && se.Name.Local == "result" {
		return x.handleMAMResult(t, &se, start)
	}
	// XEP-0280 Message Carbons: <sent> or <received> wrapper around inner message.
	if se, ok := tok.(xml.StartElement); ok && se.Name.Space == carbonsNS {
		if se.Name.Local == "sent" {
			return x.handleCarbonSent(t, start)
		}
		if se.Name.Local == "received" {
			return x.handleCarbonReceived(t, start)
		}
	}
	// Not a MAM result or carbon; parse as normal message (re-inject the token we read).
	tWithPrefix := &tokenReaderWithPrefix{first: tok, r: t}

	if x.login == nil {
		_, _, _, _, _, _, _ = parseMessageBodyAndOOB(tWithPrefix)
		return nil
	}
	body, oobURL, replyID, reactionTarget, reactionEmojis, jmi, ids, err := parseMessageBodyOOBIDs(tWithPrefix)
	if err != nil {
		x.log.Warn().Err(err).Msg("Failed to parse message body/OOB/reactions")
		return nil
	}
	from := attrValue(start.Attr, "from")
	fromBare := from
	if i := strings.Index(fromBare, "/"); i >= 0 {
		fromBare = fromBare[:i]
	}
	if fromBare == "" {
		return nil
	}
	// XEP-0353 Jingle Message Initiation (call signalling): surface as a Matrix notice.
	if jmi != nil {
		ctx, span := startReceiveSpan("jingle", attrValue(start.Attr, "type"), from, fromBare, attrValue(start.Attr, "id"), false, false, false)
		defer span.End()
		if span.IsRecording() {
			span.SetAttributes(attribute.String("xmpp.jingle.action", jmi.action))
		}
		x.handleJingleMessageInitiation(ctx, *jmi, fromBare, bareJID(from) == bareJID(string(x.loginID)))
		return nil
	}
	// Skip XEP-0085 chat state notifications (composing, paused, etc.) - messages with no body, no OOB, and no reactions.
	if body == "" && oobURL == "" && reactionTarget == "" {
		x.log.Debug().Str("from", attrValue(start.Attr, "from")).Msg("Skipping chat state notification (no body, no OOB, no reactions)")
		return nil
	}
	msgID := attrValue(start.Attr, "id")
	if msgID == "" {
		msgID = "xmp:" + time.Now().Format("20060102150405.000")
	}
	kind := "message"
	if reactionTarget != "" {
		kind = "reaction"
	}
	ctx, span := startReceiveSpan(kind, attrValue(start.Attr, "type"), from, fromBare, msgID, oobURL != "", replyID != "", reactionTarget != "")
	defer span.End()
	log := x.logFor(ctx)
	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(fromBare),
		Receiver: x.loginID,
	}
	x.addPortalIDForMAMCatchUp(string(portalKey.ID))
	senderJID := fromBare
	if individuals, ok := ParseCheogramGroupMMSJID(fromBare); ok && len(individuals) > 0 {
		if idx := strings.Index(from, "/"); idx >= 0 {
			afterSlash := from[idx+1:]
			if afterSlash != "" {
				if strings.Contains(afterSlash, "@") {
					senderJID = afterSlash
				} else if at := strings.LastIndex(fromBare, "@"); at > 0 {
					senderJID = afterSlash + "@" + fromBare[at+1:]
				} else {
					senderJID = individuals[0]
				}
			}
		}
		if senderJID == fromBare {
			if parsedSender, bodyWithoutPrefix, okBody := ParseCheogramGroupMMSSenderFromBody(body); okBody && parsedSender != "" {
				senderJID = parsedSender
				body = bodyWithoutPrefix
			} else {
				senderJID = individuals[0]
			}
		}
	}
	sender := x.makeEventSender(senderJID)
	x.maybeTriggerBackfillOnNewPortal(ctx, portalKey)

	// For XEP-0444 reactions with multiple emojis, queue a separate event for each emoji
	// This allows Matrix to display each reaction separately
	if reactionTarget != "" && len(reactionEmojis) > 0 {
		for i, emoji := range reactionEmojis {
			// Generate a unique message ID for each reaction event
			reactionMsgID := msgID
			if len(reactionEmojis) > 1 {
				reactionMsgID = fmt.Sprintf("%s-reaction-%d", msgID, i)
			}
			x.queueRemoteEvent(ctx, &jabberRemoteMessage{
				portalKey:      portalKey,
				sender:         sender,
				id:             networkid.MessageID(reactionMsgID),
				body:           "", // Reactions don't have body text
				oobURL:         "",
				replyID:        "",
				reactionTarget: reactionTarget, // XEP-0444 reaction target message ID
				reactionEmojis: []string{emoji}, // Single emoji per event
				timestamp:      time.Now(),
				x:              x, // Store reference to xmppClient for database access
			})
		}
	} else {
		// Regular message (not a reaction, or empty reaction list)
		x.queueRemoteEvent(ctx, &jabberRemoteMessage{
			portalKey:      portalKey,
			sender:         sender,
			id:             networkid.MessageID(msgID),
			body:           body,
			oobURL:         oobURL,
			replyID:        replyID, // XEP-0461 reply ID (XMPP message ID being replied to)
			reactionTarget: reactionTarget, // XEP-0444 reaction target message ID (empty for non-reactions)
			reactionEmojis: reactionEmojis, // XEP-0444 reaction emojis (empty for non-reactions)
			stanzaID:       ids.stanzaIDBy(string(x.loginID)), // archive id assigned by our own server (untrusted `by` ignored)
			originID:       ids.OriginID,
			timestamp:      time.Now(),
			x:              x, // Store reference to xmppClient for database access
		})
	}
	if replyID != "" {
		log.Debug().
			Str("reply_id", replyID).
			Str("body_preview", truncate(body, 80)).
			Msg("DEBUG: Received XMPP message with XEP-0461 reply")
	}
	if reactionTarget != "" {
		log.Debug().
			Str("reaction_target", reactionTarget).
			Strs("reaction_emojis", reactionEmojis).
			Msg("DEBUG: Received XMPP message with XEP-0444 reactions")
	}
	if oobURL != "" {
		log.Info().
			Str("type", attrValue(start.Attr, "type")).
			Str("id", msgID).
			Str("body_preview", truncate(body, 80)).
			Str("oob_url", oobURL).
			Str("reply_id", replyID).
			Str("reaction_target", reactionTarget).
			Msg("XMPP message received with OOB URL, queued for bridge")
	} else {
		log.Info().
			Str("type", attrValue(start.Attr, "type")).
			Str("id", msgID).
			Str("body_preview", truncate(body, 80)).
			Str("reply_id", replyID).
			Str("reaction_target", reactionTarget).
			Msg("XMPP message received (text only), queued for bridge")
	}
	return nil
}

// handleGroupchatWithOOB parses a full MUC message stanza (body + XEP-0066 OOB + XEP-0461 reply + XEP-0444 reactions)
// so that reactions sent by clients in XEP-0444 format are detected. The inner mux only parses the first <body>.
func (x *xmppClient) handleGroupchatWithOOB(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	from := attrValue(start.Attr, "from")
	x.log.Warn().
		Str("from", from).
		Msg("handleGroupchatWithOOB entered (parsing MUC stanza); reactions in XEP-0444 or body format will be detected here")
	if x.login == nil {
		_, _, _, _, _, _, _ = parseMessageBodyAndOOB(t)
		return nil
	}
	body, oobURL, replyID, reactionTarget, reactionEmojis, _, ids, err := parseMessageBodyOOBIDs(t)
	if err != nil {
		x.log.Warn().Err(err).Msg("Failed to parse MUC message body/OOB/reactions")
		return nil
	}
	// Skip only if no body, no OOB, and no reactions (e.g. chat state).
	if body == "" && oobURL == "" && reactionTarget == "" {
		x.log.Warn().
			Str("from", from).
			Msg("Skipping MUC message: no body, no OOB, no reactions (XEP-0444 reaction with empty body would be dropped here)")
		return nil
	}
	roomJID := from
	if i := strings.Index(roomJID, "/"); i >= 0 {
		roomJID = roomJID[:i]
	}
	occupantFull := from
	if roomJID == "" {
		return nil
	}
	// Skip bridging our own messages (self-echo): same check as handleIncomingGroupchat (wrapper handles groupchat so mux path is not used).
	if occupantFull == x.getMUCOurOccupantJID(roomJID) {
		x.log.Debug().Str("room", roomJID).Str("occupant", occupantFull).Msg("Skipping MUC self-echo (our own message)")
		return nil
	}
	msgID := attrValue(start.Attr, "id")
	if msgID == "" {
		msgID = "xmp:" + time.Now().Format("20060102150405.000")
	}
	kind := "message"
	if reactionTarget != "" {
		kind = "reaction"
	}
	ctx, span := startReceiveSpan(kind, "groupchat", occupantFull, roomJID, msgID, oobURL != "", replyID != "", reactionTarget != "")
	defer span.End()
	log := x.logFor(ctx)
	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(roomJID),
		Receiver: x.loginID,
	}
	x.addPortalIDForMAMCatchUp(string(portalKey.ID))
	// Ensure MUC ghost has display name set to nick (not full JID) before queueing.
	x.ensureMUCGhostTraced(ctx, occupantFull)
	sender := x.makeEventSender(occupantFull)

	// Body-style reaction (parseBodyReaction) is not used here; we only have XEP-0444 from parseMessageBodyAndOOB.
	// If the client sent body-style (id🤔> quote), body will be non-empty and reactionTarget empty; check for that.
	if reactionTarget == "" && body != "" {
		if targetID, emoji, ok := parseBodyReaction(body); ok {
			reactionTarget = targetID
			reactionEmojis = []string{emoji}
			body = ""
			span.SetAttributes(attribute.String("xmpp.message.kind", "reaction"), attribute.Bool("xmpp.reaction", true))
			log.Debug().
				Str("room", roomJID).
				Str("occupant", occupantFull).
				Str("reaction_target", targetID).
				Str("emoji", emoji).
				Msg("MUC body-based reaction detected, bridging as reaction")
		}
	}

	// Log every inbound MUC message (and reaction) for debugging.
	log.Info().
		Str("room", roomJID).
		Str("occupant", occupantFull).
		Str("id", msgID).
		Str("body_preview", truncate(body, 80)).
		Str("reaction_target", reactionTarget).
		Strs("reaction_emojis", reactionEmojis).
		Msg("Inbound MUC message received, queuing for bridge")
	if reactionTarget != "" && len(reactionEmojis) > 0 {
		log.Warn().
			Str("room", roomJID).
			Str("occupant", occupantFull).
			Str("reaction_target", reactionTarget).
			Strs("reaction_emojis", reactionEmojis).
			Msg("Inbound MUC reaction: will look up target and bridge to Matrix")
	}

	x.maybeTriggerBackfillOnNewPortal(ctx, portalKey)
	if reactionTarget != "" && len(reactionEmojis) > 0 {
		for i, emoji := range reactionEmojis {
			reactionMsgID := msgID
			if len(reactionEmojis) > 1 {
				reactionMsgID = fmt.Sprintf("%s-reaction-%d", msgID, i)
			}
			x.queueRemoteEvent(ctx, &jabberRemoteMessage{
				portalKey:      portalKey,
				sender:         sender,
				id:             networkid.MessageID(reactionMsgID),
				body:           "",
				oobURL:         "",
				replyID:        "",
				reactionTarget: reactionTarget,
				reactionEmojis: []string{emoji},
				timestamp:      time.Now(),
				x:              x,
			})
		}
	} else {
		x.queueRemoteEvent(ctx, &jabberRemoteMessage{
			portalKey:      portalKey,
			sender:         sender,
			id:             networkid.MessageID(msgID),
			body:           body,
			oobURL:         oobURL,
			replyID:        replyID,
			reactionTarget: reactionTarget,
			reactionEmojis: reactionEmojis,
			stanzaID:       ids.stanzaIDBy(roomJID), // only the room's own stanza-id is believed
			originID:       ids.OriginID,
			occupantID:     ids.OccupantID,
			timestamp:      time.Now(),
			x:              x,
		})
	}
	return nil
}

// serveMessages runs the XMPP session's Serve loop to handle incoming messages.
// It runs in a goroutine and blocks until the session is closed.
// mellium mux requires MessageFunc for message stanzas (Handle/HandleFunc panic for IQ/Message/Presence).
func (x *xmppClient) serveMessages() {
	// #region agent log
	x.log.Debug().Str("hypothesis", "H1").Msg("DEBUG: serveMessages attempting x.mu.Lock")
	// #endregion
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	// #region agent log
	x.log.Debug().Str("hypothesis", "H1").Bool("hasSess", sess != nil).Msg("DEBUG: serveMessages lock acquired")
	// #endregion
	if sess == nil {
		return
	}
	if x.login != nil {
		x.log.Info().Msg("XMPP message receiver (Serve) started")
		warnIfDebugIdentifiers(&x.log)
		x.log.Debug().Str("login_id", string(x.loginID)).Msg("XMPP message receiver (Serve) started")
	}
	bodyPayload := xml.Name{Local: "body"}
	chatType := stanza.MessageType("chat")
	groupchatType := stanza.MessageType("groupchat")
	mamFinPayload := xml.Name{Space: mamNS, Local: "fin"}
	m := mux.New(jabberClientNS,
		roster.Handle(roster.Handler{Push: x.handleRosterPush}),
		mux.MessageFunc(stanza.NormalMessage, bodyPayload, x.handleIncomingMessage),
		mux.MessageFunc(chatType, bodyPayload, x.handleIncomingMessage),
		mux.MessageFunc(groupchatType, bodyPayload, x.handleIncomingGroupchat),
		mux.IQ(stanza.ResultIQ, mamFinPayload, mamFinHandler{x: x}),
	)
	wrapper := &messageLoggingHandler{inner: m, log: &x.log, x: x}
	serveStart := time.Now()
	serveErr := sess.Serve(wrapper)
	if serveErr != nil && x.login != nil {
		x.log.Debug().Err(serveErr).Msg("XMPP Serve ended")
	}
	// Post-hoc session span (never held open while Serve runs).
	_, sessSpan := telemetry.Start(context.Background(), "xmpp.session", trace.WithTimestamp(serveStart))
	if sessSpan.IsRecording() {
		sessSpan.SetAttributes(attribute.String("xmpp.login.jid", string(x.loginID)))
	}
	telemetry.Fail(sessSpan, serveErr)
	sessSpan.End()
	// Clear the session so ReconnectLoop knows to reconnect after disconnects.
	x.mu.Lock()
	// Only tear down state if x.session is still the session this loop served; after Disconnect
	// (or a reconnect) it is nil or belongs to a newer session whose goroutines must keep running.
	hadSession := x.session != nil && x.session == sess
	if hadSession {
		x.log.Debug().Msg("Clearing XMPP session after Serve loop ended")
		x.session = nil
		x.endSessionLocked()
	}
	login := x.login
	x.mu.Unlock()
	if hadSession && login != nil {
		login.BridgeState.Send(status.BridgeState{StateEvent: status.StateTransientDisconnect})
	}
}

// handleIncomingMessage is called by the mux for each incoming message with a <body> and type normal or chat.
func (x *xmppClient) handleIncomingMessage(msg stanza.Message, rw xmlstream.TokenReadEncoder) error {
	if x.login == nil {
		return nil
	}
	fromBare := msg.From.Bare().String()
	if fromBare == "" {
		return nil
	}
	body := readBodyFromStream(rw)
	x.log.Info().
		Str("type", string(msg.Type)).
		Str("id", msg.ID).
		Str("body_preview", truncate(body, 80)).
		Msg("XMPP message received, queuing for bridge")
	warnIfDebugIdentifiers(&x.log)
	x.log.Debug().
		Str("from", fromBare).
		Str("type", string(msg.Type)).
		Str("id", msg.ID).
		Str("body_preview", truncate(body, 80)).
		Msg("XMPP message received, queuing for bridge")

	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(fromBare),
		Receiver: x.loginID,
	}
	x.addPortalIDForMAMCatchUp(string(portalKey.ID))
	msgID := networkid.MessageID(msg.ID)
	if msgID == "" {
		msgID = networkid.MessageID("xmp:" + time.Now().Format("20060102150405.000"))
	}
	// For Cheogram group MMS, the real sender may be in the From resource or in the body prefix <xmpp:JID>.
	senderJID := fromBare
	if individuals, ok := ParseCheogramGroupMMSJID(fromBare); ok && len(individuals) > 0 {
		if res := msg.From.Resourcepart(); res != "" {
			if strings.Contains(res, "@") {
				senderJID = res
			} else {
				domain := msg.From.Domainpart()
				if domain != "" {
					senderJID = res + "@" + domain
				} else {
					senderJID = individuals[0]
				}
			}
		} else if parsedSender, bodyWithoutPrefix, okBody := ParseCheogramGroupMMSSenderFromBody(body); okBody && parsedSender != "" {
			senderJID = parsedSender
			body = bodyWithoutPrefix
		} else {
			senderJID = individuals[0]
		}
	}
	sender := x.makeEventSender(senderJID)
	x.maybeTriggerBackfillOnNewPortal(context.Background(), portalKey)
	x.queueRemoteEvent(context.Background(), &jabberRemoteMessage{
		portalKey: portalKey,
		sender:    sender,
		id:        msgID,
		body:      body,
		replyID:   "", // handleIncomingMessage doesn't parse reply elements (uses simpler body-only parsing)
		timestamp: time.Now(),
		x:         x, // Store reference to xmppClient for database access
	})
	return nil
}

// handleIncomingGroupchat is called by the mux for each incoming message with type groupchat (MUC).
// Portal key uses the room JID (room@conference.domain); sender is the occupant (room@conference.domain/nick).
func (x *xmppClient) handleIncomingGroupchat(msg stanza.Message, rw xmlstream.TokenReadEncoder) error {
	if x.login == nil {
		return nil
	}
	roomJID := msg.From.Bare().String()
	occupantFull := msg.From.String()
	if roomJID == "" {
		return nil
	}
	body := readBodyFromStream(rw)
	// Skip empty groupchat messages (e.g. subject-only or chat state).
	if body == "" {
		x.log.Debug().Str("room", roomJID).Str("from", occupantFull).Msg("Skipping groupchat message with no body")
		return nil
	}
	var reactionTarget string
	var reactionEmojis []string
	if targetID, emoji, ok := parseBodyReaction(body); ok {
		reactionTarget = targetID
		reactionEmojis = []string{emoji}
		body = ""
		x.log.Debug().
			Str("room", roomJID).
			Str("reaction_target", targetID).
			Str("emoji", emoji).
			Msg("MUC body-based reaction detected, bridging as reaction")
	}
	// Skip bridging our own messages (self-echo): compare sender to our stored occupant JID for this room.
	if occupantFull == x.getMUCOurOccupantJID(roomJID) {
		x.log.Debug().Str("room", roomJID).Str("occupant", occupantFull).Msg("Skipping MUC self-echo (our own message)")
		return nil
	}

	x.log.Info().
		Str("room", roomJID).
		Str("occupant", occupantFull).
		Str("id", msg.ID).
		Str("body_preview", truncate(body, 80)).
		Msg("MUC message received, queuing for bridge")

	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(roomJID),
		Receiver: x.loginID,
	}
	x.addPortalIDForMAMCatchUp(string(portalKey.ID))
	// Ensure MUC ghost has display name set to nick (not full JID) before queueing.
	x.ensureMUCGhostTraced(context.Background(), occupantFull)
	x.maybeTriggerBackfillOnNewPortal(context.Background(), portalKey)
	msgID := networkid.MessageID(msg.ID)
	if msgID == "" {
		msgID = networkid.MessageID("xmp:" + time.Now().Format("20060102150405.000"))
	}
	sender := x.makeEventSender(occupantFull)
	x.queueRemoteEvent(context.Background(), &jabberRemoteMessage{
		portalKey:      portalKey,
		sender:         sender,
		id:             msgID,
		body:           body,
		replyID:        "",
		reactionTarget: reactionTarget,
		reactionEmojis: reactionEmojis,
		timestamp:      time.Now(),
		x:              x,
	})
	return nil
}

// handlePresence processes incoming presence stanzas: records our MUC occupant JID on join success,
// and detects 409 conflict for nick conflicts (retry is handled in JoinMUC).
func (x *xmppClient) handlePresence(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	fromStr := attrValue(start.Attr, "from")
	typeVal := attrValue(start.Attr, "type")
	if fromStr == "" {
		return nil
	}
	fromJID, err := jid.Parse(fromStr)
	if err != nil {
		return nil
	}
	roomBare := fromJID.Bare().String()
	ourNick := fromJID.Resourcepart()

	// Consume the rest of the presence stanza and detect error/conflict (409).
	// Also capture the XEP-0153 vcard-temp:x:update photo hash (avatar change signal), and
	// note whether this is MUC presence so we don't treat occupant presence as a 1:1 contact.
	depth := 1
	var hasConflict bool
	var sawMUCUser, sawVCardUpdate, inVCardUpdate, inPhoto bool
	var photoHash strings.Builder
	for depth > 0 {
		tok, err := t.Token()
		if err != nil {
			break
		}
		switch v := tok.(type) {
		case xml.StartElement:
			depth++
			if v.Name.Local == "error" {
				for _, a := range v.Attr {
					if a.Name.Local == "code" && a.Value == "409" {
						hasConflict = true
						break
					}
				}
			}
			if v.Name.Local == "conflict" {
				hasConflict = true
			}
			if v.Name.Space == mucUserNS {
				sawMUCUser = true
			}
			if v.Name.Local == "x" && v.Name.Space == vcardUpdateNS {
				sawVCardUpdate = true
				inVCardUpdate = true
			}
			if inVCardUpdate && v.Name.Local == "photo" {
				inPhoto = true
			}
		case xml.CharData:
			if inPhoto {
				photoHash.Write(v)
			}
		case xml.EndElement:
			depth--
			if v.Name.Local == "photo" {
				inPhoto = false
			}
			if v.Name.Local == "x" && v.Name.Space == vcardUpdateNS {
				inVCardUpdate = false
			}
		}
	}

	// XEP-0153: a 1:1 contact advertised (or cleared) their avatar hash. Refresh the ghost avatar
	// out-of-band. Skip MUC presence (occupant ghosts are keyed by full JID, not bare).
	if sawVCardUpdate && typeVal != "error" && !sawMUCUser {
		go x.refreshContactAvatarFromPresence(fromJID.Bare().String(), strings.TrimSpace(photoHash.String()))
	}

	if typeVal == "error" {
		if hasConflict {
			x.mucPendingJoinMu.Lock()
			if x.mucPendingJoin == nil {
				x.mucPendingJoin = make(map[string]string)
			}
			delete(x.mucPendingJoin, roomBare)
			x.mucPendingJoinMu.Unlock()
			x.log.Info().Str("room", roomBare).Str("nick", ourNick).Msg("MUC join nick conflict (409), will retry with alternative nick")
			go x.retryMUCJoinAfterConflict(roomBare, ourNick)
		}
		return nil
	}

	// Available presence from a MUC (From has resource): if it matches our pending join, record our occupant JID.
	if ourNick == "" {
		return nil
	}
	x.mucPendingJoinMu.Lock()
	pendingNick, pending := x.mucPendingJoin[roomBare]
	if pending && pendingNick == ourNick {
		if x.mucOurOccupantJID == nil {
			x.mucOurOccupantJID = make(map[string]string)
		}
		x.mucOurOccupantJID[roomBare] = fromJID.String()
		delete(x.mucPendingJoin, roomBare)
		x.log.Debug().Str("room", roomBare).Str("occupant_jid", fromJID.String()).Msg("MUC join confirmed, stored our occupant JID")
		// Queue ChatResync so the bridge re-fetches GetChatInfo (and can refresh room/ghost info after rejoin).
		if x.login != nil {
			x.queueRemoteEvent(context.Background(), &simplevent.ChatResync{
				EventMeta: simplevent.EventMeta{
					Type:      bridgev2.RemoteEventChatResync,
					PortalKey: networkid.PortalKey{ID: networkid.PortalID(roomBare), Receiver: x.loginID},
				},
				GetChatInfoFunc: x.GetChatInfo,
			})
		}
	}
	x.mucPendingJoinMu.Unlock()

	// Consume any remaining tokens if we exited early (shouldn't happen).
	for depth > 0 {
		tok, _ := t.Token()
		if e, ok := tok.(xml.EndElement); ok {
			depth--
			_ = e
		}
	}
	return nil
}

// readBodyFromStream reads the text content of the current element (body) from the token stream.
func readBodyFromStream(rw xmlstream.TokenReadEncoder) string {
	var b strings.Builder
	for {
		tok, err := rw.Token()
		if err != nil {
			break
		}
		if cd, ok := tok.(xml.CharData); ok {
			b.Write([]byte(cd))
		}
	}
	return strings.TrimSpace(b.String())
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// jabberRemoteMessage implements bridgev2.RemoteMessage for incoming XMPP messages.
type jabberRemoteMessage struct {
	portalKey      networkid.PortalKey
	sender         bridgev2.EventSender
	id             networkid.MessageID
	body           string
	oobURL         string   // XEP-0066 OOB media URL when present
	replyID        string   // XEP-0461 reply ID (XMPP message ID being replied to)
	reactionTarget string   // XEP-0444 reaction target message ID
	reactionEmojis []string // XEP-0444 reaction emojis
	// Stable ids stored in MessageMetadata (see dbMeta). stanzaID is only ever set from a stanza-id
	// whose `by` is a trusted entity (room for groupchat, our account for 1:1 and MAM).
	stanzaID       string
	originID       string
	occupantID     string // XEP-0421, groupchat only
	backfillSource string // "mam" for events that came out of the archive
	timestamp      time.Time
	x              *xmppClient // Reference to xmppClient for database access
	spanCtx        trace.SpanContext // trace context set by queueRemoteEvent before queueing (zero when tracing is off)
}

var (
	_ bridgev2.RemoteMessage    = (*jabberRemoteMessage)(nil)
	_ bridgev2.RemoteReaction   = (*jabberRemoteMessage)(nil)
	_ bridgev2.RemoteReactionRemove = (*jabberRemoteMessage)(nil)
	_ bridgev2.RemoteEventWithContextMutation = (*jabberRemoteMessage)(nil)
	_ bridgev2.RemotePostHandler = (*jabberRemoteMessage)(nil)
)

// parseBodyReaction detects body-based reaction format used by some clients (e.g. "message-id🥳> quoted text").
// Returns reaction target message ID, emoji, and true if body matches; otherwise empty and false.
func parseBodyReaction(body string) (targetID string, emoji string, ok bool) {
	idx := strings.Index(body, "> ")
	if idx < 2 {
		return "", "", false
	}
	prefix := body[:idx]
	runes := []rune(prefix)
	if len(runes) < 2 {
		return "", "", false
	}
	for n := 1; n <= 5 && n <= len(runes); n++ {
		tail := string(runes[len(runes)-n:])
		if isSingleEmoji(tail) {
			id := strings.TrimSpace(string(runes[:len(runes)-n]))
			if id != "" {
				return id, tail, true
			}
		}
	}
	return "", "", false
}

// isSingleEmoji checks if a string contains only a single emoji character (possibly with variation selectors).
// This is used to detect Jabber reactions (replies with just an emoji).
func isSingleEmoji(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	// Count runes - a single emoji might be multiple runes (e.g., emoji + variation selector, or skin tone modifiers)
	// But it should be relatively short (typically 1-5 runes for a single emoji with modifiers)
	runes := []rune(s)
	if len(runes) > 5 {
		return false
	}
	// Check if all runes are emoji-related (emoji, variation selectors, zero-width joiners, skin tone modifiers, etc.)
	for _, r := range runes {
		// Basic emoji ranges and common modifiers
		if !((r >= 0x1F300 && r <= 0x1F9FF) || // Emoticons, symbols, flags, people, etc.
			(r >= 0x2600 && r <= 0x26FF) || // Miscellaneous symbols
			(r >= 0x2700 && r <= 0x27BF) || // Dingbats
			(r == 0xFE0F) || // Variation selector-16 (makes emoji colorful)
			(r == 0x200D) || // Zero-width joiner (for multi-part emojis)
			(r >= 0x1F1E6 && r <= 0x1F1FF) || // Regional indicator symbols (flags)
			(r >= 0x1F3FB && r <= 0x1F3FF) || // Skin tone modifiers
			(r >= 0x1F900 && r <= 0x1F9FF) || // Supplemental Symbols and Pictographs
			(r >= 0x1FA00 && r <= 0x1FAFF)) { // Symbols and Pictographs Extended-A
			return false
		}
	}
	return true
}

func (m *jabberRemoteMessage) GetType() bridgev2.RemoteEventType {
	// XEP-0444 reactions take precedence - if present, this is a reaction
	if m.reactionTarget != "" {
		if m.x != nil {
			telemetry.LoggerSC(m.spanCtx, &m.x.log).Debug().
				Str("reaction_target", m.reactionTarget).
				Strs("reaction_emojis", m.reactionEmojis).
				Msg("DEBUG: Detected Jabber reaction (XEP-0444)")
		}
		// Empty reactions list means removal
		if len(m.reactionEmojis) == 0 {
			return bridgev2.RemoteEventReactionRemove
		}
		return bridgev2.RemoteEventReaction
	}
	// Fallback: If this is a reply with only a single emoji, treat it as a reaction (legacy behavior)
	if m.replyID != "" && isSingleEmoji(m.body) {
		// Log debug info for reaction detection
		if m.x != nil {
			telemetry.LoggerSC(m.spanCtx, &m.x.log).Debug().
				Str("reply_id", m.replyID).
				Str("emoji", strings.TrimSpace(m.body)).
				Msg("DEBUG: Detected Jabber reaction (reply with single emoji, legacy)")
		}
		return bridgev2.RemoteEventReaction
	}
	return bridgev2.RemoteEventMessage
}

func (m *jabberRemoteMessage) GetPortalKey() networkid.PortalKey  { return m.portalKey }
func (m *jabberRemoteMessage) GetSender() bridgev2.EventSender   { return m.sender }
func (m *jabberRemoteMessage) GetID() networkid.MessageID         { return m.id }
func (m *jabberRemoteMessage) AddLogContext(c zerolog.Context) zerolog.Context {
	return c.Str("message_id", string(m.id))
}

// MutateContext starts the remote-event handling span (no-op without a span context).
func (m *jabberRemoteMessage) MutateContext(ctx context.Context) context.Context {
	return telemetry.StartRemoteEvent(ctx, m.spanCtx)
}

// PostHandle ends the span started in MutateContext.
func (m *jabberRemoteMessage) PostHandle(ctx context.Context, _ *bridgev2.Portal) {
	telemetry.EndRemoteEvent(ctx)
}

// ShouldCreatePortal returns true so the bridge creates a Matrix room when the first message
// arrives for a new JID (otherwise the portal is not created and the message has nowhere to go).
func (m *jabberRemoteMessage) ShouldCreatePortal() bool { return true }

// GetTargetMessage returns the message ID being reacted to (for reactions).
func (m *jabberRemoteMessage) GetTargetMessage() (target networkid.MessageID) {
	// XEP-0444 reactions use reactionTarget
	if m.reactionTarget != "" {
		// Look up the database message ID from the XMPP reaction target ID
		if m.x != nil && m.x.conn != nil && m.x.conn.br != nil {
			telemetry.LoggerSC(m.spanCtx, &m.x.log).Debug().
				Str("reaction_target", m.reactionTarget).
				Str("portal_id", string(m.portalKey.ID)).
				Str("reaction_message_id", string(m.id)).
				Msg("Looking up reaction target (XMPP message ID -> Matrix event) for inbound reaction")
			// No ctx parameter here: parent the lookup span on the span context queueRemoteEvent stored.
			ctx, span := telemetry.StartChild(telemetry.WithParent(context.Background(), m.spanCtx), "bridge.lookup_reaction_target")
			log := m.x.logFor(ctx)
			retried := false
			defer func() {
				span.SetAttributes(attribute.Bool("bridge.found", target != ""), attribute.Bool("bridge.retried", retried))
				span.End()
			}()
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			xmppMsgID := networkid.MessageID(m.reactionTarget)
			dbMsg, err := m.x.conn.br.DB.Message.GetFirstPartByID(ctx, m.x.loginID, xmppMsgID)
			if err == nil && dbMsg != nil {
				log.Debug().
					Str("reaction_target", m.reactionTarget).
					Str("db_message_id", string(dbMsg.ID)).
					Msg("Found reaction target via direct GetFirstPartByID lookup")
				return dbMsg.ID
			}
			// If not found via direct lookup, search portal messages (for Matrix-originated messages
			// or XMPP-originated messages when the bridge stores XMPPMessageID via DBMetadata)
			searchStart := time.Now().Add(-30 * 24 * time.Hour)
			tryFind := func() networkid.MessageID {
				dbMessages, err := m.x.conn.br.DB.Message.GetMessagesBetweenTimeQuery(ctx, m.portalKey, searchStart, time.Now())
				if err != nil {
					log.Warn().Err(err).Str("reaction_target", m.reactionTarget).Msg("GetMessagesBetweenTimeQuery failed for reaction target lookup")
					return ""
				}
				log.Debug().Int("candidate_count", len(dbMessages)).Str("reaction_target", m.reactionTarget).Msg("Searching portal messages for reaction target (Matrix-originated messages have XMPPMessageID in metadata)")
				for _, msg := range dbMessages {
					if msg.Metadata != nil {
						if meta, ok := msg.Metadata.(*MessageMetadata); ok && meta != nil && meta.XMPPMessageID == m.reactionTarget {
							log.Debug().
								Str("reaction_target", m.reactionTarget).
								Str("db_message_id", string(msg.ID)).
								Msg("Found reaction target via metadata XMPPMessageID match")
							return msg.ID
						}
					}
					if string(msg.ID) == m.reactionTarget {
						log.Debug().
							Str("reaction_target", m.reactionTarget).
							Str("db_message_id", string(msg.ID)).
							Msg("Found reaction target via message ID match")
						return msg.ID
					}
				}
				// Log a few candidates for debugging (ID and XMPPMessageID from metadata)
				for i, msg := range dbMessages {
					if i >= 5 {
						break
					}
					xmppID := ""
					if msg.Metadata != nil {
						if meta, ok := msg.Metadata.(*MessageMetadata); ok && meta != nil {
							xmppID = meta.XMPPMessageID
						}
					}
					log.Debug().
						Str("candidate_id", string(msg.ID)).
						Str("candidate_xmpp_id", xmppID).
						Msg("Reaction target lookup candidate")
				}
				return ""
			}
			if found := tryFind(); found != "" {
				return found
			}
			// Retry once after short delay: reaction may have been processed before the sent message was committed (race).
			retried = true
			time.Sleep(400 * time.Millisecond)
			if found := tryFind(); found != "" {
				return found
			}
			log.Warn().
				Str("reaction_target", m.reactionTarget).
				Str("portal_id", string(m.portalKey.ID)).
				Msg("Reaction target not found: no message with this XMPP ID in portal (reaction will not be bridged to Matrix)")
		}
		return ""
	}
	// Fallback to reply-based reaction detection (legacy)
	if m.replyID == "" {
		return ""
	}
	// Look up the database message ID from the XMPP reply ID
	// This is similar to what we do in ConvertMessage for replies
	if m.x != nil && m.x.conn != nil && m.x.conn.br != nil {
		ctx, span := telemetry.StartChild(telemetry.WithParent(context.Background(), m.spanCtx), "bridge.lookup_reaction_target")
		log := m.x.logFor(ctx)
		defer func() {
			span.SetAttributes(attribute.Bool("bridge.found", target != ""), attribute.Bool("bridge.retried", false))
			span.End()
		}()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		
		xmppMsgID := networkid.MessageID(m.replyID)
		dbMsg, err := m.x.conn.br.DB.Message.GetFirstPartByID(ctx, m.x.loginID, xmppMsgID)
		if err == nil && dbMsg != nil {
			if m.x != nil {
				log.Debug().
					Str("reply_id", m.replyID).
					Str("db_message_id", string(dbMsg.ID)).
					Msg("DEBUG: Found target message for reaction via direct lookup")
			}
			return dbMsg.ID
		}
		// If not found via direct lookup, search portal messages (for Matrix-originated messages)
		searchStart := time.Now().Add(-7 * 24 * time.Hour)
		dbMessages, err := m.x.conn.br.DB.Message.GetMessagesBetweenTimeQuery(ctx, m.portalKey, searchStart, time.Now())
		if err == nil {
			for _, msg := range dbMessages {
				if msg.Metadata != nil {
					if meta, ok := msg.Metadata.(*MessageMetadata); ok && meta != nil && meta.XMPPMessageID == m.replyID {
						if m.x != nil {
							log.Debug().
								Str("reply_id", m.replyID).
								Str("db_message_id", string(msg.ID)).
								Msg("DEBUG: Found target message for reaction via metadata search")
						}
						return msg.ID
					}
				}
				if string(msg.ID) == m.replyID {
					if m.x != nil {
						log.Debug().
							Str("reply_id", m.replyID).
							Str("db_message_id", string(msg.ID)).
							Msg("DEBUG: Found target message for reaction via ID match")
					}
					return msg.ID
				}
			}
		}
		if m.x != nil {
			log.Debug().
				Str("reply_id", m.replyID).
				Msg("DEBUG: Could not find target message for reaction")
		}
	}
	return ""
}

// GetReactionEmoji returns the emoji for this reaction.
func (m *jabberRemoteMessage) GetReactionEmoji() (string, networkid.EmojiID) {
	if m.GetType() != bridgev2.RemoteEventReaction {
		panic("GetReactionEmoji() called for non-reaction event")
	}
	// XEP-0444 reactions: return the first emoji from the reactions list
	if m.reactionTarget != "" && len(m.reactionEmojis) > 0 {
		return strings.TrimSpace(m.reactionEmojis[0]), ""
	}
	// Fallback: legacy reply-based reaction (single emoji in body)
	return strings.TrimSpace(m.body), ""
}

// GetRemovedEmojiID returns the emoji ID being removed (for reaction removals).
func (m *jabberRemoteMessage) GetRemovedEmojiID() networkid.EmojiID {
	return ""
}

const (
	maxJabberMediaDownloadBytes = 50 * 1024 * 1024 // 50 MiB
	jabberMediaDownloadTimeout  = 60 * time.Second
)

// dbMeta returns the metadata persisted with every converted part of this message. bridgev2 stores
// ConvertedMessagePart.DBMetadata with the row (it never calls a GetMessageMetadata on the remote
// event), so this is how the XMPP id mapping reaches the database, synchronously with the insert.
// OriginID is only kept when it differs from the stanza id attribute.
func (m *jabberRemoteMessage) dbMeta() *MessageMetadata {
	meta := &MessageMetadata{
		XMPPMessageID:  string(m.id),
		StanzaID:       m.stanzaID,
		OccupantID:     m.occupantID,
		BackfillSource: m.backfillSource,
	}
	if m.originID != string(m.id) {
		meta.OriginID = m.originID
	}
	return meta
}

// ConvertMessage converts the XMPP message and attaches dbMeta to every part.
func (m *jabberRemoteMessage) ConvertMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI) (*bridgev2.ConvertedMessage, error) {
	cm, err := m.convertMessage(ctx, portal, intent)
	if err != nil || cm == nil {
		return cm, err
	}
	for _, part := range cm.Parts {
		if part != nil && part.DBMetadata == nil {
			part.DBMetadata = m.dbMeta()
		}
	}
	return cm, nil
}

func (m *jabberRemoteMessage) convertMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI) (*bridgev2.ConvertedMessage, error) {
	// Child-only: normally parented by bridge.handle_remote_event; never start a root trace here.
	ctx, span := telemetry.StartChild(ctx, "bridge.convert_message")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Bool("xmpp.has_oob", m.oobURL != ""))
	}
	body := strings.TrimSpace(m.body)
	// Prefer XEP-0066 OOB URL when present (Jabber client sent media with OOB).
	if m.oobURL != "" {
		// Use a logger from context if available, otherwise create a basic one
		log := zerolog.Ctx(ctx).With().Str("oob_url", m.oobURL).Logger()
		log.Debug().Msg("Processing Jabber message with OOB URL")
		content, mimeType, fileName, msgType, err := downloadHTTPSMedia(ctx, m.oobURL)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to download OOB media; falling back to text")
		} else if len(content) == 0 {
			log.Warn().Msg("OOB media download returned empty content; falling back to text")
		} else {
			log.Debug().Str("mime_type", mimeType).Str("file_name", fileName).Int("size", len(content)).Msg("Downloaded OOB media, uploading to Matrix")
			mxcURL, file, upErr := intent.UploadMedia(ctx, portal.MXID, content, fileName, mimeType)
			if upErr != nil {
				log.Warn().Err(upErr).Msg("Failed to upload OOB media to Matrix; falling back to text")
			} else {
				log.Info().
					Str("mxc", string(mxcURL)).
					Bool("has_file", file != nil).
					Str("file_name", fileName).
					Str("msg_type", string(msgType)).
					Str("mime_type", mimeType).
					Int("size", len(content)).
					Msg("Successfully converted OOB media to Matrix")
				if span.IsRecording() {
					span.SetAttributes(attribute.Int("file.size", len(content)), attribute.String("file.mime", mimeType))
				}
				caption := captionFromBodyWithOOB(m.body, m.oobURL)
				result := makeMediaPart(fileName, msgType, mxcURL, file, mimeType, len(content), string(m.id), caption)
				log.Debug().
					Str("content_msg_type", string(result.Parts[0].Content.MsgType)).
					Str("content_body", result.Parts[0].Content.Body).
					Str("content_filename", result.Parts[0].Content.FileName).
					Str("content_url", string(result.Parts[0].Content.URL)).
					Bool("content_has_file", result.Parts[0].Content.File != nil).
					Bool("content_has_info", result.Parts[0].Content.Info != nil).
					Str("content_info_mime", func() string {
						if result.Parts[0].Content.Info != nil {
							return result.Parts[0].Content.Info.MimeType
						}
						return ""
					}()).
					Int("content_info_size", func() int {
						if result.Parts[0].Content.Info != nil {
							return result.Parts[0].Content.Info.Size
						}
						return 0
					}()).
					Msg("DEBUG: makeMediaPart result - final content being sent to Matrix")
				return result, nil
			}
		}
	}
	// Single MXC URI: render as file so Matrix clients show media
	if strings.HasPrefix(body, "mxc://") && !strings.Contains(strings.TrimPrefix(body, "mxc://"), " ") {
		return &bridgev2.ConvertedMessage{
			Parts: []*bridgev2.ConvertedMessagePart{{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType: event.MsgFile,
					Body:    body,
					URL:     id.ContentURIString(body),
				},
			}},
		}, nil
	}
	// Single HTTPS URL in body (fallback): download and re-upload to Matrix only if it's actually a file
	// This prevents web pages (like https://www.amazon.com/) from being uploaded as attachments
	if strings.HasPrefix(body, "https://") && !strings.Contains(body, " ") {
		content, mimeType, fileName, msgType, err := downloadHTTPSMedia(ctx, body)
		if err == nil && len(content) > 0 {
			// Only treat as file attachment if URL has file extension or Content-Type indicates it's a file
			// This prevents web pages from being incorrectly uploaded as attachments
			if isLikelyFileAttachment(body, mimeType, fileName) {
				mxcURL, file, upErr := intent.UploadMedia(ctx, portal.MXID, content, fileName, mimeType)
				if upErr == nil {
					return makeMediaPart(fileName, msgType, mxcURL, file, mimeType, len(content), string(m.id), ""), nil
				}
			}
			// If it's not a file attachment, fall through to treat it as a regular text message with a link
		}
	}
	// Strip quoted text from body when a reply is detected (Jabber clients include quoted text prefixed with "> ")
	bodyText := m.body
	if m.replyID != "" {
		// Remove lines starting with "> " (common Jabber quote format)
		lines := strings.Split(bodyText, "\n")
		var cleanLines []string
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			// Skip lines that are just quotes (starting with ">")
			if !strings.HasPrefix(trimmed, ">") {
				cleanLines = append(cleanLines, line)
			}
		}
		bodyText = strings.Join(cleanLines, "\n")
		bodyText = strings.TrimSpace(bodyText)
		// Log if we stripped anything
		if bodyText != m.body {
			zerolog.Ctx(ctx).Debug().
				Str("original_body", m.body).
				Str("cleaned_body", bodyText).
				Msg("DEBUG: Stripped quoted text from reply body")
		}
	}
	
	part := &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: &event.MessageEventContent{MsgType: event.MsgText, Body: bodyText},
	}

	cm := &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{part}}
	
	// Handle XEP-0461 reply: look up the Matrix event ID from the XMPP message ID
	if m.replyID != "" && m.x != nil && m.x.conn != nil && m.x.conn.br != nil {
		log := zerolog.Ctx(ctx).With().Str("reply_id", m.replyID).Logger()
		log.Debug().Msg("DEBUG: Found XEP-0461 reply ID, looking up Matrix event ID")
		
		// Look up the message by XMPP message ID
		// First try direct lookup (for XMPP-originated messages)
		xmppMsgID := networkid.MessageID(m.replyID)
		log.Debug().
			Str("xmpp_reply_id", m.replyID).
			Str("login_id", string(m.x.loginID)).
			Str("portal_id", string(m.portalKey.ID)).
			Msg("DEBUG: Attempting to look up message by XMPP reply ID")
		
		dbMsg, err := m.x.conn.br.DB.Message.GetFirstPartByID(ctx, m.x.loginID, xmppMsgID)
		if err != nil {
			log.Debug().Err(err).Msg("DEBUG: Direct lookup returned error")
		}
		if dbMsg == nil {
			// If not found, search through messages in this portal to find one with matching XMPP ID in metadata
			// (for Matrix-originated messages that have XMPP ID stored in metadata)
			log.Debug().Msg("DEBUG: Direct lookup failed, searching portal messages for XMPP ID in metadata")
			// Query messages in this portal and check metadata
			// GetMessagesBetweenTimeQuery signature: (ctx, portalKey, startTime, endTime)
			// Search recent messages (last 7 days) to avoid scanning too many
			searchStart := time.Now().Add(-7 * 24 * time.Hour)
			dbMessages, err := m.x.conn.br.DB.Message.GetMessagesBetweenTimeQuery(ctx, m.portalKey, searchStart, time.Now())
			if err != nil {
				log.Debug().Err(err).Msg("DEBUG: Failed to query portal messages")
			} else {
				log.Debug().Int("message_count", len(dbMessages)).Msg("DEBUG: Searching through portal messages")
				for _, msg := range dbMessages {
					if msg.Metadata != nil {
						if meta, ok := msg.Metadata.(*MessageMetadata); ok && meta != nil {
							log.Debug().
								Str("msg_id", string(msg.ID)).
								Str("meta_xmpp_id", meta.XMPPMessageID).
								Str("looking_for", m.replyID).
								Msg("DEBUG: Checking message metadata")
							if meta.XMPPMessageID == m.replyID {
								dbMsg = msg
								log.Debug().
									Str("matrix_event_id", string(msg.ID)).
									Msg("DEBUG: Found Matrix-originated message with matching XMPP ID in metadata")
								break
							}
						}
					}
					// Also check if the message ID itself matches (for XMPP-originated messages)
					if string(msg.ID) == m.replyID {
						dbMsg = msg
						log.Debug().
							Str("matrix_event_id", string(msg.ID)).
							Msg("DEBUG: Found XMPP-originated message with matching ID")
						break
					}
				}
			}
		} else {
			log.Debug().
				Str("matrix_event_id", string(dbMsg.ID)).
				Msg("DEBUG: Found message via direct lookup")
		}
		
		if dbMsg != nil {
			// Use the database message ID directly for ReplyTo
			// The bridge framework expects the database message ID format (e.g., "matrix:$event_id" or XMPP message ID)
			cm.ReplyTo = &networkid.MessageOptionalPartID{
				MessageID: dbMsg.ID,
			}
			log.Debug().
				Str("db_message_id", string(dbMsg.ID)).
				Msg("DEBUG: Set ReplyTo in ConvertedMessage using database message ID")
		} else {
			log.Debug().Msg("DEBUG: Could not find message with XMPP reply ID in database")
		}
	}
	
	return cm, nil
}

// captionFromBodyWithOOB returns body text suitable for use as a Matrix caption when the message
// also has an OOB attachment. It trims body and removes the OOB URL if it appears in the body
// (e.g. as fallback per XEP-0066), so the caption contains only the actual message text.
func captionFromBodyWithOOB(body, oobURL string) string {
	s := strings.TrimSpace(body)
	if s == "" || oobURL == "" {
		return s
	}
	// Remove OOB URL if it appears as a whole line or as the only content
	s = strings.TrimSpace(strings.ReplaceAll(s, oobURL, ""))
	// Collapse multiple newlines and trim each line, then rejoin to avoid leaving blank lines where URL was
	lines := strings.Split(s, "\n")
	var kept []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			kept = append(kept, line)
		}
	}
	return strings.TrimSpace(strings.Join(kept, "\n"))
}

// makeMediaPart returns a ConvertedMessage with one media part and Content.Info set for Element/Matrix clients.
// file may be nil for unencrypted rooms, but should be set for encrypted rooms.
// caption, when non-empty, is used as the Matrix message body (e.g. for m.image with text); otherwise fileName is used.
func makeMediaPart(fileName string, msgType event.MessageType, mxcURL id.ContentURIString, file *event.EncryptedFileInfo, mimeType string, size int, _ string, caption string) *bridgev2.ConvertedMessage {
	displayBody := strings.TrimSpace(caption)
	if displayBody == "" {
		displayBody = fileName
		if displayBody == "" {
			displayBody = "file"
		}
	}
	content := &event.MessageEventContent{
		MsgType:   msgType,
		Body:      displayBody,
		FileName:  fileName,
		URL:       mxcURL,
		File:      file,
		Info:      &event.FileInfo{MimeType: mimeType, Size: size},
	}
	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{{
			Type:    event.EventMessage,
			Content: content,
		}},
	}
}

// downloadHTTPSMedia fetches a URL and returns body, content-type, suggested filename, and Matrix msg type.
func downloadHTTPSMedia(ctx context.Context, url string) ([]byte, string, string, event.MessageType, error) {
	ctx, span := telemetry.StartChild(ctx, "http.oob_download", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		telemetry.Fail(span, err)
		return nil, "", "", event.MsgFile, err
	}
	if span.IsRecording() {
		span.SetAttributes(telemetry.HTTPAttrs(req)...)
	}
	client := &http.Client{Timeout: jabberMediaDownloadTimeout}
	resp, err := client.Do(req)
	if err != nil {
		telemetry.Fail(span, err)
		return nil, "", "", event.MsgFile, err
	}
	defer resp.Body.Close()
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode), attribute.Int64("http.response.body.size", resp.ContentLength))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", "", event.MsgFile, nil
	}
	body := io.LimitReader(resp.Body, maxJabberMediaDownloadBytes)
	data, err := io.ReadAll(body)
	if err != nil {
		telemetry.Fail(span, err)
		return nil, "", "", event.MsgFile, err
	}
	mimeType := resp.Header.Get("Content-Type")
	if idx := strings.Index(mimeType, ";"); idx >= 0 {
		mimeType = strings.TrimSpace(mimeType[:idx])
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	fileName := ""
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if i := strings.Index(cd, "filename="); i >= 0 {
			fileName = strings.Trim(cd[i+9:], " \"'")
		}
	}
	if fileName == "" {
		fileName = path.Base(resp.Request.URL.Path)
	}
	if fileName == "" || fileName == "." {
		fileName = "file"
	}
	msgType := mimeToMsgType(mimeType)
	return data, mimeType, fileName, msgType, nil
}

func mimeToMsgType(mime string) event.MessageType {
	switch {
	case strings.HasPrefix(mime, "image/"):
		return event.MsgImage
	case strings.HasPrefix(mime, "video/"):
		return event.MsgVideo
	case strings.HasPrefix(mime, "audio/"):
		return event.MsgAudio
	default:
		return event.MsgFile
	}
}

// isLikelyFileAttachment checks if a URL is likely a file attachment vs a web page link.
// Returns true if the URL should be treated as a file attachment.
func isLikelyFileAttachment(urlStr string, mimeType string, fileName string) bool {
	// Check if URL path has a file extension
	urlPath := path.Base(urlStr)
	if urlPath != "" && urlPath != "." && urlPath != "/" {
		// Common file extensions
		ext := strings.ToLower(path.Ext(urlPath))
		if ext != "" {
			// Check for common file extensions
			fileExts := []string{".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp",
				".mp4", ".mov", ".avi", ".mkv", ".webm", ".flv",
				".mp3", ".ogg", ".wav", ".flac", ".aac", ".m4a",
				".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
				".zip", ".tar", ".gz", ".rar", ".7z",
				".txt", ".md", ".csv", ".json", ".xml", ".yaml", ".yml",
				".exe", ".dmg", ".deb", ".rpm", ".apk", ".ipa"}
			for _, fileExt := range fileExts {
				if ext == fileExt {
					return true
				}
			}
		}
	}
	
	// Check if fileName has a file extension
	if fileName != "" && fileName != "." && fileName != "file" {
		ext := strings.ToLower(path.Ext(fileName))
		if ext != "" {
			return true
		}
	}
	
	// Check Content-Type - exclude web page types
	webPageTypes := []string{"text/html", "text/plain", "application/json", "application/xml",
		"text/xml", "application/xhtml+xml", "text/css", "application/javascript",
		"text/javascript", "application/x-javascript"}
	for _, webType := range webPageTypes {
		if mimeType == webType {
			return false
		}
	}
	
	// If Content-Type indicates it's a file type (image, video, audio, or application/octet-stream),
	// treat it as a file attachment
	if strings.HasPrefix(mimeType, "image/") ||
		strings.HasPrefix(mimeType, "video/") ||
		strings.HasPrefix(mimeType, "audio/") ||
		mimeType == "application/octet-stream" ||
		strings.HasPrefix(mimeType, "application/") && !strings.Contains(mimeType, "json") && !strings.Contains(mimeType, "xml") {
		return true
	}
	
	// Default: if we can't determine, don't treat as file attachment
	// This prevents web pages from being uploaded as attachments
	return false
}

