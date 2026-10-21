package connector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

const (
	oobNS       = "jabber:x:oob"
	replyNS     = "urn:xmpp:reply:0"
	reactionsNS = "urn:xmpp:reactions:0"
	hintsNS     = "urn:xmpp:hints"
)

// bodyReader returns an xml.TokenReader that emits a single <body>content</body> element.
func bodyReader(body string) xml.TokenReader {
	start := xml.StartElement{Name: xml.Name{Local: "body"}}
	end := xml.EndElement{Name: xml.Name{Local: "body"}}
	return xmlstream.MultiReader(
		xmlstream.Token(start),
		xmlstream.Token(xml.CharData(body)),
		xmlstream.Token(end),
	)
}

// oobReader returns an xml.TokenReader for XEP-0066 Out-of-Band: <x xmlns='jabber:x:oob'><url>...</url></x>.
// Clients that support OOB display the URL as inline media.
func oobReader(url string) xml.TokenReader {
	xStart := xml.StartElement{
		Name: xml.Name{Space: oobNS, Local: "x"},
		Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: oobNS}},
	}
	xEnd := xml.EndElement{Name: xml.Name{Space: oobNS, Local: "x"}}
	urlStart := xml.StartElement{Name: xml.Name{Space: oobNS, Local: "url"}}
	urlEnd := xml.EndElement{Name: xml.Name{Space: oobNS, Local: "url"}}
	return xmlstream.MultiReader(
		xmlstream.Token(xStart),
		xmlstream.Token(urlStart),
		xmlstream.Token(xml.CharData(url)),
		xmlstream.Token(urlEnd),
		xmlstream.Token(xEnd),
	)
}

// replyReader returns an xml.TokenReader for XEP-0461 Message Replies: <reply xmlns='urn:xmpp:reply:0' id='...' to='...'/>.
// The 'to' attribute should contain the full JID of the original message's author (or bare JID in 1:1 chat).
// The 'id' attribute must contain the ID of the referenced message.
func replyReader(replyID string, replyToJID string) xml.TokenReader {
	attrs := []xml.Attr{
		{Name: xml.Name{Local: "xmlns"}, Value: replyNS},
		{Name: xml.Name{Local: "id"}, Value: replyID},
	}
	if replyToJID != "" {
		attrs = append(attrs, xml.Attr{Name: xml.Name{Local: "to"}, Value: replyToJID})
	}
	start := xml.StartElement{
		Name: xml.Name{Space: replyNS, Local: "reply"},
		Attr: attrs,
	}
	end := xml.EndElement{Name: xml.Name{Space: replyNS, Local: "reply"}}
	return xmlstream.MultiReader(
		xmlstream.Token(start),
		xmlstream.Token(end),
	)
}

// reactionsReader returns an xml.TokenReader for XEP-0444 Message Reactions: <reactions xmlns='urn:xmpp:reactions:0' id='...'><reaction>emoji</reaction></reactions>.
// The 'id' attribute must contain the ID of the message being reacted to.
// emojis is a slice of emoji strings to include in the reaction.
func reactionsReader(targetMessageID string, emojis []string) xml.TokenReader {
	attrs := []xml.Attr{
		{Name: xml.Name{Local: "xmlns"}, Value: reactionsNS},
		{Name: xml.Name{Local: "id"}, Value: targetMessageID},
	}
	reactionsStart := xml.StartElement{
		Name: xml.Name{Space: reactionsNS, Local: "reactions"},
		Attr: attrs,
	}
	reactionsEnd := xml.EndElement{Name: xml.Name{Space: reactionsNS, Local: "reactions"}}

	var readers []xml.TokenReader
	readers = append(readers, xmlstream.Token(reactionsStart))
	for _, emoji := range emojis {
		reactionStart := xml.StartElement{Name: xml.Name{Space: reactionsNS, Local: "reaction"}}
		reactionEnd := xml.EndElement{Name: xml.Name{Space: reactionsNS, Local: "reaction"}}
		readers = append(readers,
			xmlstream.Token(reactionStart),
			xmlstream.Token(xml.CharData(emoji)),
			xmlstream.Token(reactionEnd),
		)
	}
	readers = append(readers, xmlstream.Token(reactionsEnd))
	return xmlstream.MultiReader(readers...)
}

// originIDReader returns the XEP-0359 <origin-id id='...'/> element for an outgoing stanza. The id
// equals the stanza `id` attribute so receivers can match it either way (XEP-0424 retractions,
// MUC echo correlation). It deliberately does not use stanza.AddOriginID, which would generate a
// different random id.
func originIDReader(id string) xml.TokenReader {
	return stanza.OriginID{ID: id}.TokenReader()
}

// storeHintReader returns an xml.TokenReader for XEP-0334 Message Processing Hints: <store xmlns='urn:xmpp:hints'/>.
// This hint indicates that the message should be stored in the message archive.
func storeHintReader() xml.TokenReader {
	start := xml.StartElement{
		Name: xml.Name{Space: hintsNS, Local: "store"},
		Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: hintsNS}},
	}
	end := xml.EndElement{Name: xml.Name{Space: hintsNS, Local: "store"}}
	return xmlstream.MultiReader(
		xmlstream.Token(start),
		xmlstream.Token(end),
	)
}

// sendXMPPMessage sends a chat message to the given JID using the session.
// It is safe to call concurrently with Serve.
// Returns the XMPP message ID that was assigned to the message.
func sendXMPPMessage(ctx context.Context, sess *xmpp.Session, to jid.JID, body string) (string, error) {
	return sendXMPPMessageWithOOB(ctx, sess, to, body, "", "", "", "")
}

// generateXMPPMessageID generates a unique XMPP message ID for outgoing messages.
// Format: UUID v4-like hex string (32 hex chars = 16 bytes of randomness).
// This ensures uniqueness and allows replies to Matrix-originated messages via XEP-0461.
func generateXMPPMessageID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate XMPP message ID: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// sendXMPPMessageWithOOB sends a chat message with optional XEP-0066 Out-of-Band media URL and optional XEP-0461 reply.
// msgType is the XMPP message type (e.g. "chat" for DM, "groupchat" for MUC); if empty, "normal" is used.
// If oobURL is non-empty, the message includes <x xmlns='jabber:x:oob'><url>oobURL</url></x>
// so clients can display the media inline. According to XEP-0066, the body should contain the URL
// as a fallback for clients that don't support OOB. If body (caption) is provided, use it;
// otherwise use the OOB URL in the body.
// If replyID and replyToJID are non-empty, the message includes <reply xmlns='urn:xmpp:reply:0' id='...' to='...'/>
// to indicate this is a reply to a previous message (XEP-0461).
// Returns the XMPP message ID that was assigned to the message (for storage in MessageMetadata).
func sendXMPPMessageWithOOB(ctx context.Context, sess *xmpp.Session, to jid.JID, body string, oobURL string, replyID string, replyToJID string, msgType stanza.MessageType) (string, error) {
	return sendXMPPMessageWithOOBID(ctx, sess, to, body, oobURL, replyID, replyToJID, msgType, "")
}

// sendXMPPMessageWithOOBID is sendXMPPMessageWithOOB with a caller-chosen stanza id (empty: generate).
// MUC sends pass an id they registered in the echo table before the send.
func sendXMPPMessageWithOOBID(ctx context.Context, sess *xmpp.Session, to jid.JID, body string, oobURL string, replyID string, replyToJID string, msgType stanza.MessageType, xmppMsgID string) (string, error) {
	return sendXMPPOut(ctx, sess, outMessage{To: to, Type: msgType, ID: xmppMsgID, Body: body, OOBURL: oobURL, ReplyID: replyID, ReplyTo: replyToJID})
}

// outMessage describes one outbound chat/groupchat message with a body. It is the single builder for
// text, media (OOB), replies and XEP-0308 corrections; later features (chat markers, receipts) add
// their flags here instead of growing the sendXMPP* helpers.
type outMessage struct {
	To      jid.JID
	Type    stanza.MessageType // "" -> normal
	ID      string             // stanza id, also the origin-id; "" -> generate
	Body    string
	OOBURL  string // XEP-0066 media URL
	ReplyID string // XEP-0461 reply target id
	ReplyTo string // XEP-0461 reply `to` (author JID)
	Replace string // XEP-0308 correction target id ("" -> not a correction)
	// Signalling elements (see withSignalFlags): XEP-0085 <active/>, XEP-0333 <markable/>, XEP-0184 <request/>.
	Active         bool
	Markable       bool
	RequestReceipt bool
}

// replaceReader returns <replace xmlns='urn:xmpp:message-correct:0' id='...'/> (XEP-0308).
func replaceReader(targetID string) xml.TokenReader {
	start := xml.StartElement{
		Name: xml.Name{Space: correctNS, Local: "replace"},
		Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: correctNS}, {Name: xml.Name{Local: "id"}, Value: targetID}},
	}
	return xmlstream.MultiReader(xmlstream.Token(start), xmlstream.Token(xml.EndElement{Name: start.Name}))
}

// outPayload is the child content of the message stanza for m: the body (finalBody, already resolved),
// the stanza's own origin-id, then the optional OOB, reply and correction (replace + store hint) elements and the signalling flags.
func outPayload(m outMessage, finalBody, stanzaID string) xml.TokenReader {
	readers := []xml.TokenReader{bodyReader(finalBody), originIDReader(stanzaID)}
	if m.OOBURL != "" {
		readers = append(readers, oobReader(m.OOBURL))
	}
	if m.ReplyID != "" {
		readers = append(readers, replyReader(m.ReplyID, m.ReplyTo))
	}
	if m.Replace != "" {
		readers = append(readers, replaceReader(m.Replace), storeHintReader())
	}
	if m.Active {
		readers = append(readers, emptyElement(chatStatesNS, "active"))
	}
	if m.Markable {
		readers = append(readers, emptyElement(markersNS, "markable"))
	}
	if m.RequestReceipt {
		readers = append(readers, emptyElement(receiptsNS, "request"))
	}
	return xmlstream.MultiReader(readers...)
}

// sendXMPPOut builds and sends m and returns the stanza id. A correction (Replace set) carries
// <body>, <origin-id>, <replace> and a store hint. Span attributes and logs carry ids and lengths only.
func sendXMPPOut(ctx context.Context, sess *xmpp.Session, m outMessage) (string, error) {
	to, body, oobURL, replyID, msgType, xmppMsgID := m.To, m.Body, m.OOBURL, m.ReplyID, m.Type, m.ID
	ctx, span := telemetry.StartChild(ctx, "xmpp.send_message", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("xmpp.peer.jid", to.String()),
			attribute.String("xmpp.message.type", string(msgType)),
			attribute.Bool("xmpp.has_oob", oobURL != ""),
			attribute.Bool("xmpp.has_reply", replyID != ""),
			attribute.Bool("xmpp.reaction", false),
			attribute.Bool("xmpp.correction", m.Replace != ""),
		)
	}
	log := zerolog.Ctx(ctx)
	log.Debug().
		Str("input_body_len", fmt.Sprintf("%d", len(body))).
		Str("input_oob_url_len", fmt.Sprintf("%d", len(oobURL))).
		Str("to_jid", to.String()).
		Str("msg_type", string(msgType)).
		Msg("DEBUG: sendXMPPMessageWithOOB called")

	// According to XEP-0066, when OOB is present, body should contain URL as fallback
	// Use user caption if provided, otherwise use OOB URL in body
	finalBody := body
	if oobURL != "" {
		if body == "" {
			// No user caption - use OOB URL in body as fallback per XEP-0066
			finalBody = oobURL
			log.Debug().Msg("DEBUG: No caption, using OOB URL in body per XEP-0066")
		} else {
			// User provided caption - use it
			log.Debug().Msg("DEBUG: Using user caption in body")
		}
	} else if body == "" {
		// No OOB and no body - use space to ensure valid message
		finalBody = " "
		log.Debug().Msg("DEBUG: No OOB and no body, set body to single space")
	}

	// Generate XMPP message ID for this message (required for XEP-0461 replies)
	var err error
	if xmppMsgID == "" {
		xmppMsgID, err = generateXMPPMessageID()
		if err != nil {
			telemetry.Fail(span, err)
			return "", fmt.Errorf("failed to generate XMPP message ID: %w", err)
		}
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.stanza.id", xmppMsgID))
	}
	log.Debug().Str("xmpp_msg_id", xmppMsgID).Msg("DEBUG: Generated XMPP message ID")

	if msgType == "" {
		msgType = stanza.NormalMessage
	}
	msg := stanza.Message{
		To:   to,
		Type: msgType,
		ID:   xmppMsgID, // Set the message ID so XEP-0461 replies can reference it
	}
	log.Debug().
		Int("final_body_len", len(finalBody)).
		Bool("has_oob", oobURL != "").
		Str("reply_id", replyID).
		Bool("correction", m.Replace != "").
		Msg("DEBUG: Building outbound message payload")
	err = sess.Send(ctx, msg.Wrap(outPayload(m, finalBody, xmppMsgID)))
	if err != nil {
		telemetry.Fail(span, err)
		log.Debug().Err(err).Msg("DEBUG: sendXMPPMessageWithOOB send failed")
		return "", err
	}
	log.Debug().Str("xmpp_msg_id", xmppMsgID).Msg("DEBUG: sendXMPPMessageWithOOB send succeeded")
	return xmppMsgID, nil
}

// sendXMPPReaction sends a XEP-0444 Message Reactions stanza to the given JID.
// targetMessageID is the XMPP message ID of the message being reacted to.
// emoji is the emoji character for the reaction.
// msgType should be groupchat for MUC rooms so the server accepts the message.
// Returns the XMPP message ID that was assigned to the reaction message.
func sendXMPPReaction(ctx context.Context, sess *xmpp.Session, to jid.JID, targetMessageID string, emoji string, msgType stanza.MessageType) (string, error) {
	return sendXMPPReactions(ctx, sess, to, targetMessageID, []string{emoji}, msgType)
}

// sendXMPPReactions sends a XEP-0444 Message Reactions stanza to the given JID.
// targetMessageID is the XMPP message ID of the message being reacted to.
// emojis is a slice of emoji characters for all active reactions (per XEP-0444, updates must include all reactions).
// msgType should be groupchat for MUC rooms so the server accepts the message.
// Returns the XMPP message ID that was assigned to the reaction message.
func sendXMPPReactions(ctx context.Context, sess *xmpp.Session, to jid.JID, targetMessageID string, emojis []string, msgType stanza.MessageType) (string, error) {
	return sendXMPPReactionsID(ctx, sess, to, targetMessageID, emojis, msgType, "")
}

// sendXMPPReactionsID is sendXMPPReactions with a caller-chosen stanza id (empty: generate). MUC sends
// pass an id they registered in the echo table before the send.
func sendXMPPReactionsID(ctx context.Context, sess *xmpp.Session, to jid.JID, targetMessageID string, emojis []string, msgType stanza.MessageType, xmppMsgID string) (string, error) {
	ctx, span := telemetry.StartChild(ctx, "xmpp.send_message", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("xmpp.peer.jid", to.String()),
			attribute.String("xmpp.message.type", string(msgType)),
			attribute.Bool("xmpp.reaction", true),
		)
	}
	log := zerolog.Ctx(ctx)
	log.Debug().
		Str("target_message_id", targetMessageID).
		Strs("emojis", emojis).
		Str("to_jid", to.String()).
		Str("msg_type", string(msgType)).
		Msg("DEBUG: sendXMPPReactions called")

	// Generate XMPP message ID for this reaction message
	if xmppMsgID == "" {
		var err error
		xmppMsgID, err = generateXMPPMessageID()
		if err != nil {
			telemetry.Fail(span, err)
			return "", fmt.Errorf("failed to generate XMPP message ID: %w", err)
		}
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.stanza.id", xmppMsgID))
	}
	log.Debug().Str("xmpp_msg_id", xmppMsgID).Msg("DEBUG: Generated XMPP message ID for reaction")
	if msgType == "" {
		msgType = stanza.NormalMessage
	}
	msg := stanza.Message{
		To:   to,
		Type: msgType,
		ID:   xmppMsgID,
	}

	// Build the payload: <reactions> element with all <reaction> children, plus <store> hint
	payload := xmlstream.MultiReader(
		reactionsReader(targetMessageID, emojis),
		storeHintReader(),
		originIDReader(xmppMsgID),
	)

	err := sess.Send(ctx, msg.Wrap(payload))
	if err != nil {
		telemetry.Fail(span, err)
		log.Debug().Err(err).Msg("DEBUG: sendXMPPReactions send failed")
		return "", err
	}
	log.Debug().Str("xmpp_msg_id", xmppMsgID).Msg("DEBUG: sendXMPPReactions send succeeded")
	return xmppMsgID, nil
}

// retractionFallbackBody is the generic XEP-0428 fallback text of an outgoing retraction. It never
// contains the original text.
const retractionFallbackBody = "A previous message was retracted by its sender."

// retractReader returns <retract xmlns='urn:xmpp:message-retract:1' id='...'/> (XEP-0424).
func retractReader(targetRef string) xml.TokenReader {
	start := xml.StartElement{
		Name: xml.Name{Space: retractNS1, Local: "retract"},
		Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: retractNS1}, {Name: xml.Name{Local: "id"}, Value: targetRef}},
	}
	return xmlstream.MultiReader(xmlstream.Token(start), xmlstream.Token(xml.EndElement{Name: start.Name}))
}

// fallbackReader returns <fallback xmlns='urn:xmpp:fallback:0' for='ns'/> (XEP-0428).
func fallbackReader(forNS string) xml.TokenReader {
	start := xml.StartElement{
		Name: xml.Name{Space: fallbackNS, Local: "fallback"},
		Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: fallbackNS}, {Name: xml.Name{Local: "for"}, Value: forNS}},
	}
	return xmlstream.MultiReader(xmlstream.Token(start), xmlstream.Token(xml.EndElement{Name: start.Name}))
}

// retractionPayload is the child content of an outgoing retraction message: the retract element, the
// XEP-0428 fallback marker with its generic body, the store hint and the stanza's own origin-id.
func retractionPayload(targetRef, stanzaID string) xml.TokenReader {
	return xmlstream.MultiReader(
		retractReader(targetRef),
		fallbackReader(retractNS1),
		bodyReader(retractionFallbackBody),
		storeHintReader(),
		originIDReader(stanzaID),
	)
}

// sendXMPPRetraction sends an XEP-0424 retraction of targetRef (1:1: the original's origin-id/id;
// groupchat: the room-assigned stanza-id) and returns the retraction stanza's own id. Span attribute
// xmpp.retraction=true; ids only, never message content.
func sendXMPPRetraction(ctx context.Context, sess *xmpp.Session, to jid.JID, targetRef string, msgType stanza.MessageType) (string, error) {
	return sendXMPPRetractionID(ctx, sess, to, targetRef, msgType, "")
}

// sendXMPPRetractionID is sendXMPPRetraction with a caller-chosen stanza id (empty: generate).
func sendXMPPRetractionID(ctx context.Context, sess *xmpp.Session, to jid.JID, targetRef string, msgType stanza.MessageType, stanzaID string) (string, error) {
	ctx, span := telemetry.StartChild(ctx, "xmpp.send_message", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("xmpp.peer.jid", to.String()),
			attribute.String("xmpp.message.type", string(msgType)),
			attribute.Bool("xmpp.retraction", true),
		)
	}
	if stanzaID == "" {
		var err error
		stanzaID, err = generateXMPPMessageID()
		if err != nil {
			telemetry.Fail(span, err)
			return "", err
		}
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.stanza.id", stanzaID))
	}
	if msgType == "" {
		msgType = stanza.NormalMessage
	}
	msg := stanza.Message{To: to, Type: msgType, ID: stanzaID}
	if err := sess.Send(ctx, msg.Wrap(retractionPayload(targetRef, stanzaID))); err != nil {
		telemetry.Fail(span, err)
		return "", err
	}
	return stanzaID, nil
}
