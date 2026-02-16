package connector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"

	"github.com/rs/zerolog"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
	"mellium.im/xmlstream"
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
	log := zerolog.Ctx(ctx)
	log.Debug().
		Str("input_body", body).
		Str("input_body_len", fmt.Sprintf("%d", len(body))).
		Str("input_oob_url", oobURL).
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
			log.Debug().Str("using_oob_url_as_body", oobURL).Msg("DEBUG: No caption, using OOB URL in body per XEP-0066")
		} else {
			// User provided caption - use it
			log.Debug().Str("using_user_caption", body).Msg("DEBUG: Using user caption in body")
		}
	} else if body == "" {
		// No OOB and no body - use space to ensure valid message
		finalBody = " "
		log.Debug().Msg("DEBUG: No OOB and no body, set body to single space")
	}
	
	// Generate XMPP message ID for this message (required for XEP-0461 replies)
	xmppMsgID, err := generateXMPPMessageID()
	if err != nil {
		return "", fmt.Errorf("failed to generate XMPP message ID: %w", err)
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
	var readers []xml.TokenReader
	readers = append(readers, bodyReader(finalBody))
	if oobURL != "" {
		log.Debug().
			Str("final_body", finalBody).
			Str("final_body_len", fmt.Sprintf("%d", len(finalBody))).
			Str("final_oob_url", oobURL).
			Msg("DEBUG: Adding OOB payload")
		readers = append(readers, oobReader(oobURL))
	}
	if replyID != "" {
		log.Debug().
			Str("reply_id", replyID).
			Str("reply_to_jid", replyToJID).
			Msg("DEBUG: Adding XEP-0461 reply element")
		readers = append(readers, replyReader(replyID, replyToJID))
	}
	var payload xml.TokenReader
	if len(readers) == 1 {
		payload = readers[0]
	} else {
		payload = xmlstream.MultiReader(readers...)
	}
	err = sess.Send(ctx, msg.Wrap(payload))
	if err != nil {
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
	log := zerolog.Ctx(ctx)
	log.Debug().
		Str("target_message_id", targetMessageID).
		Strs("emojis", emojis).
		Str("to_jid", to.String()).
		Str("msg_type", string(msgType)).
		Msg("DEBUG: sendXMPPReactions called")
	
	// Generate XMPP message ID for this reaction message
	xmppMsgID, err := generateXMPPMessageID()
	if err != nil {
		return "", fmt.Errorf("failed to generate XMPP message ID: %w", err)
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
	)
	
	err = sess.Send(ctx, msg.Wrap(payload))
	if err != nil {
		log.Debug().Err(err).Msg("DEBUG: sendXMPPReactions send failed")
		return "", err
	}
	log.Debug().Str("xmpp_msg_id", xmppMsgID).Msg("DEBUG: sendXMPPReactions send succeeded")
	return xmppMsgID, nil
}
