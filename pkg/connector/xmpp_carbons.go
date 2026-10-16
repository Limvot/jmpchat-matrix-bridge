// Message Carbons (XEP-0280) support: receive copies of messages sent/received
// on other resources so the bridge can show them in the Matrix room immediately.

package connector

import (
	"context"
	"encoding/xml"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"mellium.im/xmlstream"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const carbonsNS = "urn:xmpp:carbons:2"

// parseInnerMessageFromCarbon reads the inner <message> from a carbon copy and returns
// from, to, type, id, body. Per XEP-0280/0297, the stream is <received><forwarded><message>...,
// so we capture from/to/id from the first <message> element we encounter (at any depth).
func parseInnerMessageFromCarbon(t xmlstream.TokenReadEncoder) (from, to, typeAttr, id, body string, jmi *jingleMessageInfo, err error) {
	depth := 0
	for {
		tok, err := t.Token()
		if err != nil {
			return from, to, typeAttr, id, body, jmi, err
		}
		switch v := tok.(type) {
		case xml.StartElement:
			depth++
			jmi = detectJingleMessage(jmi, v)
			if v.Name.Local == "message" && from == "" {
				from = attrValue(v.Attr, "from")
				to = attrValue(v.Attr, "to")
				typeAttr = attrValue(v.Attr, "type")
				id = attrValue(v.Attr, "id")
			}
			if v.Name.Local == "body" {
				inner, ierr := t.Token()
				if ierr != nil {
					continue
				}
				if cd, ok := inner.(xml.CharData); ok {
					body = strings.TrimSpace(string(cd))
				}
			}
		case xml.EndElement:
			depth--
			if depth == 0 {
				return from, to, typeAttr, id, body, jmi, nil
			}
		}
	}
}

// consumeUntilEndElement reads tokens until the closing </localName> (we're already inside the element).
func consumeUntilEndElement(t xmlstream.TokenReadEncoder, localName string) {
	for {
		tok, err := t.Token()
		if err != nil {
			return
		}
		if ee, ok := tok.(xml.EndElement); ok && ee.Name.Local == localName {
			return
		}
	}
}

// handleCarbonSent processes a <sent> carbon: message we sent from another client.
// Inner message: from=our JID, to=contact. We queue it so it appears in the portal as "from me".
func (x *xmppClient) handleCarbonSent(t xmlstream.TokenReadEncoder) error {
	if x.login == nil {
		consumeUntilEndElement(t, "sent")
		return nil
	}
	from, to, _, id, body, jmi, err := parseInnerMessageFromCarbon(t)
	consumeUntilEndElement(t, "sent")
	if err != nil {
		x.log.Debug().Err(err).Msg("Failed to parse carbon <sent> inner message")
		return nil
	}
	// XEP-0353 action we performed from another of our devices (accept/reject/proceed).
	if jmi != nil {
		ctx, span := startReceiveSpan("carbon_sent", "", from, "", id, false, false, false)
		defer span.End()
		if span.IsRecording() {
			span.SetAttributes(attribute.String("xmpp.jingle.action", jmi.action))
		}
		x.handleJingleMessageInitiation(ctx, *jmi, bareJID(from), true)
		return nil
	}
	_ = from
	toBare := to
	if idx := strings.Index(toBare, "/"); idx >= 0 {
		toBare = toBare[:idx]
	}
	if toBare == "" {
		return nil
	}
	// Skip carbon copies of chat state notifications (composing, paused, etc.) - no body.
	if body == "" {
		return nil
	}
	ctx, span := startReceiveSpan("carbon_sent", "", from, toBare, id, false, false, false)
	defer span.End()
	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(strings.ToLower(toBare)),
		Receiver: x.loginID,
	}
	x.addPortalIDForMAMCatchUp(string(portalKey.ID))
	msgID := networkid.MessageID(id)
	if msgID == "" {
		msgID = networkid.MessageID("xmp:" + time.Now().Format("20060102150405.000"))
	}
	sender := x.makeEventSender(string(x.loginID)) // "from me" for puppeting
	x.maybeTriggerBackfillOnNewPortal(ctx, portalKey)
	x.logFor(ctx).Debug().
		Str("to", toBare).
		Str("body_preview", truncate(body, 60)).
		Msg("Carbon <sent>: queuing message sent from another client")
	x.queueRemoteEvent(ctx, &jabberRemoteMessage{
		portalKey: portalKey,
		sender:    sender,
		id:        msgID,
		body:      body,
		timestamp: time.Now(),
		x:         x,
	})
	return nil
}

// handleCarbonReceived processes a <received> carbon: message we received on another client.
// Inner message: from=contact, to=our JID. Queue so it appears in the portal (may dedupe if we already got it).
func (x *xmppClient) handleCarbonReceived(t xmlstream.TokenReadEncoder) error {
	if x.login == nil {
		consumeUntilEndElement(t, "received")
		return nil
	}
	from, _, _, id, body, jmi, err := parseInnerMessageFromCarbon(t)
	consumeUntilEndElement(t, "received")
	if err != nil {
		x.log.Debug().Err(err).Msg("Failed to parse carbon <received> inner message")
		return nil
	}
	// XEP-0353 call signalling received on another device (e.g. propose/retract from the caller).
	if jmi != nil {
		ctx, span := startReceiveSpan("carbon_received", "", from, "", id, false, false, false)
		defer span.End()
		if span.IsRecording() {
			span.SetAttributes(attribute.String("xmpp.jingle.action", jmi.action))
		}
		x.handleJingleMessageInitiation(ctx, *jmi, bareJID(from), bareJID(from) == bareJID(string(x.loginID)))
		return nil
	}
	fromBare := from
	if idx := strings.Index(fromBare, "/"); idx >= 0 {
		fromBare = fromBare[:idx]
	}
	if fromBare == "" {
		return nil
	}
	// Skip carbon copies of chat state notifications (composing, paused, etc.) - no body.
	if body == "" {
		return nil
	}
	ctx, span := startReceiveSpan("carbon_received", "", from, fromBare, id, false, false, false)
	defer span.End()
	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(strings.ToLower(fromBare)),
		Receiver: x.loginID,
	}
	x.addPortalIDForMAMCatchUp(string(portalKey.ID))
	msgID := networkid.MessageID(id)
	if msgID == "" {
		msgID = networkid.MessageID("xmp:" + time.Now().Format("20060102150405.000"))
	}
	sender := x.makeEventSender(fromBare)
	x.maybeTriggerBackfillOnNewPortal(ctx, portalKey)
	zerolog.Ctx(context.Background()).Debug().
		Str("from", fromBare).
		Str("body_preview", truncate(body, 60)).
		Msg("Carbon <received>: queuing message received on another client")
	x.queueRemoteEvent(ctx, &jabberRemoteMessage{
		portalKey: portalKey,
		sender:    sender,
		id:        msgID,
		body:      body,
		timestamp: time.Now(),
		x:         x,
	})
	return nil
}
