// Call event notices via XEP-0353 Jingle Message Initiation (JMI).
//
// This does NOT implement Jingle (XEP-0166) session negotiation or media bridging.
// It only interprets the semantic JMI signalling messages that call-capable XMPP
// clients exchange, and injects informational m.notice messages into the Matrix
// room so the user notices incoming and (especially) missed calls.
//
//	<propose>            caller is ringing            -> "Incoming call"
//	<retract>            caller gave up before answer  -> "Missed call"
//	<accept>/<proceed>   answered on another device    -> "Call answered on another device"
//	<reject>             declined on another device    -> "Call declined on another device"
//	<finish>             call ended                    -> "Call ended"
//
// <propose>/<retract> are sent by the caller (the stanza carries the caller JID, so
// the portal is directly known). <accept>/<reject>/<proceed> are sent by one of our
// own resources and only carry the JMI session id, not the caller, so we remember
// sid -> {portal, caller ghost} from the <propose> to resolve later actions.

package connector

import (
	"context"
	"encoding/xml"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
)

const (
	jmiNS0      = "urn:xmpp:jingle-message:0"
	jmiNS1      = "urn:xmpp:jingle-message:1"
	jingleRTPNS = "urn:xmpp:jingle:apps:rtp:1"
)

// jingleMessageInfo holds the XEP-0353 action parsed out of a message stanza.
type jingleMessageInfo struct {
	action string // propose, retract, accept, proceed, reject, finish
	sid    string // JMI session id (the element's "id" attribute)
	media  string // "audio", "video", or "" (from the RTP <description>)
}

// callSession remembers where an incoming call belongs so later JMI actions
// (accept/reject/finish) that only carry the session id can target the right portal.
type callSession struct {
	portalKey networkid.PortalKey
	sender    bridgev2.EventSender // the caller's ghost
	media     string
	answered  bool
}

// elementInNamespace reports whether a start element belongs to ns, tolerating both
// resolved (Name.Space) and literal (xmlns attribute) forms, matching the mixed style
// used elsewhere in the inbound parsers.
func elementInNamespace(v xml.StartElement, ns string) bool {
	return v.Name.Space == ns || attrValue(v.Attr, "xmlns") == ns
}

// detectJingleMessage inspects a child start element and, if it is a JMI action or an
// RTP <description>, records it into *jmi (allocating if needed). Returns the (possibly
// new) pointer so callers can accumulate across tokens.
func detectJingleMessage(jmi *jingleMessageInfo, v xml.StartElement) *jingleMessageInfo {
	if v.Name.Local == "description" && elementInNamespace(v, jingleRTPNS) {
		if jmi == nil {
			jmi = &jingleMessageInfo{}
		}
		if m := attrValue(v.Attr, "media"); m == "video" || jmi.media == "" {
			jmi.media = m
		}
		return jmi
	}
	if !elementInNamespace(v, jmiNS0) && !elementInNamespace(v, jmiNS1) {
		return jmi
	}
	switch v.Name.Local {
	case "propose", "retract", "accept", "proceed", "reject", "finish":
		if jmi == nil {
			jmi = &jingleMessageInfo{}
		}
		jmi.action = v.Name.Local
		jmi.sid = attrValue(v.Attr, "id")
	}
	return jmi
}

// callKind returns a spoken descriptor ("video "/"voice "/"") for the media type.
func callKind(media string) string {
	switch media {
	case "video":
		return "video "
	case "audio":
		return "voice "
	default:
		return ""
	}
}

func bareJID(s string) string {
	if i := strings.Index(s, "/"); i >= 0 {
		return s[:i]
	}
	return s
}

func (x *xmppClient) storeCallSession(sid string, cs *callSession) {
	x.callSessionsMu.Lock()
	if x.callSessions == nil {
		x.callSessions = make(map[string]*callSession)
	}
	x.callSessions[sid] = cs
	x.callSessionsMu.Unlock()
}

func (x *xmppClient) getCallSession(sid string) *callSession {
	x.callSessionsMu.Lock()
	defer x.callSessionsMu.Unlock()
	return x.callSessions[sid]
}

// takeCallSession returns and removes the session for sid (nil if absent).
func (x *xmppClient) takeCallSession(sid string) *callSession {
	x.callSessionsMu.Lock()
	defer x.callSessionsMu.Unlock()
	cs := x.callSessions[sid]
	delete(x.callSessions, sid)
	return cs
}

func (x *xmppClient) markCallAnswered(sid string) {
	x.callSessionsMu.Lock()
	if cs := x.callSessions[sid]; cs != nil {
		cs.answered = true
	}
	x.callSessionsMu.Unlock()
}

// handleJingleMessageInitiation turns a parsed JMI action into a Matrix notice.
// fromBare is the bare JID the stanza came from; fromSelf is true when it originated
// from one of our own resources (e.g. an <accept> we sent from another client).
func (x *xmppClient) handleJingleMessageInitiation(ctx context.Context, jmi jingleMessageInfo, fromBare string, fromSelf bool) {
	if x.login == nil || jmi.sid == "" {
		return
	}
	if !fromSelf && x.conn != nil {
		if e := x.conn.extension(); e != nil && e.suppressCallNotice(x.loginID, fromBare) {
			return
		}
	}
	switch jmi.action {
	case "propose":
		if fromSelf {
			// Outgoing call placed from another of our devices; not surfaced for now.
			return
		}
		portalKey := networkid.PortalKey{ID: networkid.PortalID(fromBare), Receiver: x.loginID}
		sender := x.makeEventSender(fromBare)
		x.storeCallSession(jmi.sid, &callSession{portalKey: portalKey, sender: sender, media: jmi.media})
		x.maybeTriggerBackfillOnNewPortal(ctx, portalKey)
		emoji := "📞"
		if jmi.media == "video" {
			emoji = "📹"
		}
		x.emitCallNotice(ctx, portalKey, sender, jmi.sid, "propose", emoji+" Incoming "+callKind(jmi.media)+"call")
	case "retract":
		cs := x.takeCallSession(jmi.sid)
		portalKey, sender, ok := x.resolveCallTarget(cs, fromBare)
		if !ok {
			return
		}
		if cs != nil && cs.answered {
			x.emitCallNotice(ctx, portalKey, sender, jmi.sid, "finish", "📴 Call ended")
		} else {
			x.emitCallNotice(ctx, portalKey, sender, jmi.sid, "retract", "📞 Missed call")
		}
	case "accept", "proceed":
		cs := x.getCallSession(jmi.sid)
		if cs == nil {
			return // unknown session (never saw the propose) — can't target a portal
		}
		x.markCallAnswered(jmi.sid)
		x.emitCallNotice(ctx, cs.portalKey, x.makeEventSender(string(x.loginID)), jmi.sid, "accept", "✅ Call answered on another device")
	case "reject":
		cs := x.takeCallSession(jmi.sid)
		if cs == nil {
			return
		}
		x.emitCallNotice(ctx, cs.portalKey, x.makeEventSender(string(x.loginID)), jmi.sid, "reject", "🚫 Call declined on another device")
	case "finish":
		cs := x.takeCallSession(jmi.sid)
		portalKey, sender, ok := x.resolveCallTarget(cs, fromBare)
		if !ok {
			return
		}
		x.emitCallNotice(ctx, portalKey, sender, jmi.sid, "finish", "📴 Call ended")
	}
}

// resolveCallTarget picks the portal/sender for a JMI action, preferring the remembered
// session and falling back to the stanza's from JID (valid for caller-sent actions like
// retract/finish). ok is false when neither yields a usable target.
func (x *xmppClient) resolveCallTarget(cs *callSession, fromBare string) (networkid.PortalKey, bridgev2.EventSender, bool) {
	if cs != nil {
		return cs.portalKey, cs.sender, true
	}
	if fromBare == "" || bareJID(string(x.loginID)) == fromBare {
		return networkid.PortalKey{}, bridgev2.EventSender{}, false
	}
	portalKey := networkid.PortalKey{ID: networkid.PortalID(fromBare), Receiver: x.loginID}
	return portalKey, x.makeEventSender(fromBare), true
}

// emitCallNotice queues an m.notice into the portal. The deterministic ID
// ("jmi:<sid>:<action>") deduplicates the same action arriving twice (e.g. once
// directly and once as a carbon copy).
func (x *xmppClient) emitCallNotice(ctx context.Context, portalKey networkid.PortalKey, sender bridgev2.EventSender, sid, action, text string) {
	cm := &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
		Type:    event.EventMessage,
		Content: &event.MessageEventContent{MsgType: event.MsgNotice, Body: text},
	}}}
	x.queueRemoteEvent(ctx, &simplevent.PreConvertedMessage{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventMessage,
			PortalKey:    portalKey,
			Sender:       sender,
			CreatePortal: true,
			Timestamp:    time.Now(),
		},
		Data: cm,
		ID:   networkid.MessageID("jmi:" + sid + ":" + action),
	})
	x.logFor(ctx).Info().Str("sid", sid).Str("action", action).Str("portal", string(portalKey.ID)).Str("text", text).Msg("Queued Jingle call notice")
}
