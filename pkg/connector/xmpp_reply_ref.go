package connector

import (
	"context"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// replyFallback is the body range a XEP-0428 <fallback for='urn:xmpp:reply:0'> marks as fallback
// text (the quoted original). Start/End are code-point offsets into the trimmed body, End exclusive.
// Valid is false when the stanza had no such fallback with explicit offsets.
type replyFallback struct {
	Start, End int
	Valid      bool
}

// stripReplyFallback removes the fallback range from body (code points, clamped) and trims the
// newline that separated it from the reply text. It returns body unchanged when the range is not
// valid or removing it would leave nothing.
func stripReplyFallback(body string, fb replyFallback) string {
	if !fb.Valid {
		return body
	}
	runes := []rune(body)
	start, end := fb.Start, fb.End
	if start < 0 {
		start = 0
	}
	if start > len(runes) {
		start = len(runes)
	}
	if end > len(runes) {
		end = len(runes)
	}
	if end <= start {
		return body
	}
	out := string(runes[:start]) + string(runes[end:])
	if start == 0 {
		out = strings.TrimPrefix(strings.TrimPrefix(out, "\r"), "\n")
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return body
	}
	return out
}

// resolveMessageRef maps the id carried by a XEP-0461 reply or XEP-0444 reaction to the first part of
// the bridged message in exactly this portal (never another room's). In a groupchat the id is the
// room-assigned stanza-id, so stanza_id is tried first; see refReplyMUC/refReplyDM. Returns nil when
// nothing matches.
func resolveMessageRef(ctx context.Context, db *database.Database, portal networkid.PortalKey, ref string, isMUC bool) (*database.Message, error) {
	kind := refReplyDM
	if isMUC {
		kind = refReplyMUC
	}
	rows, err := findMessagesByXMPPRef(ctx, db, portal, ref, kind)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

// refIsMUC decides, without any disco IQ, whether a reply/reaction in this event refers to a
// groupchat: the origin of the event first, then the stored room type, then the JID heuristic.
func (m *jabberRemoteMessage) refIsMUC(portal *bridgev2.Portal) bool {
	if m.groupchat {
		return true
	}
	if portal != nil {
		switch portal.RoomType {
		case database.RoomTypeGroupDM:
			return true
		case database.RoomTypeDM:
			return false
		}
	}
	return isMUCJID(string(m.portalKey.ID))
}

// replyRefForMessage returns the id a Matrix->XMPP reply or reaction must carry for target. In a
// groupchat that is the room-assigned stanza-id when known (roomRef=true); otherwise, and always
// for 1:1 chats, the legacy choice: the id we sent (Matrix-originated rows) or the row ID. ok is
// false when no id can be determined.
func replyRefForMessage(target *database.Message, isMUC bool) (ref string, roomRef bool, ok bool) {
	if target == nil {
		return "", false, false
	}
	if isMUC {
		if meta := targetMeta(target); meta != nil && meta.StanzaID != "" {
			return meta.StanzaID, true, true
		}
	}
	if strings.HasPrefix(string(target.ID), "matrix:") {
		if meta := targetMeta(target); meta != nil && meta.XMPPMessageID != "" {
			return meta.XMPPMessageID, false, true
		}
		return "", false, false
	}
	if target.ID == "" {
		return "", false, false
	}
	return string(target.ID), false, true
}
