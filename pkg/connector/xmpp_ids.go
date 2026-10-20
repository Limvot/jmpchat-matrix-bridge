package connector

import (
	"encoding/xml"
	"strings"
)

// Stable-ID elements (XEP-0359 unique and stable stanza IDs, XEP-0421 occupant IDs). They are
// captured so that later features (retractions, moderation) can map an XMPP reference to the
// bridgev2 message row; see MessageMetadata.
const (
	sidNS        = "urn:xmpp:sid:0"
	occupantIDNS = "urn:xmpp:occupant-id:0"

	// maxStanzaIDRefs bounds how many <stanza-id> elements one stanza can make us remember, and
	// maxStableIDLen bounds a single id; both only limit what a hostile sender can make us hold.
	maxStanzaIDRefs = 8
	maxStableIDLen  = 1024
)

// stanzaIDRef is one <stanza-id by=... id=.../> child.
type stanzaIDRef struct {
	By string
	ID string
}

// messageIDs holds the XEP-0359/0421 identifiers found as DIRECT children of a <message>.
// Nothing here is trusted until stanzaIDBy picks an entry whose `by` is an entity we trust.
type messageIDs struct {
	StanzaIDs  []stanzaIDRef
	OriginID   string
	OccupantID string // XEP-0421 occupant-id; only meaningful for groupchat
}

// observe inspects a start element that is a direct child of the message (the caller checks the
// depth). Matching is on the resolved namespace, not on a literal xmlns attribute, so children that
// inherit their namespace (forwarded carbons/MAM) are still found.
func (m *messageIDs) observe(se xml.StartElement) {
	switch {
	case se.Name.Space == sidNS && se.Name.Local == "stanza-id":
		by, id := attrValue(se.Attr, "by"), attrValue(se.Attr, "id")
		if by != "" && id != "" && len(id) <= maxStableIDLen && len(m.StanzaIDs) < maxStanzaIDRefs {
			m.StanzaIDs = append(m.StanzaIDs, stanzaIDRef{By: by, ID: id})
		}
	case se.Name.Space == sidNS && se.Name.Local == "origin-id":
		if id := attrValue(se.Attr, "id"); m.OriginID == "" && len(id) <= maxStableIDLen {
			m.OriginID = id
		}
	case se.Name.Space == occupantIDNS && se.Name.Local == "occupant-id":
		if id := attrValue(se.Attr, "id"); m.OccupantID == "" && len(id) <= maxStableIDLen {
			m.OccupantID = id
		}
	}
}

// stanzaIDBy returns the stanza-id assigned by the given entity (bare JID, compared
// case-insensitively), or "". A stanza-id from any other `by` must not be believed: that is the room
// for groupchat and our own account for 1:1 chats.
func (m messageIDs) stanzaIDBy(by string) string {
	by = bareJID(by)
	if by == "" {
		return ""
	}
	for _, s := range m.StanzaIDs {
		if strings.EqualFold(s.By, by) {
			return s.ID
		}
	}
	return ""
}
