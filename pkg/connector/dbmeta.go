package connector

import (
	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/bridgev2/database"
)

// PortalMetadata stores portal-specific metadata for resync tracking.
type PortalMetadata struct {
	// Lazy resync tracking - resync portal if LastSync is older than 24 hours
	LastSync jsontime.Unix `json:"last_sync,omitempty"`
	// MUCPreferredNick is the preferred MUC nickname for this room (room-specific; used by JoinMUC and 409 retry).
	MUCPreferredNick string `json:"muc_preferred_nick,omitempty"`
}

// GhostMetadata stores ghost user metadata for profile refresh tracking.
type GhostMetadata struct {
	ProfileFetchedAt jsontime.UnixMilli `json:"profile_fetched_at"`
	// AvatarHash is the SHA-1 hex (XEP-0153 photo hash) of the last vCard photo applied to
	// this ghost. Used to skip redundant vCard fetches and to detect changes from presence.
	AvatarHash string `json:"avatar_hash,omitempty"`
}

// MessageMetadata stores message-specific metadata for Jabber messages.
// This gives a DB mapping: stanza_id (XMPPMessageID) ↔ matrix event id (message row/parts).
// This is extensible for future features like backfilling and puppeting.
type MessageMetadata struct {
	// XMPPMessageID is the XMPP stanza ID. For Matrix→XMPP we set it when sending; for XMPP→Matrix
	// we provide it via GetMessageMetadata() and scheduleStanzaIDMetadataUpdate() so the bridge DB row
	// has the mapping for reply/reaction lookups (GetFirstPartByID and metadata search).
	XMPPMessageID string `json:"xmpp_message_id,omitempty"`
	
	// BackfillSource is set for messages fetched via MAM (XEP-0313) backfill/catch-up.
	BackfillSource string `json:"backfill_source,omitempty"` // e.g. "mam"

	// Future extensibility (not yet used):
	// IsPuppeted bool `json:"is_puppeted,omitempty"` // True if sent via double-puppeting
}

// GetDBMetaTypes returns metadata types for database tables.
func (c *JabberConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		Portal: func() any {
			return &PortalMetadata{}
		},
		Ghost: func() any {
			return &GhostMetadata{}
		},
		Message: func() any {
			return &MessageMetadata{}
		},
		Reaction:   nil,
		UserLogin: func() any { return &JabberLoginMeta{} },
	}
}
