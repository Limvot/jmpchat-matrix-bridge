package connector

import "encoding/json"

// JabberLoginMeta is stored in UserLogin.Metadata (JSON) so we can reconnect
// with the same credentials.
//
// Server and Port are reserved for a future per-account server/port override and
// are NOT yet wired into dialXMPP: today the connection host is always derived
// from the JID's domain (A/AAAA lookup, port 5222). See the SRV/override to-do in
// README.md before relying on these fields.
type JabberLoginMeta struct {
	Password       string `json:"password"`
	Server         string `json:"server,omitempty"`
	Port           int    `json:"port,omitempty"`
	DefaultMUCNick string `json:"default_muc_nick,omitempty"` // Default MUC nickname when no room-specific nick is set

	// Extra is opaque data owned by optional build-tagged features; preserved across builds.
	Extra map[string]json.RawMessage `json:"extra,omitempty"`
}
