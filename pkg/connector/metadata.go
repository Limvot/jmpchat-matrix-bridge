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

// CopyFrom implements database.MetaMerger. bridgev2 calls it when a login that already exists is logged
// in again (instead of replacing the metadata wholesale), with other being the fresh metadata of the new
// login (which only carries the password). The new password (and Server/Port/DefaultMUCNick when set)
// win; every Extra key of the existing metadata survives unless other carries the same key. Extra is
// replaced copy-on-write: readers holding the old map never see it change.
func (m *JabberLoginMeta) CopyFrom(other any) {
	o, ok := other.(*JabberLoginMeta)
	if !ok || o == nil || o == m {
		return
	}
	m.Password = o.Password
	if o.Server != "" {
		m.Server = o.Server
	}
	if o.Port != 0 {
		m.Port = o.Port
	}
	if o.DefaultMUCNick != "" {
		m.DefaultMUCNick = o.DefaultMUCNick
	}
	if len(o.Extra) > 0 {
		extra := cloneExtra(m.Extra)
		if extra == nil {
			extra = make(map[string]json.RawMessage, len(o.Extra))
		}
		for k, v := range o.Extra {
			extra[k] = v
		}
		m.Extra = extra
	}
}

// cloneExtra returns a shallow copy of an Extra map (nil for nil/empty).
func cloneExtra(src map[string]json.RawMessage) map[string]json.RawMessage {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]json.RawMessage, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
