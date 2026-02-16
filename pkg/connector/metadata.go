package connector

// JabberLoginMeta is stored in UserLogin.Metadata (JSON) so we can reconnect
// with the same credentials. Server and Port are optional overrides; if empty,
// the JID's domain and default port (5222) are used.
type JabberLoginMeta struct {
	Password       string `json:"password"`
	Server         string `json:"server,omitempty"`
	Port           int    `json:"port,omitempty"`
	DefaultMUCNick string `json:"default_muc_nick,omitempty"` // Default MUC nickname when no room-specific nick is set
}
