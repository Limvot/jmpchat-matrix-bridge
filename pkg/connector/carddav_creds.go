package connector

import (
	"context"
	"encoding/json"

	"maunium.net/go/mautrix/bridgev2"
)

// cardDAVExtraKey is the JabberLoginMeta.Extra key holding the login's CardDAV credentials.
const cardDAVExtraKey = "carddav"

// cardDAVCreds are the stored CardDAV account details. The password is stored in plaintext in the login
// metadata, the same trust boundary as the Jabber password and SIP credentials. Books are absolute
// address-book URLs found at login (re-discovered by the sync when they stop answering).
type cardDAVCreds struct {
	V        int      `json:"v"`
	URL      string   `json:"url"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	Books    []string `json:"books,omitempty"`
}

// complete reports whether c has everything a sync needs.
func (c *cardDAVCreds) complete() bool {
	return c != nil && c.URL != "" && c.Username != "" && c.Password != ""
}

// cardDAVCredsOf returns the credentials stored in meta; ok=false if absent, invalid or incomplete.
func cardDAVCredsOf(meta *JabberLoginMeta) (*cardDAVCreds, bool) {
	if meta == nil || meta.Extra == nil {
		return nil, false
	}
	raw, ok := meta.Extra[cardDAVExtraKey]
	if !ok {
		return nil, false
	}
	var c cardDAVCreds
	if err := json.Unmarshal(raw, &c); err != nil || !c.complete() {
		return nil, false
	}
	return &c, true
}

// saveCardDAVCreds stores c (nil deletes the key) and saves the login, under extraWriteMu.
func saveCardDAVCreds(ctx context.Context, ul *bridgev2.UserLogin, c *cardDAVCreds) error {
	if c == nil {
		return setLoginExtraKey(ctx, ul, cardDAVExtraKey, nil)
	}
	cp := *c
	cp.V = 1
	raw, err := json.Marshal(&cp)
	if err != nil {
		return err // cannot happen for a struct of strings
	}
	return setLoginExtraKey(ctx, ul, cardDAVExtraKey, raw)
}
