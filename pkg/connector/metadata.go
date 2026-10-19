package connector

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"maunium.net/go/mautrix/bridgev2"
)

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

// setExtraKey sets (raw != nil) or deletes (raw == nil) one Extra key copy-on-write: the Extra map is
// read without locks elsewhere, so it is never mutated in place. Empty Extra becomes nil.
func setExtraKey(meta *JabberLoginMeta, key string, raw json.RawMessage) {
	extra := cloneExtra(meta.Extra)
	if raw == nil {
		delete(extra, key)
	} else {
		if extra == nil {
			extra = make(map[string]json.RawMessage)
		}
		extra[key] = raw
	}
	if len(extra) == 0 {
		extra = nil
	}
	meta.Extra = extra
}

// loginMeta returns ul's jabber metadata (nil if none).
func loginMeta(ul *bridgev2.UserLogin) *JabberLoginMeta {
	if ul == nil || ul.UserLogin == nil {
		return nil
	}
	meta, _ := ul.Metadata.(*JabberLoginMeta)
	return meta
}

// saveUserLogin persists ul (a var for tests).
var saveUserLogin = func(ctx context.Context, ul *bridgev2.UserLogin) error { return ul.Save(ctx) }

// extraWriteMu serializes every read-modify-write + save of JabberLoginMeta.Extra (call prefs,
// sip-login/sip-logout credentials, CardDAV credentials): each writer clones the whole map, sets one key
// and swaps the pointer, so without a shared lock one writer could overwrite another's key.
// Lock order: extraWriteMu before callsExt.mu; never acquire it while holding callsExt.mu,
// xmppClient.mu or br.cacheLock, and never hold it across network I/O (only mutate + save).
// Known residual: bridgev2's re-login CopyFrom + Save runs outside this lock.
var extraWriteMu sync.Mutex

// setLoginExtraKey sets (raw != nil) or deletes (raw == nil) one Extra key of ul and saves it, under
// extraWriteMu. On a save error only that key is restored (restoring the whole previous map could undo
// another key's write).
func setLoginExtraKey(ctx context.Context, ul *bridgev2.UserLogin, key string, raw json.RawMessage) error {
	extraWriteMu.Lock()
	defer extraWriteMu.Unlock()
	meta := loginMeta(ul)
	if meta == nil {
		return errors.New("no login metadata")
	}
	prev, had := meta.Extra[key]
	setExtraKey(meta, key, raw)
	if err := saveUserLogin(ctx, ul); err != nil {
		if had {
			setExtraKey(meta, key, prev)
		} else {
			setExtraKey(meta, key, nil)
		}
		return err
	}
	return nil
}
