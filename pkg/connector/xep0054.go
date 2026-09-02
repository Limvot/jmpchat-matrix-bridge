// Package connector, XEP-0054 (vcard-temp) support: fetch an XMPP contact's vCard
// photo and apply it as the Matrix ghost's avatar. This is inbound-only (XMPP → Matrix);
// the bridge never modifies the logged-in user's own vCard. XEP-0153 (vcard-temp:x:update)
// presence hashes are used as the reactive trigger to refresh a contact avatar on change.

package connector

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	vcardTempNS   = "vcard-temp"
	vcardUpdateNS = "vcard-temp:x:update" // XEP-0153

	avatarFetchTimeout    = 10 * time.Second
	avatarRefreshInterval = 24 * time.Hour // max age before GetUserInfo re-fetches a contact's vCard
)

// vcardResult unmarshals the <vCard xmlns='vcard-temp'> reply. Field tags use bare local
// names so they match regardless of how the server namespaces the child elements.
type vcardResult struct {
	Photo struct {
		Type   string `xml:"TYPE"`
		BinVal string `xml:"BINVAL"`
	} `xml:"PHOTO"`
}

// avatarIDFromHash builds a stable AvatarID from a photo's SHA-1 hex so the framework can
// skip reuploading when the avatar is unchanged (Ghost.UpdateInfo compares Avatar.ID).
func avatarIDFromHash(sha1Hex string) networkid.AvatarID {
	return networkid.AvatarID("sha1:" + sha1Hex)
}

// stripWhitespace removes all ASCII whitespace so base64 payloads with embedded newlines decode.
func stripWhitespace(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\n', '\r':
			return -1
		}
		return r
	}, s)
}

// fetchContactAvatar issues a XEP-0054 vCard IQ get to the contact's bare JID and returns the
// decoded photo bytes, its MIME type, and its SHA-1 hex (XEP-0153 photo hash). A contact with
// no photo returns ("", "", "") with a nil error — that is not a failure.
func (x *xmppClient) fetchContactAvatar(ctx context.Context, bareJIDStr string) (data []byte, mimeType, sha1Hex string, err error) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, "", "", nil
	}
	to, perr := jid.Parse(bareJIDStr)
	if perr != nil {
		return nil, "", "", fmt.Errorf("parse jid %q: %w", bareJIDStr, perr)
	}
	to = to.Bare()

	fetchCtx, cancel := context.WithTimeout(ctx, avatarFetchTimeout)
	defer cancel()

	payload := xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Space: vcardTempNS, Local: "vCard"}})
	iq := stanza.IQ{Type: stanza.GetIQ, To: to, ID: newStanzaID()}
	var res vcardResult
	if err := sess.UnmarshalIQElement(fetchCtx, payload, iq, &res); err != nil {
		return nil, "", "", fmt.Errorf("vcard fetch %s: %w", to, err)
	}
	binval := stripWhitespace(res.Photo.BinVal)
	if binval == "" {
		return nil, "", "", nil // no photo is not an error
	}
	decoded, derr := base64.StdEncoding.DecodeString(binval)
	if derr != nil {
		return nil, "", "", fmt.Errorf("decode vcard photo base64 for %s: %w", to, derr)
	}
	if len(decoded) == 0 {
		return nil, "", "", nil
	}
	sum := sha1.Sum(decoded)
	return decoded, strings.TrimSpace(res.Photo.Type), hex.EncodeToString(sum[:]), nil
}

// cachedContactAvatar returns the ghost's already-known avatar (from GhostMetadata.AvatarHash)
// without any network I/O, so GetUserInfo never blocks. When the cached copy is stale (never
// fetched, or older than avatarRefreshInterval) it also kicks off a deduped background fetch.
// Returns nil when there is no known avatar yet — the background fetch will apply one when it
// completes. Gating on AvatarFetchedAt (not ProfileFetchedAt) ensures pre-existing ghosts, which
// already have a recent ProfileFetchedAt but no avatar, still get an initial fetch.
func (x *xmppClient) cachedContactAvatar(bareJID string, meta *GhostMetadata) *bridgev2.Avatar {
	stale := meta == nil || meta.AvatarFetchedAt.Time.IsZero() || time.Since(meta.AvatarFetchedAt.Time) > avatarRefreshInterval
	if stale {
		x.queueAvatarRefresh(bareJID)
	}
	if meta == nil || meta.AvatarHash == "" {
		return nil
	}
	// Return the known avatar. Get is a lazy fallback the framework only calls if the ghost doesn't
	// already have this AvatarID; even then it hits fetchContactAvatar off the main path.
	hash := meta.AvatarHash
	return &bridgev2.Avatar{
		ID: avatarIDFromHash(hash),
		Get: func(ctx context.Context) ([]byte, error) {
			d, _, _, err := x.fetchContactAvatar(ctx, bareJID)
			return d, err
		},
	}
}

// queueAvatarRefresh starts a background vCard avatar fetch for a 1:1 contact unless one is already
// in flight for that JID. It never blocks the caller.
func (x *xmppClient) queueAvatarRefresh(bareJID string) {
	x.avatarRefreshingMu.Lock()
	if x.avatarRefreshing == nil {
		x.avatarRefreshing = make(map[string]bool)
		x.avatarFetchSem = make(chan struct{}, maxConcurrentAvatarFetches)
	}
	if x.avatarRefreshing[bareJID] {
		x.avatarRefreshingMu.Unlock()
		return
	}
	x.avatarRefreshing[bareJID] = true
	sem := x.avatarFetchSem
	x.avatarRefreshingMu.Unlock()

	go func() {
		sem <- struct{}{} // acquire a slot (blocks this goroutine only, not the caller)
		defer func() {
			<-sem
			x.avatarRefreshingMu.Lock()
			delete(x.avatarRefreshing, bareJID)
			x.avatarRefreshingMu.Unlock()
		}()
		x.doAvatarRefresh(bareJID)
	}()
}

// doAvatarRefresh fetches the contact's vCard photo and applies (new/changed), removes (photo gone),
// or leaves (unchanged) the ghost avatar, then stamps AvatarFetchedAt/AvatarHash. Runs on its own
// goroutine (via queueAvatarRefresh) so it never blocks GetUserInfo or the presence handler.
func (x *xmppClient) doAvatarRefresh(bareJID string) {
	ctx := context.Background()
	data, _, hash, err := x.fetchContactAvatar(ctx, bareJID)
	if err != nil {
		x.log.Debug().Err(err).Str("jid", bareJID).Msg("Failed to fetch contact avatar (vCard)")
		return // transient: don't stamp, so the next trigger retries
	}
	ghost, gerr := x.conn.br.GetExistingGhostByID(ctx, networkid.UserID(bareJID))
	if gerr != nil || ghost == nil {
		return
	}
	curHash := ""
	if meta, ok := ghost.Metadata.(*GhostMetadata); ok && meta != nil {
		curHash = meta.AvatarHash
	}

	var avatar *bridgev2.Avatar
	switch {
	case hash == "":
		x.log.Debug().Str("jid", bareJID).Msg("Contact has no vCard avatar")
		if curHash != "" {
			avatar = &bridgev2.Avatar{Remove: true} // had one before, now removed
		}
	case hash != curHash:
		avatarData := data
		avatar = &bridgev2.Avatar{
			ID:  avatarIDFromHash(hash),
			Get: func(ctx context.Context) ([]byte, error) { return avatarData, nil },
		}
		x.log.Debug().Str("jid", bareJID).Str("photo_hash", hash).Int("bytes", len(data)).Msg("Fetched contact avatar (vCard)")
	default:
		// unchanged; only stamp AvatarFetchedAt below
	}

	info := &bridgev2.UserInfo{
		Avatar: avatar, // nil = no change; UpdateInfo leaves name/other fields untouched
		ExtraUpdates: func(ctx context.Context, g *bridgev2.Ghost) (changed bool) {
			m, ok := g.Metadata.(*GhostMetadata)
			if !ok || m == nil {
				return false
			}
			m.AvatarFetchedAt.Time = time.Now()
			if m.AvatarHash != hash {
				m.AvatarHash = hash // "" clears it when the photo was removed
			}
			return true // always persist the stamped fetch time
		},
	}
	ghost.UpdateInfo(ctx, info)
}

// refreshContactAvatarFromPresence is triggered by an XEP-0153 vcard-temp:x:update presence hash.
// It queues a background refresh only when the advertised photo hash differs from what we last
// applied (an empty hash means the contact removed their avatar, which doAvatarRefresh handles).
func (x *xmppClient) refreshContactAvatarFromPresence(bareJID, photoHash string) {
	ghost, err := x.conn.br.GetExistingGhostByID(context.Background(), networkid.UserID(bareJID))
	if err != nil || ghost == nil {
		return // no ghost yet; a later GetUserInfo will trigger the initial fetch
	}
	curHash := ""
	if meta, ok := ghost.Metadata.(*GhostMetadata); ok && meta != nil {
		curHash = meta.AvatarHash
	}
	if photoHash == curHash {
		return // no change
	}
	x.queueAvatarRefresh(bareJID)
}
