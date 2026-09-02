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

// contactAvatar builds a *bridgev2.Avatar for a 1:1 contact, fetching the vCard photo only when
// the cached copy is stale (never fetched, or older than avatarRefreshInterval). When not stale it
// reuses the cached hash so the framework skips both the network round-trip and any reupload.
// Returns (nil, "") when there is no avatar to apply (no photo, fetch error, or MUC occupant).
// fetchedHash is non-empty only when a fresh fetch obtained a photo, so ExtraUpdates can persist it.
func (x *xmppClient) contactAvatar(ctx context.Context, bareJID string, meta *GhostMetadata) (avatar *bridgev2.Avatar, fetchedHash string) {
	stale := meta == nil || meta.ProfileFetchedAt.Time.IsZero() || time.Since(meta.ProfileFetchedAt.Time) > avatarRefreshInterval
	if stale {
		data, _, hash, err := x.fetchContactAvatar(ctx, bareJID)
		if err != nil {
			x.log.Debug().Err(err).Str("jid", bareJID).Msg("Failed to fetch contact avatar (vCard)")
			return nil, ""
		}
		if hash == "" {
			return nil, "" // no photo; leave any existing avatar untouched
		}
		avatarData := data
		return &bridgev2.Avatar{
			ID:  avatarIDFromHash(hash),
			Get: func(ctx context.Context) ([]byte, error) { return avatarData, nil },
		}, hash
	}
	if meta.AvatarHash == "" {
		return nil, ""
	}
	// Not stale: keep the known avatar without a network round-trip. Get is a lazy fallback the
	// framework only calls if the ghost doesn't already have this AvatarID.
	return &bridgev2.Avatar{
		ID: avatarIDFromHash(meta.AvatarHash),
		Get: func(ctx context.Context) ([]byte, error) {
			d, _, _, err := x.fetchContactAvatar(ctx, bareJID)
			return d, err
		},
	}, ""
}

// refreshContactAvatarFromPresence is triggered by an XEP-0153 vcard-temp:x:update presence hash.
// It refreshes the ghost avatar only when the advertised photo hash differs from what we last
// applied. An empty photo hash means the contact removed their avatar. Runs in its own goroutine.
func (x *xmppClient) refreshContactAvatarFromPresence(bareJID, photoHash string) {
	ctx := context.Background()
	ghost, err := x.conn.br.GetGhostByID(ctx, networkid.UserID(bareJID))
	if err != nil || ghost == nil {
		return
	}
	curHash := ""
	if meta, ok := ghost.Metadata.(*GhostMetadata); ok && meta != nil {
		curHash = meta.AvatarHash
	}
	if photoHash == curHash {
		return // no change
	}

	if photoHash == "" {
		// Contact removed their avatar: build UserInfo (for name) and force removal.
		info, ierr := x.GetUserInfo(ctx, ghost)
		if ierr != nil || info == nil {
			return
		}
		info.Avatar = &bridgev2.Avatar{Remove: true}
		prev := info.ExtraUpdates
		info.ExtraUpdates = func(ctx context.Context, g *bridgev2.Ghost) (changed bool) {
			if prev != nil {
				changed = prev(ctx, g)
			}
			if m, ok := g.Metadata.(*GhostMetadata); ok && m != nil && m.AvatarHash != "" {
				m.AvatarHash = ""
				changed = true
			}
			return changed
		}
		ghost.UpdateInfo(ctx, info)
		x.log.Debug().Str("jid", bareJID).Msg("Contact removed avatar (XEP-0153), cleared ghost avatar")
		return
	}

	// Hash changed: clear staleness so GetUserInfo re-fetches the new vCard photo.
	if meta, ok := ghost.Metadata.(*GhostMetadata); ok && meta != nil {
		meta.ProfileFetchedAt.Time = time.Time{}
	}
	info, ierr := x.GetUserInfo(ctx, ghost)
	if ierr != nil || info == nil {
		return
	}
	ghost.UpdateInfo(ctx, info)
	x.log.Debug().Str("jid", bareJID).Str("photo_hash", photoHash).Msg("Refreshed contact avatar from XEP-0153 presence")
}
