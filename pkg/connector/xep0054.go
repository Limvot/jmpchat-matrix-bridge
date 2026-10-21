// Package connector, XEP-0054 (vcard-temp) support: fetch an XMPP contact's vCard
// photo and apply it as the Matrix ghost's avatar. This is inbound-only (XMPP → Matrix);
// the bridge never modifies the logged-in user's own vCard. XEP-0153 (vcard-temp:x:update)
// presence hashes are used as the reactive trigger to refresh a contact avatar on change.

package connector

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

const (
	vcardTempNS   = "vcard-temp"
	vcardUpdateNS = "vcard-temp:x:update" // XEP-0153

	// avatarFetchTimeout bounds one vCard fetch. Large avatars routed between servers are slow: a 177 KB
	// photo fetched through a MUC took ~17 s (Prosody s2s bandwidth limits) while clients like Gajim simply
	// wait, so 10 s timed out every time. Fetches run in the background, paced, off the Serve goroutine.
	avatarFetchTimeout    = 60 * time.Second
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

// maxAvatarBytes caps a vCard photo (decoded size). Avatars are small and Matrix clients thumbnail
// anyway; the limit protects the media upload and memory from oversized or hostile vCards.
const maxAvatarBytes = 1 << 20 // 1 MiB

// errAvatarRejected marks a vCard photo that was fetched fine but refused (too large or not an
// image). It is not transient: callers must not retry the same photo in a loop.
var errAvatarRejected = errors.New("vcard photo rejected")

// avatarBase64TooLarge reports whether a base64 payload would decode to more than maxAvatarBytes.
// It is checked on the encoded length before decoding so a huge BINVAL is never decoded.
func avatarBase64TooLarge(binval string) bool {
	return base64.StdEncoding.DecodedLen(len(binval)) > maxAvatarBytes
}

// validateAvatarBytes checks decoded photo bytes: within maxAvatarBytes and sniffed as image/*
// (png, jpeg, gif, webp; SVG sniffs as text and is rejected on purpose). The error message holds
// only the size and the detected type, never bytes.
func validateAvatarBytes(b []byte) error {
	if len(b) > maxAvatarBytes {
		return fmt.Errorf("%w: %d bytes exceeds %d", errAvatarRejected, len(b), maxAvatarBytes)
	}
	if ct := http.DetectContentType(b); !strings.HasPrefix(ct, "image/") {
		return fmt.Errorf("%w: type %s", errAvatarRejected, ct)
	}
	return nil
}

// fetchContactAvatar issues a XEP-0054 vCard IQ get to the contact's bare JID and returns the
// decoded photo bytes, its MIME type, and its SHA-1 hex (XEP-0153 photo hash). A contact with
// no photo returns ("", "", "") with a nil error — that is not a failure.
func (x *xmppClient) fetchContactAvatar(ctx context.Context, bareJIDStr string) (data []byte, mimeType, sha1Hex string, err error) {
	to, perr := jid.Parse(bareJIDStr)
	if perr != nil {
		return nil, "", "", fmt.Errorf("parse jid %q: %w", bareJIDStr, perr)
	}
	to = to.Bare()
	return x.fetchVCardPhoto(ctx, to, to.String())
}

// fetchVCardPhoto sends a vcard-temp get to the given JID (a bare JID, or a MUC occupant JID) and
// returns the decoded photo, its MIME type and SHA-1 hex. No photo returns ("", "", "", nil). A
// photo over maxAvatarBytes or not an image returns an error wrapping errAvatarRejected.
// spanPeer is the value recorded as xmpp.peer.jid on the IQ span (callers keep real JIDs off spans).
func (x *xmppClient) fetchVCardPhoto(ctx context.Context, to jid.JID, spanPeer string) (data []byte, mimeType, sha1Hex string, err error) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, "", "", nil
	}

	fetchCtx, cancel := context.WithTimeout(ctx, avatarFetchTimeout)
	defer cancel()

	payload := xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Space: vcardTempNS, Local: "vCard"}})
	iq := stanza.IQ{Type: stanza.GetIQ, To: to, ID: newStanzaID()}
	var res vcardResult
	ctx, span := telemetry.StartChild(fetchCtx, "xmpp.iq.vcard_get", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("xmpp.iq.namespace", vcardTempNS),
			attribute.String("xmpp.peer.jid", spanPeer),
		)
	}
	if err := sendVCardGet(ctx, sess, payload, iq, &res); err != nil {
		telemetry.Fail(span, err)
		return nil, "", "", fmt.Errorf("vcard fetch %s: %w", to, err)
	}
	binval := stripWhitespace(res.Photo.BinVal)
	if binval == "" {
		return nil, "", "", nil // no photo is not an error
	}
	if avatarBase64TooLarge(binval) {
		return nil, "", "", fmt.Errorf("%w: encoded size %d bytes exceeds limit", errAvatarRejected, len(binval))
	}
	decoded, derr := base64.StdEncoding.DecodeString(binval)
	if derr != nil {
		return nil, "", "", fmt.Errorf("decode vcard photo base64 for %s: %w", to, derr)
	}
	if len(decoded) == 0 {
		return nil, "", "", nil
	}
	if verr := validateAvatarBytes(decoded); verr != nil {
		return nil, "", "", verr
	}
	sum := sha1.Sum(decoded)
	return decoded, strings.TrimSpace(res.Photo.Type), hex.EncodeToString(sum[:]), nil
}

// iqStanzaError is a stanza error reply to an IQ together with the reply's from attribute (who
// answered). errors.As(err, &stanza.Error{}) still works through Unwrap.
type iqStanzaError struct {
	from string
	err  stanza.Error
}

func (e *iqStanzaError) Error() string { return e.err.Error() }
func (e *iqStanzaError) Unwrap() error { return e.err }

// iqErrorFrom returns the from attribute of a stanza error reply wrapped in err ("" if unknown).
func iqErrorFrom(err error) string {
	var ie *iqStanzaError
	if errors.As(err, &ie) {
		return ie.from
	}
	return ""
}

// sendVCardGet is UnmarshalIQElement, except that an error reply keeps its from attribute
// (iqStanzaError), which the MUC breaker uses to tell a room refusal from an occupant's answer.
func sendVCardGet(ctx context.Context, sess *xmpp.Session, payload xml.TokenReader, iq stanza.IQ, v *vcardResult) (e error) {
	resp, err := sess.SendIQElement(ctx, payload, iq)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := resp.Close(); e == nil {
			e = cerr
		}
	}()
	tok, err := resp.Token()
	if err != nil {
		return err
	}
	start, ok := tok.(xml.StartElement)
	if !ok {
		return fmt.Errorf("vcard: expected IQ start token, got %T", tok)
	}
	if _, err := stanza.UnmarshalIQError(resp, start); err != nil {
		var se stanza.Error
		if errors.As(err, &se) {
			return &iqStanzaError{from: attrValue(start.Attr, "from"), err: se}
		}
		return err
	}
	d := xml.NewTokenDecoder(xmlstream.Inner(resp))
	t, err := d.Token()
	if err == io.EOF {
		return nil // empty result: no vCard, no photo
	}
	if err != nil {
		return err
	}
	inner, ok := t.(xml.StartElement)
	if !ok {
		return nil
	}
	return d.DecodeElement(v, &inner)
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
	// The ctx carries the client logger so framework errors (avatar upload, profile set) are logged.
	ctx, span := telemetry.Start(x.log.WithContext(context.Background()), "xmpp.avatar.refresh")
	defer span.End()
	log := x.logFor(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.peer.jid", bareJID))
	}
	data, _, hash, err := x.fetchContactAvatar(ctx, bareJID)
	if errors.Is(err, errAvatarRejected) {
		// Oversized or non-image photo: keep the current avatar and stamp the attempt so the
		// 24 h refresh gate (not every GetUserInfo) decides when to look again.
		log.Debug().Err(err).Str("jid", bareJID).Msg("Contact vCard avatar rejected")
		if span.IsRecording() {
			span.SetAttributes(attribute.Bool("xmpp.avatar.changed", false), attribute.Bool("xmpp.avatar.rejected", true))
		}
		if ghost, gerr := x.conn.br.GetExistingGhostByID(ctx, networkid.UserID(bareJID)); gerr == nil && ghost != nil {
			ghost.UpdateInfo(ctx, &bridgev2.UserInfo{
				ExtraUpdates: func(ctx context.Context, g *bridgev2.Ghost) bool {
					m, ok := g.Metadata.(*GhostMetadata)
					if !ok || m == nil {
						return false
					}
					m.AvatarFetchedAt.Time = time.Now()
					return true
				},
			})
		}
		return
	}
	if err != nil {
		telemetry.Fail(span, err)
		log.Debug().Err(err).Str("jid", bareJID).Msg("Failed to fetch contact avatar (vCard)")
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
		log.Debug().Str("jid", bareJID).Msg("Contact has no vCard avatar")
		if curHash != "" {
			avatar = &bridgev2.Avatar{Remove: true} // had one before, now removed
		}
	case hash != curHash:
		avatarData := data
		avatar = &bridgev2.Avatar{
			ID:  avatarIDFromHash(hash),
			Get: func(ctx context.Context) ([]byte, error) { return avatarData, nil },
		}
		log.Debug().Str("jid", bareJID).Str("photo_hash", hash).Int("bytes", len(data)).Msg("Fetched contact avatar (vCard)")
	default:
		// unchanged; only stamp AvatarFetchedAt below
	}

	if span.IsRecording() {
		span.SetAttributes(attribute.Bool("xmpp.avatar.changed", avatar != nil))
	}
	var after func(g *bridgev2.Ghost)
	var prop *mucMXC
	if avatar != nil && hash != "" {
		sum := sha256.Sum256(data)
		after = func(g *bridgev2.Ghost) {
			// Only an MXC that provably holds these bytes can be shared with MUC occupant ghosts.
			if g.AvatarMXC != "" && g.AvatarHash == sum {
				prop = &mucMXC{realHash: hash, mxc: string(g.AvatarMXC), sha256: g.AvatarHash}
			}
		}
	}
	applyGhostAvatar(ctx, realAvatarGhost{ghost}, avatar, hash, after)
	if avatar != nil && ghost.AvatarSet {
		if hash == "" {
			x.propagateContactAvatar(bareJID, nil) // removal
		} else if prop != nil {
			x.propagateContactAvatar(bareJID, prop)
		}
	}
}

// applyGhostAvatar applies avatar (nil = no change, UpdateInfo leaves name and other fields alone)
// to the ghost and stamps AvatarFetchedAt and AvatarHash=newHash ("" clears it when the photo was
// removed). after, if set, runs in ExtraUpdates under the ghost lock after the avatar was prepared,
// so it sees the final AvatarMXC/AvatarHash.
func applyGhostAvatar(ctx context.Context, ghost avatarGhost, avatar *bridgev2.Avatar, newHash string, after func(g *bridgev2.Ghost)) {
	ghost.UpdateInfo(ctx, &bridgev2.UserInfo{
		Avatar: avatar,
		ExtraUpdates: func(ctx context.Context, g *bridgev2.Ghost) (changed bool) {
			m, ok := g.Metadata.(*GhostMetadata)
			if !ok || m == nil {
				return false
			}
			m.AvatarFetchedAt.Time = time.Now()
			if m.AvatarHash != newHash {
				m.AvatarHash = newHash
			}
			if after != nil {
				after(g)
			}
			return true // always persist the stamped fetch time
		},
	})
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
