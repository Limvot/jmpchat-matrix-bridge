//go:build sipcalls

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/williamkray/mrtc-sip-bridge/pkg/obs"
	"github.com/williamkray/mrtc-sip-bridge/pkg/phone"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipua"
)

// callPrefsExtraKey is the JabberLoginMeta.Extra key holding the login's call preferences.
const callPrefsExtraKey = "call_prefs"

// callPrefs are the per-login call preferences (all default off).
type callPrefs struct {
	// RosterOnly: incoming SIP calls ring only for numbers in the login's Jabber roster.
	RosterOnly bool `json:"roster_only,omitempty"`
}

// callPrefsOf returns the preferences stored in meta (zero value when absent or invalid).
func callPrefsOf(meta *JabberLoginMeta) callPrefs {
	var p callPrefs
	if meta == nil || meta.Extra == nil {
		return p
	}
	raw, ok := meta.Extra[callPrefsExtraKey]
	if !ok {
		return p
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return callPrefs{}
	}
	return p
}

// setCallPrefs stores p in meta copy-on-write (other Extra keys are kept); the zero value deletes the key.
func setCallPrefs(meta *JabberLoginMeta, p callPrefs) {
	if meta == nil {
		return
	}
	if p == (callPrefs{}) {
		setExtraKey(meta, callPrefsExtraKey, nil)
		return
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return // cannot happen for a struct of bools
	}
	setExtraKey(meta, callPrefsExtraKey, raw)
}

// loginPrefs returns the cached preferences of the login (falls back to its stored metadata when the
// login has no calls state yet).
func (x *callsExt) loginPrefs(ul *bridgev2.UserLogin) callPrefs {
	x.mu.Lock()
	lc := x.accounts[ul.ID]
	if lc != nil {
		p := lc.prefs
		x.mu.Unlock()
		return p
	}
	x.mu.Unlock()
	return callPrefsOf(loginMeta(ul))
}

// setRosterOnly persists the roster-only preference of ul and then updates the cached copy that
// onInvite reads. Setters are serialized by prefsMu; x.mu is never held across the save. On a save
// error the metadata is rolled back and the cache is untouched.
func (x *callsExt) setRosterOnly(ctx context.Context, ul *bridgev2.UserLogin, on bool) error {
	x.prefsMu.Lock()
	defer x.prefsMu.Unlock()
	meta := loginMeta(ul)
	if meta == nil {
		return errors.New("no login metadata")
	}
	prev := callPrefsOf(meta)
	p := prev
	p.RosterOnly = on
	setCallPrefs(meta, p)
	if err := saveUserLogin(ctx, ul); err != nil {
		// Roll back only our key: restoring the whole previous Extra map could undo another key's write.
		setCallPrefs(meta, prev)
		return err
	}
	x.mu.Lock()
	if lc := x.accounts[ul.ID]; lc != nil {
		lc.prefs = p
	}
	x.mu.Unlock()
	return nil
}

// rosterGate decides whether an inbound call may reach the core. The caller number is derived exactly
// like the core does (calls/setup.go: P-Asserted-Identity wins over From, phone.Normalize with the
// default country code). It allows (true) when roster-only is off, when the caller is withheld or not a
// phone number (the core rejects those itself), when the roster has not loaded yet (fail open) and when
// the caller's Cheogram JID is in the roster. Otherwise (false) with the caller's E.164.
func rosterGate(p callPrefs, info sipua.InviteInfo, defaultCC string, inRoster func(bareJID string) (in, loaded bool)) (allow bool, e164 string) {
	if !p.RosterOnly {
		return true, ""
	}
	raw := info.AssertedUser
	if raw == "" {
		raw = info.FromUser
	}
	e164, err := phone.Normalize(raw, defaultCC)
	if err != nil {
		return true, ""
	}
	in, loaded := inRoster(cheogramJID(e164))
	if !loaded || in {
		return true, e164
	}
	return false, e164
}

// rejectFiltered rejects a call the roster gate refused: 480 (the call falls through to voicemail
// without ending other forks, unlike 603), a root span and one Info log line (number allowed, never a
// name, no metric), and a quiet notice in the owner's management room sent from its own goroutine.
func (x *callsExt) rejectFiltered(loginID networkid.UserLoginID, ul *bridgev2.UserLogin, call sipua.InboundCall, e164 string) {
	ctx, span := obs.Start(context.Background(), "bridge.calls.inbound_filtered",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("xmpp.login.jid", string(loginID)),
			obs.AttrCallRemoteE164.String(e164),
			obs.AttrOutcome.String("rejected"),
			attribute.String("reason", "not_in_roster")))
	defer span.End()
	call.Reject(480, "Temporarily Unavailable")
	if !x.firstFiltered(string(loginID)+"|"+e164, time.Now()) {
		x.logFor(ctx).Debug().Str("login", string(loginID)).Str("from", e164).
			Msg("Rejected inbound call: caller not in roster (repeat INVITE, no notice)")
		return
	}
	x.logFor(ctx).Info().Str("login", string(loginID)).Str("from", e164).
		Msg("Rejected inbound call: caller not in roster (roster-only calls)")
	if ul != nil {
		go x.sendFilteredNotice(ul, e164)
	}
}

// filteredNoticeQuiet is how long after the last filtered INVITE from a caller further INVITEs from
// that caller are treated as the same call attempt (no new notice). Every repeat extends the window.
const filteredNoticeQuiet = 2 * time.Minute

// firstFiltered records a filtered INVITE for key (login|caller) at now and reports whether it starts
// a new burst, i.e. whether a notice should be sent.
func (x *callsExt) firstFiltered(key string, now time.Time) bool {
	x.filteredMu.Lock()
	defer x.filteredMu.Unlock()
	if x.filteredSeen == nil {
		x.filteredSeen = make(map[string]time.Time)
	}
	last, seen := x.filteredSeen[key]
	x.filteredSeen[key] = now
	if len(x.filteredSeen) > 256 {
		for k, t := range x.filteredSeen {
			if now.Sub(t) >= filteredNoticeQuiet {
				delete(x.filteredSeen, k)
			}
		}
	}
	return !seen || now.Sub(last) >= filteredNoticeQuiet
}

// sendFilteredNotice tells the login's owner (management room) that a call was blocked.
func (x *callsExt) sendFilteredNotice(ul *bridgev2.UserLogin, e164 string) {
	if ul == nil || ul.User == nil || ul.User.User == nil || ul.User.ManagementRoom == "" {
		return
	}
	text := fmt.Sprintf("Blocked a call from %s: not in your Jabber roster (roster-only calls is on; `%s roster-only-calls off` to ring for everyone).",
		e164, x.commandPrefix())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := x.notify(ctx, ul.User.ManagementRoom, text); err != nil {
		x.log.Warn().Err(err).Str("login", string(ul.ID)).Msg("Failed to send filtered call notice")
	}
}
