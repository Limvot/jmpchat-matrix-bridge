//go:build sipcalls

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipua"
)

// fakeInboundCall is a sipua.InboundCall that records its final response.
type fakeInboundCall struct {
	info sipua.InviteInfo

	mu      sync.Mutex
	rejects []int
}

func (f *fakeInboundCall) Info() sipua.InviteInfo { return f.info }
func (f *fakeInboundCall) Ringing()               {}
func (f *fakeInboundCall) Answer()                {}
func (f *fakeInboundCall) Hangup()                {}
func (f *fakeInboundCall) Events() <-chan sipua.CallEvent {
	return nil
}
func (f *fakeInboundCall) Reject(code int, _ string) {
	f.mu.Lock()
	f.rejects = append(f.rejects, code)
	f.mu.Unlock()
}
func (f *fakeInboundCall) rejected() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.rejects...)
}

func TestCallPrefsMeta(t *testing.T) {
	orig := map[string]json.RawMessage{"sip": json.RawMessage(`{"a":1}`)}
	meta := &JabberLoginMeta{Extra: orig}
	if callPrefsOf(meta).RosterOnly || callPrefsOf(nil).RosterOnly {
		t.Fatal("default must be off")
	}
	setCallPrefs(meta, callPrefs{RosterOnly: true})
	if !callPrefsOf(meta).RosterOnly || string(meta.Extra["sip"]) != `{"a":1}` {
		t.Fatalf("set: %v", meta.Extra)
	}
	if _, ok := orig[callPrefsExtraKey]; ok || len(orig) != 1 {
		t.Fatal("setCallPrefs mutated the old Extra map (must be copy-on-write)")
	}
	raw, _ := json.Marshal(meta)
	var back JabberLoginMeta
	if err := json.Unmarshal(raw, &back); err != nil || !callPrefsOf(&back).RosterOnly {
		t.Fatalf("round trip: %v %s", err, raw)
	}
	setCallPrefs(meta, callPrefs{})
	if _, ok := meta.Extra[callPrefsExtraKey]; ok || len(meta.Extra) != 1 {
		t.Fatalf("zero value must delete the key: %v", meta.Extra)
	}
	delete(meta.Extra, "sip")
	setCallPrefs(meta, callPrefs{RosterOnly: true})
	setCallPrefs(meta, callPrefs{})
	if meta.Extra != nil {
		t.Fatal("empty Extra not nil")
	}
	for _, bad := range []string{`"x"`, `[1]`, `{"roster_only":"yes"}`} {
		m := &JabberLoginMeta{Extra: map[string]json.RawMessage{callPrefsExtraKey: json.RawMessage(bad)}}
		if callPrefsOf(m).RosterOnly {
			t.Errorf("invalid %s accepted", bad)
		}
	}
	setCallPrefs(nil, callPrefs{RosterOnly: true}) // no panic
}

func TestRosterGate(t *testing.T) {
	roster := map[string]bool{"+15550001111@cheogram.com": true}
	look := func(loaded bool) func(string) (bool, bool) {
		return func(j string) (bool, bool) { return roster[j], loaded }
	}
	on, off := callPrefs{RosterOnly: true}, callPrefs{}
	tests := []struct {
		name      string
		p         callPrefs
		info      sipua.InviteInfo
		loaded    bool
		wantAllow bool
		wantE164  string
	}{
		{"off", off, sipua.InviteInfo{FromUser: "+19998887777"}, true, true, ""},
		{"in roster", on, sipua.InviteInfo{FromUser: "+15550001111"}, true, true, "+15550001111"},
		{"not in roster", on, sipua.InviteInfo{FromUser: "+19998887777"}, true, false, "+19998887777"},
		{"national format uses default cc", on, sipua.InviteInfo{FromUser: "(555) 000-1111"}, true, true, "+15550001111"},
		{"PAI wins over From (allowed PAI)", on, sipua.InviteInfo{AssertedUser: "+15550001111", FromUser: "+19998887777"}, true, true, "+15550001111"},
		{"PAI wins over From (blocked PAI)", on, sipua.InviteInfo{AssertedUser: "+19998887777", FromUser: "+15550001111"}, true, false, "+19998887777"},
		{"withheld passes to core", on, sipua.InviteInfo{FromUser: "anonymous"}, true, true, ""},
		{"empty passes to core", on, sipua.InviteInfo{}, true, true, ""},
		{"roster not loaded fails open", on, sipua.InviteInfo{FromUser: "+19998887777"}, false, true, "+19998887777"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allow, e164 := rosterGate(tt.p, tt.info, "1", look(tt.loaded))
			if allow != tt.wantAllow || e164 != tt.wantE164 {
				t.Fatalf("got (%v, %q), want (%v, %q)", allow, e164, tt.wantAllow, tt.wantE164)
			}
		})
	}
}

// inviteHarness has a running account, a loaded client and a recorded HandleInvite.
type inviteHarness struct {
	*accountsHarness
	handled chan string
	client  *xmppClient
	saves   int
	saveErr error
}

func newInviteHarness(t *testing.T) *inviteHarness {
	t.Helper()
	ah := newAccountsHarness(t)
	h := &inviteHarness{accountsHarness: ah, handled: make(chan string, 4)}
	ah.x.cfg.DefaultCountryCode = "1"
	ah.x.handleInviteFn = func(_ core.LoginRef, _ string, call sipua.InboundCall) {
		h.handled <- call.Info().FromUser
	}
	saved := saveUserLogin
	saveUserLogin = func(context.Context, *bridgev2.UserLogin) error { h.saves++; return h.saveErr }
	t.Cleanup(func() { saveUserLogin = saved })
	ah.startRuntime(t)
	ul := testLogin(true)
	h.client = testClient(ul)
	ul.Client = h.client
	ah.x.loginLoaded(ul, h.client)
	waitFor(t, "account running", func() bool { a := ah.x.account(ul.ID); return a != nil && a.Running() })
	return h
}

func (h *inviteHarness) ul() *bridgev2.UserLogin { return h.x.accounts["me@example.com"].ul }

func (h *inviteHarness) invite(from, pai string) *fakeInboundCall {
	call := &fakeInboundCall{info: sipua.InviteInfo{FromUser: from, AssertedUser: pai}}
	h.x.onInvite("me@example.com", h.x.account("me@example.com"), call)
	return call
}

func (h *inviteHarness) expectHandled(t *testing.T, from string) {
	t.Helper()
	select {
	case got := <-h.handled:
		if got != from {
			t.Fatalf("handled %q, want %q", got, from)
		}
	default:
		t.Fatal("call was not passed to the core")
	}
}

func (h *inviteHarness) expectNotHandled(t *testing.T) {
	t.Helper()
	select {
	case got := <-h.handled:
		t.Fatalf("call from %q reached the core", got)
	default:
	}
}

func (h *inviteHarness) enable(t *testing.T) {
	t.Helper()
	if err := h.x.setRosterOnly(context.Background(), h.ul(), true); err != nil {
		t.Fatal(err)
	}
}

func TestOnInviteRosterGate(t *testing.T) {
	t.Run("prefs off", func(t *testing.T) {
		h := newInviteHarness(t)
		call := h.invite("+19998887777", "")
		h.expectHandled(t, "+19998887777")
		if len(call.rejected()) != 0 {
			t.Fatal("rejected")
		}
	})
	t.Run("on and in roster", func(t *testing.T) {
		h := newInviteHarness(t)
		h.enable(t)
		h.client.applyRosterItems(true, rosterEntry{jid: "+15550001111@cheogram.com"}) // unnamed counts
		call := h.invite("+15550001111", "")
		h.expectHandled(t, "+15550001111")
		if len(call.rejected()) != 0 {
			t.Fatal("rejected")
		}
	})
	t.Run("on and not in roster", func(t *testing.T) {
		h := newInviteHarness(t)
		h.enable(t)
		h.client.applyRosterItems(true, rosterEntry{jid: "+15550001111@cheogram.com"})
		call := h.invite("+19998887777", "")
		if got := call.rejected(); len(got) != 1 || got[0] != 480 {
			t.Fatalf("rejects %v, want [480]", got)
		}
		h.expectNotHandled(t)
		select {
		case n := <-h.notices:
			want := "!mgmt:example.com Blocked a call from +19998887777: not in your Jabber roster (roster-only calls is on; `!jc roster-only-calls off` to ring for everyone)."
			if n != want {
				t.Fatalf("notice %q\nwant   %q", n, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no notice")
		}
	})
	t.Run("notice failure does not block or panic", func(t *testing.T) {
		h := newInviteHarness(t)
		h.enable(t)
		h.client.applyRosterItems(true)
		sent := make(chan struct{}, 1)
		h.x.notifyFn = func(context.Context, id.RoomID, string) error { sent <- struct{}{}; return errors.New("no bot") }
		call := h.invite("+19998887777", "")
		if len(call.rejected()) != 1 {
			t.Fatal("not rejected")
		}
		select {
		case <-sent:
		case <-time.After(5 * time.Second):
			t.Fatal("notice not attempted")
		}
	})
	t.Run("on and roster not loaded rings", func(t *testing.T) {
		h := newInviteHarness(t)
		h.enable(t)
		call := h.invite("+19998887777", "")
		h.expectHandled(t, "+19998887777")
		if len(call.rejected()) != 0 {
			t.Fatal("rejected")
		}
	})
	t.Run("withheld goes to the core", func(t *testing.T) {
		h := newInviteHarness(t)
		h.enable(t)
		h.client.applyRosterItems(true)
		call := h.invite("anonymous", "")
		h.expectHandled(t, "anonymous")
		if len(call.rejected()) != 0 {
			t.Fatal("rejected")
		}
	})
	t.Run("no account or handler rejects as before", func(t *testing.T) {
		h := newInviteHarness(t)
		h.x.handleInviteFn = nil // no manager in the harness
		call := h.invite("+15550001111", "")
		if got := call.rejected(); len(got) != 1 || got[0] != 480 {
			t.Fatalf("rejects %v", got)
		}
	})
}

func TestSetRosterOnly(t *testing.T) {
	h := newInviteHarness(t)
	ul := h.ul()
	h.enable(t)
	if !h.x.loginPrefs(ul).RosterOnly || !callPrefsOf(loginMeta(ul)).RosterOnly || h.saves != 1 {
		t.Fatalf("prefs not applied/saved: saves=%d", h.saves)
	}
	if _, ok := sipCreds(loginMeta(ul)); !ok {
		t.Fatal("SIP credentials lost")
	}
	// A failing save rolls back the metadata and leaves the cache alone.
	h.saveErr = errors.New("db down")
	if err := h.x.setRosterOnly(context.Background(), ul, false); err == nil {
		t.Fatal("expected error")
	}
	if !h.x.loginPrefs(ul).RosterOnly || !callPrefsOf(loginMeta(ul)).RosterOnly {
		t.Fatal("failed save changed the preference")
	}
	h.saveErr = nil
	if err := h.x.setRosterOnly(context.Background(), ul, false); err != nil {
		t.Fatal(err)
	}
	if h.x.loginPrefs(ul).RosterOnly {
		t.Fatal("not turned off")
	}
	if _, ok := loginMeta(ul).Extra[callPrefsExtraKey]; ok {
		t.Fatal("key must be deleted when off")
	}
	// Concurrent setters and gate reads (race detector).
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_ = h.x.setRosterOnly(context.Background(), ul, i%4 == 0)
			} else {
				h.invite("+15550001111", "")
				<-h.handled
			}
		}(i)
	}
	wg.Wait()
}

func TestPrefsLoadedWithLogin(t *testing.T) {
	h := newAccountsHarness(t)
	ul := testLogin(false)
	setCallPrefs(loginMeta(ul), callPrefs{RosterOnly: true})
	h.x.loginLoaded(ul, testClient(ul)) // no runtime, no credentials: early return
	if !h.x.loginPrefs(ul).RosterOnly {
		t.Fatal("prefs not loaded in loginLoaded")
	}
}

func TestReloginKeepsPrefsAndCreds(t *testing.T) {
	ul := testLogin(true)
	setCallPrefs(loginMeta(ul), callPrefs{RosterOnly: true})
	// bridgev2 merges the new login's metadata into the existing one.
	loginMeta(ul).CopyFrom(&JabberLoginMeta{Password: "new-pw"})
	if !callPrefsOf(loginMeta(ul)).RosterOnly {
		t.Fatal("prefs lost on re-login")
	}
	if _, ok := sipCreds(loginMeta(ul)); !ok || loginMeta(ul).Password != "new-pw" {
		t.Fatal("creds lost or password not updated")
	}
}

func TestSetRosterOnlyRollbackKeepsOtherKeys(t *testing.T) {
	h := newInviteHarness(t)
	ul := h.ul()
	saved := saveUserLogin
	t.Cleanup(func() { saveUserLogin = saved })
	// Another Extra key changes while the save is in flight (e.g. a credentials write); the failed save
	// must roll back only call_prefs.
	saveUserLogin = func(_ context.Context, ul *bridgev2.UserLogin) error {
		setExtraKey(loginMeta(ul), "other", json.RawMessage(`1`))
		return errors.New("db down")
	}
	if err := h.x.setRosterOnly(context.Background(), ul, true); err == nil {
		t.Fatal("expected error")
	}
	meta := loginMeta(ul)
	if callPrefsOf(meta).RosterOnly || string(meta.Extra["other"]) != `1` {
		t.Fatalf("rollback: %v", meta.Extra)
	}
	if _, ok := sipCreds(meta); !ok {
		t.Fatal("SIP credentials lost")
	}
}
