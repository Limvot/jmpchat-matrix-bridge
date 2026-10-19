//go:build sipcalls

package connector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
	"mellium.im/xmpp"

	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
)

const (
	wfNumber = "+15550001111"
	wfJID    = "+15550001111@cheogram.com"
)

// wfHarness is a callsExt with a login whose xmpp client has a roster cache.
type wfHarness struct {
	x  *callsExt
	ul *bridgev2.UserLogin
	xc *xmppClient
	f  widgetFeatures
}

func newWFHarness(t *testing.T) *wfHarness {
	t.Helper()
	x, _ := testCallsExt(t, true)
	ul := testLogin(false)
	xc := testClient(ul)
	ul.Client = xc
	return &wfHarness{x: x, ul: ul, xc: xc, f: widgetFeatures{x}}
}

func (h *wfHarness) online() { h.xc.session = &xmpp.Session{} }

func wfActionCode(t *testing.T, err error) string {
	t.Helper()
	var ae *hostkit.ActionError
	if !errors.As(err, &ae) {
		t.Fatalf("not an ActionError: %v", err)
	}
	return ae.Code
}

func TestWidgetFeaturesWiring(t *testing.T) {
	x, _ := testCallsExt(t, true)
	if _, ok := x.widgetDeps(true).Features.(widgetFeatures); !ok {
		t.Fatalf("widgetDeps().Features = %T", x.widgetDeps(true).Features)
	}
	if _, ok := x.widgetDeps(false).Features.(widgetFeatures); !ok {
		t.Fatal("removal deps lost Features")
	}
}

func TestWidgetRosterOnlyState(t *testing.T) {
	h := newWFHarness(t)
	if st := h.f.RosterOnly(h.ul); st.On || st.Warning != "" {
		t.Fatalf("off: %+v", st)
	}
	// The cached prefs win over metadata.
	h.x.accounts = map[networkid.UserLoginID]*loginCalls{h.ul.ID: {prefs: callPrefs{RosterOnly: true}}}
	if st := h.f.RosterOnly(h.ul); !st.On || st.Warning != widgetRosterNotLoaded {
		t.Fatalf("on, not loaded: %+v", st)
	}
	h.xc.applyRosterItems(true, rosterEntry{jid: "alice@example.com", name: "Alice"})
	if st := h.f.RosterOnly(h.ul); !st.On || st.Warning != widgetRosterEmpty {
		t.Fatalf("on, no phone contacts: %+v", st)
	}
	h.xc.applyRosterItems(false, rosterEntry{jid: wfJID})
	if st := h.f.RosterOnly(h.ul); !st.On || st.Warning != "" {
		t.Fatalf("on with a phone contact: %+v", st)
	}
	// Off never warns, even with an empty roster.
	h.x.accounts[h.ul.ID].prefs = callPrefs{}
	h.xc.applyRosterItems(true)
	if st := h.f.RosterOnly(h.ul); st.On || st.Warning != "" {
		t.Fatalf("off: %+v", st)
	}
}

func TestWidgetSetRosterOnly(t *testing.T) {
	h := newWFHarness(t)
	var saves int
	var saveErr error
	saved := saveUserLogin
	saveUserLogin = func(context.Context, *bridgev2.UserLogin) error { saves++; return saveErr }
	t.Cleanup(func() { saveUserLogin = saved })
	h.xc.applyRosterItems(true, rosterEntry{jid: wfJID})

	st, err := h.f.SetRosterOnly(context.Background(), h.ul, true)
	if err != nil || !st.On || saves != 1 || !callPrefsOf(loginMeta(h.ul)).RosterOnly {
		t.Fatalf("on: %+v %v saves=%d", st, err, saves)
	}
	saveErr = errors.New("db down")
	st, err = h.f.SetRosterOnly(context.Background(), h.ul, false)
	if err == nil || st.On {
		t.Fatalf("save error: %+v %v", st, err)
	}
	if !callPrefsOf(loginMeta(h.ul)).RosterOnly || !h.f.RosterOnly(h.ul).On {
		t.Fatal("state changed despite the save error")
	}
}

func TestWidgetContactName(t *testing.T) {
	h := newWFHarness(t)
	h.xc.applyRosterItems(true,
		rosterEntry{jid: wfJID, name: "Alice"},
		rosterEntry{jid: "+15550002222@cheogram.com"})
	if n, ok := h.f.ContactName(h.ul, wfNumber); n != "Alice" || !ok {
		t.Errorf("named: %q %v", n, ok)
	}
	if n, ok := h.f.ContactName(h.ul, "+15550002222"); n != "" || !ok {
		t.Errorf("unnamed: %q %v", n, ok)
	}
	if n, ok := h.f.ContactName(h.ul, "+15550003333"); n != "" || ok {
		t.Errorf("absent: %q %v", n, ok)
	}
	other := testLogin(false)
	other.Client = &stubJabberClient{}
	if n, ok := h.f.ContactName(other, wfNumber); n != "" || ok {
		t.Errorf("non-xmpp login: %q %v", n, ok)
	}
}

func TestWidgetSaveContact(t *testing.T) {
	h := newWFHarness(t)
	type call struct{ jid, name string }
	var calls []call
	var setErr error
	h.x.setRoster = func(_ context.Context, _ *xmppClient, jid, name string) error {
		calls = append(calls, call{jid, name})
		return setErr
	}
	ctx := context.Background()

	if _, err := h.f.SaveContact(ctx, h.ul, "12345", "Zed"); wfActionCode(t, err) != hostkit.CodeInvalidNumber {
		t.Errorf("invalid number: %v", err)
	}
	if _, err := h.f.SaveContact(ctx, h.ul, wfNumber, "\x00\x01 \n"); wfActionCode(t, err) != hostkit.CodeInvalidName {
		t.Errorf("control-only name: %v", err)
	}
	// Not logged in to XMPP: the exact save-contact text.
	_, err := h.f.SaveContact(ctx, h.ul, wfNumber, "Zed")
	var ae *hostkit.ActionError
	if !errors.As(err, &ae) || ae.Code != hostkit.CodeUnavailable ||
		ae.Message != "Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again." {
		t.Errorf("offline: %v", err)
	}
	// A non-XMPP login.
	other := testLogin(false)
	other.Client = &stubJabberClient{}
	if _, err = h.f.SaveContact(ctx, other, wfNumber, "Zed"); wfActionCode(t, err) != hostkit.CodeUnavailable {
		t.Errorf("non-xmpp: %v", err)
	}
	if len(calls) != 0 {
		t.Fatal("roster set before checks passed")
	}

	h.online()
	h.xc.applyRosterItems(true)
	sc, err := h.f.SaveContact(ctx, h.ul, wfNumber, "  Zed  ")
	if err != nil || !sc.Changed || sc.Name != "Zed" || sc.PreviousName != "" || len(calls) != 1 || calls[0] != (call{wfJID, "Zed"}) {
		t.Fatalf("new: %+v %v %v", sc, err, calls)
	}
	// Already saved under exactly that name: nothing is sent.
	h.xc.applyRosterItems(false, rosterEntry{jid: wfJID, name: "Zed"})
	sc, err = h.f.SaveContact(ctx, h.ul, wfNumber, "Zed")
	if err != nil || sc.Changed || sc.PreviousName != "Zed" || len(calls) != 1 {
		t.Fatalf("unchanged: %+v %v %v", sc, err, calls)
	}
	// Rename.
	sc, err = h.f.SaveContact(ctx, h.ul, wfNumber, "Yan")
	if err != nil || !sc.Changed || sc.PreviousName != "Zed" || len(calls) != 2 || calls[1].name != "Yan" {
		t.Fatalf("rename: %+v %v %v", sc, err, calls)
	}
	// An unnamed roster entry still gets the name.
	h.xc.applyRosterItems(false, rosterEntry{jid: wfJID})
	if sc, err = h.f.SaveContact(ctx, h.ul, wfNumber, "Zed"); err != nil || !sc.Changed || sc.PreviousName != "" {
		t.Fatalf("unnamed entry: %+v %v", sc, err)
	}
	// Errors: deadline passes through, others are wrapped.
	setErr = context.DeadlineExceeded
	if _, err = h.f.SaveContact(ctx, h.ul, wfNumber, "Xi"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("deadline: %v", err)
	}
	setErr = errors.New("boom")
	if _, err = h.f.SaveContact(ctx, h.ul, wfNumber, "Xi"); err == nil || !errors.Is(err, setErr) || errors.As(err, &ae) {
		t.Errorf("generic: %v", err)
	}
}

type backfillCall struct {
	jid string
}

func TestWidgetOpenChat(t *testing.T) {
	ctx := context.Background()
	t.Run("created room backfills once", func(t *testing.T) {
		h := newWFHarness(t)
		h.x.openChatRoom = func(_ context.Context, ref core.LoginRef, e164 string) (*core.CallRoom, error) {
			if ref != core.LoginRef(h.ul.ID) || e164 != wfNumber {
				t.Errorf("open(%q, %q)", ref, e164)
			}
			return &core.CallRoom{RoomID: id.RoomID("!new:example.com"), Created: true}, nil
		}
		got := make(chan backfillCall, 2)
		h.x.backfillChat = func(_ context.Context, xc *xmppClient, jid string) {
			if xc != h.xc {
				t.Error("backfill for another client")
			}
			got <- backfillCall{jid}
		}
		room, err := h.f.OpenChat(ctx, h.ul, wfNumber)
		if err != nil || room.RoomID != "!new:example.com" || !room.Created || room.Pending {
			t.Fatalf("%+v %v", room, err)
		}
		select {
		case b := <-got:
			if b.jid != wfJID {
				t.Errorf("backfill jid %q", b.jid)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no backfill")
		}
	})
	t.Run("existing room does not backfill", func(t *testing.T) {
		h := newWFHarness(t)
		h.x.openChatRoom = func(context.Context, core.LoginRef, string) (*core.CallRoom, error) {
			return &core.CallRoom{RoomID: "!old:example.com"}, nil
		}
		var n int
		var mu sync.Mutex
		h.x.backfillChat = func(context.Context, *xmppClient, string) { mu.Lock(); n++; mu.Unlock() }
		room, err := h.f.OpenChat(ctx, h.ul, wfNumber)
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		if err != nil || room.Created || room.RoomID != "!old:example.com" || n != 0 {
			t.Fatalf("%+v %v backfills=%d", room, err, n)
		}
	})
	t.Run("deadline gives pending and creation finishes", func(t *testing.T) {
		h := newWFHarness(t)
		release := make(chan struct{})
		finished := make(chan struct{})
		h.x.openChatRoom = func(cctx context.Context, _ core.LoginRef, _ string) (*core.CallRoom, error) {
			<-release
			if cctx.Err() != nil {
				t.Errorf("creation context cancelled with the request: %v", cctx.Err())
			}
			close(finished)
			return &core.CallRoom{RoomID: "!slow:example.com", Created: true}, nil
		}
		backfilled := make(chan struct{})
		h.x.backfillChat = func(context.Context, *xmppClient, string) { close(backfilled) }
		rctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		room, err := h.f.OpenChat(rctx, h.ul, wfNumber)
		if err != nil || !room.Pending || room.RoomID != "" {
			t.Fatalf("%+v %v", room, err)
		}
		close(release)
		for _, ch := range []chan struct{}{finished, backfilled} {
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatal("background creation did not complete")
			}
		}
	})
	t.Run("error is wrapped", func(t *testing.T) {
		h := newWFHarness(t)
		boom := errors.New("boom")
		h.x.openChatRoom = func(context.Context, core.LoginRef, string) (*core.CallRoom, error) { return nil, boom }
		h.x.backfillChat = func(context.Context, *xmppClient, string) { t.Error("backfill after an error") }
		var ae *hostkit.ActionError
		if _, err := h.f.OpenChat(ctx, h.ul, wfNumber); !errors.Is(err, boom) || errors.As(err, &ae) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("invalid number and non-xmpp login", func(t *testing.T) {
		h := newWFHarness(t)
		if _, err := h.f.OpenChat(ctx, h.ul, "611"); wfActionCode(t, err) != hostkit.CodeInvalidNumber {
			t.Errorf("invalid: %v", err)
		}
		other := testLogin(false)
		other.Client = &stubJabberClient{}
		if _, err := h.f.OpenChat(ctx, other, wfNumber); wfActionCode(t, err) != hostkit.CodeUnavailable {
			t.Errorf("non-xmpp: %v", err)
		}
	})
}
