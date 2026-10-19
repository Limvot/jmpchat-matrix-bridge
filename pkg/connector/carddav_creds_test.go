package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

func cardDAVTestLogin() *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{
		ID:       "me@example.com",
		Metadata: &JabberLoginMeta{Password: "jabber-pw"},
	}}
}

func swapSaveUserLogin(t *testing.T, fn func(context.Context, *bridgev2.UserLogin) error) {
	t.Helper()
	saved := saveUserLogin
	saveUserLogin = fn
	t.Cleanup(func() { saveUserLogin = saved })
}

func TestCardDAVCredsRoundTrip(t *testing.T) {
	saves := 0
	swapSaveUserLogin(t, func(context.Context, *bridgev2.UserLogin) error { saves++; return nil })
	ul := cardDAVTestLogin()
	if _, ok := cardDAVCredsOf(loginMeta(ul)); ok {
		t.Fatal("creds before set")
	}
	in := &cardDAVCreds{URL: "https://dav.example.org/", Username: "alice", Password: "s3cret", Books: []string{"https://dav.example.org/alice/a/"}}
	if err := saveCardDAVCreds(context.Background(), ul, in); err != nil {
		t.Fatal(err)
	}
	got, ok := cardDAVCredsOf(loginMeta(ul))
	if !ok || got.V != 1 || got.URL != in.URL || got.Username != "alice" || got.Password != "s3cret" || len(got.Books) != 1 || saves != 1 {
		t.Fatalf("got %+v ok=%v saves=%d", got, ok, saves)
	}
	if err := saveCardDAVCreds(context.Background(), ul, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok = cardDAVCredsOf(loginMeta(ul)); ok || loginMeta(ul).Extra != nil {
		t.Fatalf("creds not removed: %v", loginMeta(ul).Extra)
	}
	// Incomplete or invalid blobs are ignored.
	for _, raw := range []string{`{"v":1,"url":"https://x"}`, `junk`} {
		m := &JabberLoginMeta{Extra: map[string]json.RawMessage{cardDAVExtraKey: json.RawMessage(raw)}}
		if _, ok = cardDAVCredsOf(m); ok {
			t.Errorf("accepted %q", raw)
		}
	}
}

func TestCardDAVCredsSaveErrorRestoresOnlyItsKey(t *testing.T) {
	ul := cardDAVTestLogin()
	setExtraKey(loginMeta(ul), "other", json.RawMessage(`1`))
	boom := errors.New("db down")
	swapSaveUserLogin(t, func(_ context.Context, ul *bridgev2.UserLogin) error {
		// A concurrent writer of another key lands during our save; it must survive the rollback.
		setExtraKey(loginMeta(ul), "late", json.RawMessage(`2`))
		return boom
	})
	err := saveCardDAVCreds(context.Background(), ul, &cardDAVCreds{URL: "https://x", Username: "u", Password: "p"})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	extra := loginMeta(ul).Extra
	if _, ok := extra[cardDAVExtraKey]; ok || string(extra["other"]) != "1" || string(extra["late"]) != "2" {
		t.Fatalf("rollback wrong: %v", extra)
	}
}

func TestCardDAVCredsSurviveReLogin(t *testing.T) {
	ul := cardDAVTestLogin()
	swapSaveUserLogin(t, func(context.Context, *bridgev2.UserLogin) error { return nil })
	if err := saveCardDAVCreds(context.Background(), ul, &cardDAVCreds{URL: "https://x", Username: "u", Password: "p"}); err != nil {
		t.Fatal(err)
	}
	loginMeta(ul).CopyFrom(&JabberLoginMeta{Password: "new-pw"})
	if _, ok := cardDAVCredsOf(loginMeta(ul)); !ok || loginMeta(ul).Password != "new-pw" {
		t.Fatalf("carddav creds lost on re-login: %+v", loginMeta(ul))
	}
}

// Concurrent writers of different Extra keys (CardDAV and the SIP/call-prefs ones) must not lose
// each other's key: every writer clones the map, sets one key and swaps the pointer.
func TestSetLoginExtraKeyConcurrentWriters(t *testing.T) {
	swapSaveUserLogin(t, func(context.Context, *bridgev2.UserLogin) error { return nil })
	ul := cardDAVTestLogin()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = saveCardDAVCreds(context.Background(), ul, &cardDAVCreds{URL: "https://x", Username: "u", Password: "p"})
		}()
		go func(i int) {
			defer wg.Done()
			// The tagged setters (sip-login, roster-only-calls) use the same helper/lock pair.
			_ = setLoginExtraKey(context.Background(), ul, "call_prefs", json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)))
		}(i)
	}
	wg.Wait()
	extra := loginMeta(ul).Extra
	if _, ok := extra[cardDAVExtraKey]; !ok {
		t.Fatalf("carddav key lost: %v", extra)
	}
	if _, ok := extra["call_prefs"]; !ok {
		t.Fatalf("call_prefs key lost: %v", extra)
	}
}
