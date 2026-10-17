//go:build sipcalls

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/mrtc-sip-bridge/pkg/calls"
	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
	"github.com/williamkray/mrtc-sip-bridge/pkg/rtc"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipaccount"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipua"
)

// eventLog records lifecycle events in order.
type eventLog struct {
	mu  sync.Mutex
	evs []string
}

func (l *eventLog) add(s string) {
	l.mu.Lock()
	l.evs = append(l.evs, s)
	l.mu.Unlock()
}

func (l *eventLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.evs...)
}

// fakeUA is a sipua.UA driven by the test.
type fakeUA struct {
	n        int
	log      *eventLog
	startErr error

	mu      sync.Mutex
	cfg     sipua.Config
	stopped bool
}

func (f *fakeUA) Start(context.Context) error {
	if f.startErr != nil {
		f.log.add(fmt.Sprintf("startfail%d", f.n))
		return f.startErr
	}
	f.log.add(fmt.Sprintf("start%d", f.n))
	return nil
}

func (f *fakeUA) Stop(context.Context) error {
	f.mu.Lock()
	f.stopped = true
	f.mu.Unlock()
	f.log.add(fmt.Sprintf("stop%d", f.n))
	return nil
}

func (f *fakeUA) State() sipua.StateEvent { return sipua.StateEvent{} }

func (f *fakeUA) Dial(sipua.DialRequest) (sipua.OutboundCall, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeUA) isStopped() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped
}

// emit reports a state change like the UA goroutine does.
func (f *fakeUA) emit(ev sipua.StateEvent) {
	f.mu.Lock()
	cb := f.cfg.OnStateChange
	f.mu.Unlock()
	cb(ev)
}

// uaFactory creates fake UAs; failFirst makes that many creations fail to start.
type uaFactory struct {
	log       *eventLog
	mu        sync.Mutex
	uas       []*fakeUA
	failFirst int
}

func (u *uaFactory) New(cfg sipua.Config) (sipua.UA, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	f := &fakeUA{n: len(u.uas) + 1, log: u.log, cfg: cfg}
	if len(u.uas) < u.failFirst {
		f.startErr = errors.New("bind: address in use")
	}
	u.uas = append(u.uas, f)
	return f, nil
}

func (u *uaFactory) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.uas)
}

func (u *uaFactory) get(i int) *fakeUA {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.uas[i]
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

var testSIPCreds = sipaccount.Credentials{
	Username: "12345678", Password: "pw-secret", Registrar: "reg.example.com:5060",
	Transport: "udp", DisplayNumber: "+15550001111",
}

func testRuntime(t *testing.T, size int) *sipaccount.Runtime {
	t.Helper()
	rt, err := sipaccount.NewRuntime(context.Background(), sipaccount.NetConfig{
		SIPBindIP: "127.0.0.1", ExternalIP: "127.0.0.1",
		SIPPortMin: 15060, SIPPortMax: 15060 + size - 1, RTPPortMin: 41000, RTPPortMax: 41100,
		RegisterExpiry: 60 * time.Second, RegisterRefresh: 25 * time.Second,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	return rt
}

// accountsHarness is a calls extension with fake UAs, a recorded EndLogin and recorded notices.
type accountsHarness struct {
	x       *callsExt
	ua      *uaFactory
	log     *eventLog
	notices chan string
}

func newAccountsHarness(t *testing.T) *accountsHarness {
	t.Helper()
	x, _ := testCallsExt(t, true)
	x.enabled = true
	x.cfg.ExternalIP = "127.0.0.1"
	x.cfg.STUNServer = ""
	h := &accountsHarness{x: x, log: &eventLog{}, notices: make(chan string, 8)}
	h.ua = &uaFactory{log: h.log}
	x.newUA = h.ua.New
	x.endLoginFn = func(_ context.Context, ref core.LoginRef) { h.log.add("end:" + string(ref)) }
	x.notifyFn = func(_ context.Context, roomID id.RoomID, text string) error {
		h.notices <- string(roomID) + " " + text
		return nil
	}
	x.listPortals = func(context.Context) ([]*bridgev2.Portal, error) { return nil, nil }
	x.retryDelay = 10 * time.Millisecond
	x.newRuntime = func(context.Context, sipaccount.NetConfig, zerolog.Logger) (*sipaccount.Runtime, error) {
		return testRuntime(t, 4), nil
	}
	x.newCallStack = func(*bridgev2.Bridge, *hostkit.Config, hostkit.CallStackDeps) (*calls.Manager, *rtc.ServiceURLResolver) {
		return nil, nil
	}
	return h
}

func (h *accountsHarness) startRuntime(t *testing.T) {
	t.Helper()
	if err := h.x.start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func testLogin(withCreds bool) *bridgev2.UserLogin {
	meta := &JabberLoginMeta{Password: "xmpp-pw"}
	if withCreds {
		c := testSIPCreds
		setSIPCreds(meta, &c)
	}
	return &bridgev2.UserLogin{
		UserLogin: &database.UserLogin{ID: "me@example.com", UserMXID: "@me:example.com", Metadata: meta},
		User:      &bridgev2.User{User: &database.User{MXID: "@me:example.com", ManagementRoom: "!mgmt:example.com"}},
	}
}

func testClient(ul *bridgev2.UserLogin) *xmppClient {
	return &xmppClient{loginID: ul.ID, login: ul}
}

func TestAccountAttachedBeforeRuntimeStartedByStart(t *testing.T) {
	h := newAccountsHarness(t)
	ul := testLogin(true)
	h.x.loginLoaded(ul, testClient(ul))
	if h.ua.count() != 0 || h.x.hasAccount(ul.ID) {
		t.Fatal("account started before the runtime existed")
	}
	// A login without credentials stays without an account.
	other := testLogin(false)
	other.ID = "other@example.com"
	h.x.loginLoaded(other, testClient(other))

	h.startRuntime(t)
	waitFor(t, "UA start", func() bool { return h.ua.count() == 1 && h.x.account(ul.ID).Running() })
	if !h.x.hasAccount(ul.ID) || h.x.hasAccount(other.ID) {
		t.Fatal("wrong accounts attached")
	}
	if n := h.x.loginNumber(core.LoginRef(ul.ID)); n != testSIPCreds.DisplayNumber {
		t.Fatalf("loginNumber = %q", n)
	}
	if _, err := h.x.dial(core.LoginRef(other.ID), sipua.DialRequest{}); !errors.Is(err, sipua.ErrNotRegistered) {
		t.Fatalf("dial without account: %v", err)
	}
}

func TestClientReplacementTakesOverAccount(t *testing.T) {
	h := newAccountsHarness(t)
	h.startRuntime(t)
	ul := testLogin(true)
	c1, c2 := testClient(ul), testClient(ul)
	h.x.loginLoaded(ul, c1)
	waitFor(t, "UA start", func() bool { return h.ua.count() == 1 && h.x.account(ul.ID).Running() })

	h.x.loginLoaded(ul, c2)
	h.x.loginStopped(c1) // old client of a replaced login: no-op
	time.Sleep(20 * time.Millisecond)
	if h.ua.count() != 1 || h.ua.get(0).isStopped() || !h.x.hasAccount(ul.ID) {
		t.Fatalf("takeover broke the account: uas=%d events=%v", h.ua.count(), h.log.list())
	}

	h.x.loginStopped(c2)
	waitFor(t, "teardown", func() bool { return len(h.log.list()) == 3 })
	h.x.teardown.Wait()
	if got, want := h.log.list(), []string{"start1", "end:me@example.com", "stop1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events %v, want %v", got, want)
	}
	if h.x.hasAccount(ul.ID) {
		t.Fatal("account still attached after the owner stopped")
	}
	h.x.loginStopped(c2) // second stop: no-op
	if n := len(h.log.list()); n != 3 {
		t.Fatalf("second stop did something: %v", h.log.list())
	}
}

// TestReloginStopsOldBeforeNew follows bridgev2's NewLogin for an existing login: the old client is
// disconnected first, then the same *UserLogin gets new metadata (without Extra) and a new client.
func TestReloginStopsOldBeforeNew(t *testing.T) {
	h := newAccountsHarness(t)
	h.startRuntime(t)
	ul := testLogin(true)
	c1 := testClient(ul)
	h.x.loginLoaded(ul, c1)
	waitFor(t, "UA 1 start", func() bool { return h.ua.count() == 1 && h.x.account(ul.ID).Running() })

	h.x.loginStopped(c1)
	ul.Metadata = &JabberLoginMeta{Password: "new-xmpp-pw"}
	c2 := testClient(ul)
	h.x.loginLoaded(ul, c2)
	waitFor(t, "UA 2 start", func() bool { return h.ua.count() == 2 && h.x.account(ul.ID).Running() })
	h.x.teardown.Wait()
	// The old account is torn down asynchronously (bridgev2 holds its login cache lock in Disconnect), so
	// start2 may interleave with the old account's end/stop, which stay ordered.
	evs := h.log.list()
	idx := map[string]int{}
	for i, e := range evs {
		idx[e] = i
	}
	if len(evs) != 4 || !(idx["start1"] < idx["end:me@example.com"] && idx["end:me@example.com"] < idx["stop1"]) {
		t.Fatalf("events %v", evs)
	}
	if _, ok := idx["start2"]; !ok {
		t.Fatalf("events %v: no start2", evs)
	}
	creds, ok := sipCreds(loginMeta(ul))
	if !ok || *creds != testSIPCreds {
		t.Fatal("SIP credentials lost on re-login")
	}

	// A different login object with the same ID (logout, then a fresh login) does not inherit them.
	h.x.loginStopped(c2)
	h.x.teardown.Wait()
	fresh := testLogin(false)
	h.x.loginLoaded(fresh, testClient(fresh))
	if _, ok = sipCreds(loginMeta(fresh)); ok || h.x.hasAccount(fresh.ID) {
		t.Fatal("fresh login inherited SIP credentials")
	}
}

func TestAttachAccountReplacesPrevious(t *testing.T) {
	h := newAccountsHarness(t)
	h.startRuntime(t)
	ul := testLogin(true)
	ul.Client = testClient(ul)
	h.x.loginLoaded(ul, ul.Client.(*xmppClient))
	waitFor(t, "UA 1 start", func() bool { return h.ua.count() == 1 && h.x.account(ul.ID).Running() })

	// sip-login flow: stop the previous account first, then register and attach the new one.
	if !h.x.detachAccount(context.Background(), ul.ID) {
		t.Fatal("detach found no account")
	}
	if h.x.hasAccount(ul.ID) {
		t.Fatal("account still attached after detach")
	}
	acct := h.x.newLoginAccount(ul, testSIPCreds)
	if err := acct.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !h.x.attachAccount(ul, acct) {
		t.Fatal("attach refused")
	}
	if got, want := h.log.list(), []string{"start1", "end:me@example.com", "stop1", "start2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events %v, want %v", got, want)
	}
	if h.x.account(ul.ID) != acct {
		t.Fatal("new account not attached")
	}
	if h.x.detachAccount(context.Background(), "nobody@example.com") {
		t.Fatal("detach of an unknown login reported an account")
	}
}

// TestLoginStoppedReturnsQuickly: Disconnect runs under bridgev2's login cache lock, so the account
// teardown (EndLogin, Stop) must not block loginStopped.
func TestLoginStoppedReturnsQuickly(t *testing.T) {
	h := newAccountsHarness(t)
	h.startRuntime(t)
	release := make(chan struct{})
	h.x.endLoginFn = func(ctx context.Context, ref core.LoginRef) { <-release; h.log.add("end:" + string(ref)) }
	ul := testLogin(true)
	c := testClient(ul)
	h.x.loginLoaded(ul, c)
	waitFor(t, "UA start", func() bool { return h.ua.count() == 1 && h.x.account(ul.ID).Running() })
	done := make(chan struct{})
	go func() { h.x.loginStopped(c); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loginStopped blocked on the teardown")
	}
	if h.x.hasAccount(ul.ID) {
		t.Fatal("account still attached")
	}
	close(release)
	h.x.teardown.Wait()
	if got := h.log.list(); len(got) != 3 || got[2] != "stop1" {
		t.Fatalf("events %v", got)
	}
}

// TestStopWaitsForTeardown: stop waits for in-flight teardowns before closing the runtime.
func TestStopWaitsForTeardown(t *testing.T) {
	h := newAccountsHarness(t)
	h.startRuntime(t)
	release := make(chan struct{})
	h.x.endLoginFn = func(context.Context, core.LoginRef) { <-release }
	ul := testLogin(true)
	c := testClient(ul)
	h.x.loginLoaded(ul, c)
	waitFor(t, "UA start", func() bool { return h.ua.count() == 1 && h.x.account(ul.ID).Running() })
	h.x.loginStopped(c)
	stopped := make(chan struct{})
	go func() { h.x.stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop did not wait for the teardown")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not finish")
	}
	if !h.ua.get(0).isStopped() {
		t.Fatal("UA not stopped")
	}
}

func TestAttachAccountRefusals(t *testing.T) {
	h := newAccountsHarness(t)
	h.startRuntime(t)
	ul := testLogin(false)
	ul.Client = testClient(ul)
	mk := func() *sipaccount.Account { return h.x.newLoginAccount(ul, testSIPCreds) }
	// No loginCalls entry (login not loaded / already stopped): refused, none created.
	if h.x.attachAccount(ul, mk()) || h.x.accounts[ul.ID] != nil {
		t.Fatal("attached without a loaded login")
	}
	// Loaded with another client than ul.Client (replaced meanwhile).
	h.x.loginLoaded(ul, testClient(ul))
	if h.x.attachAccount(ul, mk()) || h.x.hasAccount(ul.ID) {
		t.Fatal("attached for a stale client")
	}
	// Matching client: accepted; after stop(): refused.
	h.x.loginLoaded(ul, ul.Client.(*xmppClient))
	if !h.x.attachAccount(ul, mk()) {
		t.Fatal("matching client refused")
	}
	h.x.stop()
	if h.x.attachAccount(ul, mk()) {
		t.Fatal("attached after stop")
	}
}

func TestStartRetry(t *testing.T) {
	h := newAccountsHarness(t)
	h.ua.failFirst = 4 // every port of the 4-port pool fails on the first attempt
	h.startRuntime(t)
	ul := testLogin(true)
	h.x.loginLoaded(ul, testClient(ul))
	waitFor(t, "retry start", func() bool { a := h.x.account(ul.ID); return a != nil && a.Running() })
	if n := h.ua.count(); n != 5 {
		t.Fatalf("%d UAs created, want 4 failed + 1 retry", n)
	}
}

func TestCredsRoundTripThroughExtra(t *testing.T) {
	meta := &JabberLoginMeta{Password: "x", Extra: map[string]json.RawMessage{"other": json.RawMessage(`{"a":1}`)}}
	c := testSIPCreds
	setSIPCreds(meta, &c)
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	var back JabberLoginMeta
	if err = json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	got, ok := sipCreds(&back)
	if !ok || *got != testSIPCreds {
		t.Fatalf("round trip: %+v %v", got, ok)
	}
	if string(back.Extra["other"]) != `{"a":1}` {
		t.Fatalf("other Extra key lost: %s", back.Extra["other"])
	}
	setSIPCreds(&back, nil)
	if _, ok = back.Extra["sip"]; ok || len(back.Extra) != 1 {
		t.Fatalf("delete: %v", back.Extra)
	}
	delete(back.Extra, "other")
	setSIPCreds(&back, &c)
	setSIPCreds(&back, nil)
	if back.Extra != nil {
		t.Fatal("empty Extra not removed")
	}

	incomplete := &JabberLoginMeta{}
	setSIPCreds(incomplete, &sipaccount.Credentials{Username: "u"})
	if _, ok = sipCreds(incomplete); ok {
		t.Fatal("incomplete credentials accepted")
	}
	bad := &JabberLoginMeta{Extra: map[string]json.RawMessage{"sip": json.RawMessage(`"nope"`)}}
	if _, ok = sipCreds(bad); ok {
		t.Fatal("invalid credentials accepted")
	}
	if _, ok = sipCreds(nil); ok {
		t.Fatal("nil meta")
	}
	setSIPCreds(nil, &c) // no panic
	if strings.Contains(string(raw), "pw-secret") == false {
		t.Fatal("credentials not stored in the metadata JSON")
	}
}

func TestAuthFailedNoticeOnce(t *testing.T) {
	h := newAccountsHarness(t)
	h.startRuntime(t)
	ul := testLogin(true)
	h.x.loginLoaded(ul, testClient(ul))
	waitFor(t, "UA start", func() bool { return h.ua.count() == 1 && h.x.account(ul.ID).Running() })
	ua := h.ua.get(0)

	expectNotice := func() {
		t.Helper()
		select {
		case n := <-h.notices:
			want := "!mgmt:example.com SIP registration for +15550001111 was rejected (SIP 403). Fix the password with `!jc sip-login`."
			if n != want {
				t.Fatalf("notice %q, want %q", n, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no notice")
		}
	}
	expectNone := func() {
		t.Helper()
		select {
		case n := <-h.notices:
			t.Fatalf("unexpected notice %q", n)
		case <-time.After(30 * time.Millisecond):
		}
	}
	authFailed := sipua.StateEvent{State: sipua.StateAuthFailed, StatusCode: 403, Reason: "Forbidden"}
	ua.emit(authFailed)
	expectNotice()
	ua.emit(sipua.StateEvent{State: sipua.StateTransientError})
	ua.emit(authFailed)
	expectNone()
	ua.emit(sipua.StateEvent{State: sipua.StateRegistered})
	ua.emit(authFailed)
	expectNotice()
}

func TestPLSweepFilters(t *testing.T) {
	x, _ := testCallsExt(t, true)
	x.enabled = true
	const me networkid.UserLoginID = "me@example.com"
	x.listPortals = func(context.Context) ([]*bridgev2.Portal, error) {
		return []*bridgev2.Portal{
			testPortal("+15550000001@cheogram.com", me, database.RoomTypeDM, "!a:x"),        // updated
			testPortal("+15550000002@cheogram.com", me, database.RoomTypeDefault, "!b:x"),   // unchanged
			testPortal("+15550000003@cheogram.com", me, database.RoomTypeDM, "!fail:x"),     // fails
			testPortal("+15550000004@cheogram.com", "other@x", database.RoomTypeDM, "!c:x"), // other login
			testPortal("+15550000005@cheogram.com", me, database.RoomTypeGroupDM, "!d:x"),   // group DM
			testPortal("+15550000001,+15550000002@cheogram.com", me, database.RoomTypeDM, "!e:x"),
			testPortal("room@conference.example.com", me, database.RoomTypeDefault, "!f:x"),
			testPortal("bob@example.com", me, database.RoomTypeDM, "!g:x"),
			testPortal("+15550000006@cheogram.com", me, database.RoomTypeDM, ""), // no room yet
		}, nil
	}
	var mu sync.Mutex
	var seen []id.RoomID
	x.ensurePL = func(_ context.Context, roomID id.RoomID) (bool, error) {
		mu.Lock()
		seen = append(seen, roomID)
		mu.Unlock()
		switch roomID {
		case "!a:x":
			return true, nil
		case "!fail:x":
			return false, errors.New("forbidden")
		}
		return false, nil
	}
	checked, updated, failed, err := x.runPLSweep(context.Background(), me)
	if err != nil || checked != 3 || updated != 1 || failed != 1 {
		t.Fatalf("checked=%d updated=%d failed=%d err=%v", checked, updated, failed, err)
	}
	if want := []id.RoomID{"!a:x", "!b:x", "!fail:x"}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("rooms %v, want %v", seen, want)
	}
}

// TestStartPassesCallsKeyPrefix: runtime errors name the jabber config path (network.calls.*).
func TestStartPassesCallsKeyPrefix(t *testing.T) {
	h := newAccountsHarness(t)
	var got string
	h.x.newRuntime = func(_ context.Context, cfg sipaccount.NetConfig, _ zerolog.Logger) (*sipaccount.Runtime, error) {
		got = cfg.KeyPrefix
		return testRuntime(t, 4), nil
	}
	h.startRuntime(t)
	if got != "network.calls." {
		t.Fatalf("KeyPrefix %q", got)
	}
}
