//go:build sipcalls

package connector

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/mautrix/jabber/pkg/telemetry"
	"github.com/williamkray/mrtc-sip-bridge/pkg/calls"
	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
	"github.com/williamkray/mrtc-sip-bridge/pkg/obs"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipua"
)

// cmdBot records the bot's replies and redactions.
type cmdBot struct {
	bridgev2.MatrixAPI
	mu      sync.Mutex
	replies []string
	redact  []id.EventID
}

func (b *cmdBot) SendMessage(_ context.Context, _ id.RoomID, typ event.Type, content *event.Content, _ *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch c := content.Parsed.(type) {
	case *event.MessageEventContent:
		b.replies = append(b.replies, c.Body)
	case *event.RedactionEventContent:
		b.redact = append(b.redact, c.Redacts)
	}
	return &mautrix.RespSendEvent{EventID: "$reply"}, nil
}

func (b *cmdBot) last() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.replies) == 0 {
		return ""
	}
	return b.replies[len(b.replies)-1]
}

func (b *cmdBot) all() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Join(b.replies, "\n")
}

func (b *cmdBot) redacted(e id.EventID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.redact {
		if r == e {
			return true
		}
	}
	return false
}

// lockedBuf is a bytes.Buffer safe for concurrent log writes.
type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// scriptedFactory wraps the fake UA factory so that each UA reports the next scripted registration
// result right after its Start (nil = never answers; newErr = UA creation fails).
type scriptedFactory struct {
	*uaFactory
	mu      sync.Mutex
	results []*sipua.StateEvent
	n       int
	newErr  error
}

type emitUA struct {
	*fakeUA
	ev *sipua.StateEvent
}

func (e *emitUA) Start(ctx context.Context) error {
	if err := e.fakeUA.Start(ctx); err != nil {
		return err
	}
	if e.ev != nil {
		go e.fakeUA.emit(*e.ev)
	}
	return nil
}

func (s *scriptedFactory) New(cfg sipua.Config) (sipua.UA, error) {
	s.mu.Lock()
	var ev *sipua.StateEvent
	if s.n < len(s.results) {
		ev = s.results[s.n]
	}
	s.n++
	newErr := s.newErr
	s.mu.Unlock()
	if newErr != nil {
		return nil, newErr
	}
	ua, err := s.uaFactory.New(cfg)
	if err != nil {
		return nil, err
	}
	return &emitUA{fakeUA: ua.(*fakeUA), ev: ev}, nil
}

var (
	evRegistered = &sipua.StateEvent{State: sipua.StateRegistered}
	evAuthFailed = &sipua.StateEvent{State: sipua.StateAuthFailed, StatusCode: 403, Reason: "Forbidden"}
	evTransient  = &sipua.StateEvent{State: sipua.StateTransientError, Reason: "no route to host"}
)

const (
	testSIPPassword = "hunter2-secret-pw"
	testNewUser     = "87654321"
)

// cmdHarness runs the sip commands against a fake runtime and fake UAs.
type cmdHarness struct {
	*accountsHarness
	sf    *scriptedFactory
	bot   *cmdBot
	user  *bridgev2.User
	ul    *bridgev2.UserLogin
	logs  *lockedBuf
	zlog  zerolog.Logger
	saves int
	saveE error
	proc  *commands.Processor
	evSeq int
}

func newCmdHarness(t *testing.T, withCreds bool, results ...*sipua.StateEvent) *cmdHarness {
	t.Helper()
	ah := newAccountsHarness(t)
	h := &cmdHarness{accountsHarness: ah, bot: &cmdBot{}, logs: &lockedBuf{}}
	h.zlog = zerolog.New(h.logs)
	h.sf = &scriptedFactory{uaFactory: ah.ua, results: results}
	h.x.newUA = h.sf.New
	h.x.cfg.DefaultCountryCode = "1"
	h.x.br.Log = h.zlog
	h.x.log = h.zlog
	h.ul = testLogin(withCreds)
	h.ul.Client = testClient(h.ul)
	h.x.accounts[h.ul.ID] = &loginCalls{id: h.ul.ID, client: h.ul.Client.(*xmppClient)} // ul stays nil so start() does not start it
	h.user = &bridgev2.User{User: &database.User{MXID: h.ul.UserMXID, ManagementRoom: "!mgmt:example.com"}}
	h.x.br.Config = &bridgeconfig.BridgeConfig{CommandPrefix: "!jc"}

	savedLookup, savedSave, savedWait := lookupCommandLogin, saveUserLogin, sipRegisterTimeout
	lookupCommandLogin = func(*commands.Event) *bridgev2.UserLogin { return h.ul }
	saveUserLogin = func(context.Context, *bridgev2.UserLogin) error { h.saves++; return h.saveE }
	sipRegisterTimeout = 200 * time.Millisecond
	t.Cleanup(func() { lookupCommandLogin, saveUserLogin, sipRegisterTimeout = savedLookup, savedSave, savedWait })

	h.proc = commands.NewProcessor(&bridgev2.Bridge{Log: zerolog.Nop()}).(*commands.Processor)
	registerSIPCommands(h.x, h.proc)
	return h
}

// handler returns the registered command handler of name.
func (h *cmdHarness) handler(t *testing.T, name string) *commands.FullHandler {
	t.Helper()
	m := reflect.ValueOf(h.proc).Elem().FieldByName("handlers")
	m = reflect.NewAt(m.Type(), unsafe.Pointer(m.UnsafeAddr())).Elem()
	v := m.MapIndex(reflect.ValueOf(name))
	if !v.IsValid() {
		t.Fatalf("command %q not registered", name)
	}
	fh, ok := v.Interface().(*commands.FullHandler)
	if !ok {
		t.Fatalf("command %q is not a FullHandler", name)
	}
	return fh
}

func (h *cmdHarness) event(args ...string) *commands.Event {
	h.evSeq++
	return &commands.Event{
		Bot:        h.bot,
		Bridge:     &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{CommandPrefix: "!jc"}},
		RoomID:     "!mgmt:example.com",
		OrigRoomID: "!mgmt:example.com",
		EventID:    id.EventID("$evt" + string(rune('a'+h.evSeq))),
		User:       h.user,
		Args:       args,
		RawArgs:    strings.Join(args, " "),
		Ctx:        context.Background(),
		Log:        &h.zlog,
	}
}

func (h *cmdHarness) run(t *testing.T, name string, args ...string) *commands.Event {
	t.Helper()
	ce := h.event(args...)
	h.handler(t, name).Func(ce)
	return ce
}

// answer sends one prompt answer through the command state like bridgev2 does.
func (h *cmdHarness) answer(t *testing.T, msg string) *commands.Event {
	t.Helper()
	st := commands.LoadCommandState(h.user)
	if st == nil || st.Next == nil {
		t.Fatalf("no command state for answer %q", msg)
	}
	ce := h.event(msg)
	st.Next.Run(ce)
	return ce
}

// login runs the whole sip-login prompt sequence and returns the final reply.
func (h *cmdHarness) login(t *testing.T, user, password string) string {
	t.Helper()
	h.run(t, "sip-login")
	for _, a := range []string{user, password, "Reg.Example.com:5060", "udp", "+15550002222"} {
		h.answer(t, a)
	}
	if st := commands.LoadCommandState(h.user); st != nil {
		t.Fatal("command state left after the last answer")
	}
	return h.bot.last()
}

func TestSIPLoginOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name      string
		res       *sipua.StateEvent
		newErr    error
		wantReply string
		wantSaved bool
	}{
		{"ok", evRegistered, nil, "Registered +15550002222 via reg.example.com:5060. Incoming calls to that number ring in its Cheogram room; start a call in a number's room or use `!jc call <number>`.", true},
		{"auth_failed", evAuthFailed, nil, "The registrar rejected the credentials (SIP 403 Forbidden). Your previous SIP registration was stopped.", false},
		{"timeout", nil, nil, "Timed out registering with reg.example.com:5060. Your previous SIP registration was stopped.", false},
		{"transient_error", evTransient, nil, "Could not register with reg.example.com:5060: no route to host. Your previous SIP registration was stopped.", false},
		{"error", nil, errors.New("invalid config"), "Could not start the SIP client: invalid config. Your previous SIP registration was stopped.", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newCmdHarness(t, true, evRegistered, tt.res) // UA 1: the existing account, UA 2: this attempt
			h.startRuntime(t)
			h.x.loginLoaded(h.ul, h.ul.Client.(*xmppClient))
			waitFor(t, "existing account", func() bool { a := h.x.account(h.ul.ID); return a != nil && a.Running() })
			h.sf.mu.Lock()
			if tt.newErr != nil {
				h.sf.newErr = tt.newErr
			}
			h.sf.mu.Unlock()

			got := h.login(t, testNewUser, testSIPPassword)
			if got != tt.wantReply {
				t.Fatalf("reply\n got %q\nwant %q", got, tt.wantReply)
			}
			creds, _ := sipCreds(loginMeta(h.ul))
			if tt.wantSaved {
				if creds.Username != testNewUser || creds.Password != testSIPPassword || creds.DisplayNumber != "+15550002222" || creds.Transport != "udp" {
					t.Fatalf("saved creds %+v", creds)
				}
				if h.saves != 1 || h.x.account(h.ul.ID) == nil {
					t.Fatalf("saves=%d account=%v", h.saves, h.x.account(h.ul.ID))
				}
			} else {
				if creds == nil || creds.Username != testSIPCreds.Username || h.saves != 0 {
					t.Fatalf("creds changed or saved on failure: %+v saves=%d", creds, h.saves)
				}
				if h.x.hasAccount(h.ul.ID) {
					t.Fatal("failed attempt left an account attached")
				}
			}
			// The previous account was stopped (and its calls ended) before anything new started.
			evs := h.log.list()
			stop, startNew := indexOf(evs, "stop1"), indexOf(evs, "start2")
			if stop < 0 || (startNew >= 0 && stop > startNew) || indexOf(evs, "end:me@example.com") > stop {
				t.Fatalf("events %v: previous account must stop first", evs)
			}
		})
	}
}

func indexOf(l []string, s string) int {
	for i, v := range l {
		if v == s {
			return i
		}
	}
	return -1
}

func TestSIPLoginSaveFailure(t *testing.T) {
	h := newCmdHarness(t, false, evRegistered)
	h.startRuntime(t)
	h.saveE = errors.New("db down")
	got := h.login(t, testNewUser, testSIPPassword)
	if got != "Registered, but saving failed: db down." {
		t.Fatalf("reply %q", got)
	}
	if _, ok := sipCreds(loginMeta(h.ul)); ok || h.x.hasAccount(h.ul.ID) {
		t.Fatal("creds or account kept after a failed save")
	}
	waitFor(t, "UA stop", func() bool { return h.ua.get(0).isStopped() })
}

func TestSIPLoginRefusals(t *testing.T) {
	h := newCmdHarness(t, false)
	h.startRuntime(t)
	// Arguments are rejected (and the message redacted: it may hold a password).
	ce := h.run(t, "sip-login", testSIPPassword)
	if got := h.bot.last(); got != "Run `!jc sip-login` without arguments; the bridge asks for each value." {
		t.Fatalf("args reply %q", got)
	}
	if !h.bot.redacted(ce.EventID) || commands.LoadCommandState(h.user) != nil {
		t.Fatal("args message not redacted or prompt started")
	}
	// Calls disabled.
	h.x.enabled = false
	h.run(t, "sip-login")
	if got := h.bot.last(); got != "SIP calls are disabled on this bridge (network.calls.enabled)." {
		t.Fatalf("disabled reply %q", got)
	}
	h.x.enabled = true
	// Stub client.
	h.ul.Client = &stubJabberClient{}
	h.run(t, "sip-login")
	if got := h.bot.last(); got != "Log in to Jabber first." {
		t.Fatalf("stub reply %q", got)
	}
	// No login.
	lookupCommandLogin = func(*commands.Event) *bridgev2.UserLogin { return nil }
	h.run(t, "sip-status")
	if got := h.bot.last(); got != "You are not logged in. Use `!jc login` first." {
		t.Fatalf("no login reply %q", got)
	}
}

func TestSIPLoginRefusedOutsideManagementRoom(t *testing.T) {
	h := newCmdHarness(t, false)
	h.startRuntime(t)
	ce := h.event()
	ce.RoomID = "!portal:example.com"
	h.handler(t, "sip-login").Func(ce)
	want := "Run `!jc sip-login` in your management room (your DM with the bridge bot): answers typed in a chat room would be sent to the contact."
	if got := h.bot.last(); got != want {
		t.Fatalf("reply %q", got)
	}
	if commands.LoadCommandState(h.user) != nil {
		t.Fatal("prompt state stored in a portal room")
	}
}

// TestSIPLoginDisconnectedMeanwhile: a login that was disconnected during the registration leaves no
// running registration behind; the credentials stay saved.
func TestSIPLoginDisconnectedMeanwhile(t *testing.T) {
	h := newCmdHarness(t, false, evRegistered)
	h.startRuntime(t)
	h.run(t, "sip-login")
	for _, a := range []string{testNewUser, testSIPPassword, "Reg.Example.com:5060", "udp"} {
		h.answer(t, a)
	}
	h.x.loginStopped(h.ul.Client.(*xmppClient)) // logout/shutdown while the prompt is open
	h.answer(t, "+15550002222")
	want := "Registered, but the Jabber login was disconnected meanwhile; run `!jc sip-login` again."
	if got := h.bot.last(); got != want {
		t.Fatalf("reply %q", got)
	}
	waitFor(t, "UA stop", func() bool { return h.ua.get(0).isStopped() })
	if h.x.hasAccount(h.ul.ID) {
		t.Fatal("account attached for a disconnected login")
	}
	if _, ok := sipCreds(loginMeta(h.ul)); !ok {
		t.Fatal("credentials not kept")
	}
}

func TestSIPLoginConcurrentSubmit(t *testing.T) {
	h := newCmdHarness(t, false) // the registrar never answers: the first submit blocks until timeout
	h.startRuntime(t)
	h.x.mu.Lock()
	h.x.loginBusy[h.ul.ID] = true // a submit is in flight
	h.x.mu.Unlock()
	h.run(t, "sip-login")
	for _, a := range []string{testNewUser, testSIPPassword, "Reg.Example.com:5060", "udp", "+15550002222"} {
		h.answer(t, a)
	}
	if got := h.bot.last(); got != "A SIP login is already in progress for this login." {
		t.Fatalf("reply %q", got)
	}
	if h.ua.count() != 0 {
		t.Fatal("a UA was created by the refused submit")
	}
	h.x.mu.Lock()
	delete(h.x.loginBusy, h.ul.ID)
	h.x.mu.Unlock()
}

func TestSIPLoginInvalidInputReprompts(t *testing.T) {
	h := newCmdHarness(t, false, evRegistered)
	h.startRuntime(t)
	h.run(t, "sip-login")
	h.answer(t, testNewUser)
	h.answer(t, testSIPPassword)
	h.answer(t, "bad/registrar")
	if !strings.HasPrefix(h.bot.last(), "Invalid value:") {
		t.Fatalf("reply %q", h.bot.last())
	}
	if commands.LoadCommandState(h.user) == nil {
		t.Fatal("prompt state dropped on invalid input")
	}
}

func TestSIPLogout(t *testing.T) {
	h := newCmdHarness(t, true, evRegistered)
	h.startRuntime(t)
	h.x.loginLoaded(h.ul, h.ul.Client.(*xmppClient))
	waitFor(t, "account", func() bool { a := h.x.account(h.ul.ID); return a != nil && a.Running() })
	// Other Extra keys survive.
	meta := loginMeta(h.ul)
	meta.Extra["other"] = []byte(`1`)

	h.run(t, "sip-logout")
	if got := h.bot.last(); got != "SIP account removed. Calls to +15550001111 no longer ring here." {
		t.Fatalf("reply %q", got)
	}
	if _, ok := sipCreds(meta); ok || h.saves != 1 || h.x.hasAccount(h.ul.ID) || !h.ua.get(0).isStopped() {
		t.Fatalf("creds=%v saves=%d account=%v", ok, h.saves, h.x.hasAccount(h.ul.ID))
	}
	if string(meta.Extra["other"]) != "1" {
		t.Fatal("other Extra keys lost")
	}
	if evs := h.log.list(); indexOf(evs, "end:me@example.com") < 0 {
		t.Fatalf("calls not ended: %v", evs)
	}
	// Second time: nothing to remove.
	h.run(t, "sip-logout")
	if got := h.bot.last(); got != "No SIP account is set up for this login." || h.saves != 1 {
		t.Fatalf("reply %q saves=%d", got, h.saves)
	}
}

func TestSIPStatus(t *testing.T) {
	h := newCmdHarness(t, true, evRegistered)
	h.startRuntime(t)
	// Credentials without a running account.
	h.run(t, "sip-status")
	got := h.bot.last()
	masked := sipua.MaskUsername(testSIPCreds.Username)
	for _, want := range []string{"Calls enabled: yes", "Cheogram: on", "SIP account: " + masked + " via reg.example.com:5060 (udp)", "Number: +15550001111", "Registration: stopped"} {
		if !strings.Contains(got, want) {
			t.Errorf("status lacks %q:\n%s", want, got)
		}
	}
	h.x.loginLoaded(h.ul, h.ul.Client.(*xmppClient))
	waitFor(t, "registered", func() bool { a := h.x.account(h.ul.ID); return a != nil && a.State().State == sipua.StateRegistered })
	h.run(t, "sip-status")
	got = h.bot.last()
	if !strings.Contains(got, "Registration: registered") {
		t.Errorf("status:\n%s", got)
	}
	if strings.Contains(got, testSIPCreds.Username) || strings.Contains(got, testSIPCreds.Password) {
		t.Errorf("status leaks secrets:\n%s", got)
	}
	// No account.
	h2 := newCmdHarness(t, false)
	h2.x.enabled = false
	h2.run(t, "sip-status")
	if got := h2.bot.last(); !strings.Contains(got, "Calls enabled: no") || !strings.Contains(got, "SIP account: none") {
		t.Errorf("status:\n%s", got)
	}
}

func TestRegistrationState(t *testing.T) {
	if got := registrationState(nil); got != "stopped" {
		t.Fatal(got)
	}
}

// fakeCallAPI records what the call commands send.
type fakeCallAPI struct {
	mu     sync.Mutex
	req    calls.PlaceCallRequest
	digits string
	placed int
}

func (f *fakeCallAPI) PlaceCall(_ context.Context, _ core.LoginRef, req calls.PlaceCallRequest) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.req, f.placed = req, f.placed+1
	return "call1", "+15551234567", nil
}
func (f *fakeCallAPI) CheckDestination(core.LoginRef, string) error { return nil }
func (f *fakeCallAPI) Hangup(context.Context, core.LoginRef, id.RoomID) error {
	return nil
}
func (f *fakeCallAPI) SendDTMF(_ context.Context, _ core.LoginRef, _ id.RoomID, digits string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.digits = digits
	return len(digits), nil
}

// callHarness registers the call commands with a fake call API.
func callHarness(t *testing.T, withCreds bool) (*cmdHarness, *fakeCallAPI) {
	t.Helper()
	h := newCmdHarness(t, withCreds, evRegistered)
	h.startRuntime(t)
	api := &fakeCallAPI{}
	deps := h.x.callCommandDeps()
	deps.Calls = func() hostkit.CallAPI { return api }
	hostkit.RegisterCallCommands(h.proc, deps)
	return h, api
}

func portalOf(ce *commands.Event, portalID string, rt database.RoomType) *commands.Event {
	ce.Portal = &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID(portalID), Receiver: "me@example.com"}, MXID: "!portal:example.com", RoomType: rt}}
	return ce
}

func TestCallCommands(t *testing.T) {
	// No account: refused, nothing placed.
	h, api := callHarness(t, false)
	for _, name := range []string{"call", "hangup", "dtmf"} {
		h.run(t, name, "+15551234567")
		if got := h.bot.last(); got != "No SIP account is set up for this login. Use `!jc sip-login`." {
			t.Fatalf("%s reply %q", name, got)
		}
	}
	if api.placed != 0 || api.digits != "" {
		t.Fatal("command reached the call API without an account")
	}
	// Stored credentials but the account is stopped.
	h, api = callHarness(t, true)
	h.run(t, "call", "+15551234567")
	if got := h.bot.last(); !strings.Contains(got, "not registered right now") || api.placed != 0 {
		t.Fatalf("stopped account reply %q", got)
	}

	// With a running account.
	h.x.loginLoaded(h.ul, h.ul.Client.(*xmppClient))
	waitFor(t, "account", func() bool { a := h.x.account(h.ul.ID); return a != nil && a.Running() })
	h.run(t, "call", "+15551234567")
	if api.req.Number != "+15551234567" || !strings.HasPrefix(h.bot.last(), "Calling +15551234567.") {
		t.Fatalf("call: req %+v reply %q", api.req, h.bot.last())
	}
	// Room form in a Cheogram 1:1 room places a room call; other rooms get the usage reply.
	ce := portalOf(h.event(), "+15551234567@cheogram.com", database.RoomTypeDM)
	h.handler(t, "call").Func(ce)
	if api.req.RoomID != "!portal:example.com" {
		t.Fatalf("room call: req %+v reply %q", api.req, h.bot.last())
	}
	placed := api.placed
	for _, p := range []struct {
		id string
		rt database.RoomType
	}{
		{"room@conference.example.org", database.RoomTypeDefault},
		{"friend@example.org", database.RoomTypeDM},
		{"+15551234567,+15557654321@cheogram.com", database.RoomTypeGroupDM},
	} {
		h.handler(t, "call").Func(portalOf(h.event(), p.id, p.rt))
		if got := h.bot.last(); !strings.HasPrefix(got, "Usage: `!jc call <number>`") || api.placed != placed {
			t.Fatalf("room %q: reply %q placed=%d", p.id, got, api.placed)
		}
	}
}

func TestCallAPINilInterface(t *testing.T) {
	x, _ := testCallsExt(t, true)
	if api := x.callCommandDeps().Calls(); api != nil {
		t.Fatalf("Calls() = %#v, want a nil interface while the manager does not exist", api)
	}
}

func TestSIPCommandsHelp(t *testing.T) {
	h := newCmdHarness(t, false)
	for _, n := range []string{"sip-login", "sip-logout", "sip-status"} {
		fh := h.handler(t, n)
		if !fh.RequiresLogin || fh.Help.Section != commands.HelpSectionAuth {
			t.Errorf("%s: login=%v section=%v", n, fh.RequiresLogin, fh.Help.Section)
		}
	}
	hostkit.RegisterCallCommands(h.proc, h.x.callCommandDeps())
	for _, n := range []string{"call", "hangup", "dtmf"} {
		h.handler(t, n)
	}
}

// TestPrivacy runs sip-login with a password and dtmf with digits and checks that neither the password,
// the digits nor the unmasked SIP username reach logs, spans or replies.
func TestSIPCommandsPrivacy(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	obs.Use(obs.Providers{Tracer: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))})
	t.Cleanup(func() {
		// Restore what telemetry.Init installed, if another test enabled tracing in this process.
		if telemetry.Enabled() {
			obs.Use(obs.Providers{Tracer: otel.GetTracerProvider()})
			return
		}
		obs.Use(obs.Providers{})
	})

	for _, outcome := range []*sipua.StateEvent{evRegistered, evAuthFailed} {
		h, _ := callHarness(t, false)
		h.sf.mu.Lock()
		h.sf.results = []*sipua.StateEvent{outcome}
		h.sf.mu.Unlock()
		h.run(t, "sip-login")
		var pwEvt *commands.Event
		for i, a := range []string{testNewUser, testSIPPassword, "reg.example.com:5060", "udp", "+15550002222"} {
			ce := h.answer(t, a)
			if i == 1 {
				pwEvt = ce
			}
		}
		if !h.bot.redacted(pwEvt.EventID) {
			t.Fatal("password message not redacted")
		}
		if outcome == evRegistered {
			dt := h.run(t, "dtmf", "4242#")
			if !h.bot.redacted(dt.EventID) {
				t.Fatal("dtmf message not redacted")
			}
			// A different logger must not matter: also log through the extension and the command logger.
			h.x.logFor(dt.Ctx).Info().Msg("probe")
		}
		h.x.stop()
		for what, s := range map[string]string{"replies": h.bot.all(), "logs": h.logs.String()} {
			for _, secret := range []string{testSIPPassword, "4242", testNewUser} {
				if strings.Contains(s, secret) {
					t.Errorf("%s contain %q:\n%s", what, secret, s)
				}
			}
		}
		if !strings.Contains(h.logs.String(), sipua.MaskUsername(testNewUser)) {
			t.Logf("masked username not logged (allowed): %s", sipua.MaskUsername(testNewUser))
		}
	}
	checked := 0
	check := func(where, s string) {
		for _, secret := range []string{testSIPPassword, "4242", testNewUser} {
			if strings.Contains(s, secret) {
				t.Errorf("%q found in %s: %q", secret, where, s)
			}
		}
	}
	attrs := func(where string, kvs []attribute.KeyValue) {
		for _, kv := range kvs {
			check(where+" key", string(kv.Key))
			check(where+" "+string(kv.Key), kv.Value.Emit())
		}
	}
	for _, s := range rec.Ended() {
		checked++
		check("span name", s.Name())
		attrs("span "+s.Name(), s.Attributes())
		check("span status", s.Status().Description)
		for _, ev := range s.Events() {
			check("event name", ev.Name)
			attrs("event "+ev.Name, ev.Attributes)
		}
	}
	if len(rec.Ended()) == 0 || findSpan(rec, "cmd.sip_login.submit") == nil || findSpan(rec, "cmd.dtmf") == nil {
		t.Fatalf("expected spans not recorded (%d spans)", checked)
	}
}

func findSpan(rec *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	for _, s := range rec.Ended() {
		if s.Name() == name {
			return s
		}
	}
	return nil
}
