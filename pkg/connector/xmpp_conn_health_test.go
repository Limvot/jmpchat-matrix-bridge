package connector

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/status"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

type condSASLErr struct{ cond string }

func (e condSASLErr) Error() string         { return "sasl failure" }
func (e condSASLErr) SASLCondition() string { return e.cond }

func TestClassifyAuth(t *testing.T) {
	permanent := []string{"not-authorized", "account-disabled", "credentials-expired", "invalid-authzid"}
	temporary := []string{"temporary-auth-failure", "aborted", "mechanism-too-weak", "encryption-required"}
	for _, c := range permanent {
		cond, perm, ok := classifyAuth(condSASLErr{c})
		if cond != c || !perm || !ok {
			t.Errorf("%s: got %q %v %v", c, cond, perm, ok)
		}
	}
	for _, c := range temporary {
		cond, perm, ok := classifyAuth(fmt.Errorf("wrapped: %w", &dialError{msg: "x", err: condSASLErr{c}}))
		if cond != c || perm || !ok {
			t.Errorf("%s: got %q %v %v", c, cond, perm, ok)
		}
	}
	if _, _, ok := classifyAuth(errors.New("tcp reset")); ok {
		t.Error("non-SASL error classified")
	}
}

// TestDialSASLFailureClassified guards the reflection in saslCondition: mellium's saslerr.Error is in an
// internal package, so a mellium upgrade that moves or renames it must fail here, loudly.
func TestDialSASLFailureClassified(t *testing.T) {
	for _, tc := range []struct {
		cond      string
		permanent bool
	}{{"not-authorized", true}, {"temporary-auth-failure", false}, {"account-disabled", true}} {
		t.Run(tc.cond, func(t *testing.T) { testDialSASLFailure(t, tc.cond, tc.permanent) })
	}
}

func testDialSASLFailure(t *testing.T, wantCond string, wantPerm bool) {
	cert, pool := selfSignedCert(t, "example.com")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("no loopback")
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if tc.Handshake() != nil {
			return
		}
		r := bufio.NewReader(tc)
		readUntil := func(marker string) bool {
			var acc strings.Builder
			for {
				b, err := r.ReadByte()
				if err != nil {
					return false
				}
				acc.WriteByte(b)
				if strings.Contains(acc.String(), marker) {
					return true
				}
			}
		}
		if !readUntil("<stream:stream") || !readUntil(">") {
			return
		}
		fmt.Fprint(tc, "<?xml version='1.0'?><stream:stream xmlns='jabber:client' xmlns:stream='http://etherx.jabber.org/streams' version='1.0' from='example.com' id='t1'>"+
			"<stream:features><mechanisms xmlns='urn:ietf:params:xml:ns:xmpp-sasl'><mechanism>PLAIN</mechanism></mechanisms></stream:features>")
		if !readUntil("</auth>") {
			return
		}
		fmt.Fprint(tc, "<failure xmlns='urn:ietf:params:xml:ns:xmpp-sasl'><"+wantCond+"/></failure>")
		time.Sleep(200 * time.Millisecond)
	}()

	old := xmppTLSConfigHook
	xmppTLSConfigHook = func(c *tls.Config) { c.RootCAs = pool }
	defer func() { xmppTLSConfigHook = old }()

	port := ln.Addr().(*net.TCPAddr).Port
	opts := dialOptions{Server: "127.0.0.1", Port: port, DirectTLS: true, AllowPrivate: true}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = dialXMPP(ctx, jid.MustParse("user@example.com"), "pw", opts, nil)
	if err == nil {
		t.Fatal("dial succeeded against a failing server")
	}
	cond, perm, ok := classifyAuth(err)
	if !ok || cond != wantCond || perm != wantPerm {
		t.Fatalf("saslCondition no longer recognises mellium's SASL failure (did mellium move internal/saslerr?): err=%v cond=%q ok=%v", err, cond, ok)
	}
}

// stateRecorder collects bridge states sent by a client under test.
type stateRecorder struct {
	mu     sync.Mutex
	states []status.BridgeState
}

func (r *stateRecorder) hook(s status.BridgeState) {
	r.mu.Lock()
	r.states = append(r.states, s)
	r.mu.Unlock()
}

func (r *stateRecorder) last() status.BridgeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.states) == 0 {
		return status.BridgeState{}
	}
	return r.states[len(r.states)-1]
}

func (r *stateRecorder) count(ev status.BridgeStateEvent) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, s := range r.states {
		if s.StateEvent == ev {
			n++
		}
	}
	return n
}

func healthClient(t *testing.T, dial func() error) (*xmppClient, *stateRecorder, *atomic.Int32) {
	t.Helper()
	rec := &stateRecorder{}
	calls := &atomic.Int32{}
	old := dialXMPPFunc
	dialXMPPFunc = func(context.Context, jid.JID, string, dialOptions, *zerolog.Logger) (*xmpp.Session, error) {
		calls.Add(1)
		return nil, dial()
	}
	t.Cleanup(func() { dialXMPPFunc = old })
	x := &xmppClient{loginID: "me@example.com", meta: &JabberLoginMeta{Password: "pw"}, log: zerolog.Nop(), stateHook: rec.hook}
	return x, rec, calls
}

func TestAuthFailureThreshold(t *testing.T) {
	t.Run("not-authorized needs two", func(t *testing.T) {
		x, rec, _ := healthClient(t, func() error { return condSASLErr{"not-authorized"} })
		notices := make(chan string, 2)
		x.mgmtNoticeHook = func(s string) { notices <- s }
		oldB := reconnectBackoffStart
		reconnectBackoffStart = 30 * time.Millisecond
		defer func() { reconnectBackoffStart = oldB }()
		x.Connect(context.Background())
		if x.authFailed.Load() || rec.count(status.StateBadCredentials) != 0 {
			t.Fatal("first not-authorized must stay transient")
		}
		time.Sleep(40 * time.Millisecond)
		x.Connect(context.Background())
		if !x.authFailed.Load() || rec.last().StateEvent != status.StateBadCredentials || rec.last().Error != "jabber-not-authorized" {
			t.Fatalf("second not-authorized: %+v", rec.last())
		}
		select {
		case n := <-notices:
			if !strings.Contains(n, "me@example.com") || !strings.Contains(n, "not-authorized") {
				t.Fatalf("notice = %q", n)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("no management room notice")
		}
	})
	t.Run("temporary-auth-failure stays transient", func(t *testing.T) {
		x, rec, _ := healthClient(t, func() error { return condSASLErr{"temporary-auth-failure"} })
		for i := 0; i < 4; i++ {
			x.Connect(context.Background())
		}
		if x.authFailed.Load() || rec.count(status.StateBadCredentials) != 0 {
			t.Fatal("temporary-auth-failure must never be final")
		}
	})
	t.Run("account-disabled is immediate", func(t *testing.T) {
		x, rec, _ := healthClient(t, func() error { return condSASLErr{"account-disabled"} })
		x.mgmtNoticeHook = func(string) {}
		x.Connect(context.Background())
		if !x.authFailed.Load() || rec.last().Error != "jabber-account-disabled" {
			t.Fatalf("state = %+v", rec.last())
		}
	})
	t.Run("network error stays transient", func(t *testing.T) {
		x, rec, _ := healthClient(t, func() error { return errors.New("dial tcp: refused") })
		x.Connect(context.Background())
		if x.authFailed.Load() || rec.last().StateEvent != status.StateTransientDisconnect {
			t.Fatalf("state = %+v", rec.last())
		}
	})
}

func TestConnectAfterAuthFailedResendsState(t *testing.T) {
	x, rec, calls := healthClient(t, func() error { return errors.New("must not dial") })
	x.authFailed.Store(true)
	x.authCond = "credentials-expired"
	x.Connect(context.Background())
	if calls.Load() != 0 {
		t.Fatal("Connect dialed after a final auth failure")
	}
	if got := rec.last(); got.StateEvent != status.StateBadCredentials || got.Error != "jabber-credentials-expired" {
		t.Fatalf("state = %+v", got)
	}
}

func TestReconnectLoopStopsOnBadCredentials(t *testing.T) {
	oldB := reconnectBackoffStart
	reconnectBackoffStart = 10 * time.Millisecond
	defer func() { reconnectBackoffStart = oldB }()
	x, rec, calls := healthClient(t, func() error { return condSASLErr{"not-authorized"} })
	x.mgmtNoticeHook = func(string) {}
	done := make(chan struct{})
	go func() { x.ReconnectLoop(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop kept running after bad credentials")
	}
	if calls.Load() != 2 || rec.count(status.StateBadCredentials) != 1 {
		t.Fatalf("dials=%d states=%+v", calls.Load(), rec.states)
	}
}

func TestReconnectLoopWakesOnSessionEnd(t *testing.T) {
	oldP := reconnectPoll
	reconnectPoll = time.Minute
	defer func() { reconnectPoll = oldP }()
	x, _, calls := healthClient(t, func() error { return condSASLErr{"account-disabled"} })
	x.mgmtNoticeHook = func(string) {}
	x.session = &xmpp.Session{}
	done := make(chan struct{})
	go func() { x.ReconnectLoop(context.Background()); close(done) }()
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("loop dialed while connected")
	}
	x.mu.Lock()
	x.session = nil
	x.mu.Unlock()
	x.wakeReconnect()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not wake on session end")
	}
	if calls.Load() != 1 {
		t.Fatalf("dials = %d", calls.Load())
	}
}

func TestReconnectLoopStopsOnCtxDuringBackoff(t *testing.T) {
	x, _, calls := healthClient(t, func() error { return errors.New("refused") })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { x.ReconnectLoop(ctx); close(done) }()
	for calls.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // now in the 5 s backoff
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop ignored ctx during backoff")
	}
}

func TestAuthFailureWindow(t *testing.T) {
	oldB := reconnectBackoffStart
	reconnectBackoffStart = 80 * time.Millisecond
	defer func() { reconnectBackoffStart = oldB }()
	t.Run("back to back failures count once", func(t *testing.T) {
		x, rec, _ := healthClient(t, func() error { return condSASLErr{"not-authorized"} })
		x.mgmtNoticeHook = func(string) {}
		x.Connect(context.Background()) // bridgev2's own Connect
		x.Connect(context.Background()) // the reconnect loop's first pass
		if x.authFailed.Load() || rec.count(status.StateBadCredentials) != 0 {
			t.Fatal("two not-authorized inside the window must not lock the login out")
		}
		time.Sleep(100 * time.Millisecond)
		x.Connect(context.Background())
		if !x.authFailed.Load() || rec.last().StateEvent != status.StateBadCredentials {
			t.Fatalf("a not-authorized after the window must count: %+v", rec.last())
		}
	})
	t.Run("spaced failures count, transient in between", func(t *testing.T) {
		var fail atomic.Bool
		fail.Store(true)
		x, rec, _ := healthClient(t, func() error {
			if fail.Load() {
				return condSASLErr{"not-authorized"}
			}
			return condSASLErr{"temporary-auth-failure"}
		})
		x.mgmtNoticeHook = func(string) {}
		x.Connect(context.Background())
		fail.Store(false)
		x.Connect(context.Background()) // unrelated transient failure inside the window
		fail.Store(true)
		x.Connect(context.Background()) // still inside the window of the first counted one
		if x.authFailed.Load() {
			t.Fatal("window is measured from the previously counted failure")
		}
		time.Sleep(100 * time.Millisecond)
		x.Connect(context.Background())
		if !x.authFailed.Load() || rec.last().StateEvent != status.StateBadCredentials {
			t.Fatalf("state = %+v", rec.last())
		}
	})
}

func TestUnreadableSASLConditionIsTransient(t *testing.T) {
	cond, permanent, ok := classifyAuth(condSASLErr{saslConditionUnreadable})
	if !ok || permanent || cond != saslConditionUnreadable {
		t.Fatalf("cond=%q permanent=%v ok=%v", cond, permanent, ok)
	}
	oldB := reconnectBackoffStart
	reconnectBackoffStart = 0
	defer func() { reconnectBackoffStart = oldB }()
	x, rec, _ := healthClient(t, func() error { return condSASLErr{saslConditionUnreadable} })
	for i := 0; i < 5; i++ {
		x.Connect(context.Background())
	}
	if x.authFailed.Load() || rec.count(status.StateBadCredentials) != 0 {
		t.Fatal("an unreadable condition must never be a final auth failure")
	}
}

func TestReconnectLoopBacksOffAfterShortSession(t *testing.T) {
	oldB, oldM := reconnectBackoffStart, minHealthySession
	reconnectBackoffStart, minHealthySession = 100*time.Millisecond, time.Hour
	defer func() { reconnectBackoffStart, minHealthySession = oldB, oldM }()
	x := &xmppClient{loginID: "me@example.com", log: zerolog.Nop()}
	var mu sync.Mutex
	var times []time.Time
	x.connectHook = func(context.Context) {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		x.mu.Lock()
		x.session = &xmpp.Session{}
		x.mu.Unlock()
		go func() { // the server kicks the session right after bind
			time.Sleep(5 * time.Millisecond)
			x.mu.Lock()
			x.session = nil
			x.mu.Unlock()
			x.wakeReconnect()
		}()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { x.ReconnectLoop(ctx); close(done) }()
	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		n := len(times)
		mu.Unlock()
		if n >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("loop did not reconnect")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if d := times[1].Sub(times[0]); d < 95*time.Millisecond {
		t.Errorf("second connect after %v, want >= first backoff (100ms)", d)
	}
	if d := times[2].Sub(times[1]); d < 195*time.Millisecond {
		t.Errorf("third connect after %v, want the doubled backoff (200ms)", d)
	}
}

func TestReconnectLoopResetsBackoffAfterHealthySession(t *testing.T) {
	oldB, oldM := reconnectBackoffStart, minHealthySession
	reconnectBackoffStart, minHealthySession = 100*time.Millisecond, time.Millisecond
	defer func() { reconnectBackoffStart, minHealthySession = oldB, oldM }()
	x := &xmppClient{loginID: "me@example.com", log: zerolog.Nop()}
	var calls atomic.Int32
	x.connectHook = func(context.Context) {
		calls.Add(1)
		x.mu.Lock()
		x.session = &xmpp.Session{}
		x.mu.Unlock()
		go func() {
			time.Sleep(20 * time.Millisecond)
			x.mu.Lock()
			x.session = nil
			x.mu.Unlock()
			x.wakeReconnect()
		}()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { x.ReconnectLoop(ctx); close(done) }()
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done
	if calls.Load() < 5 {
		t.Fatalf("only %d connects: a healthy session must reconnect at once", calls.Load())
	}
}

func TestKeepaliveErrorReplyProvesAlive(t *testing.T) {
	keepaliveFast(t)
	sess := &xmpp.Session{}
	x := &xmppClient{log: zerolog.Nop(), session: sess}
	x.lastInbound.Store(time.Now().Add(-time.Hour).UnixNano())
	var n atomic.Int32
	x.pingFunc = func(context.Context, *xmpp.Session, jid.JID) error {
		n.Add(1)
		return fmt.Errorf("ping: %w", stanza.Error{Type: stanza.Cancel, Condition: stanza.FeatureNotImplemented})
	}
	x.closeConnHook = func(*xmpp.Session) { t.Error("closed despite an error reply") }
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	x.runKeepalive(ctx, sess, jid.MustParse("example.com"))
	if n.Load() < 3 {
		t.Fatalf("only %d pings", n.Load())
	}
}

func keepaliveFast(t *testing.T) {
	t.Helper()
	ot, oi, oto, om := keepaliveTick, keepaliveIdle, keepaliveTimeout, keepaliveMaxMiss
	keepaliveTick, keepaliveIdle, keepaliveTimeout, keepaliveMaxMiss = 10*time.Millisecond, 50*time.Millisecond, time.Second, 2
	t.Cleanup(func() { keepaliveTick, keepaliveIdle, keepaliveTimeout, keepaliveMaxMiss = ot, oi, oto, om })
}

func TestKeepaliveClosesAfterMisses(t *testing.T) {
	keepaliveFast(t)
	sess := &xmpp.Session{}
	x := &xmppClient{log: zerolog.Nop(), session: sess}
	x.lastInbound.Store(time.Now().Add(-time.Hour).UnixNano())
	var pings atomic.Int32
	x.pingFunc = func(context.Context, *xmpp.Session, jid.JID) error {
		pings.Add(1)
		return errors.New("timeout")
	}
	closed := make(chan struct{})
	x.closeConnHook = func(s *xmpp.Session) {
		if s == sess {
			close(closed)
		}
	}
	done := make(chan struct{})
	go func() { x.runKeepalive(context.Background(), sess, jid.MustParse("example.com")); close(done) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("session not closed")
	}
	<-done
	if pings.Load() != 2 {
		t.Fatalf("pings = %d, want 2", pings.Load())
	}
}

func TestKeepaliveSkipsWhenTrafficRecent(t *testing.T) {
	keepaliveFast(t)
	keepaliveIdle = time.Hour
	sess := &xmpp.Session{}
	x := &xmppClient{log: zerolog.Nop(), session: sess}
	x.lastInbound.Store(time.Now().UnixNano())
	x.pingFunc = func(context.Context, *xmpp.Session, jid.JID) error {
		t.Error("pinged despite recent traffic")
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	x.runKeepalive(ctx, sess, jid.MustParse("example.com"))
}

func TestKeepaliveSuccessResetsMisses(t *testing.T) {
	keepaliveFast(t)
	sess := &xmpp.Session{}
	x := &xmppClient{log: zerolog.Nop(), session: sess}
	var n atomic.Int32
	x.pingFunc = func(context.Context, *xmpp.Session, jid.JID) error {
		if n.Add(1)%2 == 1 {
			return errors.New("timeout") // every other ping fails: never two in a row
		}
		return nil
	}
	x.closeConnHook = func(*xmpp.Session) { t.Error("closed despite replies") }
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	x.runKeepalive(ctx, sess, jid.MustParse("example.com"))
	if n.Load() < 4 {
		t.Fatalf("only %d pings", n.Load())
	}
}

func TestKeepaliveExitsOnNewSession(t *testing.T) {
	keepaliveFast(t)
	old, cur := &xmpp.Session{}, &xmpp.Session{}
	x := &xmppClient{log: zerolog.Nop(), session: cur}
	x.pingFunc = func(context.Context, *xmpp.Session, jid.JID) error {
		t.Error("pinged a stale session")
		return errors.New("x")
	}
	x.closeConnHook = func(*xmpp.Session) { t.Error("closed a stale session") }
	done := make(chan struct{})
	go func() { x.runKeepalive(context.Background(), old, jid.MustParse("example.com")); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("keepalive for a replaced session kept running")
	}
}

func TestPingIQAnswered(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com", log: zerolog.Nop()}
	out := serveStanza(t, x, `<iq xmlns='jabber:client' type='get' id='p1' from='example.com'><ping xmlns='urn:xmpp:ping'/></iq>`)
	if !strings.Contains(out, `type="result"`) || !strings.Contains(out, `id="p1"`) {
		t.Fatalf("ping not answered with a result: %q", out)
	}
}

func TestInboundStampsLastInbound(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com", log: zerolog.Nop()}
	serveStanza(t, x, `<iq xmlns='jabber:client' type='get' id='p2' from='example.com'><ping xmlns='urn:xmpp:ping'/></iq>`)
	if time.Since(time.Unix(0, x.lastInbound.Load())) > time.Minute {
		t.Fatal("lastInbound not stamped")
	}
}
