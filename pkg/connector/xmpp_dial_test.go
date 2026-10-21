package connector

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
)

type fakeResolver struct {
	srv     map[string][]*net.SRV // by service
	srvErr  map[string]error
	hosts   map[string][]string
	hostErr error
}

func (f *fakeResolver) LookupSRV(_ context.Context, service, _, _ string) (string, []*net.SRV, error) {
	return "", f.srv[service], f.srvErr[service]
}

func (f *fakeResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	if f.hostErr != nil {
		return nil, f.hostErr
	}
	return f.hosts[host], nil
}

func labels(eps []xmppEndpoint) []string {
	out := make([]string, len(eps))
	for i, e := range eps {
		out[i] = e.label()
	}
	return out
}

func eqStrings(a, b []string) bool { return strings.Join(a, "|") == strings.Join(b, "|") }

func TestResolveEndpointsSRVOrder(t *testing.T) {
	r := &fakeResolver{srv: map[string][]*net.SRV{
		"xmpp-client": {
			{Target: "c.example.com.", Port: 5222, Priority: 10, Weight: 5},
			{Target: "a.example.com.", Port: 5222, Priority: 5, Weight: 1},
			{Target: "b.example.com.", Port: 5222, Priority: 5, Weight: 9},
		},
		"xmpps-client": {
			{Target: "t.example.com.", Port: 5223, Priority: 5, Weight: 0},
		},
	}}
	got := labels(resolveXMPPEndpoints(context.Background(), r, "example.com"))
	want := []string{"t.example.com:5223 (tls)", "b.example.com:5222", "a.example.com:5222", "c.example.com:5222"}
	if !eqStrings(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestResolveEndpointsDotTarget(t *testing.T) {
	// xmpp-client "." and an xmpps record: only the direct TLS endpoint, no 5222 fallback.
	r := &fakeResolver{srv: map[string][]*net.SRV{
		"xmpp-client":  {{Target: ".", Port: 0}},
		"xmpps-client": {{Target: "t.example.com.", Port: 5223}},
	}}
	if got := labels(resolveXMPPEndpoints(context.Background(), r, "example.com")); !eqStrings(got, []string{"t.example.com:5223 (tls)"}) {
		t.Fatalf("got %v", got)
	}
	// STARTTLS explicitly unavailable and nothing else: no endpoints at all.
	r = &fakeResolver{srv: map[string][]*net.SRV{"xmpp-client": {{Target: "."}}}}
	if got := resolveXMPPEndpoints(context.Background(), r, "example.com"); len(got) != 0 {
		t.Fatalf("expected none, got %v", got)
	}
}

func TestResolveEndpointsFallback5222(t *testing.T) {
	for _, err := range []error{errors.New("no such host"), context.DeadlineExceeded, nil} {
		r := &fakeResolver{srvErr: map[string]error{"xmpp-client": err, "xmpps-client": err}}
		eps := resolveXMPPEndpoints(context.Background(), r, "example.com")
		if len(eps) != 1 || eps[0] != (xmppEndpoint{Host: "example.com", Port: 5222, Source: "fallback"}) {
			t.Fatalf("err=%v got %+v", err, eps)
		}
	}
}

func TestResolveEndpointsPartialSRVError(t *testing.T) {
	r := &fakeResolver{
		srv:    map[string][]*net.SRV{"xmpp-client": {{Target: "a.example.com.", Port: 5222}}},
		srvErr: map[string]error{"xmpp-client": errors.New("some targets malformed")},
	}
	if got := labels(resolveXMPPEndpoints(context.Background(), r, "example.com")); !eqStrings(got, []string{"a.example.com:5222"}) {
		t.Fatalf("got %v", got)
	}
}

func TestResolveEndpointsCapAndDedupe(t *testing.T) {
	var recs []*net.SRV
	for i := 0; i < 12; i++ {
		recs = append(recs, &net.SRV{Target: fmt.Sprintf("h%d.example.com.", i), Port: 5222, Priority: uint16(i)})
	}
	r := &fakeResolver{srv: map[string][]*net.SRV{"xmpp-client": recs}}
	if eps := resolveXMPPEndpoints(context.Background(), r, "example.com"); len(eps) != maxXMPPEndpoints {
		t.Fatalf("len %d", len(eps))
	}
	r.srv["xmpp-client"] = []*net.SRV{
		{Target: "a.example.com.", Port: 5222, Priority: 1},
		{Target: "A.example.com", Port: 5222, Priority: 2},
	}
	if eps := resolveXMPPEndpoints(context.Background(), r, "example.com"); len(eps) != 1 {
		t.Fatalf("dedupe: %v", labels(eps))
	}
}

func TestParseServerOverride(t *testing.T) {
	tests := []struct {
		in   string
		want dialOptions
		auto bool
		bad  bool
	}{
		{in: "", auto: true},
		{in: "  AUTO ", auto: true},
		{in: "xmpp.example.com", want: dialOptions{Server: "xmpp.example.com", Port: 5222}},
		{in: "xmpp.example.com:5555", want: dialOptions{Server: "xmpp.example.com", Port: 5555}},
		{in: "tls://xmpp.example.com", want: dialOptions{Server: "xmpp.example.com", Port: 5223, DirectTLS: true}},
		{in: "tls://xmpp.example.com:443", want: dialOptions{Server: "xmpp.example.com", Port: 443, DirectTLS: true}},
		{in: "xmpp://xmpp.example.com", want: dialOptions{Server: "xmpp.example.com", Port: 5222}},
		{in: "tcp://xmpp.example.com:1", want: dialOptions{Server: "xmpp.example.com", Port: 1}},
		{in: "[2001:db8::1]:5222", want: dialOptions{Server: "2001:db8::1", Port: 5222}},
		{in: "[2001:db8::1]", want: dialOptions{Server: "2001:db8::1", Port: 5222}},
		{in: "2001:db8::1", want: dialOptions{Server: "2001:db8::1", Port: 5222}},
		{in: "tls://[2001:db8::1]", want: dialOptions{Server: "2001:db8::1", Port: 5223, DirectTLS: true}},
		{in: "192.0.2.1:5222", want: dialOptions{Server: "192.0.2.1", Port: 5222}},
		{in: "https://xmpp.example.com", bad: true},
		{in: "xmpp.example.com:0", bad: true},
		{in: "xmpp.example.com:65536", bad: true},
		{in: "xmpp.example.com:abc", bad: true},
		{in: "xmpp.example.com:", bad: true},
		{in: "xmpp.example.com/path", bad: true},
		{in: "user@xmpp.example.com", bad: true},
		{in: ":5222", bad: true},
		{in: "tls://", bad: true},
		{in: "a:b:c", bad: true},
	}
	for _, tc := range tests {
		got, auto, err := parseServerOverride(tc.in)
		if tc.bad {
			if err == nil {
				t.Errorf("%q: expected error, got %+v", tc.in, got)
			}
			continue
		}
		if err != nil || auto != tc.auto || got != tc.want {
			t.Errorf("%q: got %+v auto=%v err=%v, want %+v auto=%v", tc.in, got, auto, err, tc.want, tc.auto)
		}
	}
}

// withDialSeams installs a fake resolver and a fake per-endpoint dialer; it returns the endpoints tried.
func withDialSeams(t *testing.T, r netResolver, fn func(xmppEndpoint) (*xmpp.Session, error)) *[]xmppEndpoint {
	t.Helper()
	oldR, oldE := xmppResolver, dialEndpointFunc
	tried := &[]xmppEndpoint{}
	xmppResolver = r
	dialEndpointFunc = func(_ context.Context, ep xmppEndpoint, _ jid.JID, _ string, _ dialOptions, _ *zerolog.Logger) (*xmpp.Session, error) {
		*tried = append(*tried, ep)
		return fn(ep)
	}
	t.Cleanup(func() { xmppResolver, dialEndpointFunc = oldR, oldE })
	return tried
}

func TestOverrideWinsOverSRV(t *testing.T) {
	r := &fakeResolver{srv: map[string][]*net.SRV{"xmpp-client": {{Target: "srv.example.com.", Port: 5222}}}}
	tried := withDialSeams(t, r, func(xmppEndpoint) (*xmpp.Session, error) { return nil, errors.New("down") })
	_, err := dialXMPP(context.Background(), jid.MustParse("user@example.com"), "pw", dialOptions{Server: "own.example.net", DirectTLS: true}, nil)
	if err == nil || len(*tried) != 1 || (*tried)[0] != (xmppEndpoint{Host: "own.example.net", Port: 5223, DirectTLS: true, Source: "override"}) {
		t.Fatalf("err=%v tried=%v", err, *tried)
	}
}

func TestDialWalksEndpointsOnNonSASLError(t *testing.T) {
	r := &fakeResolver{srv: map[string][]*net.SRV{"xmpp-client": {
		{Target: "a.example.com.", Port: 5222, Priority: 1}, {Target: "b.example.com.", Port: 5222, Priority: 2},
	}}}
	tried := withDialSeams(t, r, func(xmppEndpoint) (*xmpp.Session, error) { return nil, errors.New("tcp refused") })
	if _, err := dialXMPP(context.Background(), jid.MustParse("user@example.com"), "pw", dialOptions{}, nil); err == nil || len(*tried) != 2 {
		t.Fatalf("err=%v tried=%v", err, *tried)
	}
}

type fakeSASLErr struct{}

func (fakeSASLErr) Error() string         { return "server said: secret text" }
func (fakeSASLErr) SASLCondition() string { return "not-authorized" }

func TestDialStopsOnSASLFailure(t *testing.T) {
	r := &fakeResolver{srv: map[string][]*net.SRV{"xmpp-client": {
		{Target: "a.example.com.", Port: 5222, Priority: 1}, {Target: "b.example.com.", Port: 5222, Priority: 2},
	}}}
	tried := withDialSeams(t, r, func(xmppEndpoint) (*xmpp.Session, error) {
		return nil, &dialError{msg: "a.example.com:5222: SASL authentication failed (not-authorized)", err: fakeSASLErr{}}
	})
	_, err := dialXMPP(context.Background(), jid.MustParse("user@example.com"), "pw", dialOptions{}, nil)
	if len(*tried) != 1 {
		t.Fatalf("second endpoint tried: %v", *tried)
	}
	if cond, ok := saslCondition(err); !ok || cond != "not-authorized" {
		t.Fatalf("saslCondition: %q %v", cond, ok)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("server text leaked: %v", err)
	}
}

func TestSaslConditionNegative(t *testing.T) {
	if _, ok := saslCondition(errors.New("x")); ok {
		t.Fatal("plain error classified")
	}
	if _, ok := saslCondition(nil); ok {
		t.Fatal("nil classified")
	}
}

func TestOverridePrivateAddressBlocked(t *testing.T) {
	r := &fakeResolver{hosts: map[string][]string{"internal.example.net": {"10.0.0.5"}}}
	oldR := xmppResolver
	xmppResolver = r
	defer func() { xmppResolver = oldR }()
	origin := jid.MustParse("user@example.com")
	for _, host := range []string{"internal.example.net", "127.0.0.1", "[::1]"} {
		opts, _, err := parseServerOverride(host)
		if err != nil {
			t.Fatal(err)
		}
		_, err = dialEndpoint(context.Background(), overrideEndpoint(opts), origin, "pw", opts, nil)
		if err == nil || err.Error() != errPrivateOverride {
			t.Fatalf("%s: err=%v", host, err)
		}
	}
}

func TestOverridePrivateAllowedByConfig(t *testing.T) {
	// With AllowPrivate the guard passes and dialing proceeds to a real (refused) loopback connect.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("no loopback")
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	opts := dialOptions{Server: "127.0.0.1", Port: port, AllowPrivate: true}
	_, err = dialEndpoint(context.Background(), overrideEndpoint(opts), jid.MustParse("user@example.com"), "pw", opts, nil)
	if err == nil || strings.Contains(err.Error(), errPrivateOverride) {
		t.Fatalf("expected a connect error, got %v", err)
	}
}

func selfSignedCert(t *testing.T, dnsName string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: dnsName},
		DNSNames:              []string{dnsName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// directTLSServer accepts one connection, completes the TLS handshake and closes; the channel reports
// whether the server side handshake succeeded.
func directTLSServer(t *testing.T, cert tls.Certificate) (int, chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("no loopback")
	}
	t.Cleanup(func() { ln.Close() })
	result := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			result <- err
			return
		}
		defer c.Close()
		result <- tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}).Handshake()
	}()
	return ln.Addr().(*net.TCPAddr).Port, result
}

func TestDialDirectTLSVerifiesJIDDomain(t *testing.T) {
	origin := jid.MustParse("user@example.com")

	run := func(certName string) (clientErr, serverErr error) {
		cert, pool := selfSignedCert(t, certName)
		port, res := directTLSServer(t, cert)
		old := xmppTLSConfigHook
		xmppTLSConfigHook = func(c *tls.Config) { c.RootCAs = pool }
		defer func() { xmppTLSConfigHook = old }()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// The endpoint host is an IP literal: it must not be used for verification.
		ep := xmppEndpoint{Host: "127.0.0.1", Port: uint16(port), DirectTLS: true, Source: "srv-tls"}
		_, clientErr = dialEndpoint(ctx, ep, origin, "pw", dialOptions{AllowPrivate: true}, nil)
		select {
		case serverErr = <-res:
		case <-time.After(5 * time.Second):
			serverErr = errors.New("server timeout")
		}
		return
	}

	var hostErr x509.HostnameError
	var verr *tls.CertificateVerificationError
	isVerify := func(err error) bool { return errors.As(err, &hostErr) || errors.As(err, &verr) }

	// Cert for the JID domain: the TLS handshake succeeds (the stream then fails: the server closed).
	cErr, sErr := run("example.com")
	if sErr != nil {
		t.Fatalf("handshake with matching cert failed: server=%v client=%v", sErr, cErr)
	}
	if cErr == nil || isVerify(cErr) {
		t.Fatalf("expected a post-handshake stream error, got %v", cErr)
	}

	// Cert for another name (e.g. the SRV target): verification must fail.
	if cErr, _ = run("other.example.net"); cErr == nil || !isVerify(cErr) {
		t.Fatalf("expected certificate verification error, got %v", cErr)
	}
}

func TestBuildLoginMeta(t *testing.T) {
	m, opts, err := buildLoginMeta("pw", "", false)
	if err != nil || m.serverExplicit || m.Server != "" || opts != (dialOptions{}) {
		t.Fatalf("plain flow: %+v %+v %v", m, opts, err)
	}
	m, opts, err = buildLoginMeta("pw", "tls://xmpp.example.net", true)
	if err != nil || !m.serverExplicit || m.Server != "xmpp.example.net" || m.Port != 5223 || !m.DirectTLS ||
		opts != (dialOptions{Server: "xmpp.example.net", Port: 5223, DirectTLS: true}) {
		t.Fatalf("custom flow: %+v %+v %v", m, opts, err)
	}
	m, opts, err = buildLoginMeta("pw", "auto", true)
	if err != nil || !m.serverExplicit || m.Server != "" || opts.Server != "" {
		t.Fatalf("auto: %+v %+v %v", m, opts, err)
	}
	if _, _, err = buildLoginMeta("pw", "", true); err == nil {
		t.Fatal("empty server accepted in custom flow")
	}
	if _, _, err = buildLoginMeta("pw", "ftp://x", true); err == nil {
		t.Fatal("bad scheme accepted")
	}
}

func TestLoginFlowsIncludeCustomServer(t *testing.T) {
	c := &JabberConnector{}
	ids := map[string]bool{}
	for _, f := range c.GetLoginFlows() {
		ids[f.ID] = true
	}
	if !ids["password"] || !ids["password-server"] {
		t.Fatalf("flows: %v", ids)
	}
	p, err := c.CreateLogin(context.Background(), nil, "password-server")
	if err != nil || !p.(*jabberLoginProcess).customServer {
		t.Fatalf("CreateLogin: %v", err)
	}
	if _, err := c.CreateLogin(context.Background(), nil, "bogus"); err == nil {
		t.Fatal("unknown flow accepted")
	}
	step, err := p.(*jabberLoginProcess).Start(context.Background())
	if err != nil || step.StepID != "fi.mau.jabber.password_server" || len(step.UserInputParams.Fields) != 3 {
		t.Fatalf("Start: %+v %v", step, err)
	}
}

func TestSRVPrivateNonStandardPortBlocked(t *testing.T) {
	r := &fakeResolver{hosts: map[string][]string{"srv.example.net": {"10.0.0.5"}}}
	oldR := xmppResolver
	xmppResolver = r
	defer func() { xmppResolver = oldR }()
	origin := jid.MustParse("user@example.com")
	for _, src := range []string{"srv", "srv-tls"} {
		ep := xmppEndpoint{Host: "srv.example.net", Port: 6379, Source: src, DirectTLS: src == "srv-tls"}
		if _, err := dialEndpoint(context.Background(), ep, origin, "pw", dialOptions{}, nil); err == nil || err.Error() != errPrivateSRV {
			t.Fatalf("%s: err=%v", src, err)
		}
	}
	// Allowed by config: the guard is skipped (the connect then fails or times out).
	oldT := dialEndpointTimeout
	dialEndpointTimeout = 200 * time.Millisecond
	defer func() { dialEndpointTimeout = oldT }()
	ep := xmppEndpoint{Host: "srv.example.net", Port: 6379, Source: "srv"}
	if _, err := dialEndpoint(context.Background(), ep, origin, "pw", dialOptions{AllowPrivate: true}, nil); err == nil || err.Error() == errPrivateSRV {
		t.Fatalf("guard applied despite AllowPrivate: %v", err)
	}
}

func TestSRVPrivateStandardPortAndFallbackAllowed(t *testing.T) {
	r := &fakeResolver{hosts: map[string][]string{"lan.example.net": {"127.0.0.1"}}}
	oldR := xmppResolver
	xmppResolver = r
	defer func() { xmppResolver = oldR }()
	oldT := dialEndpointTimeout
	dialEndpointTimeout = 200 * time.Millisecond
	defer func() { dialEndpointTimeout = oldT }()
	origin := jid.MustParse("user@example.com")
	for _, ep := range []xmppEndpoint{
		{Host: "lan.example.net", Port: 5222, Source: "srv"},
		{Host: "lan.example.net", Port: 5223, Source: "srv-tls", DirectTLS: true},
		{Host: "lan.example.net", Port: 5222, Source: "fallback"},
	} {
		_, err := dialEndpoint(context.Background(), ep, origin, "pw", dialOptions{}, nil)
		if err != nil && (err.Error() == errPrivateSRV || err.Error() == errPrivateOverride) {
			t.Fatalf("%v blocked: %v", ep, err)
		}
	}
}

func TestDialEndpointTimeoutSilentServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("no loopback")
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	oldT := dialEndpointTimeout
	dialEndpointTimeout = 300 * time.Millisecond
	defer func() { dialEndpointTimeout = oldT }()
	ep := xmppEndpoint{Host: "127.0.0.1", Port: uint16(ln.Addr().(*net.TCPAddr).Port), DirectTLS: true, Source: "srv-tls"}
	start := time.Now()
	_, err = dialEndpoint(context.Background(), ep, jid.MustParse("user@example.com"), "pw", dialOptions{}, nil)
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("err=%v after %v", err, time.Since(start))
	}
}

func TestDirectTLSTriesNextIPAfterHandshakeFailure(t *testing.T) {
	// The first IP (IPv6 sorts first) closes during the handshake; the second is a working TLS server.
	l6, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skip("no ipv6 loopback")
	}
	defer l6.Close()
	port := l6.Addr().(*net.TCPAddr).Port
	l4, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		t.Skip("port not free on v4")
	}
	defer l4.Close()
	go func() {
		c, err := l6.Accept()
		if err == nil {
			c.Close()
		}
	}()
	cert, pool := selfSignedCert(t, "example.com")
	served := make(chan error, 1)
	go func() {
		c, err := l4.Accept()
		if err != nil {
			served <- err
			return
		}
		defer c.Close()
		served <- tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}).Handshake()
	}()
	old := xmppTLSConfigHook
	xmppTLSConfigHook = func(c *tls.Config) { c.RootCAs = pool }
	defer func() { xmppTLSConfigHook = old }()
	oldR := xmppResolver
	xmppResolver = &fakeResolver{hosts: map[string][]string{"h.example.net": {"127.0.0.1", "::1"}}}
	defer func() { xmppResolver = oldR }()
	ep := xmppEndpoint{Host: "h.example.net", Port: uint16(port), DirectTLS: true, Source: "fallback"}
	_, _ = dialEndpoint(context.Background(), ep, jid.MustParse("user@example.com"), "pw", dialOptions{}, nil)
	select {
	case e := <-served:
		if e != nil {
			t.Fatalf("second IP handshake: %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second IP never tried")
	}
}

func TestExistingDialOptions(t *testing.T) {
	mk := func(id string, m *JabberLoginMeta) *bridgev2.UserLogin {
		return &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID(id), Metadata: m}}
	}
	logins := []*bridgev2.UserLogin{
		mk("other@example.com", &JabberLoginMeta{Server: "x.example.net"}),
		mk("me@example.com", &JabberLoginMeta{Server: "xmpp.example.net", Port: 5223, DirectTLS: true}),
	}
	got := existingDialOptions(logins, "me@example.com")
	if got != (dialOptions{Server: "xmpp.example.net", Port: 5223, DirectTLS: true}) {
		t.Fatalf("got %+v", got)
	}
	if got := existingDialOptions(logins, "none@example.com"); got != (dialOptions{}) {
		t.Fatalf("got %+v", got)
	}
	if got := existingDialOptions(nil, "me@example.com"); got != (dialOptions{}) {
		t.Fatalf("got %+v", got)
	}
}

func TestLoginErrorsAreFixedMessages(t *testing.T) {
	old := dialXMPPFunc
	defer func() { dialXMPPFunc = old }()
	c := &JabberConnector{br: &bridgev2.Bridge{Log: zerolog.Nop()}}
	for _, e := range []error{
		errors.New("dial tcp 10.1.2.3:6379: connect: connection refused"),
		errors.New("srv.example.net:6379: " + errPrivateSRV),
	} {
		e := e
		dialXMPPFunc = func(context.Context, jid.JID, string, dialOptions, *zerolog.Logger) (*xmpp.Session, error) {
			return nil, e
		}
		p := &jabberLoginProcess{conn: c}
		_, err := p.SubmitUserInput(context.Background(), map[string]string{"jid": "user@example.com", "password": "pw"})
		if err == nil || strings.Contains(err.Error(), "10.1.2.3") || strings.Contains(err.Error(), "6379") {
			t.Fatalf("leaked detail: %v", err)
		}
	}
}

func TestPrivateOverrideConfigSurvivesUpgrade(t *testing.T) {
	c := &JabberConnector{}
	example, _, upgrader := c.GetConfig()
	out, err := configupgrade.DoBytes([]byte("allow_private_server_override: true\n"), &configupgrade.StructUpgrader{
		SimpleUpgrader: func(h configupgrade.Helper) { upgrader.DoUpgrade(h) },
		Base:           example,
	})
	if err != nil {
		t.Fatal(err)
	}
	var cfg JabberConfig
	if err = yaml.Unmarshal(out, &cfg); err != nil || !cfg.AllowPrivateServerOverride {
		t.Fatalf("setting dropped: %v\n%s", err, out)
	}
}
