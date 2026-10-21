// Adapted from github.com/keplersj/mautrix-xmpp (MIT, see LICENSE-THIRD-PARTY).
// (resolveXMPPEndpoints, parseServerOverride and saslCondition follow that project's pkg/xmppx.)

package connector

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"mellium.im/sasl"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// saslMechanisms lists SASL mechanisms in preferred order (stronger first).
// Many servers do not advertise PLAIN; SCRAM-SHA-1/256 are widely supported.
var saslMechanisms = []sasl.Mechanism{
	sasl.ScramSha256Plus, sasl.ScramSha256,
	sasl.ScramSha1Plus, sasl.ScramSha1,
	sasl.Plain,
}

const (
	xmppDefaultPort    = 5222 // STARTTLS (RFC 6120)
	xmppDirectTLSPort  = 5223 // implicit TLS (XEP-0368 default for overrides)
	maxXMPPEndpoints   = 8
	errPrivateOverride = "server override points to a private or loopback address"
)

// dialOptions are the per-login connection settings.
type dialOptions struct {
	Server       string // override host ("" = DNS discovery)
	Port         int    // override port (0 = default for the mode)
	DirectTLS    bool   // override uses XEP-0368 implicit TLS
	AllowPrivate bool   // network.allow_private_server_override: the override may point at private/loopback addresses
}

// xmppEndpoint is one candidate way to reach the server.
type xmppEndpoint struct {
	Host      string
	Port      uint16
	DirectTLS bool
	Source    string // "override", "srv-tls", "srv", "fallback" (span/log attribute)
}

func (e xmppEndpoint) addr() string { return net.JoinHostPort(e.Host, strconv.Itoa(int(e.Port))) }

// label is used in errors and logs: host:port, with a (tls) suffix for direct TLS.
func (e xmppEndpoint) label() string {
	if e.DirectTLS {
		return e.addr() + " (tls)"
	}
	return e.addr()
}

// netResolver is the DNS seam (net.DefaultResolver in production).
type netResolver interface {
	LookupSRV(ctx context.Context, service, proto, name string) (string, []*net.SRV, error)
	LookupHost(ctx context.Context, host string) ([]string, error)
}

var xmppResolver netResolver = net.DefaultResolver

// dialXMPPFunc is the seam for Connect and SubmitUserInput tests.
var dialXMPPFunc = dialXMPP

// dialEndpointFunc is the per-endpoint seam (tests).
var dialEndpointFunc = dialEndpoint

// xmppTLSConfigHook lets tests add RootCAs; it is applied last and is nil in production.
var xmppTLSConfigHook func(*tls.Config)

func xmppTLSConfig(domain string, direct bool) *tls.Config {
	cfg := &tls.Config{ServerName: domain, MinVersion: tls.VersionTLS12}
	if direct {
		cfg.NextProtos = []string{"xmpp-client"}
	}
	if xmppTLSConfigHook != nil {
		xmppTLSConfigHook(cfg)
	}
	return cfg
}

// dialError carries a message that is safe to log (no server-supplied SASL text) while keeping the
// underlying error reachable for errors.Is/As and saslCondition.
type dialError struct {
	msg string
	err error
}

func (e *dialError) Error() string { return e.msg }
func (e *dialError) Unwrap() error { return e.err }

// saslCondition reports the SASL failure condition carried by err (e.g. "not-authorized"), if err is a
// SASL authentication failure.
//
// mellium's SASL failure type lives in an internal package (mellium.im/xmpp/internal/saslerr), so it is
// read through reflection. This depends on mellium internals: a mellium upgrade that moves the type
// makes this return false, which is guarded by TestDialSASLFailureClassified (Phase C).
func saslCondition(err error) (cond string, ok bool) {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if sc, isSC := e.(interface{ SASLCondition() string }); isSC {
			return sc.SASLCondition(), true
		}
		v := reflect.ValueOf(e)
		if v.Kind() == reflect.Struct && v.Type().PkgPath() == "mellium.im/xmpp/internal/saslerr" && v.Type().Name() == "Error" {
			cond = "not-authorized"
			if f := v.FieldByName("Condition"); f.IsValid() && f.CanInterface() {
				if s, isStr := f.Interface().(fmt.Stringer); isStr && s.String() != "none" {
					cond = s.String()
				}
			}
			return cond, true
		}
	}
	return "", false
}

// parseServerOverride parses a per-login server override: "", "auto" (clear: auto=true), "host",
// "host:port", "[v6]:port", a bare IPv6 literal, or "tls://host[:port]" (direct TLS, default 5223).
// "xmpp://" and "tcp://" select STARTTLS (default 5222). The port is always filled in.
func parseServerOverride(s string) (opts dialOptions, auto bool, err error) {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "auto") {
		return dialOptions{}, true, nil
	}
	if i := strings.Index(s, "://"); i >= 0 {
		switch strings.ToLower(s[:i]) {
		case "tls":
			opts.DirectTLS = true
		case "xmpp", "tcp":
		default:
			return dialOptions{}, false, errors.New("unsupported scheme in server address (use host:port or tls://host:port)")
		}
		s = s[i+3:]
	}
	if s == "" {
		return dialOptions{}, false, errors.New("empty server address")
	}
	if strings.ContainsAny(s, "/@?# \t") {
		return dialOptions{}, false, errors.New("server address must be host, host:port or tls://host:port (no path or user info)")
	}
	host, portStr := "", ""
	switch {
	case strings.HasPrefix(s, "["):
		end := strings.Index(s, "]")
		if end < 0 {
			return dialOptions{}, false, errors.New("invalid IPv6 address in server address")
		}
		host = s[1:end]
		if rest := s[end+1:]; rest != "" {
			if !strings.HasPrefix(rest, ":") || len(rest) == 1 {
				return dialOptions{}, false, errors.New("invalid server address")
			}
			portStr = rest[1:]
		}
	default:
		if _, perr := netip.ParseAddr(s); perr == nil {
			host = s // bare IPv4 or IPv6 literal
		} else {
			switch strings.Count(s, ":") {
			case 0:
				host = s
			case 1:
				i := strings.Index(s, ":")
				host, portStr = s[:i], s[i+1:]
				if portStr == "" {
					return dialOptions{}, false, errors.New("invalid port in server address")
				}
			default:
				return dialOptions{}, false, errors.New("invalid server address (wrap IPv6 literals in [])")
			}
		}
	}
	if host == "" {
		return dialOptions{}, false, errors.New("missing host in server address")
	}
	opts.Server = host
	if portStr == "" {
		opts.Port = xmppDefaultPort
		if opts.DirectTLS {
			opts.Port = xmppDirectTLSPort
		}
		return opts, false, nil
	}
	p, perr := strconv.Atoi(portStr)
	if perr != nil || p < 1 || p > 65535 {
		return dialOptions{}, false, errors.New("invalid port in server address")
	}
	opts.Port = p
	return opts, false, nil
}

// overrideEndpoint builds the single endpoint of a server override.
func overrideEndpoint(opts dialOptions) xmppEndpoint {
	port := opts.Port
	if port <= 0 || port > 65535 {
		port = xmppDefaultPort
		if opts.DirectTLS {
			port = xmppDirectTLSPort
		}
	}
	return xmppEndpoint{Host: strings.TrimSuffix(opts.Server, "."), Port: uint16(port), DirectTLS: opts.DirectTLS, Source: "override"}
}

// resolveXMPPEndpoints finds the candidate endpoints for a domain via SRV records (RFC 6120 section 3.2.1,
// XEP-0368), falling back to domain:5222 when there are none.
func resolveXMPPEndpoints(ctx context.Context, r netResolver, domain string) []xmppEndpoint {
	ctx, span := telemetry.StartChild(ctx, "xmpp.dial.srv")
	defer span.End()
	lookupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	type cand struct {
		xmppEndpoint
		prio, weight uint16
	}
	var cands []cand
	disabled := map[bool]bool{}
	tlsCount := 0
	for _, direct := range []bool{true, false} {
		service := "xmpp-client"
		if direct {
			service = "xmpps-client"
		}
		// Go returns valid records plus an error when some targets are malformed: use them anyway.
		_, srvs, _ := r.LookupSRV(lookupCtx, service, "tcp", domain)
		for _, srv := range srvs {
			if srv == nil {
				continue
			}
			if srv.Target == "." {
				disabled[direct] = true // RFC 2782: service explicitly unavailable
				continue
			}
			host := strings.TrimSuffix(srv.Target, ".")
			if host == "" || srv.Port == 0 {
				continue
			}
			src := "srv"
			if direct {
				src = "srv-tls"
				tlsCount++
			}
			cands = append(cands, cand{xmppEndpoint{Host: host, Port: srv.Port, DirectTLS: direct, Source: src}, srv.Priority, srv.Weight})
		}
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].prio != cands[j].prio {
			return cands[i].prio < cands[j].prio
		}
		if cands[i].DirectTLS != cands[j].DirectTLS {
			return cands[i].DirectTLS // XEP-0368 section 3: direct TLS wins ties
		}
		return cands[i].weight > cands[j].weight
	})
	out := make([]xmppEndpoint, 0, len(cands)+1)
	seen := map[string]bool{}
	for _, c := range cands {
		key := strings.ToLower(c.addr()) + "/" + strconv.FormatBool(c.DirectTLS)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, c.xmppEndpoint)
		if len(out) >= maxXMPPEndpoints {
			break
		}
	}
	fallback := false
	if len(out) == 0 && !disabled[false] {
		out = append(out, xmppEndpoint{Host: domain, Port: xmppDefaultPort, Source: "fallback"})
		fallback = true
	}
	span.SetAttributes(attribute.Int("srv.count", len(out)), attribute.Int("srv.tls_count", tlsCount), attribute.Bool("srv.fallback", fallback))
	return out
}

// sortIPsPreferIPv6 returns a copy of the slice with IPv6 addresses first, then IPv4.
// Used for Phase 2.2 dual-stack: we try IPv6 when the resolver returns both families.
func sortIPsPreferIPv6(ips []string) []string {
	var v6, v4 []string
	for _, ip := range ips {
		if net.ParseIP(ip).To4() == nil {
			v6 = append(v6, ip)
		} else {
			v4 = append(v4, ip)
		}
	}
	out := make([]string, 0, len(ips))
	out = append(out, v6...)
	out = append(out, v4...)
	return out
}

// dialXMPP opens an XMPP client session with the given origin and password: an explicit server override
// (opts) or, otherwise, SRV discovery (xmpps-client, then xmpp-client, then domain:5222). Endpoints are
// tried in order until a session is established; a SASL failure stops the walk (wrong credentials will
// not improve on the next host and repeated failures can lock the account). Certificates are always
// verified against the JID's domain. Caller must close the session when done.
func dialXMPP(ctx context.Context, origin jid.JID, password string, opts dialOptions, log *zerolog.Logger) (*xmpp.Session, error) {
	domain := origin.Domainpart()
	ctx, dialSpan := telemetry.StartChild(ctx, "xmpp.dial",
		trace.WithAttributes(attribute.String("server.address", domain)))
	defer dialSpan.End()

	var endpoints []xmppEndpoint
	if opts.Server != "" {
		endpoints = []xmppEndpoint{overrideEndpoint(opts)}
	} else {
		endpoints = resolveXMPPEndpoints(ctx, xmppResolver, domain)
	}
	if log != nil {
		labels := make([]string, len(endpoints))
		for i, e := range endpoints {
			labels[i] = e.label()
		}
		log.Debug().Strs("endpoints", labels).Msg("XMPP dial: endpoint list")
	}
	var lastErr error
	for _, ep := range endpoints {
		if err := ctx.Err(); err != nil {
			lastErr = err
			break
		}
		sess, err := dialEndpointFunc(ctx, ep, origin, password, opts, log)
		if err == nil {
			return sess, nil
		}
		lastErr = err
		if _, isSASL := saslCondition(err); isSASL {
			break
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no XMPP client service found for %s", domain)
	}
	telemetry.Fail(dialSpan, lastErr)
	return nil, lastErr
}

// dialEndpoint connects to one endpoint and negotiates the stream (TLS, SASL, bind).
func dialEndpoint(ctx context.Context, ep xmppEndpoint, origin jid.JID, password string, opts dialOptions, log *zerolog.Logger) (*xmpp.Session, error) {
	t0 := time.Now()
	domain := origin.Domainpart()

	// Step 1: resolve the host (IP literals need no lookup).
	var ips []string
	if _, perr := netip.ParseAddr(ep.Host); perr == nil {
		ips = []string{ep.Host}
	} else {
		_, dnsSpan := telemetry.StartChild(ctx, "xmpp.dial.dns")
		resolveCtx, resolveCancel := context.WithTimeout(ctx, 15*time.Second)
		var err error
		ips, err = xmppResolver.LookupHost(resolveCtx, ep.Host)
		resolveCancel()
		if err != nil {
			telemetry.Fail(dnsSpan, err)
			dnsSpan.End()
			return nil, &dialError{msg: fmt.Sprintf("%s: DNS lookup: %v", ep.label(), err), err: err}
		}
		dnsSpan.SetAttributes(attribute.Int("ip_count", len(ips)))
		dnsSpan.End()
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("%s: DNS lookup: no addresses", ep.label())
	}
	if ep.Source == "override" && !opts.AllowPrivate {
		for _, ip := range ips {
			if a, perr := netip.ParseAddr(ip); perr != nil || blockedCardDAVAddr(a) {
				return nil, errors.New(errPrivateOverride)
			}
		}
	}
	ips = sortIPsPreferIPv6(ips)
	if log != nil {
		log.Debug().Dur("dns_ms", time.Since(t0)).Str("host", ep.Host).Int("ips", len(ips)).
			Msg("XMPP dial: DNS resolved (dual-stack, IPv6 preferred)")
	}

	// Step 2: TCP connect (and the TLS handshake for direct TLS), each resolved IP in order.
	t1 := time.Now()
	dialer := net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	_, tcpSpan := telemetry.StartChild(ctx, "xmpp.dial.tcp", trace.WithAttributes(
		attribute.String("server.address", ep.Host),
		attribute.Int("server.port", int(ep.Port)),
		attribute.Bool("xmpp.direct_tls", ep.DirectTLS),
		attribute.String("xmpp.endpoint.source", ep.Source)))
	var conn net.Conn
	var err error
	attempts := 0
	for _, ip := range ips {
		attempts++
		var c net.Conn
		c, err = dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip, strconv.Itoa(int(ep.Port))))
		if err != nil {
			continue
		}
		if ep.DirectTLS {
			// Verified against the JID domain, never the SRV target or override host.
			tc := tls.Client(c, xmppTLSConfig(domain, true))
			if err = tc.HandshakeContext(ctx); err != nil {
				_ = c.Close()
				break // a certificate problem is the same on every IP of this host
			}
			conn = tc
		} else {
			conn = c
		}
		break
	}
	tcpSpan.SetAttributes(attribute.Int("tcp.attempts", attempts))
	if conn == nil {
		telemetry.Fail(tcpSpan, err)
		tcpSpan.End()
		return nil, &dialError{msg: fmt.Sprintf("%s: connect: %v", ep.label(), err), err: err}
	}
	tcpSpan.End()
	if log != nil {
		log.Debug().Dur("tcp_ms", time.Since(t1)).Str("host", ep.Host).Bool("direct_tls", ep.DirectTLS).
			Msg("XMPP dial: connected, negotiating stream")
	}

	// Step 3: XMPP stream (TLS + SASL + bind). StartTLS is prohibited when the session is already secure.
	features := []xmpp.StreamFeature{
		xmpp.StartTLS(xmppTLSConfig(domain, false)),
		xmpp.SASL("", password, saslMechanisms...),
		xmpp.BindResource(),
	}
	var state xmpp.SessionState
	if ep.DirectTLS {
		state |= xmpp.Secure
	}
	streamCtx, streamSpan := telemetry.StartChild(ctx, "xmpp.dial.stream")
	sess, err := xmpp.NewSession(streamCtx, origin.Domain(), origin, conn, state,
		xmpp.NewNegotiator(func(*xmpp.Session, *xmpp.StreamConfig) xmpp.StreamConfig {
			return xmpp.StreamConfig{Features: features}
		}))
	if err != nil {
		telemetry.Fail(streamSpan, err)
		streamSpan.End()
		_ = conn.Close()
		if cond, isSASL := saslCondition(err); isSASL {
			// Server-supplied SASL text is never put into returned or logged errors.
			return nil, &dialError{msg: fmt.Sprintf("%s: SASL authentication failed (%s)", ep.label(), cond), err: err}
		}
		return nil, &dialError{msg: fmt.Sprintf("%s: %v", ep.label(), err), err: err}
	}
	streamSpan.End()
	if sess.State()&xmpp.Secure == 0 {
		_ = sess.Close()
		return nil, fmt.Errorf("%s: session is not secure", ep.label())
	}
	if log != nil {
		log.Debug().Dur("stream_ms", time.Since(t1)).Dur("total_ms", time.Since(t0)).
			Msg("XMPP dial: session established")
	}
	return sess, nil
}
