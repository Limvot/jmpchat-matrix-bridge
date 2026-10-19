package connector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/publicsuffix"
)

// Classified CardDAV failure kinds. Spans and logs record only these, never err.Error() (server error
// text can embed response bodies, paths and names).
const (
	kindAuthFailed     = "auth_failed"
	kindNotFound       = "not_found"
	kindTLS            = "tls"
	kindTimeout        = "timeout"
	kindDNS            = "dns"
	kindRefusedPrivate = "refused_private"
	kindTooLarge       = "too_large"
	kindDecode         = "decode"
	kindHTTP           = "http_error"
	kindRedirect       = "redirect_refused"
	kindInsecure       = "insecure"
	kindBadURL         = "bad_url"
	kindNetwork        = "network"
	kindCrossHost      = "cross_host"
	kindNoBook         = "no_address_book"
)

// cardDAVError is a failure with a classified kind. msg is safe to show to the user (host only, no
// paths, credentials or server text).
type cardDAVError struct {
	kind   string
	status int
	msg    string
	err    error
}

func (e *cardDAVError) Error() string { return e.msg }
func (e *cardDAVError) Unwrap() error { return e.err }

func newCDError(kind, format string, args ...any) *cardDAVError {
	return &cardDAVError{kind: kind, msg: fmt.Sprintf(format, args...)}
}

// errPrivateTarget is returned by the dialer for a blocked address.
var errPrivateTarget = errors.New("carddav: private or local address refused")

// errBodyTooLarge is returned by limitedBody when the response exceeds the cap.
var errBodyTooLarge = errors.New("carddav: response too large")

// cardDAVErrorKind classifies any error from the HTTP/DAV layer.
func cardDAVErrorKind(err error) string {
	if err == nil {
		return ""
	}
	var ce *cardDAVError
	if errors.As(err, &ce) {
		return ce.kind
	}
	var (
		dnsErr  *net.DNSError
		certErr *tls.CertificateVerificationError
		unkAuth x509.UnknownAuthorityError
		hostErr x509.HostnameError
		certInv x509.CertificateInvalidError
		tlsErr  tls.RecordHeaderError
	)
	switch {
	case errors.Is(err, errPrivateTarget):
		return kindRefusedPrivate
	case errors.Is(err, errBodyTooLarge):
		return kindTooLarge
	case errors.Is(err, context.DeadlineExceeded):
		return kindTimeout
	case errors.As(err, &dnsErr):
		return kindDNS
	case errors.As(err, &certErr), errors.As(err, &unkAuth), errors.As(err, &hostErr), errors.As(err, &certInv), errors.As(err, &tlsErr):
		return kindTLS
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return kindTimeout
	}
	return kindNetwork
}

var (
	cgnatPrefix  = netip.MustParsePrefix("100.64.0.0/10")
	zeroNetPfx   = netip.MustParsePrefix("0.0.0.0/8")
	nat64Prefix  = netip.MustParsePrefix("64:ff9b::/96")
	sixToFourPfx = netip.MustParsePrefix("2002::/16")
)

// blockedCardDAVAddr reports whether a CardDAV connection to a must be refused: loopback, private
// (RFC1918/ULA), link-local (incl. 169.254.169.254), unspecified, multicast, CGNAT, and NAT64/6to4
// forms embedding such a v4 address.
func blockedCardDAVAddr(a netip.Addr) bool {
	a = a.Unmap()
	if !a.IsValid() {
		return true
	}
	if a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() ||
		a.IsInterfaceLocalMulticast() || a.IsMulticast() || a.IsUnspecified() {
		return true
	}
	if a.Is4() {
		return cgnatPrefix.Contains(a) || zeroNetPfx.Contains(a)
	}
	b := a.As16()
	switch {
	case nat64Prefix.Contains(a):
		return blockedCardDAVAddr(netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}))
	case sixToFourPfx.Contains(a):
		return blockedCardDAVAddr(netip.AddrFrom4([4]byte{b[2], b[3], b[4], b[5]}))
	}
	return false
}

// cardDAVRootCAs is a test seam: extra trusted roots for httptest TLS servers (nil in production).
var cardDAVRootCAs *x509.CertPool

// newCardDAVHTTPClient builds the CardDAV HTTP client (a var so tests can replace it). It never follows
// redirects (discovery handles them), ignores proxy environment variables (a proxy would be vetted by
// the dialer instead of the target) and refuses private targets unless allowed.
var newCardDAVHTTPClient = func(cfg CardDAVConfig) *http.Client {
	dialer := &net.Dialer{Timeout: 15 * time.Second}
	if !cfg.AllowPrivateHosts {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return errPrivateTarget
			}
			if blockedCardDAVAddr(ap.Addr()) {
				return errPrivateTarget
			}
			return nil
		}
	}
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           dialer.DialContext,
			TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: cardDAVRootCAs},
			TLSHandshakeTimeout:   15 * time.Second,
			ResponseHeaderTimeout: cardDAVReportTimeout,
			MaxIdleConns:          4,
			IdleConnTimeout:       30 * time.Second,
			DisableCompression:    false,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// limitedBody errors (instead of silently truncating) once more than max bytes were read.
type limitedBody struct {
	rc   io.ReadCloser
	left int64
}

func (b *limitedBody) Read(p []byte) (int, error) {
	if b.left < 0 {
		return 0, errBodyTooLarge
	}
	n, err := b.rc.Read(p)
	b.left -= int64(n)
	if b.left < 0 {
		return n, errBodyTooLarge
	}
	return n, err
}

func (b *limitedBody) Close() error { return b.rc.Close() }

// normalizeCardDAVURL applies the URL input rules: https:// is added to a bare host, userinfo is rejected,
// http:// needs allow_insecure_http, query and fragment are dropped.
func normalizeCardDAVURL(in string, cfg CardDAVConfig) (*url.URL, error) {
	s := strings.TrimSpace(in)
	if s == "" {
		return nil, newCDError(kindBadURL, "Please give a server address, for example https://dav.example.org/")
	}
	if !strings.Contains(s, "://") {
		s = "https://" + s
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" || u.Hostname() == "" {
		return nil, newCDError(kindBadURL, "That does not look like a valid server address.")
	}
	if u.User != nil || strings.Contains(u.Host, "@") {
		return nil, newCDError(kindBadURL, "Do not put a username or password in the address; the bridge asks for them separately.")
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
	case "http":
		if !cfg.AllowInsecureHTTP {
			return nil, newCDError(kindInsecure, "CardDAV URLs must use https:// (your password would be sent unencrypted).")
		}
	default:
		return nil, newCDError(kindBadURL, "CardDAV URLs must use https://.")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.RawQuery, u.Fragment, u.RawFragment = "", "", ""
	return u, nil
}

// sameSite reports whether host b may receive the credentials entered for host a: the same host, or
// the same registrable domain (eTLD+1), so iCloud's pNN-contacts.icloud.com passes but a foreign host
// does not. IPs and unlisted names must match exactly.
func sameSite(a, b string) bool {
	a, b = strings.ToLower(a), strings.ToLower(b)
	if a == b {
		return true
	}
	if _, err := netip.ParseAddr(strings.Trim(a, "[]")); err == nil {
		return false
	}
	if _, err := netip.ParseAddr(strings.Trim(b, "[]")); err == nil {
		return false
	}
	ea, err1 := publicsuffix.EffectiveTLDPlusOne(a)
	eb, err2 := publicsuffix.EffectiveTLDPlusOne(b)
	return err1 == nil && err2 == nil && ea == eb
}
