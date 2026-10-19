package connector

import (
	"context"
	"net/netip"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func credsFor(f *fakeDAV, urlPath string) *cardDAVCreds {
	return &cardDAVCreds{URL: f.srv.URL + urlPath, Username: f.user, Password: f.pass}
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func contactNames(s *fetchSummary) []string {
	var out []string
	for _, c := range s.Contacts {
		out = append(out, c.Name)
	}
	sort.Strings(out)
	return out
}

func wantKind(t *testing.T, err error, kind string) {
	t.Helper()
	if err == nil || cardDAVErrorKind(err) != kind {
		t.Fatalf("err = %v (kind %q), want kind %q", err, cardDAVErrorKind(err), kind)
	}
}

func TestCardDAVDiscoverViaWellKnownAndFetch(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "",
		vc("Ann", "(555) 111-2222"), vc("Bob", "555 111 3333", "555 111 4444"), brokenVCard)
	creds := credsFor(f, "") // bare host: discovery through /.well-known/carddav (301 on PROPFIND)
	s, err := fetchContacts(ctxT(t), testCardDAVCfg(), creds)
	if err != nil {
		t.Fatal(err)
	}
	if got := contactNames(s); !reflect.DeepEqual(got, []string{"Ann", "Bob"}) {
		t.Errorf("contacts = %v", got)
	}
	if s.Cards != 3 || s.Skips[reasonBadCard] != 1 || s.BooksOK != 1 || s.BooksFailed != 0 {
		t.Errorf("summary = %+v", s)
	}
	seen := strings.Join(f.seen(), "\n")
	if !strings.Contains(seen, "PROPFIND /.well-known/carddav") || !strings.Contains(seen, "REPORT /dav/alice/contacts/") {
		t.Errorf("requests:\n%s", seen)
	}
}

func TestCardDAVExplicitBookURL(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "", vc("Ann", "555 111 2222"))
	f.addBook("Other", "/dav/alice/other/", "", vc("Zed", "555 111 9999"))
	s, err := fetchContacts(ctxT(t), testCardDAVCfg(), credsFor(f, "/dav/alice/contacts/"))
	if err != nil {
		t.Fatal(err)
	}
	if got := contactNames(s); !reflect.DeepEqual(got, []string{"Ann"}) {
		t.Errorf("contacts = %v", got)
	}
}

func TestCardDAVAuthFailure(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "", vc("Ann", "555 111 2222"))
	c := credsFor(f, "/dav/")
	c.Password = "wrong"
	_, err := fetchContacts(ctxT(t), testCardDAVCfg(), c)
	wantKind(t, err, kindAuthFailed)
	if strings.Contains(err.Error(), "wrong") || strings.Contains(err.Error(), "/dav") {
		t.Errorf("error leaks details: %v", err)
	}
}

func TestCardDAVNoBooks(t *testing.T) {
	f := newFakeDAV(t)
	_, _, err := validateCardDAVLogin(ctxT(t), testCardDAVCfg(), f.srv.URL+"/dav/", f.user, f.pass, "")
	wantKind(t, err, kindNoBook)
}

func TestCardDAVSkipsGeneratedBooksAndSelector(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "", vc("Ann", "555 111 2222"))
	f.addBook("Recently contacted", "/dav/alice/z-app-generated--contactsinteraction--recent/", "", vc("Gen", "555 111 7777"))
	f.addBook("Work", "/dav/alice/work/", "", vc("Wil", "555 111 8888"))
	creds, books, err := validateCardDAVLogin(ctxT(t), testCardDAVCfg(), f.srv.URL+"/dav/", f.user, f.pass, "")
	if err != nil || len(books) != 2 || len(creds.Books) != 2 {
		t.Fatalf("books = %+v err = %v", books, err)
	}
	s, err := fetchContacts(ctxT(t), testCardDAVCfg(), creds)
	if err != nil || !reflect.DeepEqual(contactNames(s), []string{"Ann", "Wil"}) {
		t.Fatalf("contacts = %v err = %v", contactNames(s), err)
	}
	// Selector by name (case-insensitive) and by last path segment.
	for _, sel := range []string{"work", "WORK"} {
		_, books, err = validateCardDAVLogin(ctxT(t), testCardDAVCfg(), f.srv.URL+"/dav/", f.user, f.pass, sel)
		if err != nil || len(books) != 1 || books[0].Name != "Work" {
			t.Fatalf("selector %q: %+v %v", sel, books, err)
		}
	}
	_, _, err = validateCardDAVLogin(ctxT(t), testCardDAVCfg(), f.srv.URL+"/dav/", f.user, f.pass, "nope")
	wantKind(t, err, kindNoBook)
	if !strings.Contains(err.Error(), "Available: Contacts, Work") {
		t.Errorf("error should list names: %v", err)
	}
}

// SOGo layout: base /SOGo/dav/, principal /SOGo/dav/alice/, home /SOGo/dav/alice/Contacts/, a
// personal book, SOGo's collected addresses (skipped by default) and a global directory book that
// refuses the REPORT. The failing book is skipped and counted; the sync carries on.
func TestCardDAVSOGoLayoutWithFailingBook(t *testing.T) {
	f := newFakeDAV(t)
	f.wellKnownTo, f.rootDAV = "/SOGo/dav/", "/SOGo/dav/"
	f.principal, f.home = "/SOGo/dav/alice/", "/SOGo/dav/alice/Contacts/"
	f.addBook("Personal Address Book", "/SOGo/dav/alice/Contacts/personal/", "",
		vc("Ann", "555 111 2222"), vc("Bob", "555 111 3333"))
	f.addBook("Collected Address Book", "/SOGo/dav/alice/Contacts/collected/", "", vc("Collected", "555 111 5555"))
	f.addBook("Global Address Book", "/SOGo/dav/alice/Contacts/public/", "fail500", vc("Corp", "555 111 6666"))
	creds, books, err := validateCardDAVLogin(ctxT(t), testCardDAVCfg(), f.srv.URL+"/", f.user, f.pass, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 2 { // collected is skipped; personal + the global one remain
		t.Fatalf("books = %+v", books)
	}
	s, err := fetchContacts(ctxT(t), testCardDAVCfg(), creds)
	if err != nil {
		t.Fatalf("one failing book must not fail the sync: %v", err)
	}
	if s.BooksOK != 1 || s.BooksFailed != 1 || s.FailKinds[kindHTTP] != 1 {
		t.Errorf("summary = %+v", s)
	}
	if !reflect.DeepEqual(contactNames(s), []string{"Ann", "Bob"}) {
		t.Errorf("contacts = %v", contactNames(s))
	}
	// An explicit SOGo book URL works directly.
	s, err = fetchContacts(ctxT(t), testCardDAVCfg(), credsFor(f, "/SOGo/dav/alice/Contacts/personal/"))
	if err != nil || len(s.Contacts) != 2 {
		t.Fatalf("explicit book: %v %+v", err, s)
	}
	// All books failing is an error.
	f.books[0].mode = "fail500"
	_, err = fetchContacts(ctxT(t), testCardDAVCfg(), creds)
	if err == nil {
		t.Fatal("all books failing must be an error")
	}
}

func TestCardDAVMultigetFallback(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "reject_query", vc("Ann", "555 111 2222"), brokenVCard, vc("Cy", "555 111 3333"))
	s, err := fetchContacts(ctxT(t), testCardDAVCfg(), credsFor(f, "/dav/alice/contacts/"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(contactNames(s), []string{"Ann", "Cy"}) || s.Skips[reasonBadCard] != 1 {
		t.Errorf("contacts = %v skips = %v", contactNames(s), s.Skips)
	}
}

func TestCardDAVTruncation507IsNotAnError(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "trunc507", vc("Ann", "555 111 2222"))
	s, err := fetchContacts(ctxT(t), testCardDAVCfg(), credsFor(f, "/dav/alice/contacts/"))
	if err != nil || !s.Truncated || len(s.Contacts) != 1 {
		t.Fatalf("err=%v summary=%+v", err, s)
	}
}

func TestCardDAVBodyOverCap(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "huge")
	saved := cardDAVResponseCap
	cardDAVResponseCap = 64 << 10
	t.Cleanup(func() { cardDAVResponseCap = saved })
	_, err := fetchContacts(ctxT(t), testCardDAVCfg(), credsFor(f, "/dav/alice/contacts/"))
	wantKind(t, err, kindTooLarge)
}

func TestCardDAVRedirectToHTTPRefused(t *testing.T) {
	f := newFakeDAV(t)
	f.wellKnownTo = "http://" + strings.TrimPrefix(f.srv.URL, "https://") + "/dav/"
	_, _, err := validateCardDAVLogin(ctxT(t), testCardDAVCfg(), f.srv.URL, f.user, f.pass, "")
	wantKind(t, err, kindInsecure)
}

func TestCardDAVRedirectToOtherSiteRefused(t *testing.T) {
	f := newFakeDAV(t)
	f.wellKnownTo = "https://evil.example.net/dav/"
	_, _, err := validateCardDAVLogin(ctxT(t), testCardDAVCfg(), f.srv.URL, f.user, f.pass, "")
	wantKind(t, err, kindCrossHost)
}

func TestCardDAVHomeSetHrefs(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "", vc("Ann", "555 111 2222"))
	// Absolute href on the same host works (host is kept, not dropped).
	f.homeHref = f.srv.URL + "/dav/alice/"
	if _, books, err := validateCardDAVLogin(ctxT(t), testCardDAVCfg(), f.srv.URL+"/dav/", f.user, f.pass, ""); err != nil || len(books) != 1 {
		t.Fatalf("absolute same-host home-set: %v %+v", books, err)
	}
	// An absolute href on a foreign site is refused before credentials are sent there.
	f.homeHref = "https://evil.example.net/dav/alice/"
	_, _, err := validateCardDAVLogin(ctxT(t), testCardDAVCfg(), f.srv.URL+"/dav/", f.user, f.pass, "")
	wantKind(t, err, kindCrossHost)
}

func TestCardDAVLoopbackRefusedUnlessAllowed(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "", vc("Ann", "555 111 2222"))
	cfg := CardDAVConfig{}
	_, _, err := validateCardDAVLogin(ctxT(t), cfg, f.srv.URL+"/dav/", f.user, f.pass, "")
	wantKind(t, err, kindRefusedPrivate)
	if !strings.Contains(err.Error(), "allow_private_hosts") {
		t.Errorf("refusal should name the admin knob: %v", err)
	}
	if n := len(f.seen()); n != 0 {
		t.Errorf("server was contacted %d times despite the block", n)
	}
}

func TestCardDAVProxyEnvIgnored(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "", vc("Ann", "555 111 2222"))
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("ALL_PROXY", "http://127.0.0.1:1")
	if _, err := fetchContacts(ctxT(t), testCardDAVCfg(), credsFor(f, "/dav/alice/contacts/")); err != nil {
		t.Fatalf("proxy env must be ignored: %v", err)
	}
}

func TestCardDAVStaleBookIsRediscovered(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "", vc("Ann", "555 111 2222"))
	creds := credsFor(f, "/dav/")
	creds.Books = []string{f.srv.URL + "/dav/alice/old-book/"}
	s, err := fetchContacts(ctxT(t), testCardDAVCfg(), creds)
	if err != nil || len(s.Contacts) != 1 {
		t.Fatalf("err=%v %+v", err, s)
	}
}

func TestCardDAVMaxContactsPerBook(t *testing.T) {
	f := newFakeDAV(t)
	b := f.addBook("Contacts", "/dav/alice/contacts/", "")
	_ = b
	c := newCardDAVClient(testCardDAVCfg(), mustURL(t, f.srv.URL), f.user, f.pass)
	f.books[0].cards["a.vcf"], f.books[0].cards["b.vcf"] = vc("A", "555 111 2222"), vc("B", "555 111 3333")
	_, err := c.fetchBook(ctxT(t), cdBook{URL: f.srv.URL + "/dav/alice/contacts/"}, "1", 1)
	wantKind(t, err, kindTooLarge)
}

func TestNormalizeCardDAVURL(t *testing.T) {
	cfg := CardDAVConfig{}
	for _, tc := range []struct {
		in, want, kind string
		insecure       bool
	}{
		{"dav.example.org", "https://dav.example.org", "", false},
		{"https://dav.example.org/SOGo/dav/alice/?x=1#frag", "https://dav.example.org/SOGo/dav/alice/", "", false},
		{"HTTPS://Dav.Example.org:8443/x", "https://Dav.Example.org:8443/x", "", false},
		{"https://alice:pw@dav.example.org/", "", kindBadURL, false},
		{"http://dav.example.org/", "", kindInsecure, false},
		{"http://dav.example.org/", "http://dav.example.org/", "", true},
		{"ftp://dav.example.org/", "", kindBadURL, false},
		{"", "", kindBadURL, false},
		{"https://", "", kindBadURL, false},
	} {
		c := cfg
		c.AllowInsecureHTTP = tc.insecure
		u, err := normalizeCardDAVURL(tc.in, c)
		if tc.kind != "" {
			wantKind(t, err, tc.kind)
			continue
		}
		if err != nil || u.String() != tc.want {
			t.Errorf("normalize(%q) = %v, %v; want %q", tc.in, u, err, tc.want)
		}
	}
}

func TestBlockedCardDAVAddr(t *testing.T) {
	for addr, blocked := range map[string]bool{
		"127.0.0.1": true, "::1": true, "10.1.2.3": true, "172.16.0.1": true, "192.168.1.1": true,
		"169.254.169.254": true, "0.0.0.0": true, "0.1.2.3": true, "100.64.0.1": true, "100.127.255.255": true,
		"224.0.0.1": true, "fe80::1": true, "fc00::1": true, "fd12::1": true, "::": true,
		"::ffff:127.0.0.1": true, "::ffff:10.0.0.1": true, "64:ff9b::a00:1": true, "64:ff9b::7f00:1": true,
		"2002:7f00:1::": true,
		"8.8.8.8":       false, "93.184.216.34": false, "100.63.255.255": false, "100.128.0.1": false,
		"2606:4700::1111": false, "::ffff:8.8.8.8": false, "64:ff9b::808:808": false,
	} {
		if got := blockedCardDAVAddr(netip.MustParseAddr(addr)); got != blocked {
			t.Errorf("blocked(%s) = %v, want %v", addr, got, blocked)
		}
	}
}

func TestSameSite(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"contacts.icloud.com", "p42-contacts.icloud.com", true},
		{"dav.example.org", "dav.example.org", true},
		{"DAV.example.org", "dav.example.org", true},
		{"mail.example.org", "files.example.org", true},
		{"example.org", "evil.example.net", false},
		{"a.co.uk", "b.co.uk", false},
		{"10.0.0.1", "11.0.0.1", false},
		{"10.0.0.1", "10.0.0.1", true},
		{"localhost", "otherhost", false},
	} {
		if got := sameSite(tc.a, tc.b); got != tc.want {
			t.Errorf("sameSite(%q,%q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
