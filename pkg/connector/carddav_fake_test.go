package connector

import (
	"crypto/x509"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
)

// fakeBook is one address book of the fake server.
type fakeBook struct {
	path  string // e.g. /dav/alice/contacts/
	name  string
	cards map[string]string // file name -> vCard
	mode  string            // "", "fail500", "huge", "trunc507", "reject_query", "empty_query", "gone"
	// reportTo, when set, answers REPORT with a 308 to this path.
	reportTo string
}

// fakeDAV is a minimal CardDAV server (PROPFIND/REPORT, Basic auth) on httptest.NewTLSServer.
type fakeDAV struct {
	t           *testing.T
	srv         *httptest.Server
	user, pass  string
	wellKnownTo string // path (or absolute URL) the 301 of /.well-known/carddav points to
	rootDAV     string // path answering with current-user-principal (the "base" URL)
	principal   string
	home        string
	homeHref    string // overrides how the home-set href is written (absolute URLs etc.)
	rootUseless bool   // "/" answers 207 with a plain collection (no principal, home set or book)
	books       []*fakeBook

	mu       sync.Mutex
	requests []string
}

func newFakeDAV(t *testing.T) *fakeDAV {
	t.Helper()
	f := &fakeDAV{t: t, user: "alice", pass: "s3cret",
		wellKnownTo: "/dav/", rootDAV: "/dav/", principal: "/dav/principals/alice/", home: "/dav/alice/"}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(f.srv.Certificate())
	saved := cardDAVRootCAs
	cardDAVRootCAs = pool
	t.Cleanup(func() { cardDAVRootCAs = saved })
	return f
}

func (f *fakeDAV) addBook(name, path string, mode string, cards ...string) *fakeBook {
	b := &fakeBook{path: path, name: name, mode: mode, cards: map[string]string{}}
	for i, c := range cards {
		b.cards[fmt.Sprintf("card%d.vcf", i+1)] = c
	}
	f.books = append(f.books, b)
	return b
}

func (f *fakeDAV) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

func trimSlash(p string) string { return strings.TrimRight(p, "/") }

func xmlEsc(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

const msHead = `<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:carddav">`

func respOK(href, props string) string {
	return `<d:response><d:href>` + xmlEsc(href) + `</d:href><d:propstat><d:prop>` + props +
		`</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`
}

func (f *fakeDAV) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path)
	f.mu.Unlock()
	if u, p, ok := r.BasicAuth(); !ok || u != f.user || p != f.pass {
		w.Header().Set("WWW-Authenticate", `Basic realm="x"`)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	p := trimSlash(r.URL.Path)
	if p == "/.well-known/carddav" && f.wellKnownTo != "" {
		w.Header().Set("Location", f.wellKnownTo)
		w.WriteHeader(http.StatusMovedPermanently)
		return
	}
	for _, b := range f.books {
		if trimSlash(b.path) == p {
			f.handleBook(w, r, b)
			return
		}
	}
	var props []string
	switch {
	case p == "" && f.rootUseless:
		props = []string{respOK("/", `<d:resourcetype><d:collection/></d:resourcetype>`)}
	case p == trimSlash(f.rootDAV):
		props = []string{respOK(f.rootDAV, `<d:resourcetype><d:collection/></d:resourcetype><d:current-user-principal><d:href>`+xmlEsc(f.principal)+`</d:href></d:current-user-principal>`)}
	case p == trimSlash(f.principal):
		href := f.homeHref
		if href == "" {
			href = f.home
		}
		props = []string{respOK(f.principal, `<d:resourcetype><d:principal/></d:resourcetype><c:addressbook-home-set><d:href>`+xmlEsc(href)+`</d:href></c:addressbook-home-set>`)}
	case p == trimSlash(f.home):
		props = []string{respOK(f.home, `<d:resourcetype><d:collection/></d:resourcetype>`)}
		if r.Header.Get("Depth") == "1" {
			for _, b := range f.books {
				props = append(props, respOK(b.path, `<d:resourcetype><d:collection/><c:addressbook/></d:resourcetype><d:displayname>`+xmlEsc(b.name)+`</d:displayname>`))
			}
		}
	default:
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeMS(w, strings.Join(props, ""))
}

func writeMS(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_, _ = w.Write([]byte(msHead + body + `</d:multistatus>`))
}

func (f *fakeDAV) handleBook(w http.ResponseWriter, r *http.Request, b *fakeBook) {
	if b.mode == "gone" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	body := readAll(r)
	names := make([]string, 0, len(b.cards))
	for n := range b.cards {
		names = append(names, n)
	}
	sort.Strings(names)
	switch r.Method {
	case "PROPFIND":
		out := respOK(b.path, `<d:resourcetype><d:collection/><c:addressbook/></d:resourcetype><d:displayname>`+xmlEsc(b.name)+`</d:displayname>`)
		if r.Header.Get("Depth") == "1" {
			for _, n := range names {
				out += respOK(b.path+n, `<d:getetag>"1"</d:getetag>`)
			}
		}
		writeMS(w, out)
	case "REPORT":
		if b.reportTo != "" {
			w.Header().Set("Location", b.reportTo)
			w.WriteHeader(http.StatusPermanentRedirect)
			return
		}
		multiget := strings.Contains(body, "addressbook-multiget")
		switch {
		case b.mode == "fail500":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Internal error with Secret Contact Name"))
			return
		case b.mode == "reject_query" && !multiget:
			w.WriteHeader(http.StatusBadRequest)
			return
		case b.mode == "huge":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusMultiStatus)
			_, _ = w.Write([]byte(msHead))
			chunk := strings.Repeat("x", 4096)
			for i := 0; i < 64; i++ {
				_, _ = w.Write([]byte(`<d:response><d:href>/x</d:href><d:propstat><d:prop><d:displayname>` + chunk + `</d:displayname></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`))
			}
			_, _ = w.Write([]byte(`</d:multistatus>`))
			return
		}
		out := ""
		if b.mode == "empty_query" && !multiget { // 207 listing the cards without address-data
			for _, n := range names {
				out += respOK(b.path+n, `<d:getetag>"1"</d:getetag>`)
			}
			writeMS(w, out)
			return
		}
		for _, n := range names {
			if multiget && !strings.Contains(body, "<d:href>"+b.path+n+"</d:href>") {
				continue
			}
			out += respOK(b.path+n, `<d:getetag>"1"</d:getetag><c:address-data>`+xmlEsc(strings.ReplaceAll(b.cards[n], "\n", "\r\n"))+`</c:address-data>`)
		}
		if b.mode == "trunc507" {
			out += `<d:response><d:href>` + b.path + `</d:href><d:status>HTTP/1.1 507 Insufficient Storage</d:status></d:response>`
		}
		writeMS(w, out)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func readAll(r *http.Request) string {
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := r.Body.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			return b.String()
		}
	}
}

// helpers for test vCards

func vc(fn string, tels ...string) string {
	s := "BEGIN:VCARD\nVERSION:3.0\nFN:" + fn + "\n"
	for _, t := range tels {
		s += "TEL:" + t + "\n"
	}
	return s + "END:VCARD"
}

const brokenVCard = "BEGIN:VCARD\nVERSION:3.0\nFN:Broken\nTEL:555 000 9999\n" // no END

func testCardDAVCfg() CardDAVConfig {
	return CardDAVConfig{AllowPrivateHosts: true, DefaultCountryCode: "1"}
}
