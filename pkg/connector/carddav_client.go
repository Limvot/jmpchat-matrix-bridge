package connector

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/emersion/go-vcard"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/mautrix/jabber/pkg/telemetry"
)

const maxCardDAVRedirectHops = 3

// cardDAVResponseCap is the response size cap (a var so tests can lower it).
var cardDAVResponseCap int64 = cardDAVMaxResponseBytes

// cardDAVClient is a small read-only CardDAV client (PROPFIND/REPORT with tolerant per-response
// decoding). It is created per operation; credentials are never logged or put in errors.
type cardDAVClient struct {
	hc       *http.Client
	cfg      CardDAVConfig
	entered  *url.URL // the URL the user gave: credentials only go to this host or its registrable domain
	user     string
	pass     string
	maxBytes int64
}

func newCardDAVClient(cfg CardDAVConfig, entered *url.URL, user, pass string) *cardDAVClient {
	return &cardDAVClient{
		hc: newCardDAVHTTPClient(cfg), cfg: cfg, entered: entered, user: user, pass: pass,
		maxBytes: cardDAVResponseCap,
	}
}

func (c *cardDAVClient) host() string { return c.entered.Hostname() }

// cdBook is a discovered address book (absolute URL).
type cdBook struct {
	URL  string
	Name string
}

const discoveryPropfind = `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:carddav"><d:prop>` +
	`<d:resourcetype/><d:displayname/><d:current-user-principal/><c:addressbook-home-set/>` +
	`</d:prop></d:propfind>`

// request sends a PROPFIND/REPORT, following up to maxCardDAVRedirectHops redirects manually with the
// same method and body (every hop is re-vetted by davRequest: https rule, same-site rule, SSRF dialer).
// It returns the result and the answering URL.
func (c *cardDAVClient) request(ctx context.Context, method string, u *url.URL, depth, body string, timeout time.Duration, handle func(davResponse)) (davResult, *url.URL, error) {
	cur := u
	for hop := 0; ; hop++ {
		res, redirect, err := c.davRequest(ctx, method, cur, depth, body, timeout, handle)
		if err != nil || redirect == nil {
			return res, cur, err
		}
		if hop >= maxCardDAVRedirectHops {
			return res, cur, newCDError(kindRedirect, "Too many redirects from %s.", c.host())
		}
		redirect.Fragment, redirect.RawFragment = "", ""
		cur = redirect
	}
}

// propfind runs the discovery PROPFIND (redirects followed by request). It returns the answering URL.
func (c *cardDAVClient) propfind(ctx context.Context, u *url.URL, depth string, handle func(davResponse)) (*url.URL, error) {
	_, final, err := c.request(ctx, "PROPFIND", u, depth, discoveryPropfind, cardDAVRequestTimeout, handle)
	return final, err
}

// probe PROPFINDs u at depth 0 and returns the response describing u itself.
func (c *cardDAVClient) probe(ctx context.Context, u *url.URL) (davResponse, *url.URL, error) {
	var all []davResponse
	final, err := c.propfind(ctx, u, "0", func(r davResponse) { all = append(all, r) })
	if err != nil {
		return davResponse{}, final, err
	}
	if len(all) == 0 {
		return davResponse{}, final, newCDError(kindDecode, "The server sent an answer the bridge could not read.")
	}
	want := strings.TrimRight(final.Path, "/")
	for _, r := range all {
		if strings.TrimRight(r.Href.Path, "/") == want {
			return r, final, nil
		}
	}
	return all[0], final, nil
}

// listBooks lists the address-book collections directly below home.
func (c *cardDAVClient) listBooks(ctx context.Context, home *url.URL) ([]cdBook, error) {
	var books []cdBook
	self := strings.TrimRight(home.Path, "/")
	_, err := c.propfind(ctx, home, "1", func(r davResponse) {
		if !r.IsBook || strings.TrimRight(r.Href.Path, "/") == self {
			return
		}
		if !sameSite(c.host(), r.Href.Hostname()) {
			return
		}
		books = append(books, cdBook{URL: r.Href.String(), Name: r.Name})
	})
	return books, err
}

// discover finds the address books for start: the URL itself if it is an address book, else the
// home-set of its principal (also via /.well-known/carddav when start is a bare host), filtered by
// selectBooks.
func (c *cardDAVClient) discover(ctx context.Context, start *url.URL, selector string) ([]cdBook, error) {
	info, final, err := c.probe(ctx, start)
	// A bare host whose root does not answer as DAV, or answers without anything usable (a web-server
	// root redirecting to the webmail UI, as is common in front of SOGo), gets the /.well-known/carddav
	// hop.
	useless := err == nil && !info.IsBook && info.Principal == nil && len(info.HomeSets) == 0
	if (useless || err != nil) && (start.Path == "" || start.Path == "/") {
		if k := cardDAVErrorKind(err); useless || k == kindNotFound || k == kindHTTP || k == kindDecode {
			wk := *start
			wk.Path = "/.well-known/carddav"
			if winfo, wfinal, werr := c.probe(ctx, &wk); werr == nil {
				info, final, err = winfo, wfinal, nil
			} else if wk := cardDAVErrorKind(werr); !useless && wk != kindNotFound && wk != kindHTTP && wk != kindDecode {
				err = werr // a security refusal or auth failure is more telling than the first 404
			}
		}
	}
	if err != nil {
		return nil, err
	}
	var all []cdBook
	if info.IsBook {
		all = []cdBook{{URL: final.String(), Name: info.Name}}
		if selector == "" {
			return all, nil // an explicit address-book URL is used as given
		}
	} else {
		var home *url.URL
		if len(info.HomeSets) > 0 {
			home = info.HomeSets[0]
		} else if info.Principal != nil {
			pinfo, _, perr := c.probe(ctx, info.Principal)
			if perr != nil {
				return nil, perr
			}
			if len(pinfo.HomeSets) > 0 {
				home = pinfo.HomeSets[0]
			}
		}
		if home == nil {
			home = final // the URL may itself be the home collection
		}
		if all, err = c.listBooks(ctx, home); err != nil {
			return nil, err
		}
	}
	return selectBooks(all, selector, c.host())
}

// skippedBook reports address books that are not the user's contacts: Nextcloud's generated books
// ("Recently contacted"), SOGo's "collected" addresses.
func skippedBook(b cdBook) bool {
	seg := strings.ToLower(path.Base(strings.TrimRight(mustPath(b.URL), "/")))
	name := strings.ToLower(b.Name)
	return strings.HasPrefix(seg, "z-app-generated") || strings.HasPrefix(name, "z-app-generated") ||
		seg == "collected" || strings.Contains(name, "collected") || strings.Contains(name, "recently contacted")
}

func mustPath(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Path
}

// selectBooks applies the selector (case-insensitive match on name or last path segment) or, without
// one, drops generated/collected books. host is only used in the message.
func selectBooks(all []cdBook, selector, host string) ([]cdBook, error) {
	selector = strings.TrimSpace(selector)
	var out []cdBook
	if selector != "" {
		for _, b := range all {
			seg := path.Base(strings.TrimRight(mustPath(b.URL), "/"))
			if strings.EqualFold(b.Name, selector) || strings.EqualFold(seg, selector) {
				out = append(out, b)
			}
		}
		if len(out) == 0 {
			var names []string
			for _, b := range all {
				if !skippedBook(b) {
					names = append(names, bookLabel(b))
				}
			}
			return nil, newCDError(kindNoBook, "No address book named %s. Available: %s.", selector, strings.Join(names, ", "))
		}
		return out, nil
	}
	for _, b := range all {
		if !skippedBook(b) {
			out = append(out, b)
		}
	}
	if len(out) == 0 {
		return nil, newCDError(kindNoBook, "Connected to %s, but found no address book. Try the server's base URL, or the address book's full URL.", host)
	}
	return out, nil
}

// bookLabel is the name shown to the user for a book (its display name, else its last path segment).
func bookLabel(b cdBook) string {
	if b.Name != "" {
		return b.Name
	}
	return path.Base(strings.TrimRight(mustPath(b.URL), "/"))
}

var cardDataProps = []string{"FN", "N", "NICKNAME", "ORG", "TEL", "KIND", "UID", "X-ABLabel"}

func addressDataXML() string {
	var b strings.Builder
	b.WriteString("<c:address-data>")
	for _, p := range cardDataProps {
		b.WriteString(`<c:prop name="` + p + `"/>`)
	}
	b.WriteString("</c:address-data>")
	return b.String()
}

func queryReportBody() string {
	return `<?xml version="1.0" encoding="utf-8"?><c:addressbook-query xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:carddav">` +
		`<d:prop><d:getetag/>` + addressDataXML() + `</d:prop><c:filter/></c:addressbook-query>`
}

func multigetBody(hrefs []string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?><c:addressbook-multiget xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:carddav">`)
	b.WriteString(`<d:prop><d:getetag/>` + addressDataXML() + `</d:prop>`)
	for _, h := range hrefs {
		var esc bytes.Buffer
		_ = xml.EscapeText(&esc, []byte(h))
		b.WriteString("<d:href>" + esc.String() + "</d:href>")
	}
	b.WriteString(`</c:addressbook-multiget>`)
	return b.String()
}

// bookFetch is what one address book yielded.
type bookFetch struct {
	Contacts  []cardContact
	Skips     map[string]int
	Cards     int
	Truncated bool
	Bytes     int64
}

func newBookFetch() *bookFetch { return &bookFetch{Skips: map[string]int{}} }

// addCard decodes one address-data blob (a bad card is counted, never fatal).
func (f *bookFetch) addCard(r davResponse, defaultCC string) {
	if r.Data == "" {
		return
	}
	f.Cards++
	card, err := vcard.NewDecoder(strings.NewReader(r.Data)).Decode()
	if err != nil {
		f.Skips[reasonBadCard]++
		return
	}
	contact, skips := extractContact(card, r.Href.Path, defaultCC)
	for k, v := range skips {
		f.Skips[k] += v
	}
	if contact != nil {
		f.Contacts = append(f.Contacts, *contact)
	}
}

// fetchBook downloads one address book with an addressbook-query REPORT; when the server rejects the
// query, or answers it without any card data, it falls back to a listing plus addressbook-multiget in
// batches of 100. budget is the number of
// cards still allowed; exceeding it fails the book (too_large).
func (c *cardDAVClient) fetchBook(ctx context.Context, b cdBook, defaultCC string, budget int) (*bookFetch, error) {
	u, err := url.Parse(b.URL)
	if err != nil {
		return nil, newCDError(kindBadURL, "bad address book address")
	}
	f := newBookFetch()
	// A redirect is followed (not ignored: an unfollowed one would look like an empty address book).
	res, _, err := c.request(ctx, "REPORT", u, "1", queryReportBody(), cardDAVReportTimeout, func(r davResponse) { f.addCard(r, defaultCC) })
	var ce *cardDAVError
	switch {
	case err == nil && f.Cards == 0 && !res.Truncated:
		// A 207 without any card data: some servers mishandle the empty <c:filter/>. List the book and
		// multiget instead (for a really empty book this costs one PROPFIND); keep the empty answer if
		// that fails or finds nothing either.
		mf := newBookFetch()
		if mres, merr := c.fetchViaMultiget(ctx, u, defaultCC, mf); merr == nil && mf.Cards > 0 {
			f, res = mf, mres
		}
	case err != nil && asCDError(err, &ce) && ce.kind == kindHTTP && ce.status != http.StatusUnauthorized:
		f = newBookFetch()
		if res, err = c.fetchViaMultiget(ctx, u, defaultCC, f); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	}
	f.Truncated, f.Bytes = res.Truncated, res.Bytes
	if res.Bad > 0 {
		f.Skips[reasonBadCard] += res.Bad
	}
	if f.Cards > budget {
		return nil, &cardDAVError{kind: kindTooLarge, msg: "The address book has more than the allowed number of contacts."}
	}
	return f, nil
}

func asCDError(err error, target **cardDAVError) bool {
	ce, ok := err.(*cardDAVError)
	if ok {
		*target = ce
	}
	return ok
}

func (c *cardDAVClient) fetchViaMultiget(ctx context.Context, book *url.URL, defaultCC string, f *bookFetch) (davResult, error) {
	var hrefs []string
	self := strings.TrimRight(book.Path, "/")
	if _, err := c.propfind(ctx, book, "1", func(r davResponse) {
		if r.IsBook || strings.TrimRight(r.Href.Path, "/") == self || !sameSite(c.host(), r.Href.Hostname()) {
			return
		}
		hrefs = append(hrefs, r.Href.EscapedPath())
	}); err != nil {
		return davResult{}, err
	}
	var total davResult
	for i := 0; i < len(hrefs); i += 100 {
		end := min(i+100, len(hrefs))
		res, _, err := c.request(ctx, "REPORT", book, "1", multigetBody(hrefs[i:end]), cardDAVReportTimeout, func(r davResponse) { f.addCard(r, defaultCC) })
		if err != nil {
			return total, err
		}
		total.Bad += res.Bad
		total.Bytes += res.Bytes
		total.Truncated = total.Truncated || res.Truncated
	}
	return total, nil
}

// fetchSummary aggregates all selected books.
type fetchSummary struct {
	Contacts    []cardContact
	Skips       map[string]int
	Cards       int
	BooksOK     int
	BooksFailed int
	FailKinds   map[string]int
	Truncated   bool
	Bytes       int64
	Books       []cdBook // the books actually used (re-discovered ones when stale)
}

// fetchAll fetches every book; a failing book is skipped and counted (never aborts the others).
func (c *cardDAVClient) fetchAll(ctx context.Context, books []cdBook, defaultCC string) *fetchSummary {
	s := &fetchSummary{Skips: map[string]int{}, FailKinds: map[string]int{}, Books: books}
	for _, b := range books {
		if ctx.Err() != nil {
			s.BooksFailed++
			s.FailKinds[kindTimeout]++
			continue
		}
		f, err := c.fetchBook(ctx, b, defaultCC, cardDAVMaxContacts-s.Cards)
		if err != nil {
			s.BooksFailed++
			s.FailKinds[cardDAVErrorKind(err)]++
			continue
		}
		s.BooksOK++
		s.Cards += f.Cards
		s.Contacts = append(s.Contacts, f.Contacts...)
		s.Truncated = s.Truncated || f.Truncated
		s.Bytes += f.Bytes
		for k, v := range f.Skips {
			s.Skips[k] += v
		}
	}
	return s
}

// failureError builds the user-facing error for a run where every book failed.
func (s *fetchSummary) failureError(host string) error {
	if s.BooksOK > 0 || s.BooksFailed == 0 {
		return nil
	}
	for _, k := range []string{kindAuthFailed, kindTimeout, kindTLS, kindDNS, kindRefusedPrivate, kindTooLarge, kindNotFound} {
		if s.FailKinds[k] > 0 {
			return &cardDAVError{kind: k, msg: fmt.Sprintf("Could not read any address book from %s (%s).", host, k)}
		}
	}
	return &cardDAVError{kind: kindHTTP, msg: fmt.Sprintf("Could not read any address book from %s.", host)}
}

// fetchContacts reads all contacts for creds. Books that vanished (404) trigger one re-discovery.
func fetchContacts(ctx context.Context, cfg CardDAVConfig, creds *cardDAVCreds) (*fetchSummary, error) {
	ctx, span := telemetry.StartChild(ctx, "carddav.fetch")
	defer span.End()
	entered, err := normalizeCardDAVURL(creds.URL, cfg)
	if err != nil {
		span.SetAttributes(attribute.String("carddav.outcome", kindBadURL))
		span.SetStatus(codes.Error, kindBadURL)
		return nil, err
	}
	host := entered.Hostname()
	span.SetAttributes(attribute.String("carddav.host", host))
	c := newCardDAVClient(cfg, entered, creds.Username, creds.Password)
	var books []cdBook
	for _, u := range creds.Books {
		books = append(books, cdBook{URL: u})
	}
	fail := func(err error) (*fetchSummary, error) {
		k := cardDAVErrorKind(err)
		span.SetAttributes(attribute.String("carddav.outcome", k))
		span.SetStatus(codes.Error, k)
		return nil, err
	}
	if len(books) == 0 {
		if books, err = c.discover(ctx, entered, creds.Book); err != nil {
			return fail(err)
		}
	}
	s := c.fetchAll(ctx, books, cfg.cc())
	if s.FailKinds[kindNotFound] > 0 {
		if nb, derr := c.discover(ctx, entered, creds.Book); derr == nil {
			s = c.fetchAll(ctx, nb, cfg.cc())
		}
	}
	span.SetAttributes(
		attribute.Int("carddav.books", len(s.Books)),
		attribute.Int("carddav.books_failed", s.BooksFailed),
		attribute.Int("carddav.cards", s.Cards),
		attribute.Int64("carddav.bytes", s.Bytes),
		attribute.Bool("carddav.truncated", s.Truncated),
	)
	if ferr := s.failureError(host); ferr != nil {
		return fail(ferr)
	}
	span.SetAttributes(attribute.String("carddav.outcome", "ok"))
	return s, nil
}

// validateCardDAVLogin connects and discovers before anything is saved. It returns the credentials to
// store (with the discovered book URLs) and the books.
func validateCardDAVLogin(ctx context.Context, cfg CardDAVConfig, rawURL, user, pass, selector string) (*cardDAVCreds, []cdBook, error) {
	entered, err := normalizeCardDAVURL(rawURL, cfg)
	if err != nil {
		return nil, nil, err
	}
	c := newCardDAVClient(cfg, entered, user, pass)
	books, err := c.discover(ctx, entered, selector)
	if err != nil {
		return nil, nil, err
	}
	creds := &cardDAVCreds{URL: entered.String(), Username: user, Password: pass, Book: strings.TrimSpace(selector)}
	for _, b := range books {
		creds.Books = append(creds.Books, b.URL)
	}
	return creds, books, nil
}
