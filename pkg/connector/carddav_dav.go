package connector

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	nsDAV     = "DAV:"
	nsCardDAV = "urn:ietf:params:xml:ns:carddav"
)

// xResponse is one <response> of a multistatus, decoded independently of its siblings.
type xResponse struct {
	Hrefs     []string    `xml:"DAV: href"`
	Status    string      `xml:"DAV: status"`
	Propstats []xPropstat `xml:"DAV: propstat"`
}

type xPropstat struct {
	Status string `xml:"DAV: status"`
	Prop   xProp  `xml:"DAV: prop"`
}

type xProp struct {
	ResourceType struct {
		Children []struct {
			XMLName xml.Name
		} `xml:",any"`
	} `xml:"DAV: resourcetype"`
	DisplayName string `xml:"DAV: displayname"`
	Principal   struct {
		Href string `xml:"DAV: href"`
	} `xml:"DAV: current-user-principal"`
	HomeSet struct {
		Hrefs []string `xml:"DAV: href"`
	} `xml:"urn:ietf:params:xml:ns:carddav addressbook-home-set"`
	AddressData string `xml:"urn:ietf:params:xml:ns:carddav address-data"`
}

// davResponse is a decoded response entry. Props merges the 2xx propstats.
type davResponse struct {
	Href      *url.URL // resolved against the request URL (host kept)
	Status    int      // response-level status (0 if none)
	IsBook    bool     // resourcetype contains carddav addressbook
	Name      string
	Principal *url.URL
	HomeSets  []*url.URL
	Data      string
}

// davResult is the outcome of one multistatus request.
type davResult struct {
	Final     *url.URL // the URL that answered (after redirects)
	Responses int      // entries decoded
	Bad       int      // entries that could not be decoded or carried a non-2xx status
	Truncated bool     // the server reported a 507 truncation for the request URI (RFC 6352 8.6.1)
	Bytes     int64
}

// parseDAVStatus returns the code of "HTTP/1.1 207 Multi-Status" (0 if unparsable).
func parseDAVStatus(s string) int {
	f := strings.Fields(s)
	if len(f) < 2 {
		return 0
	}
	n, _ := strconv.Atoi(f[1])
	return n
}

func okStatus(code int) bool { return code >= 200 && code < 300 }

// convertResponse builds a davResponse from a decoded entry. ok=false for entries that are unusable
// (no href, or a non-2xx response status).
func convertResponse(base *url.URL, xr *xResponse) (davResponse, bool, int) {
	var out davResponse
	if len(xr.Hrefs) == 0 {
		return out, false, 0
	}
	h, err := url.Parse(strings.TrimSpace(xr.Hrefs[0]))
	if err != nil {
		return out, false, 0
	}
	out.Href = base.ResolveReference(h)
	out.Status = parseDAVStatus(xr.Status)
	if out.Status != 0 && !okStatus(out.Status) {
		return out, false, out.Status
	}
	for _, ps := range xr.Propstats {
		if st := parseDAVStatus(ps.Status); st != 0 && !okStatus(st) {
			continue
		}
		p := &ps.Prop
		for _, c := range p.ResourceType.Children {
			if c.XMLName.Space == nsCardDAV && c.XMLName.Local == "addressbook" {
				out.IsBook = true
			}
		}
		if p.DisplayName != "" {
			out.Name = strings.TrimSpace(p.DisplayName)
		}
		if h := strings.TrimSpace(p.Principal.Href); h != "" {
			if u, err := url.Parse(h); err == nil {
				out.Principal = base.ResolveReference(u)
			}
		}
		for _, h := range p.HomeSet.Hrefs {
			if u, err := url.Parse(strings.TrimSpace(h)); err == nil && strings.TrimSpace(h) != "" {
				out.HomeSets = append(out.HomeSets, base.ResolveReference(u))
			}
		}
		if p.AddressData != "" {
			out.Data = p.AddressData
		}
	}
	return out, true, 0
}

// decodeMultistatus stream-decodes a multistatus body, calling handle for every usable response. Each
// <response> is decoded on its own: an undecodable or non-2xx entry is counted in Bad, a 507 entry sets
// Truncated, and neither fails the request. A read error (size cap, timeout) or malformed XML stops the
// stream and is returned together with what was decoded so far.
func decodeMultistatus(r io.Reader, base *url.URL, handle func(davResponse)) (res davResult, err error) {
	d := xml.NewDecoder(r)
	d.Strict = false
	for {
		tok, terr := d.Token()
		if terr == io.EOF {
			return res, nil
		}
		if terr != nil {
			return res, terr
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Space != nsDAV || se.Name.Local != "response" {
			continue
		}
		var xr xResponse
		if derr := d.DecodeElement(&xr, &se); derr != nil {
			if errors.Is(derr, errBodyTooLarge) || !isXMLSyntaxErr(derr) {
				return res, derr
			}
			res.Bad++
			return res, derr
		}
		dr, usable, status := convertResponse(base, &xr)
		if status == http.StatusInsufficientStorage {
			res.Truncated = true
			continue
		}
		if !usable {
			res.Bad++
			continue
		}
		res.Responses++
		handle(dr)
	}
}

func isXMLSyntaxErr(err error) bool {
	var se *xml.SyntaxError
	return errors.As(err, &se)
}

// davRequest sends one PROPFIND/REPORT and decodes the multistatus. It never follows redirects: a
// 301/302/307/308 is returned as (redirect, nil) for the caller to vet. Non-207 statuses become
// classified errors without server text.
func (c *cardDAVClient) davRequest(ctx context.Context, method string, u *url.URL, depth, body string, timeout time.Duration, handle func(davResponse)) (res davResult, redirect *url.URL, err error) {
	if !sameSite(c.entered.Hostname(), u.Hostname()) {
		return res, nil, newCDError(kindCrossHost, "The server pointed to a different site (%s); the bridge will not send your password there.", u.Hostname())
	}
	if u.Scheme == "http" && !c.cfg.AllowInsecureHTTP || u.Scheme != "http" && u.Scheme != "https" {
		return res, nil, newCDError(kindInsecure, "CardDAV URLs must use https:// (your password would be sent unencrypted).")
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, rerr := http.NewRequestWithContext(rctx, method, u.String(), bytes.NewReader([]byte(body)))
	if rerr != nil {
		return res, nil, newCDError(kindBadURL, "That does not look like a valid server address.")
	}
	req.SetBasicAuth(c.user, c.pass)
	req.Header.Set("Content-Type", `application/xml; charset="utf-8"`)
	req.Header.Set("Accept", "application/xml, text/xml")
	if depth != "" {
		req.Header.Set("Depth", depth)
	}
	resp, derr := c.hc.Do(req)
	if derr != nil {
		return res, nil, c.wrapNetErr(derr)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusMultiStatus:
	case http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		loc, perr := resp.Location()
		if perr != nil {
			return res, nil, newCDError(kindRedirect, "%s sent an unusable redirect.", c.host())
		}
		return res, loc, nil
	case http.StatusUnauthorized:
		return res, nil, &cardDAVError{kind: kindAuthFailed, status: 401, msg: "The server rejected the username or password (HTTP 401)."}
	case http.StatusForbidden:
		return res, nil, &cardDAVError{kind: kindAuthFailed, status: 403, msg: "The server refused access (HTTP 403)."}
	case http.StatusNotFound, http.StatusGone:
		return res, nil, &cardDAVError{kind: kindNotFound, status: resp.StatusCode, msg: "The server has nothing at that address (HTTP " + strconv.Itoa(resp.StatusCode) + ")."}
	default:
		return res, nil, &cardDAVError{kind: kindHTTP, status: resp.StatusCode, msg: "The server answered HTTP " + strconv.Itoa(resp.StatusCode) + "."}
	}
	counter := &countingReader{r: &limitedBody{rc: resp.Body, left: c.maxBytes}}
	base := resp.Request.URL
	res, derr = decodeMultistatus(counter, base, handle)
	res.Final, res.Bytes = u, counter.n
	if derr != nil {
		if errors.Is(derr, errBodyTooLarge) {
			return res, nil, &cardDAVError{kind: kindTooLarge, msg: "The server's answer was too large.", err: derr}
		}
		if isXMLSyntaxErr(derr) {
			return res, nil, &cardDAVError{kind: kindDecode, msg: "The server sent an answer the bridge could not read.", err: derr}
		}
		return res, nil, c.wrapNetErr(derr)
	}
	return res, nil, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// wrapNetErr classifies a transport error into a cardDAVError whose message names only the host.
func (c *cardDAVClient) wrapNetErr(err error) *cardDAVError {
	kind := cardDAVErrorKind(err)
	var msg string
	switch kind {
	case kindRefusedPrivate:
		msg = "That address points to a private or local network, which the bridge refuses. (Bridge admin: set `network.carddav.allow_private_hosts: true` to allow it.)"
	case kindTLS:
		msg = "Could not verify the TLS certificate of " + c.host() + "."
	case kindTimeout:
		msg = "Timed out talking to " + c.host() + "."
	case kindDNS:
		msg = "Could not find " + c.host() + " (DNS lookup failed)."
	case kindTooLarge:
		msg = "The server's answer was too large."
	default:
		msg = "Could not reach " + c.host() + "."
	}
	return &cardDAVError{kind: kind, msg: msg, err: err}
}
