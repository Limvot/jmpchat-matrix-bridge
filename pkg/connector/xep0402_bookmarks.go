package connector

import (
	"encoding/xml"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"mellium.im/xmpp/bookmarks"
	"mellium.im/xmpp/disco"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

// XEP-0402 (PEP native bookmarks), pure core. Nothing here does I/O.
//
// The upstream bookmarks.Channel codec is deliberately NOT used for anything that is written back:
// decoded from a session token reader it loses <extensions> (innerxml needs a byte-backed decoder),
// and re-encoding emits every extension element's xmlns twice (not well-formed XML on the c2s stream).
// bookmarkConf keeps extensions as copied tokens and strips xmlns attributes on write.

const (
	// pubsubEventNS is the namespace of PEP notification payloads.
	pubsubEventNS = "http://jabber.org/protocol/pubsub#event"
	// pubsubPublishOptionsFeature is the disco feature of servers that honour <publish-options>.
	pubsubPublishOptionsFeature = "http://jabber.org/protocol/pubsub#publish-options"
	// bookmarkImportMaxRooms is the per-session budget of rooms imported from bookmarks (not used yet).
	bookmarkImportMaxRooms = 50
	// bookmarkEventMaxChanges caps the changes taken from one PEP event.
	bookmarkEventMaxChanges = 100
)

// bookmarkConf is one XEP-0402 <conference/> item.
type bookmarkConf struct {
	// ID is the item id exactly as stored on the server (the room's bare JID, possibly not lower-cased).
	// A read-modify-write republishes under the same id.
	ID       string
	Autojoin bool
	Name     string
	Nick     string
	Password string // never log, trace or store anywhere but the join presence
	// ext are the child tokens of <extensions/> (other clients' data), copied, preserved verbatim.
	ext []xml.Token
}

// bookmarkKey is the key every comparison uses (cache, portal lookup, import diff, event apply):
// the lower-cased bare JID. Portal IDs are lower-cased while bookmark ids may not be.
func bookmarkKey(id string) string {
	id = strings.TrimSpace(id)
	if i := strings.IndexByte(id, '/'); i >= 0 {
		id = id[:i]
	}
	return strings.ToLower(id)
}

// equal reports whether two bookmarks have the same content (the id is not compared).
func (c bookmarkConf) equal(o bookmarkConf) bool {
	return c.Autojoin == o.Autojoin && c.Name == o.Name && c.Nick == o.Nick && c.Password == o.Password &&
		reflect.DeepEqual(c.ext, o.ext)
}

// eofSplitReader turns the xmlstream convention (token returned together with io.EOF) into the plain
// one (token, then nil+EOF), so consumers can treat a returned token as always valid.
type eofSplitReader struct {
	r       xml.TokenReader
	pending error
}

func (e *eofSplitReader) Token() (xml.Token, error) {
	if e.pending != nil {
		return nil, e.pending
	}
	tok, err := e.r.Token()
	if tok != nil && err != nil {
		e.pending = err
		return tok, nil
	}
	return tok, err
}

func splitEOF(r xml.TokenReader) xml.TokenReader { return &eofSplitReader{r: r} }

var errNoConference = errors.New("bookmarks: no <conference/> element")

// decodeBookmarkConf decodes the first <conference/> element read from r (typically the inner reader of
// a pubsub item). id is the item id. A malformed item returns an error and never panics.
func decodeBookmarkConf(id string, r xml.TokenReader) (bookmarkConf, error) {
	r = splitEOF(r)
	for {
		tok, err := r.Token()
		if err != nil {
			if err == io.EOF {
				return bookmarkConf{}, errNoConference
			}
			return bookmarkConf{}, err
		}
		if se, ok := tok.(xml.StartElement); ok {
			if se.Name.Space != bookmarks.NS || se.Name.Local != "conference" {
				return bookmarkConf{}, errNoConference
			}
			return decodeConferenceBody(r, se, id)
		}
	}
}

// decodeConferenceBody reads the content of a <conference/> whose start element was just consumed,
// through its end element. r must already be split (splitEOF).
func decodeConferenceBody(r xml.TokenReader, start xml.StartElement, id string) (bookmarkConf, error) {
	c := bookmarkConf{ID: id}
	for _, a := range start.Attr {
		if a.Name.Space != "" {
			continue
		}
		switch a.Name.Local {
		case "name":
			c.Name = a.Value
		case "autojoin":
			c.Autojoin, _ = strconv.ParseBool(a.Value)
		}
	}
	var (
		depth = 1
		child string // local name of the current direct child ("" for foreign ones)
		inExt bool
		stack []xml.Name // resolved names of the open extension elements
		text  strings.Builder
	)
	for {
		tok, err := r.Token()
		if err != nil {
			if err == io.EOF {
				return bookmarkConf{}, io.ErrUnexpectedEOF
			}
			return bookmarkConf{}, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			switch {
			case depth == 2:
				child = ""
				if t.Name.Space == bookmarks.NS {
					child = t.Name.Local
				}
				text.Reset()
				inExt = child == "extensions"
			case inExt:
				cp := copyExtStart(t)
				stack = append(stack, cp.Name)
				c.ext = append(c.ext, cp)
			}
		case xml.EndElement:
			switch {
			case depth == 1:
				return c, nil
			case depth == 2:
				switch child {
				case "nick":
					c.Nick = text.String()
				case "password":
					c.Password = text.String()
				}
				child, inExt = "", false
			case inExt:
				name := t.Name
				if n := len(stack); n > 0 {
					name, stack = stack[n-1], stack[:n-1]
				}
				c.ext = append(c.ext, xml.EndElement{Name: name})
			}
			depth--
		case xml.CharData:
			switch {
			case depth == 2 && (child == "nick" || child == "password"):
				text.Write(t)
			case depth > 2 && inExt:
				c.ext = append(c.ext, xml.CopyToken(t))
			}
		}
	}
}

// copyExtStart copies a start element for storage. A decoder that did not translate namespaces leaves
// the default namespace only in the xmlns attribute; fold it into the name, then drop every
// xmlns/xmlns:* declaration (Name.Space carries the namespaces; the encoder regenerates declarations,
// and keeping them would emit them twice).
func copyExtStart(se xml.StartElement) xml.StartElement {
	cp := xml.StartElement{Name: se.Name}
	for _, a := range se.Attr {
		if a.Name.Space == "" && a.Name.Local == "xmlns" && cp.Name.Space == "" {
			cp.Name.Space = a.Value
		}
	}
	for _, a := range se.Attr {
		if !isNSDecl(a) {
			cp.Attr = append(cp.Attr, a)
		}
	}
	return cp
}

// isNSDecl reports whether a is an xmlns or xmlns:prefix declaration.
func isNSDecl(a xml.Attr) bool {
	return (a.Name.Space == "" && a.Name.Local == "xmlns") || a.Name.Space == "xmlns"
}

// tokens returns the <conference/> element. Namespaces ride on Name.Space only (the encoder
// regenerates declarations); every xmlns/xmlns:* attribute of an extension is dropped.
func (c bookmarkConf) tokens() []xml.Token {
	space := func(local string) xml.Name { return xml.Name{Space: bookmarks.NS, Local: local} }
	start := xml.StartElement{Name: space("conference")}
	start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "autojoin"}, Value: strconv.FormatBool(c.Autojoin)})
	if c.Name != "" {
		start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "name"}, Value: c.Name})
	}
	toks := []xml.Token{start}
	text := func(local, v string) {
		if v == "" {
			return
		}
		se := xml.StartElement{Name: space(local)}
		toks = append(toks, se, xml.CharData(v), se.End())
	}
	text("nick", c.Nick)
	text("password", c.Password)
	if len(c.ext) > 0 {
		se := xml.StartElement{Name: space("extensions")}
		toks = append(toks, se)
		for _, t := range c.ext {
			if s, ok := t.(xml.StartElement); ok {
				cp := xml.StartElement{Name: s.Name}
				for _, a := range s.Attr {
					if !isNSDecl(a) {
						cp.Attr = append(cp.Attr, a)
					}
				}
				toks = append(toks, cp)
				continue
			}
			toks = append(toks, xml.CopyToken(t))
		}
		toks = append(toks, se.End())
	}
	return append(toks, start.End())
}

// sliceTokenReader serves a token slice (plain convention: nil, io.EOF at the end).
type sliceTokenReader struct{ toks []xml.Token }

func (s *sliceTokenReader) Token() (xml.Token, error) {
	if len(s.toks) == 0 {
		return nil, io.EOF
	}
	t := s.toks[0]
	s.toks = s.toks[1:]
	return t, nil
}

// TokenReader returns the <conference/> element as a token stream (implements xmlstream.Marshaler).
func (c bookmarkConf) TokenReader() xml.TokenReader { return &sliceTokenReader{toks: c.tokens()} }

// mergeBookmark applies a bridge-side change onto the server's current item (nil when there is none)
// and reports whether anything changed (when not, the publish is skipped). Name, password and
// extensions of an existing item are never touched. The nick rule (plan 3.3): an existing non-empty
// nick is never overwritten; nick is only written when the item has none, and callers pass "" when
// the confirmed nick was a bridge-local conflict alternative. A missing name defaults to the room
// localpart (never the Matrix room name), only on creation or when the publish happens anyway.
func mergeBookmark(existing *bookmarkConf, roomKey string, wantAutojoin bool, nick string) (bookmarkConf, bool) {
	var out bookmarkConf
	changed := false
	if existing == nil {
		out = bookmarkConf{ID: roomKey}
		changed = true
	} else {
		out = *existing
		out.ext = append([]xml.Token(nil), existing.ext...)
	}
	if out.Autojoin != wantAutojoin {
		out.Autojoin = wantAutojoin
		changed = true
	}
	if out.Nick == "" && nick != "" {
		out.Nick = nick
		changed = true
	}
	if changed && out.Name == "" {
		local := roomKey
		if i := strings.IndexByte(local, '@'); i > 0 {
			local = local[:i]
		}
		out.Name = local
	}
	return out, changed
}

// importStats are the counters of selectBookmarkImports.
type importStats struct {
	Total    int // bookmarks seen
	Autojoin int // with autojoin=true
	Existing int // skipped: already a portal with a Matrix room for this login (or a duplicate id)
	Invalid  int // skipped: not a bare room JID, or a Cheogram DM/group JID
	Capped   int // skipped: over the budget
}

// selectBookmarkImports picks the autojoin bookmarks that should become new bridged rooms, in
// bookmark order, at most max (the remaining per-session budget).
func selectBookmarkImports(bms []bookmarkConf, portals []*database.Portal, login networkid.UserLoginID, max int) (toJoin []bookmarkConf, st importStats) {
	have := make(map[string]bool, len(portals))
	for _, p := range portals {
		if p != nil && p.Receiver == login && p.MXID != "" {
			have[strings.ToLower(string(p.ID))] = true
		}
	}
	seen := map[string]bool{}
	st.Total = len(bms)
	for _, b := range bms {
		if !b.Autojoin {
			continue
		}
		st.Autojoin++
		id := strings.TrimSpace(b.ID)
		j, err := jid.Parse(id)
		if err != nil || j.Localpart() == "" || j.Resourcepart() != "" || IsCheogramDMJID(id) {
			st.Invalid++
			continue
		}
		key := bookmarkKey(id)
		if have[key] || seen[key] {
			st.Existing++
			continue
		}
		seen[key] = true
		if len(toJoin) >= max {
			st.Capped++
			continue
		}
		toJoin = append(toJoin, b)
	}
	return toJoin, st
}

// bookmarkChange is one effect of a PEP event on the bookmarks node.
type bookmarkChange struct {
	ID      string
	Conf    *bookmarkConf // set for a published item
	Retract bool          // the item was retracted
	Purge   bool          // the whole node was purged or deleted (informational: nothing destructive is done)
}

// parseBookmarkEvent parses the payload of a PEP message
// (<event xmlns='.../pubsub#event'><items node='urn:xmpp:bookmarks:1'>...). Only the bookmarks node
// counts. ok is false for anything else or for malformed input (never an error: the caller is a Serve
// handler and must not fail). At most bookmarkEventMaxChanges changes are returned; truncated says
// more were present. Conference contents are decoded but callers must never log them.
func parseBookmarkEvent(r xml.TokenReader) (changes []bookmarkChange, truncated, ok bool) {
	r = splitEOF(r)
	var (
		inEvent bool
		inItems bool // inside <items node=bookmarks>
		curID   string
	)
	add := func(c bookmarkChange) {
		if len(changes) >= bookmarkEventMaxChanges {
			truncated = true
			return
		}
		changes = append(changes, c)
	}
	for {
		tok, err := r.Token()
		if err != nil {
			if err == io.EOF {
				return changes, truncated, ok
			}
			return nil, false, false
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch {
			case t.Name.Space == pubsubEventNS && t.Name.Local == "event":
				inEvent = true
			case !inEvent:
				// the message's other children before the event
			case t.Name.Space == pubsubEventNS && t.Name.Local == "items":
				if attrValueLocal(t.Attr, "node") == bookmarks.NS {
					inItems, ok = true, true
				} else if skipElement(r) != nil {
					return nil, false, false
				}
			case t.Name.Space == pubsubEventNS && (t.Name.Local == "purge" || t.Name.Local == "delete"):
				if attrValueLocal(t.Attr, "node") == bookmarks.NS {
					ok = true
					add(bookmarkChange{Purge: true})
				}
			case inItems && t.Name.Space == pubsubEventNS && t.Name.Local == "item":
				curID = attrValueLocal(t.Attr, "id")
			case inItems && t.Name.Space == pubsubEventNS && t.Name.Local == "retract":
				if id := attrValueLocal(t.Attr, "id"); id != "" {
					add(bookmarkChange{ID: id, Retract: true})
				}
			case inItems && t.Name.Space == bookmarks.NS && t.Name.Local == "conference":
				if len(changes) >= bookmarkEventMaxChanges {
					truncated = true
					if skipElement(r) != nil {
						return nil, false, false
					}
					continue
				}
				c, err := decodeConferenceBody(r, t, curID)
				if err != nil {
					return nil, false, false
				}
				if curID != "" {
					add(bookmarkChange{ID: curID, Conf: &c})
				}
			}
		case xml.EndElement:
			if t.Name.Space == pubsubEventNS {
				switch t.Name.Local {
				case "items":
					inItems = false
				case "item":
					curID = ""
				case "event":
					inEvent = false
				}
			}
		}
	}
}

// attrValueLocal returns the value of the un-namespaced attribute local.
func attrValueLocal(attrs []xml.Attr, local string) string {
	for _, a := range attrs {
		if a.Name.Space == "" && a.Name.Local == local {
			return a.Value
		}
	}
	return ""
}

// skipElement consumes tokens through the end of the element whose start was just read.
func skipElement(r xml.TokenReader) error {
	depth := 1
	for depth > 0 {
		tok, err := r.Token()
		if err != nil {
			return err
		}
		switch tok.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		}
	}
	return nil
}

// bookmarkEventTrusted is the spoofing guard of the PEP event handler: our own account's PEP service
// delivers events with from = our exact bare JID and type headline. A contact, a MUC occupant, our own
// full JID or any other entity is rejected.
func bookmarkEventTrusted(msg stanza.Message, ownBare jid.JID) bool {
	own := ownBare.Bare()
	if own.String() == "" || msg.Type != stanza.HeadlineMessage {
		return false
	}
	return msg.From.Equal(own)
}

// bookmarkSupport is the outcome of the capability gate.
type bookmarkSupport struct {
	Read   bool // identity pubsub/pep present: bookmarks can be fetched
	Write  bool // plus pubsub#publish-options: writes can be made private
	Compat bool // urn:xmpp:bookmarks:1#compat seen (informational only, never required)
}

// bookmarkSupportFromInfo evaluates the disco#info of our own bare JID (the PEP service).
func bookmarkSupportFromInfo(i disco.Info) bookmarkSupport {
	var s bookmarkSupport
	for _, id := range i.Identity {
		if id.Category == "pubsub" && id.Type == "pep" {
			s.Read = true
		}
	}
	publishOptions := false
	for _, f := range i.Features {
		switch f.Var {
		case pubsubPublishOptionsFeature:
			publishOptions = true
		case bookmarks.NSCompat:
			s.Compat = true
		}
	}
	s.Write = s.Read && publishOptions
	return s
}
