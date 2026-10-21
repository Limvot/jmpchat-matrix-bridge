package connector

import (
	"bytes"
	"encoding/xml"
	"io"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
	"mellium.im/xmpp/bookmarks"
	"mellium.im/xmpp/disco"
	"mellium.im/xmpp/disco/info"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

// tokenReaderOf wraps a std decoder so it is used only through xml.TokenReader (the session path).
func tokenReaderOf(s string) xml.TokenReader {
	return onlyTokens{xml.NewDecoder(strings.NewReader(s))}
}

// onlyTokens hides the *xml.Decoder so xml.NewTokenDecoder cannot unwrap it back to a byte-backed
// decoder (a session's token readers are never byte-backed).
type onlyTokens struct{ d *xml.Decoder }

func (o onlyTokens) Token() (xml.Token, error) { return o.d.Token() }

// encodeTokens runs tokens through a std xml.Encoder (what the session writes with).
func encodeTokens(t *testing.T, r xml.TokenReader) string {
	t.Helper()
	var buf bytes.Buffer
	enc := xml.NewEncoder(&buf)
	r = splitEOF(r)
	for {
		tok, err := r.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodeToken(tok); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

const confWithExt = `<conference xmlns="urn:xmpp:bookmarks:1" name="Room One" autojoin="1">` +
	`<nick>alice</nick><password>s3cret</password>` +
	`<extensions>` +
	`<notify xmlns="urn:xmpp:notification-settings:1" when="never"><x:deep xmlns:x="urn:x:y" x:attr="v">text &amp; more</x:deep></notify>` +
	`<group xmlns="urn:gajim:group">Friends</group>` +
	`</extensions></conference>`

func TestBookmarkConfDecode(t *testing.T) {
	for _, tc := range []struct {
		autojoin string
		want     bool
	}{{"1", true}, {"true", true}, {"0", false}, {"false", false}, {"", false}, {"garbage", false}} {
		attr := ""
		if tc.autojoin != "" {
			attr = ` autojoin="` + tc.autojoin + `"`
		}
		c, err := decodeBookmarkConf("Room@Muc.Example.com", tokenReaderOf(`<conference xmlns="urn:xmpp:bookmarks:1" name="N"`+attr+`><nick>me</nick><password>pw</password></conference>`))
		if err != nil {
			t.Fatal(err)
		}
		if c.Autojoin != tc.want || c.Name != "N" || c.Nick != "me" || c.Password != "pw" || c.ID != "Room@Muc.Example.com" {
			t.Errorf("autojoin=%q: %+v", tc.autojoin, c)
		}
	}
}

func TestBookmarkConfDecodeErrors(t *testing.T) {
	for name, in := range map[string]string{
		"empty":         ``,
		"wrong element": `<other xmlns="urn:xmpp:bookmarks:1"/>`,
		"wrong ns":      `<conference xmlns="urn:other"/>`,
		"truncated":     `<conference xmlns="urn:xmpp:bookmarks:1"><nick>x`,
	} {
		if _, err := decodeBookmarkConf("r@x", tokenReaderOf(in)); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

// The real read-modify-write path: decode through a token reader, re-encode through a std encoder,
// re-parse. Extensions survive, with no duplicate xmlns.
func TestBookmarkConfRoundTripPreservesExtensions(t *testing.T) {
	c, err := decodeBookmarkConf("Mixed@Case.example.com", tokenReaderOf(confWithExt))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.ext) == 0 {
		t.Fatal("extensions were not captured")
	}
	out := encodeTokens(t, c.TokenReader())
	if strings.Count(out, `xmlns="urn:xmpp:notification-settings:1"`) != 1 || strings.Contains(out, `xmlns="urn:xmpp:notification-settings:1" xmlns=`) {
		t.Errorf("duplicate xmlns: %s", out)
	}
	// Well-formed (std parser rejects duplicate attributes) and equal after another round.
	for dec := xml.NewDecoder(strings.NewReader(out)); ; {
		if _, err := dec.Token(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("re-encoded XML is not well-formed: %v\n%s", err, out)
		}
	}
	c2, err := decodeBookmarkConf("Mixed@Case.example.com", tokenReaderOf(out))
	if err != nil {
		t.Fatal(err)
	}
	if !c.equal(c2) {
		t.Fatalf("round trip changed the bookmark:\n%+v\n%+v", c, c2)
	}
	if c2.ID != "Mixed@Case.example.com" || c2.Name != "Room One" || c2.Nick != "alice" || c2.Password != "s3cret" || !c2.Autojoin {
		t.Errorf("fields lost: %+v", c2)
	}
	// Namespaces, local names and text of the extensions are intact.
	var names []string
	var text strings.Builder
	for _, tk := range c2.ext {
		switch v := tk.(type) {
		case xml.StartElement:
			names = append(names, v.Name.Space+" "+v.Name.Local)
		case xml.CharData:
			text.Write(v)
		}
	}
	want := []string{"urn:xmpp:notification-settings:1 notify", "urn:x:y deep", "urn:gajim:group group"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("extension elements = %v", names)
	}
	if text.String() != "text & moreFriends" {
		t.Errorf("extension text = %q", text.String())
	}
	// The prefixed attribute keeps its namespace.
	found := false
	for _, tk := range c2.ext {
		if v, ok := tk.(xml.StartElement); ok && v.Name.Local == "deep" {
			for _, a := range v.Attr {
				if a.Name.Space == "urn:x:y" && a.Name.Local == "attr" && a.Value == "v" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("prefixed attribute lost")
	}
}

// A decoder that does not translate namespaces leaves the default namespace in the xmlns attribute.
func TestBookmarkConfExtensionRawNamespaceFolded(t *testing.T) {
	toks := []xml.Token{
		xml.StartElement{Name: xml.Name{Space: bookmarks.NS, Local: "conference"}},
		xml.StartElement{Name: xml.Name{Space: bookmarks.NS, Local: "extensions"}},
		xml.StartElement{Name: xml.Name{Local: "raw"}, Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: "urn:raw:1"}, {Name: xml.Name{Space: "xmlns", Local: "p"}, Value: "urn:p"}}},
		xml.EndElement{Name: xml.Name{Local: "raw"}},
		xml.EndElement{Name: xml.Name{Space: bookmarks.NS, Local: "extensions"}},
		xml.EndElement{Name: xml.Name{Space: bookmarks.NS, Local: "conference"}},
	}
	c, err := decodeBookmarkConf("r@x", &sliceTokenReader{toks: toks})
	if err != nil {
		t.Fatal(err)
	}
	out := encodeTokens(t, c.TokenReader())
	if !strings.Contains(out, `<raw xmlns="urn:raw:1">`) || strings.Count(out, "xmlns=") != 3 {
		// conference, extensions, raw: one declaration each
		t.Errorf("out = %s", out)
	}
}

// Documents why upstream bookmarks.Channel must never be used for read-modify-write.
func TestUpstreamChannelLosesExtensionsOnTokenDecode(t *testing.T) {
	var ch bookmarks.Channel
	d := xml.NewTokenDecoder(tokenReaderOf(confWithExt))
	if err := d.Decode(&ch); err != nil {
		t.Skipf("upstream decode failed (%v); library changed, re-evaluate", err)
	}
	if len(ch.Extensions) != 0 {
		t.Fatalf("upstream now keeps extensions on a token decode (%q); re-evaluate whether bookmarkConf is still needed", ch.Extensions)
	}
}

func TestMergeBookmark(t *testing.T) {
	existing := func() *bookmarkConf {
		c, err := decodeBookmarkConf("Room@Muc.example.com", tokenReaderOf(confWithExt))
		if err != nil {
			t.Fatal(err)
		}
		return &c
	}
	t.Run("flip autojoin keeps everything else", func(t *testing.T) {
		e := existing()
		got, changed := mergeBookmark(e, "room@muc.example.com", false, "bridgenick")
		if !changed || got.Autojoin || got.ID != "Room@Muc.Example.com" && got.ID != "Room@Muc.example.com" {
			t.Fatalf("got %+v changed=%v", got, changed)
		}
		if got.Name != "Room One" || got.Nick != "alice" || got.Password != "s3cret" || len(got.ext) != len(e.ext) {
			t.Errorf("fields modified: %+v", got)
		}
		if !e.Autojoin {
			t.Error("existing must not be mutated")
		}
	})
	t.Run("unchanged", func(t *testing.T) {
		if _, changed := mergeBookmark(existing(), "room@muc.example.com", true, "x"); changed {
			t.Error("autojoin already true and nick present: no change expected")
		}
	})
	t.Run("new gets localpart name and nick", func(t *testing.T) {
		got, changed := mergeBookmark(nil, "room@groups.example.com", true, "me")
		if !changed || got.ID != "room@groups.example.com" || got.Name != "room" || got.Nick != "me" || !got.Autojoin {
			t.Errorf("got %+v changed=%v", got, changed)
		}
	})
	t.Run("empty nick is omitted, existing empty nick filled", func(t *testing.T) {
		got, _ := mergeBookmark(nil, "a@b", true, "")
		if got.Nick != "" {
			t.Errorf("nick = %q", got.Nick)
		}
		e := &bookmarkConf{ID: "a@b", Autojoin: true, Name: "A"}
		got, changed := mergeBookmark(e, "a@b", true, "me")
		if !changed || got.Nick != "me" || got.Name != "A" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("existing nameless item only gets a name when published anyway", func(t *testing.T) {
		e := &bookmarkConf{ID: "a@b", Autojoin: true}
		if got, changed := mergeBookmark(e, "a@b", true, ""); changed || got.Name != "" {
			t.Errorf("got %+v changed=%v", got, changed)
		}
		if got, changed := mergeBookmark(e, "a@b", false, ""); !changed || got.Name != "a" {
			t.Errorf("got %+v changed=%v", got, changed)
		}
	})
}

func TestBookmarkKey(t *testing.T) {
	for in, want := range map[string]string{"Room@Muc.Example.COM": "room@muc.example.com", " a@b/res ": "a@b", "": ""} {
		if got := bookmarkKey(in); got != want {
			t.Errorf("bookmarkKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func bmPortal(pid, receiver string, mxid string) *database.Portal {
	return &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID(pid), Receiver: networkid.UserLoginID(receiver)}, MXID: id.RoomID(mxid)}
}

func TestSelectBookmarkImports(t *testing.T) {
	bms := []bookmarkConf{
		{ID: "a@muc.example.com", Autojoin: true},
		{ID: "off@muc.example.com", Autojoin: false},
		{ID: "Have@muc.example.com", Autojoin: true},      // portal with MXID, mixed case
		{ID: "left@muc.example.com", Autojoin: true},      // portal without MXID: still imported
		{ID: "other@muc.example.com", Autojoin: true},     // portal of another login: imported
		{ID: "res@muc.example.com/nick", Autojoin: true},  // not bare
		{ID: "+15551234567@cheogram.com", Autojoin: true}, // Cheogram DM
		{ID: "not a jid", Autojoin: true},                 // invalid
		{ID: "muc.example.com", Autojoin: true},           // no localpart
		{ID: "A@muc.example.com", Autojoin: true},         // duplicate of the first (case)
		{ID: "b@muc.example.com", Autojoin: true},
		{ID: "c@muc.example.com", Autojoin: true},
	}
	portals := []*database.Portal{
		bmPortal("have@muc.example.com", "me", "!r:hs"),
		bmPortal("left@muc.example.com", "me", ""),
		bmPortal("other@muc.example.com", "someone-else", "!o:hs"),
	}
	got, st := selectBookmarkImports(bms, portals, "me", 3)
	var ids []string
	for _, b := range got {
		ids = append(ids, b.ID)
	}
	if strings.Join(ids, ",") != "a@muc.example.com,left@muc.example.com,other@muc.example.com" {
		t.Errorf("imports = %v", ids)
	}
	want := importStats{Total: 12, Autojoin: 11, Existing: 2, Invalid: 4, Capped: 2}
	if st != want {
		t.Errorf("stats = %+v, want %+v", st, want)
	}
	if got, st := selectBookmarkImports(bms, nil, "me", 0); len(got) != 0 || st.Capped == 0 {
		t.Errorf("zero budget imported %d (%+v)", len(got), st)
	}
}

func TestSelectBookmarkImportsCapConstant(t *testing.T) {
	if bookmarkImportMaxRooms != 50 {
		t.Fatalf("budget = %d", bookmarkImportMaxRooms)
	}
}

const eventFmt = `<event xmlns="http://jabber.org/protocol/pubsub#event"><items node="%s">%s</items></event>`

func eventOf(node, inner string) string {
	return strings.Replace(strings.Replace(eventFmt, "%s", node, 1), "%s", inner, 1)
}

func TestParseBookmarkEvent(t *testing.T) {
	in := eventOf("urn:xmpp:bookmarks:1",
		`<item id="Room@muc.example.com">`+confWithExt+`</item>`+
			`<retract id="gone@muc.example.com"/>`+
			`<item id="off@muc.example.com"><conference xmlns="urn:xmpp:bookmarks:1" autojoin="false"/></item>`)
	ch, trunc, ok := parseBookmarkEvent(tokenReaderOf(in))
	if !ok || trunc || len(ch) != 3 {
		t.Fatalf("ok=%v trunc=%v changes=%+v", ok, trunc, ch)
	}
	if ch[0].ID != "Room@muc.example.com" || ch[0].Conf == nil || !ch[0].Conf.Autojoin || ch[0].Conf.Nick != "alice" || len(ch[0].Conf.ext) == 0 {
		t.Errorf("change 0 = %+v", ch[0])
	}
	if ch[1].ID != "gone@muc.example.com" || !ch[1].Retract || ch[1].Conf != nil {
		t.Errorf("change 1 = %+v", ch[1])
	}
	if ch[2].ID != "off@muc.example.com" || ch[2].Conf == nil || ch[2].Conf.Autojoin {
		t.Errorf("change 2 = %+v", ch[2])
	}
}

func TestParseBookmarkEventNodeAndNamespace(t *testing.T) {
	for name, in := range map[string]string{
		"wrong node":      eventOf("urn:xmpp:bookmarks:0", `<item id="a@b"><conference xmlns="urn:xmpp:bookmarks:1" autojoin="1"/></item>`),
		"wrong event ns":  `<event xmlns="urn:other"><items node="urn:xmpp:bookmarks:1"><item id="a@b"><conference xmlns="urn:xmpp:bookmarks:1"/></item></items></event>`,
		"no event":        `<body>hi</body>`,
		"empty":           ``,
		"malformed":       `<event xmlns="http://jabber.org/protocol/pubsub#event"><items node="urn:xmpp:bookmarks:1"><item id="a@b"><conference xmlns="urn:xmpp:bookmarks:1"`,
		"mismatched tags": `<event xmlns="http://jabber.org/protocol/pubsub#event"><items node="urn:xmpp:bookmarks:1"></event>`,
	} {
		if ch, _, ok := parseBookmarkEvent(tokenReaderOf(in)); ok || len(ch) != 0 {
			t.Errorf("%s: ok=%v changes=%+v", name, ok, ch)
		}
	}
}

func TestParseBookmarkEventPurgeIsInformational(t *testing.T) {
	in := `<event xmlns="http://jabber.org/protocol/pubsub#event"><purge node="urn:xmpp:bookmarks:1"/></event>`
	ch, _, ok := parseBookmarkEvent(tokenReaderOf(in))
	if !ok || len(ch) != 1 || !ch[0].Purge {
		t.Fatalf("ok=%v %+v", ok, ch)
	}
	in = `<event xmlns="http://jabber.org/protocol/pubsub#event"><delete node="something:else"/></event>`
	if ch, _, ok := parseBookmarkEvent(tokenReaderOf(in)); ok || len(ch) != 0 {
		t.Fatalf("foreign delete: ok=%v %+v", ok, ch)
	}
}

func TestParseBookmarkEventCapsChanges(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < bookmarkEventMaxChanges+25; i++ {
		sb.WriteString(`<item id="r` + strings.Repeat("x", i%7) + string(rune('a'+i%26)) + `@m"><conference xmlns="urn:xmpp:bookmarks:1" autojoin="1"/></item>`)
	}
	ch, trunc, ok := parseBookmarkEvent(tokenReaderOf(eventOf("urn:xmpp:bookmarks:1", sb.String())))
	if !ok || !trunc || len(ch) != bookmarkEventMaxChanges {
		t.Fatalf("ok=%v trunc=%v len=%d", ok, trunc, len(ch))
	}
}

func TestBookmarkEventTrusted(t *testing.T) {
	own := jid.MustParse("me@example.com/laptop")
	mk := func(from string, typ stanza.MessageType) stanza.Message {
		m := stanza.Message{Type: typ}
		if from != "" {
			m.From = jid.MustParse(from)
		}
		return m
	}
	for _, tc := range []struct {
		name string
		msg  stanza.Message
		want bool
	}{
		{"own bare headline", mk("me@example.com", stanza.HeadlineMessage), true},
		{"own bare, different case domain parse", mk("me@EXAMPLE.com", stanza.HeadlineMessage), true},
		{"own full jid", mk("me@example.com/laptop", stanza.HeadlineMessage), false},
		{"other bare", mk("you@example.com", stanza.HeadlineMessage), false},
		{"muc occupant", mk("room@muc.example.com/me", stanza.HeadlineMessage), false},
		{"server", mk("example.com", stanza.HeadlineMessage), false},
		{"no from", mk("", stanza.HeadlineMessage), false},
		{"own bare but chat", mk("me@example.com", stanza.ChatMessage), false},
		{"own bare but normal", mk("me@example.com", stanza.NormalMessage), false},
	} {
		if got := bookmarkEventTrusted(tc.msg, own); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestBookmarkSupportFromInfo(t *testing.T) {
	pep := info.Identity{Category: "pubsub", Type: "pep"}
	feat := func(vs ...string) []info.Feature {
		var fs []info.Feature
		for _, v := range vs {
			fs = append(fs, info.Feature{Var: v})
		}
		return fs
	}
	for _, tc := range []struct {
		name string
		info disco.Info
		want bookmarkSupport
	}{
		{"prosody/snikket", disco.Info{Identity: []info.Identity{{Category: "account", Type: "registered"}, pep},
			Features: feat("http://jabber.org/protocol/pubsub#publish-options", "urn:xmpp:bookmarks:1#compat", "http://jabber.org/protocol/pubsub#persistent-items")},
			bookmarkSupport{Read: true, Write: true, Compat: true}},
		{"pep without publish-options", disco.Info{Identity: []info.Identity{pep}, Features: feat("urn:xmpp:bookmarks:1#compat")},
			bookmarkSupport{Read: true, Compat: true}},
		{"pep only", disco.Info{Identity: []info.Identity{pep}}, bookmarkSupport{Read: true}},
		{"no pep identity", disco.Info{Identity: []info.Identity{{Category: "account", Type: "registered"}}, Features: feat("http://jabber.org/protocol/pubsub#publish-options")},
			bookmarkSupport{}},
		{"wrong pubsub type", disco.Info{Identity: []info.Identity{{Category: "pubsub", Type: "service"}}, Features: feat("http://jabber.org/protocol/pubsub#publish-options")},
			bookmarkSupport{}},
		{"empty", disco.Info{}, bookmarkSupport{}},
	} {
		if got := bookmarkSupportFromInfo(tc.info); got != tc.want {
			t.Errorf("%s: got %+v want %+v", tc.name, got, tc.want)
		}
	}
}
