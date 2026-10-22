package connector

import (
	"bytes"
	"encoding/xml"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"mellium.im/xmpp/stanza"
)

// bufRW is a TokenReadEncoder that captures what the handler writes (flush on read).
type bufRW struct {
	*xml.Decoder
	enc *xml.Encoder
	out *bytes.Buffer
}

func (b *bufRW) Encode(v interface{}) error    { return b.enc.Encode(v) }
func (b *bufRW) EncodeToken(t xml.Token) error { return b.enc.EncodeToken(t) }
func (b *bufRW) Flush() error                  { return b.enc.Flush() }
func (b *bufRW) EncodeElement(v interface{}, s xml.StartElement) error {
	return b.enc.EncodeElement(v, s)
}

func serveStanza(t *testing.T, x *xmppClient, stanzaXML string) string {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(stanzaXML))
	var start xml.StartElement
	for {
		tok, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		if se, ok := tok.(xml.StartElement); ok {
			start = se
			break
		}
	}
	out := &bytes.Buffer{}
	rw := &bufRW{Decoder: dec, enc: xml.NewEncoder(out), out: out}
	if err := x.newServeHandler().HandleXMPP(rw, &start); err != nil {
		t.Fatalf("handler returned an error (would end the session): %v", err)
	}
	rw.enc.Flush()
	return out.String()
}

type discoReply struct {
	XMLName xml.Name `xml:"iq"`
	Type    string   `xml:"type,attr"`
	ID      string   `xml:"id,attr"`
	To      string   `xml:"to,attr"`
	Query   struct {
		Node     string `xml:"node,attr"`
		Identity []struct {
			Category string `xml:"category,attr"`
			Type     string `xml:"type,attr"`
			Name     string `xml:"name,attr"`
		} `xml:"identity"`
		Feature []struct {
			Var string `xml:"var,attr"`
		} `xml:"feature"`
	} `xml:"http://jabber.org/protocol/disco#info query"`
	Error *struct {
		Inner struct {
			XMLName xml.Name
		} `xml:",any"`
	} `xml:"error"`
}

func TestCapsDiscoResponder(t *testing.T) {
	x := newOuterFromTestClient()
	iqFor := func(node string) string {
		n := ""
		if node != "" {
			n = ` node="` + node + `"`
		}
		return `<iq xmlns="jabber:client" type="get" id="d1" from="peer@example.com/res" to="me@example.com/bridge"><query xmlns="http://jabber.org/protocol/disco#info"` + n + `/></iq>`
	}
	for _, node := range []string{"", bridgeCapsNode + "#" + bridgeCapsVer()} {
		out := serveStanza(t, x, iqFor(node))
		var r discoReply
		if err := xml.Unmarshal([]byte(out), &r); err != nil {
			t.Fatalf("node %q: %v\n%s", node, err, out)
		}
		if r.Type != "result" || r.ID != "d1" || r.To != "peer@example.com/res" || r.Query.Node != node {
			t.Errorf("node %q: reply = %+v\n%s", node, r, out)
		}
		if len(r.Query.Identity) != 1 || r.Query.Identity[0].Category != "client" || r.Query.Identity[0].Type != "pc" || r.Query.Identity[0].Name != "jmpchat-matrix-bridge" {
			t.Errorf("identity = %+v", r.Query.Identity)
		}
		var got []string
		for _, f := range r.Query.Feature {
			got = append(got, f.Var)
		}
		if strings.Join(got, ",") != strings.Join(bridgeFeatureVars, ",") {
			t.Errorf("features = %v", got)
		}
	}
	// Unknown node: item-not-found, not a result.
	out := serveStanza(t, x, iqFor("urn:other#node"))
	if !strings.Contains(out, `type="error"`) || !strings.Contains(out, "item-not-found") {
		t.Errorf("unknown node reply = %s", out)
	}
}

func TestCapsPresenceCarriesC(t *testing.T) {
	var buf bytes.Buffer
	enc := xml.NewEncoder(&buf)
	r := splitEOF(stanza.Presence{}.Wrap(bridgeCaps().TokenReader()))
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
	enc.Flush()
	out := buf.String()
	for _, want := range []string{`<presence`, `xmlns="http://jabber.org/protocol/caps"`, `hash="sha-1"`, `node="` + bridgeCapsNode + `"`, `ver="` + bridgeCapsVer() + `"`} {
		if !strings.Contains(out, want) {
			t.Errorf("presence %q lacks %q", out, want)
		}
	}
}

func headlineClient() (*xmppClient, *bytes.Buffer) {
	x := newOuterFromTestClient()
	buf := &bytes.Buffer{}
	x.log = zerolog.New(buf)
	return x, buf
}

const pepEvent = `<message xmlns="jabber:client" type="headline" from="%s" to="me@example.com/bridge">` +
	`<event xmlns="http://jabber.org/protocol/pubsub#event"><items node="urn:xmpp:bookmarks:1">` +
	`<item id="room@muc.example.com"><conference xmlns="urn:xmpp:bookmarks:1" name="Secret Name" autojoin="true"><nick>alice</nick><password>s3cret</password>` +
	`<extensions><x xmlns="urn:ext">ext-payload</x></extensions></conference></item>` +
	`<retract id="gone@muc.example.com"/></items></event></message>`

func TestPEPBookmarkEventRoutedQueuedAndOnlyCounted(t *testing.T) {
	x, logs := headlineClient()
	serveStanza(t, x, strings.Replace(pepEvent, "%s", "me@example.com", 1))
	out := logs.String()
	if !strings.Contains(out, `"message":"Bookmarks: PEP event received"`) || !strings.Contains(out, `"published":1`) || !strings.Contains(out, `"retracted":1`) {
		t.Fatalf("log = %s", out)
	}
	for _, secret := range []string{"s3cret", "Secret Name", "ext-payload", "room@muc.example.com", "alice"} {
		if strings.Contains(out, secret) {
			t.Errorf("event log leaks %q: %s", secret, out)
		}
	}
}

func TestPEPBookmarkEventSpoofIgnoredAndHandlerSurvives(t *testing.T) {
	for _, from := range []string{"evil@example.com", "me@example.com/laptop", "room@muc.example.com/me", "example.com"} {
		x, logs := headlineClient()
		serveStanza(t, x, strings.Replace(pepEvent, "%s", from, 1))
		if strings.Contains(logs.String(), "PEP event received") {
			t.Errorf("event from %q was accepted: %s", from, logs.String())
		}
	}
	// Malformed / foreign payloads never return an error to the mux.
	x, _ := headlineClient()
	serveStanza(t, x, `<message xmlns="jabber:client" type="headline" from="me@example.com"><event xmlns="http://jabber.org/protocol/pubsub#event"><items node="urn:xmpp:bookmarks:1"><item id="a@b"><conference xmlns="urn:xmpp:bookmarks:1" autojoin="1"><nick>x</nick></conference></item></items></event></message>`)
	serveStanza(t, x, `<message xmlns="jabber:client" type="headline" from="me@example.com"><event xmlns="http://jabber.org/protocol/pubsub#event"><items node="eu.siacs.conversations.axolotl.devicelist"><item id="1"/></items></event></message>`)
}
