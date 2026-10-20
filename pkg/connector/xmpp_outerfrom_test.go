package connector

import (
	"encoding/xml"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// decoderRW adapts an xml.Decoder to xmlstream.TokenReadEncoder for handler tests.
type decoderRW struct {
	*xml.Decoder
	*xml.Encoder
}

// startOfMessage returns a reader positioned after the outer <message> start and that start element.
func startOfMessage(t *testing.T, stanza string) (*decoderRW, xml.StartElement) {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(stanza))
	for {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("read outer start: %v", err)
		}
		if se, ok := tok.(xml.StartElement); ok {
			return &decoderRW{Decoder: dec, Encoder: xml.NewEncoder(io.Discard)}, se
		}
	}
}

func TestIsOwnAccountFrom(t *testing.T) {
	for _, tc := range []struct {
		from string
		want bool
	}{
		{"", true},
		{"me@example.com", true},
		{"ME@Example.com", true},
		{"me@example.com/phone", false},
		{"evil@example.com", false},
		{"evil@example.com/me@example.com", false},
		{"me@example.com.evil.org", false},
		{"room@conference.example.com/me", false},
	} {
		if got := isOwnAccountFrom(tc.from, "me@example.com"); got != tc.want {
			t.Errorf("isOwnAccountFrom(%q) = %v, want %v", tc.from, got, tc.want)
		}
	}
}

func newOuterFromTestClient() *xmppClient {
	return &xmppClient{
		loginID: networkid.UserLoginID("me@example.com"),
		log:     zerolog.Nop(),
		login:   &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "me@example.com"}},
		// conn is nil on purpose: a carbon that is not dropped would dereference it when queuing.
	}
}

// A contact must not be able to inject a "sent by me" or "received from X" carbon.
func TestSpoofedCarbonsDropped(t *testing.T) {
	for _, tc := range []struct{ name, kind, stanza string }{
		{"sent from contact", "sent",
			`<message from="evil@example.com/x" to="me@example.com" type="chat"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/phone" to="bob@example.com" id="m1"><body>forged</body></message></forwarded></sent></message>`},
		{"received from contact", "received",
			`<message from="evil@example.com/x" to="me@example.com" type="chat"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" id="m2"><body>forged</body></message></forwarded></received></message>`},
		{"sent from MUC occupant", "sent",
			`<message from="room@conference.example.com/me" to="me@example.com" type="chat"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/phone" to="bob@example.com" id="m3"><body>forged</body></message></forwarded></sent></message>`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newOuterFromTestClient()
			r, start := startOfMessage(t, tc.stanza)
			// Panics (nil conn) if the carbon is not dropped before queuing.
			if err := x.handleMessageWithOOB(r, &start); err != nil {
				t.Fatalf("handleMessageWithOOB: %v", err)
			}
			// The carbon element must have been consumed so the stream stays in sync.
			tok, err := r.Token()
			if err != nil {
				t.Fatalf("next token: %v", err)
			}
			if ee, ok := tok.(xml.EndElement); !ok || ee.Name.Local != "message" {
				t.Errorf("expected </message> after carbon, got %#v", tok)
			}
		})
	}
}

const mamResultStanza = `<message%s to="me@example.com"><result xmlns="urn:xmpp:mam:2" queryid="q1" id="arch1"><forwarded xmlns="urn:xmpp:forward:0"><delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z"/><message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat" id="m1"><body>hello</body></message></forwarded></result></message>`

func runMAMResult(t *testing.T, outerFromAttr string) (item mamResultItem, got bool) {
	t.Helper()
	x := newOuterFromTestClient()
	ch := x.mam.register("q1", networkid.PortalKey{ID: "bob@example.com", Receiver: x.loginID}, false)
	r, start := startOfMessage(t, strings.Replace(mamResultStanza, "%s", outerFromAttr, 1))
	if err := x.handleMessageWithOOB(r, &start); err != nil {
		t.Fatalf("handleMessageWithOOB: %v", err)
	}
	select {
	case item = <-ch:
		return item, true
	default:
		return item, false
	}
}

func TestMAMResultOuterFrom(t *testing.T) {
	for _, from := range []string{``, ` from="me@example.com"`, ` from="Me@Example.com"`} {
		item, got := runMAMResult(t, from)
		if !got || item.Body != "hello" || item.StanzaID != "m1" {
			t.Errorf("outer from %q: want delivered item, got %v %+v", from, got, item)
		}
	}
	for _, from := range []string{` from="evil@example.com"`, ` from="bob@example.com/p"`, ` from="room@conference.example.com/me"`} {
		if item, got := runMAMResult(t, from); got {
			t.Errorf("outer from %q: spoofed MAM result delivered: %+v", from, item)
		}
	}
}
