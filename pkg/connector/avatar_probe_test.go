package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

func tokensString(t *testing.T, r xml.TokenReader) string {
	t.Helper()
	var buf bytes.Buffer
	e := xml.NewEncoder(&buf)
	if _, err := xmlstream.Copy(e, r); err != nil {
		t.Fatal(err)
	}
	e.Flush()
	return buf.String()
}

func TestProbePayloads(t *testing.T) {
	if got := tokensString(t, vcardGetPayload()); !strings.Contains(got, `<vCard xmlns="vcard-temp"`) {
		t.Errorf("vcard payload: %s", got)
	}
	got := tokensString(t, pubsubItemsGetPayload(avatarMetadataNS, 1, ""))
	if !strings.Contains(got, `<items node="urn:xmpp:avatar:metadata" max_items="1"`) || strings.Contains(got, "<item ") {
		t.Errorf("metadata payload: %s", got)
	}
	got = tokensString(t, pubsubItemsGetPayload(avatarDataNS, 0, "abc"))
	if !strings.Contains(got, `<item id="abc"`) || strings.Contains(got, "max_items") || strings.Contains(got, `items node="urn:xmpp:avatar:data" item=`) {
		t.Errorf("data payload: %s", got)
	}
}

var probePNG = []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")

func TestProbeOutcomeFormatting(t *testing.T) {
	b64 := base64.StdEncoding.EncodeToString(probePNG)
	vc := `<vCard xmlns="vcard-temp"><PHOTO><TYPE>image/png</TYPE><BINVAL>` + b64 + `</BINVAL></PHOTO></vCard>`
	if got := describeVCard([]byte(vc)); got != "answered, photo 16 B image/png" {
		t.Errorf("vcard: %q", got)
	}
	if got := describeVCard([]byte(`<vCard xmlns="vcard-temp"><FN>x</FN></vCard>`)); got != "answered, no photo" {
		t.Errorf("vcard no photo: %q", got)
	}
	if got := describeVCard(nil); !strings.Contains(got, "no photo") {
		t.Errorf("empty: %q", got)
	}
	md := `<pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="n"><item id="h1"><metadata xmlns="urn:xmpp:avatar:metadata"><info bytes="123" id="h1" type="image/png" url="http://secret"/></metadata></item></items></pubsub>`
	id, d := parseMetadata([]byte(md))
	if id != "h1" || d != "answered, item h1, 123 B image/png" || strings.Contains(d, "secret") {
		t.Errorf("metadata: %q %q", id, d)
	}
	if id, d := parseMetadata([]byte(`<pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="n"/></pubsub>`)); id != "" || d != "answered, no item" {
		t.Errorf("no item: %q %q", id, d)
	}
	data := `<pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="n"><item id="h1"><data xmlns="urn:xmpp:avatar:data">` + b64 + `</data></item></items></pubsub>`
	if got := describeAvatarData([]byte(data)); got != "answered, data 16 B image/png" {
		t.Errorf("data: %q", got)
	}
	di := `<query xmlns="http://jabber.org/protocol/disco#info"><feature var="vcard-temp"/><feature var="http://jabber.org/protocol/pubsub#publish"/></query>`
	if got := describeDisco([]byte(di)); got != "answered, 2 features, vcard-temp: yes, pubsub: yes" {
		t.Errorf("disco: %q", got)
	}
	se := &iqStanzaError{from: "room@c.example", err: stanza.Error{Type: stanza.Cancel, Condition: stanza.ServiceUnavailable}}
	if got := formatProbeErr(se); got != "error cancel/service-unavailable from room@c.example" {
		t.Errorf("stanza err: %q", got)
	}
	if got := formatProbeErr(context.DeadlineExceeded); got != "timeout" {
		t.Errorf("timeout: %q", got)
	}
}

// TestAvatarProbeOnlySendsIQGet runs the whole probe against a recording fake sender: every stanza
// that reaches the wire must be an IQ get addressed to the target (the probe has no other path).
func TestAvatarProbeOnlySendsIQGet(t *testing.T) {
	to := jid.MustParse("room@conf.example/nick")
	var sent []string
	var itemQueried string
	send := func(ctx context.Context, payload xml.TokenReader, iq stanza.IQ) ([]byte, error) {
		s := tokensString(t, payload)
		sent = append(sent, "iq:"+string(iq.Type)+":"+iq.To.String())
		switch {
		case strings.Contains(s, "vCard"):
			return nil, &iqStanzaError{from: "room@conf.example", err: stanza.Error{Type: stanza.Cancel, Condition: stanza.Forbidden}}
		case strings.Contains(s, "avatar:metadata"):
			return []byte(`<pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="n"><item id="h1"><metadata xmlns="urn:xmpp:avatar:metadata"><info bytes="5" id="h1" type="image/png"/></metadata></item></items></pubsub>`), nil
		case strings.Contains(s, "avatar:data"):
			if strings.Contains(s, `<item id="h1"`) {
				itemQueried = "h1"
			}
			return nil, context.DeadlineExceeded
		default:
			return []byte(`<query xmlns="http://jabber.org/protocol/disco#info"/>`), nil
		}
	}
	nop := zerolog.Nop()
	lines := runAvatarProbe(context.Background(), send, to, time.Second, &nop)
	if len(lines) != 4 {
		t.Fatalf("lines: %v", lines)
	}
	if itemQueried != "h1" {
		t.Error("data probe did not carry the item id child")
	}
	if len(sent) != 4 {
		t.Fatalf("sent: %v", sent)
	}
	for _, s := range sent {
		if s != "iq:get:room@conf.example/nick" {
			t.Errorf("non-get-IQ or wrong target sent: %s", s)
		}
	}
	if !strings.Contains(lines[0], "error cancel/forbidden from room@conf.example") || !strings.Contains(lines[2], "timeout") {
		t.Errorf("lines: %v", lines)
	}
	if r := formatProbeReply("x@y/z", lines); !strings.HasPrefix(r, "Avatar probe for `x@y/z`") {
		t.Errorf("reply: %s", r)
	}
}
