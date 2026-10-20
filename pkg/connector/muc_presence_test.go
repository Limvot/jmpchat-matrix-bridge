package connector

import (
	"encoding/xml"
	"strings"
	"testing"
)

const testHash = "0123456789abcdef0123456789abcdef01234567"

func parseTestPresence(t *testing.T, doc string) presenceInfo {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(doc))
	tok, err := dec.Token()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tok.(xml.StartElement); !ok {
		t.Fatalf("first token %T", tok)
	}
	return parsePresence(dec)
}

func TestParsePresence(t *testing.T) {
	const mucU = `xmlns='http://jabber.org/protocol/muc#user'`
	tests := []struct {
		name  string
		doc   string
		check func(t *testing.T, p presenceInfo)
	}{
		{"hash", `<presence><x xmlns='vcard-temp:x:update'><photo>` + testHash + `</photo></x></presence>`, func(t *testing.T, p presenceInfo) {
			if p.photo != photoHash || p.hash != testHash || p.rawPhoto != testHash || !p.sawVCardUpdate || p.isMUC {
				t.Errorf("%+v", p)
			}
		}},
		{"empty photo is none", `<presence><x xmlns='vcard-temp:x:update'><photo/></x></presence>`, func(t *testing.T, p presenceInfo) {
			if p.photo != photoNone || p.rawPhoto != "" || !p.sawVCardUpdate {
				t.Errorf("%+v", p)
			}
		}},
		{"whitespace photo is none", `<presence><x xmlns='vcard-temp:x:update'><photo> 
 </photo></x></presence>`, func(t *testing.T, p presenceInfo) {
			if p.photo != photoNone {
				t.Errorf("%+v", p)
			}
		}},
		{"no x:update is unknown", `<presence><show>away</show></presence>`, func(t *testing.T, p presenceInfo) {
			if p.photo != photoUnknown || p.sawVCardUpdate {
				t.Errorf("%+v", p)
			}
		}},
		{"x:update without photo is unknown", `<presence><x xmlns='vcard-temp:x:update'/></presence>`, func(t *testing.T, p presenceInfo) {
			if p.photo != photoUnknown || !p.sawVCardUpdate || p.rawPhoto != "" {
				t.Errorf("%+v", p)
			}
		}},
		{"bad length is unknown", `<presence><x xmlns='vcard-temp:x:update'><photo>abc123</photo></x></presence>`, func(t *testing.T, p presenceInfo) {
			if p.photo != photoUnknown || p.rawPhoto != "abc123" {
				t.Errorf("%+v", p)
			}
		}},
		{"non-hex is unknown", `<presence><x xmlns='vcard-temp:x:update'><photo>` + strings.Repeat("g", 40) + `</photo></x></presence>`, func(t *testing.T, p presenceInfo) {
			if p.photo != photoUnknown {
				t.Errorf("%+v", p)
			}
		}},
		{"uppercase is lowercased", `<presence><x xmlns='vcard-temp:x:update'><photo>` + strings.ToUpper(testHash) + `</photo></x></presence>`, func(t *testing.T, p presenceInfo) {
			if p.photo != photoHash || p.hash != testHash || p.rawPhoto != strings.ToUpper(testHash) {
				t.Errorf("%+v", p)
			}
		}},
		{"item jid resource stripped", `<presence><x ` + mucU + `><item affiliation='member' role='participant' jid='bob@example.com/phone'/></x></presence>`, func(t *testing.T, p presenceInfo) {
			if !p.isMUC || p.itemJID != "bob@example.com" {
				t.Errorf("%+v", p)
			}
		}},
		{"303 unavailable with nick", `<presence type='unavailable'><x ` + mucU + `><item nick='newnick'/><status code='303'/></x></presence>`, func(t *testing.T, p presenceInfo) {
			if p.newNick != "newnick" || !p.hasStatus("303") || p.hasStatus("110") {
				t.Errorf("%+v", p)
			}
		}},
		{"110 self and occupant-id", `<presence><x ` + mucU + `><status code='110'/><status code='100'/></x><occupant-id xmlns='urn:xmpp:occupant-id:0' id='abc'/></presence>`, func(t *testing.T, p presenceInfo) {
			if !p.hasStatus("110") || !p.hasStatus("100") || !p.hasOccupantID {
				t.Errorf("%+v", p)
			}
		}},
		{"409 conflict code", `<presence type='error'><error code='409' type='cancel'/></presence>`, func(t *testing.T, p presenceInfo) {
			if !p.conflict {
				t.Errorf("%+v", p)
			}
		}},
		{"conflict condition", `<presence type='error'><error type='cancel'><conflict xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></error></presence>`, func(t *testing.T, p presenceInfo) {
			if !p.conflict {
				t.Errorf("%+v", p)
			}
		}},
		{"photo outside x:update ignored", `<presence><photo>` + testHash + `</photo></presence>`, func(t *testing.T, p presenceInfo) {
			if p.photo != photoUnknown || p.rawPhoto != "" {
				t.Errorf("%+v", p)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.check(t, parseTestPresence(t, tc.doc)) })
	}
}
