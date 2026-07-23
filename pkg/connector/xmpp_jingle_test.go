package connector

import (
	"encoding/xml"
	"strings"
	"testing"
)

// parseMessageChildren feeds the children of the given <message> stanza to
// parseMessageBodyAndOOB, mimicking how the inbound handler invokes it (positioned
// just after the <message> start element).
func parseMessageChildren(t *testing.T, stanza string) *jingleMessageInfo {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(stanza))
	// Advance past the outer <message> start element.
	for {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("failed to read outer element: %v", err)
		}
		if _, ok := tok.(xml.StartElement); ok {
			break
		}
	}
	_, _, _, _, _, jmi, err := parseMessageBodyAndOOB(dec)
	if err != nil {
		t.Fatalf("parseMessageBodyAndOOB error: %v", err)
	}
	return jmi
}

func TestParseJingleMessageInitiation(t *testing.T) {
	tests := []struct {
		name       string
		stanza     string
		wantAction string
		wantSID    string
		wantMedia  string
	}{
		{
			name:       "propose voice",
			stanza:     `<message from="alice@example.com/phone" to="me@example.com"><propose xmlns="urn:xmpp:jingle-message:0" id="call-1"><description xmlns="urn:xmpp:jingle:apps:rtp:1" media="audio"/></propose></message>`,
			wantAction: "propose",
			wantSID:    "call-1",
			wantMedia:  "audio",
		},
		{
			name:       "propose video prefers video over audio",
			stanza:     `<message from="alice@example.com/phone"><propose xmlns="urn:xmpp:jingle-message:0" id="call-2"><description xmlns="urn:xmpp:jingle:apps:rtp:1" media="audio"/><description xmlns="urn:xmpp:jingle:apps:rtp:1" media="video"/></propose></message>`,
			wantAction: "propose",
			wantSID:    "call-2",
			wantMedia:  "video",
		},
		{
			name:       "retract",
			stanza:     `<message from="alice@example.com/phone"><retract xmlns="urn:xmpp:jingle-message:0" id="call-3"><reason xmlns="urn:xmpp:jingle:1"><cancel/></reason></retract></message>`,
			wantAction: "retract",
			wantSID:    "call-3",
		},
		{
			name:       "accept",
			stanza:     `<message from="me@example.com/laptop" to="me@example.com"><accept xmlns="urn:xmpp:jingle-message:0" id="call-4"/></message>`,
			wantAction: "accept",
			wantSID:    "call-4",
		},
		{
			name:       "reject",
			stanza:     `<message from="me@example.com/laptop"><reject xmlns="urn:xmpp:jingle-message:0" id="call-5"/></message>`,
			wantAction: "reject",
			wantSID:    "call-5",
		},
		{
			name:       "finish jmi v1",
			stanza:     `<message from="alice@example.com/phone"><finish xmlns="urn:xmpp:jingle-message:1" id="call-6"/></message>`,
			wantAction: "finish",
			wantSID:    "call-6",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jmi := parseMessageChildren(t, tc.stanza)
			if jmi == nil {
				t.Fatalf("expected JMI info, got nil")
			}
			if jmi.action != tc.wantAction {
				t.Errorf("action = %q, want %q", jmi.action, tc.wantAction)
			}
			if jmi.sid != tc.wantSID {
				t.Errorf("sid = %q, want %q", jmi.sid, tc.wantSID)
			}
			if jmi.media != tc.wantMedia {
				t.Errorf("media = %q, want %q", jmi.media, tc.wantMedia)
			}
		})
	}
}

// A plain text message must not be mistaken for a call.
func TestParseNonJingleMessage(t *testing.T) {
	stanza := `<message from="alice@example.com/phone" type="chat"><body>hello there</body></message>`
	if jmi := parseMessageChildren(t, stanza); jmi != nil {
		t.Fatalf("expected nil JMI for plain message, got %+v", jmi)
	}
}
