package connector

import (
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

func parseFull(t *testing.T, stanza string) (body string, jmi *jingleMessageInfo, ids messageIDs) {
	t.Helper()
	r, _ := startOfMessage(t, stanza)
	body, _, _, _, _, jmi, ids, err := parseMessageBodyOOBIDs(r)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return body, jmi, ids
}

func TestParseRetractionForms(t *testing.T) {
	const fb = `<fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:message-retract:1"/>`
	for _, tc := range []struct {
		name   string
		stanza string
		want   retractInfo // Reason is checked separately where set
	}{
		{"1:1 with fallback marker",
			`<message type="chat" from="bob@example.com/p"><retract xmlns="urn:xmpp:message-retract:1" id="ORIG"/>` + fb + `<body>A previous message was retracted</body><store xmlns="urn:xmpp:hints"/></message>`,
			retractInfo{Target: "ORIG", HasFallback: true}},
		{"1:1 without fallback marker (old sender)",
			`<message type="chat" from="bob@example.com/p"><retract xmlns="urn:xmpp:message-retract:1" id="ORIG"/><body>bob retracted a message</body></message>`,
			retractInfo{Target: "ORIG"}},
		{"groupchat self retraction",
			`<message type="groupchat" from="room@muc.example.com/bob"><retract xmlns="urn:xmpp:message-retract:1" id="S-ROOM"/><body>x</body><stanza-id xmlns="urn:xmpp:sid:0" by="room@muc.example.com" id="S-RETRACT"/></message>`,
			retractInfo{Target: "S-ROOM"}},
		{"moderation with by, occupant-id and reason",
			`<message type="groupchat" from="room@muc.example.com"><retract xmlns="urn:xmpp:message-retract:1" id="S-ROOM"><moderated xmlns="urn:xmpp:message-moderate:1" by="room@muc.example.com/mod"><occupant-id xmlns="urn:xmpp:occupant-id:0" id="MODOCC"/></moderated><reason>spam</reason></retract></message>`,
			retractInfo{Target: "S-ROOM", Moderated: true, ModeratedByPresent: true, HasReason: true, Reason: "spam"}},
		{"moderation without by",
			`<message type="groupchat" from="room@muc.example.com"><retract xmlns="urn:xmpp:message-retract:1" id="S-ROOM"><moderated xmlns="urn:xmpp:message-moderate:1"/></retract></message>`,
			retractInfo{Target: "S-ROOM", Moderated: true}},
		{"legacy :0 retraction",
			`<message type="chat" from="bob@example.com/p"><apply-to xmlns="urn:xmpp:fasten:0" id="ORIG"><retract xmlns="urn:xmpp:message-retract:0"/></apply-to><body>retracted</body></message>`,
			retractInfo{Target: "ORIG", Legacy: true}},
		{"legacy :0 moderation",
			`<message type="groupchat" from="room@muc.example.com"><apply-to xmlns="urn:xmpp:fasten:0" id="S-ROOM"><moderated xmlns="urn:xmpp:message-moderate:0" by="room@muc.example.com/mod"><retract xmlns="urn:xmpp:message-retract:0"/></moderated><reason>off topic</reason></apply-to></message>`,
			retractInfo{Target: "S-ROOM", Legacy: true, Moderated: true, ModeratedByPresent: true, HasReason: true, Reason: "off topic"}},
		{"both forms yield one retraction, :1 wins",
			`<message type="groupchat" from="room@muc.example.com"><apply-to xmlns="urn:xmpp:fasten:0" id="LEGACY"><moderated xmlns="urn:xmpp:message-moderate:0"><retract xmlns="urn:xmpp:message-retract:0"/></moderated></apply-to><retract xmlns="urn:xmpp:message-retract:1" id="S-ROOM"><moderated xmlns="urn:xmpp:message-moderate:1"/></retract></message>`,
			retractInfo{Target: "S-ROOM", Moderated: true, BothForms: true}},
		{"inherited namespace (no literal xmlns on the child)",
			`<message xmlns="urn:xmpp:message-retract:1" type="chat"><retract id="ORIG"/></message>`,
			retractInfo{Target: "ORIG"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, jmi, ids := parseFull(t, tc.stanza)
			if ids.Retract == nil {
				t.Fatal("no retraction parsed")
			}
			if *ids.Retract != tc.want {
				t.Errorf("retract = %+v, want %+v", *ids.Retract, tc.want)
			}
			if body != "" {
				t.Errorf("fallback body leaked: %q", body)
			}
			if jmi != nil {
				t.Errorf("retraction mistaken for a call: %+v", jmi)
			}
		})
	}
}

func TestParseRetractionNotMatched(t *testing.T) {
	for name, stanza := range map[string]string{
		// XEP-0353 call retraction is not a message retraction and must keep reaching the JMI handler.
		"jingle-message retract":              `<message from="alice@example.com/phone"><retract xmlns="urn:xmpp:jingle-message:0" id="call-3"><reason xmlns="urn:xmpp:jingle:1"><cancel/></reason></retract><body>call retracted</body></message>`,
		"retract inside an unrelated payload": `<message type="chat"><x xmlns="urn:test"><retract xmlns="urn:xmpp:message-retract:1" id="X"/></x><body>hi</body></message>`,
		"retract in a wrong namespace":        `<message type="chat"><retract xmlns="urn:other" id="X"/><body>hi</body></message>`,
		"apply-to without a :0 retract":       `<message type="chat"><apply-to xmlns="urn:xmpp:fasten:0" id="X"><other xmlns="urn:test"/></apply-to><body>hi</body></message>`,
		"plain message":                       `<message type="chat"><body>hi</body></message>`,
	} {
		t.Run(name, func(t *testing.T) {
			body, jmi, ids := parseFull(t, stanza)
			if ids.Retract != nil {
				t.Errorf("unexpected retraction %+v", *ids.Retract)
			}
			if strings.Contains(name, "jingle") {
				if jmi == nil || jmi.action != "retract" || jmi.sid != "call-3" {
					t.Errorf("jingle retract lost: %+v", jmi)
				}
			}
			if name != "jingle-message retract" && body != "hi" {
				t.Errorf("body = %q, must not be suppressed without a retract element", body)
			}
		})
	}
}

func TestParseRetractionDetails(t *testing.T) {
	// The moderator's occupant-id nested in <moderated> is not the stanza's occupant-id.
	_, _, ids := parseFull(t, `<message type="groupchat" from="room@muc.example.com"><retract xmlns="urn:xmpp:message-retract:1" id="S"><moderated xmlns="urn:xmpp:message-moderate:1"><occupant-id xmlns="urn:xmpp:occupant-id:0" id="MODOCC"/></moderated></retract></message>`)
	if ids.OccupantID != "" {
		t.Errorf("nested moderator occupant-id taken as top-level: %q", ids.OccupantID)
	}
	// A top-level one is.
	_, _, ids = parseFull(t, `<message type="groupchat" from="room@muc.example.com/bob"><retract xmlns="urn:xmpp:message-retract:1" id="S"/><occupant-id xmlns="urn:xmpp:occupant-id:0" id="OCC"/></message>`)
	if ids.OccupantID != "OCC" {
		t.Errorf("top-level occupant-id = %q", ids.OccupantID)
	}
	// Reason is trimmed and capped.
	long := strings.Repeat("é", maxReasonLen+50)
	_, _, ids = parseFull(t, `<message type="groupchat"><retract xmlns="urn:xmpp:message-retract:1" id="S"><reason>  `+long+`  </reason></retract></message>`)
	if got := []rune(ids.Retract.Reason); len(got) != maxReasonLen {
		t.Errorf("reason length = %d, want %d", len(got), maxReasonLen)
	}
	// A <reason> outside the retract element is not its reason.
	_, _, ids = parseFull(t, `<message type="groupchat"><retract xmlns="urn:xmpp:message-retract:1" id="S"/><reason>nope</reason></message>`)
	if ids.Retract.HasReason || ids.Retract.Reason != "" {
		t.Errorf("foreign reason attached: %+v", *ids.Retract)
	}
	// An archived original that was retracted is a tombstone, never a retraction event.
	body, _, ids := parseFull(t, `<message type="chat" from="bob@example.com" id="ORIG"><retracted xmlns="urn:xmpp:message-retract:1" stamp="2026-01-01T00:00:00Z"/></message>`)
	if ids.Retract != nil || !ids.Tombstone || body != "" {
		t.Errorf("tombstone: retract=%v tombstone=%v body=%q", ids.Retract, ids.Tombstone, body)
	}
}

const carbonRetractFmt = `<message from="me@example.com"><%[1]s xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="%[2]s" to="%[3]s" type="chat" id="r1"><retract xmlns="urn:xmpp:message-retract:1" id="ORIG"/><fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:message-retract:1"/><body>retracted</body></message></forwarded></%[1]s></message>`

func TestCarbonRetractionParsed(t *testing.T) {
	for _, tc := range []struct{ kind, from, to string }{
		{"sent", "me@example.com/laptop", "bob@example.com"},
		{"received", "bob@example.com/p", "me@example.com"},
	} {
		stanza := strings.NewReplacer("%[1]s", tc.kind, "%[2]s", tc.from, "%[3]s", tc.to).Replace(carbonRetractFmt)
		r, _ := startOfMessage(t, stanza)
		if _, err := r.Token(); err != nil { // step past <sent>/<received>
			t.Fatal(err)
		}
		_, _, _, _, body, _, ids, err := parseInnerMessageFromCarbon(r)
		if err != nil {
			t.Fatal(err)
		}
		if ids.Retract == nil || ids.Retract.Target != "ORIG" || body != "" {
			t.Errorf("%s: retract=%+v body=%q", tc.kind, ids.Retract, body)
		}
	}
}

func TestMAMRetractionAndTombstoneItems(t *testing.T) {
	deliver := func(inner string) mamResultItem {
		t.Helper()
		x := newOuterFromTestClient()
		ch := x.mam.register("q1", networkid.PortalKey{ID: "bob@example.com", Receiver: x.loginID}, false)
		r, start := startOfMessage(t, `<message to="me@example.com"><result xmlns="urn:xmpp:mam:2" queryid="q1" id="A1"><forwarded xmlns="urn:xmpp:forward:0"><delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z"/>`+inner+`</forwarded></result></message>`)
		if err := x.handleMessageWithOOB(r, &start); err != nil {
			t.Fatal(err)
		}
		select {
		case it := <-ch:
			return it
		default:
			t.Fatal("no item delivered")
			return mamResultItem{}
		}
	}
	it := deliver(`<message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat" id="r1"><retract xmlns="urn:xmpp:message-retract:1" id="ORIG"/><body>bob retracted a message</body></message>`)
	if it.Retract == nil || it.Retract.Target != "ORIG" || it.Body != "" || it.Retracted {
		t.Errorf("retraction item = %+v", it)
	}
	it = deliver(`<message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat" id="ORIG"><retracted xmlns="urn:xmpp:message-retract:1" id="r1" stamp="2026-01-01T00:00:00Z"/></message>`)
	if it.Retract != nil || !it.Retracted || it.Body != "" {
		t.Errorf("tombstone item = %+v", it)
	}
}
