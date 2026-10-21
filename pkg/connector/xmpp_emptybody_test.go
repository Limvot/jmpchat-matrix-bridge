package connector

import (
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

// innerWithFallback is a reply whose XEP-0428 fallback contains an empty <body/>, followed by
// the real body and every direct child that must survive the fallback.
const innerWithFallback = `<message xmlns="jabber:client" from="%s" to="%s" type="chat" id="m1">` +
	`<reply xmlns="urn:xmpp:reply:0" id="T1"/>` +
	`<fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:reply:0"><body start="0" end="10"/></fallback>` +
	`<body>real body</body>` +
	`<origin-id xmlns="urn:xmpp:sid:0" id="O1"/>` +
	`<stanza-id xmlns="urn:xmpp:sid:0" by="me@example.com" id="S1"/>` +
	`<markable xmlns="urn:xmpp:chat-markers:0"/>` +
	`<replace xmlns="urn:xmpp:message-correct:0" id="R1"/>` +
	`<request xmlns="urn:xmpp:receipts"/>` +
	`</message>`

func TestCarbonParsesAfterEmptyFallbackBody(t *testing.T) {
	for _, tc := range []struct{ kind, from, to string }{
		{"sent", "me@example.com/laptop", "bob@example.com"},
		{"received", "bob@example.com/p", "me@example.com"},
	} {
		inner := strings.Replace(strings.Replace(innerWithFallback, "%s", tc.from, 1), "%s", tc.to, 1)
		stanza := `<message from="me@example.com"><` + tc.kind + ` xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0">` + inner + `</forwarded></` + tc.kind + `></message>`
		r, _ := startOfMessage(t, stanza)
		if _, err := r.Token(); err != nil {
			t.Fatal(err)
		}
		_, _, _, _, body, _, ids, err := parseInnerMessageFromCarbon(r)
		if err != nil {
			t.Fatal(err)
		}
		if body != "real body" {
			t.Errorf("%s: body = %q", tc.kind, body)
		}
		if ids.OriginID != "O1" || ids.stanzaIDBy("me@example.com") != "S1" {
			t.Errorf("%s: ids origin=%q stanza=%q", tc.kind, ids.OriginID, ids.stanzaIDBy("me@example.com"))
		}
		if !ids.Extras.Markable || ids.Extras.Replace != "R1" || !ids.Extras.ReceiptRequest {
			t.Errorf("%s: extras = %+v", tc.kind, ids.Extras)
		}
	}
}

func TestMAMParsesAfterEmptyFallbackBody(t *testing.T) {
	x := newOuterFromTestClient()
	ch := x.mam.register("q1", networkid.PortalKey{ID: "bob@example.com", Receiver: x.loginID}, false)
	inner := strings.Replace(strings.Replace(innerWithFallback, "%s", "bob@example.com/p", 1), "%s", "me@example.com", 1)
	r, start := startOfMessage(t, `<message to="me@example.com"><result xmlns="urn:xmpp:mam:2" queryid="q1" id="A1"><forwarded xmlns="urn:xmpp:forward:0"><delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z"/>`+inner+`</forwarded></result></message>`)
	if err := x.handleMessageWithOOB(r, &start); err != nil {
		t.Fatal(err)
	}
	select {
	case it := <-ch:
		if it.Body != "real body" || it.OriginID != "O1" || it.Extras.Replace != "R1" || !it.Extras.Markable || !it.Extras.ReceiptRequest {
			t.Errorf("item = %+v", it)
		}
	default:
		t.Fatal("no item delivered")
	}
}

func TestSubjectBufferBoundedByOneHugeChunk(t *testing.T) {
	huge := strings.Repeat("é", 1<<20) // 2 MiB in one CharData chunk
	_, _, ids := parseFull(t, `<message type="groupchat"><subject>`+huge+`</subject></message>`)
	if ids.Extras.Subject == nil || len([]rune(*ids.Extras.Subject)) != maxSubjectRunes {
		t.Fatalf("subject = %v", ids.Extras.Subject)
	}
	var m messageIDs
	m.subjectDepth = 2
	m.trackChars([]byte(huge))
	if got := len(m.subjectBuf); got > 4*maxSubjectRunes+4 {
		t.Errorf("subject buffer = %d bytes, want <= %d", got, 4*maxSubjectRunes+4)
	}
}
