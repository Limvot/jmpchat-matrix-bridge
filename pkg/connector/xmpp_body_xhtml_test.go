package connector

import "testing"

func TestParseBodyIgnoresXHTMLAndExtraBodies(t *testing.T) {
	cases := map[string]string{
		"xhtml-im": `<message xmlns="jabber:client" type="groupchat"><body>plain text</body>` +
			`<html xmlns="http://jabber.org/protocol/xhtml-im"><body xmlns="http://www.w3.org/1999/xhtml"><p>plain <b>text</b></p></body></html></message>`,
		"xhtml-im first": `<message xmlns="jabber:client" type="groupchat">` +
			`<html xmlns="http://jabber.org/protocol/xhtml-im"><body xmlns="http://www.w3.org/1999/xhtml"><p>plain text</p></body></html><body>plain text</body></message>`,
		"second xml:lang body": `<message type="chat"><body>plain text</body><body xml:lang="de">Klartext</body></message>`,
	}
	for name, stanza := range cases {
		body, _, _ := parseFallback(t, stanza)
		if body != "plain text" {
			t.Errorf("%s: body = %q, want %q", name, body, "plain text")
		}
	}
}

func TestParseBodyReplyFallbackStillUsesOwnBody(t *testing.T) {
	body, rf, rid := parseFallback(t, `<message type="groupchat"><body>&gt; bob: hi
answer</body><reply xmlns="urn:xmpp:reply:0" to="room@muc.example.com/bob" id="S1"/>`+
		`<fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:reply:0"><body start="0" end="9"/></fallback>`+
		`<html xmlns="http://jabber.org/protocol/xhtml-im"><body xmlns="http://www.w3.org/1999/xhtml">answer</body></html></message>`)
	if body != "> bob: hi\nanswer" || rid != "S1" || !rf.Valid {
		t.Fatalf("body=%q rid=%q fallback=%+v", body, rid, rf)
	}
}
