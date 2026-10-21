package connector

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// Issue #12: message content (bodies, captions, reaction emoji, media URLs) must not appear in log output, at any
// level; only lengths and ids.
func TestInboundMessageContentNotLogged(t *testing.T) {
	const secret = "TOPSECRETBODY"
	x, _ := hookClient()
	x.conn = &JabberConnector{}
	buf := &bmLogBuf{}
	x.log = zerolog.New(buf).Level(zerolog.TraceLevel)
	x.mucOurOccupantJID = map[string]string{selfEchoRoom: selfEchoRoom + "/me"}

	runHandler(t, x, `<message type="chat" from="bob@example.com/res" to="me@example.com" id="c1"><body>`+secret+` chat</body></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" to="me@example.com" id="c2"><body>`+secret+` oob</body><x xmlns="jabber:x:oob"><url>https://example.com/f.png</url></x></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" to="me@example.com" id="c3"><body>`+secret+` reply</body><reply xmlns="urn:xmpp:reply:0" id="c1"/></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="g1"><body>`+secret+` muc</body></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="g2"><body>`+secret+` mine</body></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="g3"><reactions xmlns="urn:xmpp:reactions:0" id="S1"><reaction>🦄</reaction></reactions></message>`)
	serveStanza(t, x, `<message xmlns="jabber:client" type="chat" from="bob@example.com/res" id="s1"><body>`+secret+` serve</body></message>`)
	serveStanza(t, x, `<message xmlns="jabber:client" type="groupchat" from="room@muc.example.com/bob" id="s2"><body>`+secret+` serve muc</body></message>`)

	// XEP-0308 corrections (live DM, MUC, carbon): the corrected text is content too.
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" to="me@example.com" id="k1"><body>`+secret+` fix dm</body><replace xmlns="urn:xmpp:message-correct:0" id="c1"/></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="k2"><body>`+secret+` fix muc</body><replace xmlns="urn:xmpp:message-correct:0" id="g1"/></message>`)
	runHandler(t, x, `<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/l" to="bob@example.com" type="chat" id="k3"><body>`+secret+` fix carbon</body><replace xmlns="urn:xmpp:message-correct:0" id="c1"/></message></forwarded></sent></message>`)

	out := buf.String()
	if out == "" {
		t.Fatal("nothing was logged; the test proves nothing")
	}
	for _, leaked := range []string{secret, "🦄", "example.com/f.png"} {
		if strings.Contains(out, leaked) {
			t.Errorf("logs leak message content %q:\n%s", leaked, out)
		}
	}
	if strings.Contains(out, "body_preview") {
		t.Errorf("body_preview is still logged:\n%s", out)
	}
}
