package connector

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
)

func topicEvent(t *testing.T, got *[]bridgev2.RemoteEvent) *simplevent.ChatInfoChange {
	t.Helper()
	if len(*got) != 1 {
		t.Fatalf("queued %d events, want 1", len(*got))
	}
	evt, ok := (*got)[0].(*simplevent.ChatInfoChange)
	if !ok {
		t.Fatalf("queued %T", (*got)[0])
	}
	return evt
}

func TestMUCSubjectQueuesTopicChange(t *testing.T) {
	x, got := subjClient()
	runHandler(t, x, "<message type=\"groupchat\" from=\"room@muc.example.com/alice\" id=\"s1\"><subject>Release day\nplan</subject></message>")
	evt := topicEvent(t, got)
	if evt.GetPortalKey().ID != "room@muc.example.com" || evt.GetPortalKey().Receiver != retractTestLogin || evt.CreatePortal {
		t.Errorf("event meta = %+v", evt.EventMeta)
	}
	if evt.GetSender() != (bridgev2.EventSender{}) {
		t.Errorf("sender = %+v, want empty (bridge bot)", evt.GetSender())
	}
	ci := evt.ChatInfoChange.ChatInfo
	if ci == nil || ci.Topic == nil || *ci.Topic != "Release day\nplan" {
		t.Errorf("topic = %v", ci)
	}
}

func TestMUCSubjectEmptyClearsTopic(t *testing.T) {
	x, got := subjClient()
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com" id="s1"><subject/></message>`)
	ci := topicEvent(t, got).ChatInfoChange.ChatInfo
	if ci.Topic == nil || *ci.Topic != "" {
		t.Errorf("topic = %v, want pointer to empty string", ci.Topic)
	}
}

func TestMUCSubjectWithBodyIsMessage(t *testing.T) {
	x, got := subjClient()
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="s1"><subject>ignored</subject><body>hello</body></message>`)
	for _, e := range *got {
		if _, ok := e.(*simplevent.ChatInfoChange); ok {
			t.Fatal("a message with a body must not change the topic")
		}
	}
}

func TestDMSubjectIgnored(t *testing.T) {
	x, got := subjClient()
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="s1"><subject>hello</subject></message>`)
	if len(*got) != 0 {
		t.Errorf("DM subject queued %v", *got)
	}
}

func TestComposeMUCTopicPrefersSubject(t *testing.T) {
	for _, tc := range []struct{ desc, subj, want string }{
		{"desc", "subj", "subj"},
		{"desc", "", "desc"},
		{"", "subj", "subj"},
		{"", "", ""},
	} {
		if got := composeMUCTopic(tc.desc, tc.subj); got != tc.want {
			t.Errorf("composeMUCTopic(%q,%q) = %q, want %q", tc.desc, tc.subj, got, tc.want)
		}
	}
}

// subjClient is a hookClient with a connector, so message paths that consult the backfill config work.
func subjClient() (*xmppClient, *[]bridgev2.RemoteEvent) {
	x, got := hookClient()
	x.conn = &JabberConnector{}
	return x, got
}

func TestSanitizeSubject(t *testing.T) {
	if got := sanitizeSubject("a\x07b\nc\td\x00"); got != "ab\nc\td" {
		t.Errorf("sanitizeSubject = %q", got)
	}
}

func TestPlainFromFormattedBody(t *testing.T) {
	got := plainFromFormattedBody("  <p>hello <strong>world</strong></p>  ")
	if strings.Contains(got, "<") || !strings.Contains(got, "hello") || !strings.Contains(got, "world") || got != strings.TrimSpace(got) {
		t.Errorf("plainFromFormattedBody = %q", got)
	}
}

func remoteMsg(t *testing.T, got *[]bridgev2.RemoteEvent) *jabberRemoteMessage {
	t.Helper()
	if len(*got) != 1 {
		t.Fatalf("queued %d events, want 1", len(*got))
	}
	m, ok := (*got)[0].(*jabberRemoteMessage)
	if !ok {
		t.Fatalf("queued %T", (*got)[0])
	}
	return m
}

const omemoFallback = "I sent you an OMEMO encrypted message but your client doesn't seem to support that."

func TestEncryptedOMEMOBecomesNotice(t *testing.T) {
	enc := `<encrypted xmlns="eu.siacs.conversations.axolotl"><header sid="1"/><payload>AAAA</payload></encrypted><body>` + omemoFallback + `</body>`
	carbon := `<message from="me@example.com"><%[1]s xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="%[2]s" to="%[3]s" type="chat" id="e1">` + enc + `</message></forwarded></%[1]s></message>`
	for _, tc := range []struct{ name, stanza string }{
		{"live dm", `<message type="chat" from="bob@example.com/p" id="e1">` + enc + `</message>`},
		{"live muc", `<message type="groupchat" from="room@muc.example.com/bob" id="e1">` + enc + `</message>`},
		{"carbon sent", fmt.Sprintf(carbon, "sent", "me@example.com/l", "bob@example.com")},
		{"carbon received", fmt.Sprintf(carbon, "received", "bob@example.com/p", "me@example.com")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, got := subjClient()
			runHandler(t, x, tc.stanza)
			m := remoteMsg(t, got)
			if m.encrypted != encKindOMEMO || m.body != "" || m.oobURL != "" || string(m.id) != "e1" {
				t.Errorf("event = kind %q body %q oob %q id %q", m.encrypted, m.body, m.oobURL, m.id)
			}
			cm, err := m.convertMessage(context.Background(), nil, nil)
			if err != nil || len(cm.Parts) != 1 {
				t.Fatalf("convert = %v, %v", cm, err)
			}
			c := cm.Parts[0].Content
			if c.MsgType != event.MsgNotice || c.Body != encryptedNotice(encKindOMEMO) || strings.Contains(c.Body, "doesn't seem") {
				t.Errorf("content = %+v", c)
			}
		})
	}
}

func TestAESGCMURLBecomesNotice(t *testing.T) {
	const url = "aesgcm://files.example.com/abc#0123456789abcdef"
	for _, stanza := range []string{
		`<message type="chat" from="bob@example.com/p" id="a1"><body>` + url + `</body></message>`,
		`<message type="chat" from="bob@example.com/p" id="a1"><body>` + url + `</body><x xmlns="jabber:x:oob"><url>` + url + `</url></x></message>`,
		`<message type="groupchat" from="room@muc.example.com/bob" id="a1"><body>` + url + `</body></message>`,
	} {
		x, got := subjClient()
		runHandler(t, x, stanza)
		m := remoteMsg(t, got)
		if m.encrypted != encKindFile || m.body != "" || m.oobURL != "" {
			t.Errorf("%s: kind %q body %q oob %q", stanza, m.encrypted, m.body, m.oobURL)
		}
	}
}

func TestOMEMOKeyTransportDropped(t *testing.T) {
	x, got := subjClient()
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="k1"><encrypted xmlns="eu.siacs.conversations.axolotl"><header sid="1"/></encrypted></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="k2"><encrypted xmlns="urn:xmpp:omemo:2"><header sid="1"/></encrypted></message>`)
	if len(*got) != 0 {
		t.Errorf("key transport queued %v", *got)
	}
}

func TestPlainMessageNotEncrypted(t *testing.T) {
	x, got := subjClient()
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="p1"><body>hello</body></message>`)
	if m := remoteMsg(t, got); m.encrypted != "" || m.body != "hello" {
		t.Errorf("event = %+v", m)
	}
}

// MAM catch-up/on-view and FetchMessages classify with the same helpers.
func TestEncryptedMAMItem(t *testing.T) {
	it := mamItem("m1", "bob@example.com/p", "")
	it.Body = omemoFallback
	it.Extras.Encryption = "omemo2"
	kind, body, _ := applyEncryption(it.Extras, it.Body, "")
	if kind != encKindOMEMO || body != "" {
		t.Errorf("kind %q body %q", kind, body)
	}
	if !mamItemBackfillable(it, nil) {
		t.Error("encrypted items keep their fallback body and must stay backfillable")
	}
	if c := mamBackfillContent(it); c.MsgType != event.MsgNotice || c.Body != encryptedNotice(encKindOMEMO) {
		t.Errorf("content = %+v", c)
	}
	plain := mamItem("m2", "bob@example.com/p", "")
	plain.Body = "hi"
	if c := mamBackfillContent(plain); c.MsgType != event.MsgText || c.Body != "hi" {
		t.Errorf("plain content = %+v", c)
	}
	for ext, want := range map[string]string{"openpgp": encKindOpenPGP, "pgp": encKindOpenPGP, "eme:urn:example:x": encKindOther} {
		it.Extras.Encryption = ext
		if got := encryptedKind(it.Extras, "x", ""); got != want {
			t.Errorf("%s -> %q, want %q", ext, got, want)
		}
	}
}

func TestSubjectAndEncryptedContentNotLogged(t *testing.T) {
	const secret = "SUBJECTSECRET"
	x, _ := subjClient()
	buf := &bmLogBuf{}
	x.log = zerolog.New(buf).Level(zerolog.TraceLevel)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/alice" id="s1"><subject>`+secret+` topic</subject></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="e1"><encrypted xmlns="eu.siacs.conversations.axolotl"><payload>AA</payload></encrypted><body>`+secret+` fallback</body></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="e2"><encrypted xmlns="urn:xmpp:omemo:2"><payload>AA</payload></encrypted><body>`+secret+` muc fallback</body></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="e3"><body>aesgcm://example.com/`+secret+`#key</body></message>`)
	out := buf.String()
	if out == "" {
		t.Fatal("nothing logged; the test proves nothing")
	}
	if strings.Contains(out, secret) {
		t.Errorf("logs leak subject or encrypted content:\n%s", out)
	}
}
