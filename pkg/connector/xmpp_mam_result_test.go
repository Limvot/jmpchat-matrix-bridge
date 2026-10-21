package connector

// Regression tests for the MAM <result> parser: a XEP-0482 fallback marker embeds a self-closing
// <body/> (<fallback ...><body end='33' start='0'/></fallback>), and archived SMS/MMS from
// Cheogram is full of them. The old body parser peeked the token after every <body> start element
// and silently ate the matching EndElement of self-closing/empty bodies, leaking one depth level
// per marker; the parse then ran past the end of the element, hit io.EOF and returned it — which
// mellium's Session.Serve treats as a clean stream close, answering with </stream:stream> and
// killing the XMPP session in the middle of the MAM flood (endless connect/backfill/die loop).

import (
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

// parseOneMAMResult feeds one archived <result> (wrapped in an outer <message>) through the same
// handler path the Serve loop uses. It returns the delivered item (ok=false when nothing was
// delivered), the error, and the token left on the reader after the handler returned — the
// handler must stop exactly at the end of the <result> and leave the outer </message> unread.
func parseOneMAMResult(t *testing.T, stanza string) (item mamResultItem, ok bool, next xml.Token, err error) {
	t.Helper()
	x := newOuterFromTestClient()
	ch := x.mam.register("Q1", networkid.PortalKey{ID: "+15551234567@cheogram.com", Receiver: x.loginID}, true)
	r, start := startOfMessage(t, stanza)
	if herr := x.handleMessageWithOOB(r, &start); herr != nil {
		return item, false, nil, herr
	}
	next, nerr := r.Token()
	if nerr != nil {
		t.Fatalf("handler over-read the stanza (next token: %v)", nerr)
	}
	select {
	case item = <-ch:
		ok = true
	default:
	}
	return item, ok, next, nil
}

func TestMAMResultFallbackBodyDoesNotKillParse(t *testing.T) {
	const stanza = `<message from="me@example.com" to="me@example.com/res" type="chat">` +
		`<result xmlns="urn:xmpp:mam:2" id="A1" queryid="Q1"><forwarded xmlns="urn:xmpp:forward:0">` +
		`<delay xmlns="urn:xmpp:delay" stamp="2026-10-16T01:41:49.359989Z"/>` +
		`<message to="me@example.com" id="c1" type="chat" from="+16783574144,+17742765057@cheogram.com" xmlns="jabber:client">` +
		`<body>real text here</body>` +
		`<addresses xmlns="http://jabber.org/protocol/address"><address type="ofrom" jid="+17742765057@cheogram.com"/></addresses>` +
		`<fallback for="http://jabber.org/protocol/address" xmlns="urn:xmpp:fallback:0"><body end="33" start="0"/></fallback>` +
		`</message></forwarded></result></message>`

	item, ok, next, err := parseOneMAMResult(t, stanza)
	if err != nil {
		t.Fatalf("handleMessageWithOOB: %v", err)
	}
	if !ok {
		t.Fatal("no item delivered")
	}
	if item.Body != "real text here" {
		t.Errorf("body = %q, want %q", item.Body, "real text here")
	}
	end, isEnd := next.(xml.EndElement)
	if !isEnd || end.Name.Local != "message" {
		t.Errorf("handler must stop at </result>, got %#v", next)
	}
}

func TestMAMResultEmptyBodyDoesNotKillParse(t *testing.T) {
	for _, bodyEl := range []string{"<body></body>", "<body/>"} {
		t.Run(bodyEl, func(t *testing.T) {
			const tmpl = `<message from="me@example.com"><result xmlns="urn:xmpp:mam:2" id="A1" queryid="Q1">` +
				`<forwarded xmlns="urn:xmpp:forward:0"><message to="me@example.com" id="c1" type="chat" from="bob@cheogram.com">%s</message>` +
				`</forwarded></result></message>`
			item, ok, next, err := parseOneMAMResult(t, strings.ReplaceAll(tmpl, "%s", bodyEl))
			if err != nil {
				t.Fatalf("handleMessageWithOOB: %v", err)
			}
			if !ok {
				t.Fatal("no item delivered")
			}
			if item.Body != "" {
				t.Errorf("body = %q, want empty", item.Body)
			}
			if end, isEnd := next.(xml.EndElement); !isEnd || end.Name.Local != "message" {
				t.Errorf("handler must stop at </result>, got %#v", next)
			}
		})
	}
}

func TestMAMResultNestedFallbackBodyDoesNotReplaceRealBody(t *testing.T) {
	const stanza = `<message from="me@example.com"><result xmlns="urn:xmpp:mam:2" id="A1" queryid="Q1">` +
		`<forwarded xmlns="urn:xmpp:forward:0"><message to="me@example.com" id="c1" type="chat" from="bob@cheogram.com">` +
		`<body>hello there</body>` +
		`<fallback for="urn:xmpp:message-retract:1" xmlns="urn:xmpp:fallback:0"><body>A previous message was retracted</body></fallback>` +
		`</message></forwarded></result></message>`

	item, ok, next, err := parseOneMAMResult(t, stanza)
	if err != nil {
		t.Fatalf("handleMessageWithOOB: %v", err)
	}
	if !ok {
		t.Fatal("no item delivered")
	}
	if item.Body != "hello there" {
		t.Errorf("body = %q, want %q (fallback text must not replace the message body)", item.Body, "hello there")
	}
	if end, isEnd := next.(xml.EndElement); !isEnd || end.Name.Local != "message" {
		t.Errorf("handler must stop at </result>, got %#v", next)
	}
}

func TestMAMResultManyFallbackMarkers(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<message from="me@example.com"><result xmlns="urn:xmpp:mam:2" id="A1" queryid="Q1">` +
		`<forwarded xmlns="urn:xmpp:forward:0"><message to="me@example.com" id="c1" type="chat" from="bob@cheogram.com">` +
		`<body>one body</body>`)
	for i := 0; i < 5; i++ {
		b.WriteString(`<fallback for="http://jabber.org/protocol/address" xmlns="urn:xmpp:fallback:0"><body end="33" start="0"/></fallback>`)
	}
	b.WriteString(`</message></forwarded></result></message>`)

	item, ok, next, err := parseOneMAMResult(t, b.String())
	if err != nil {
		t.Fatalf("handleMessageWithOOB: %v", err)
	}
	if !ok {
		t.Fatal("no item delivered")
	}
	if item.Body != "one body" {
		t.Errorf("body = %q, want %q", item.Body, "one body")
	}
	if end, isEnd := next.(xml.EndElement); !isEnd || end.Name.Local != "message" {
		t.Errorf("handler must stop at </result>, got %#v", next)
	}
}

// A truncated <result> must surface as a real error, never as a bare io.EOF: Serve maps a
// handler's io.EOF to a clean stream close and tears the session down silently.
func TestMAMResultTruncatedStanzaIsNotBareEOF(t *testing.T) {
	const stanza = `<message from="me@example.com"><result xmlns="urn:xmpp:mam:2" id="A1" queryid="Q1">` +
		`<forwarded xmlns="urn:xmpp:forward:0"><message to="me@example.com" id="c1" type="chat" from="bob@cheogram.com">` +
		`<body>text`
	x := newOuterFromTestClient()
	x.mam.register("Q1", networkid.PortalKey{ID: "bob@cheogram.com", Receiver: x.loginID}, true)
	r, start := startOfMessage(t, stanza)
	err := x.handleMessageWithOOB(r, &start)
	if err == nil {
		t.Fatal("expected an error for a truncated result")
	}
	if errors.Is(err, io.EOF) {
		t.Fatalf("bare io.EOF leaked to Serve (it would silently close the stream): %v", err)
	}
}

// deliverMessage and closeAndRemove used to race: the send ran outside the mutex while the close
// deleted and closed the channel under it (send-on-closed-channel panic in the Serve goroutine).
func TestMamStateSendCloseRace(t *testing.T) {
	for i := 0; i < 500; i++ {
		var m mamState
		ch := m.register("q", networkid.PortalKey{}, true)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			m.deliverMessage("q", mamResultItem{})
		}()
		go func() {
			defer wg.Done()
			m.closeAndRemove("q")
		}()
		wg.Wait()
		if m.deliverMessage("q", mamResultItem{}) {
			t.Fatal("deliverMessage must report failure after closeAndRemove")
		}
		for {
			select {
			case _, ok := <-ch:
				if !ok { // closed by closeAndRemove: receiving would spin forever
					goto drained
				}
			default:
				goto drained
			}
		}
	drained:
	}
}

// Archived MMS carry XEP-0066 OOB media; without parsing it, backfilled media only appears as a
// bare link in the body text instead of a Matrix image attachment.
func TestMAMResultOOBMediaParsed(t *testing.T) {
	const stanza = `<message from="me@example.com"><result xmlns="urn:xmpp:mam:2" id="A1" queryid="Q1">` +
		`<forwarded xmlns="urn:xmpp:forward:0"><message to="me@example.com" id="c1" type="chat" from="bob@cheogram.com">` +
		`<body>caption text https://cache.cheogram.com/pic.jpg</body>` +
		`<x xmlns="jabber:x:oob"><url>https://cache.cheogram.com/pic.jpg</url></x>` +
		`<fallback for="http://jabber.org/protocol/address" xmlns="urn:xmpp:fallback:0"><body end="33" start="0"/></fallback>` +
		`</message></forwarded></result></message>`

	item, ok, next, err := parseOneMAMResult(t, stanza)
	if err != nil {
		t.Fatalf("handleMessageWithOOB: %v", err)
	}
	if !ok {
		t.Fatal("no item delivered")
	}
	if item.OOBURL != "https://cache.cheogram.com/pic.jpg" {
		t.Errorf("OOBURL = %q, want the OOB url", item.OOBURL)
	}
	if item.Body != "caption text https://cache.cheogram.com/pic.jpg" {
		t.Errorf("body = %q", item.Body)
	}
	if end, isEnd := next.(xml.EndElement); !isEnd || end.Name.Local != "message" {
		t.Errorf("handler must stop at </result>, got %#v", next)
	}
}

// Older archives may only carry a media <reference uri=...>; use it when there is no OOB element.
func TestMAMResultMediaReferenceFallback(t *testing.T) {
	const stanza = `<message from="me@example.com"><result xmlns="urn:xmpp:mam:2" id="A1" queryid="Q1">` +
		`<forwarded xmlns="urn:xmpp:forward:0"><message to="me@example.com" id="c1" type="chat" from="bob@cheogram.com">` +
		`<body>https://cache.cheogram.com/vid.mp4</body>` +
		`<reference xmlns="urn:xmpp:reference:0" uri="https://cache.cheogram.com/vid.mp4"><sources><media-sharing xmlns="urn:xmpp:media-sharing:0"><file><name>vid.mp4</name></file></media-sharing></sources></reference>` +
		`</message></forwarded></result></message>`

	item, ok, _, err := parseOneMAMResult(t, stanza)
	if err != nil {
		t.Fatalf("handleMessageWithOOB: %v", err)
	}
	if !ok {
		t.Fatal("no item delivered")
	}
	if item.OOBURL != "https://cache.cheogram.com/vid.mp4" {
		t.Errorf("OOBURL = %q, want the reference uri", item.OOBURL)
	}
}

// A message with no media must not grow an OOB URL out of unrelated elements.
func TestMAMResultNoOOB(t *testing.T) {
	const stanza = `<message from="me@example.com"><result xmlns="urn:xmpp:mam:2" id="A1" queryid="Q1">` +
		`<forwarded xmlns="urn:xmpp:forward:0"><message to="me@example.com" id="c1" type="chat" from="bob@cheogram.com">` +
		`<body>plain text</body></message></forwarded></result></message>`

	item, ok, _, err := parseOneMAMResult(t, stanza)
	if err != nil {
		t.Fatalf("handleMessageWithOOB: %v", err)
	}
	if !ok {
		t.Fatal("no item delivered")
	}
	if item.OOBURL != "" {
		t.Errorf("OOBURL = %q, want empty", item.OOBURL)
	}
}

// Media-only MMS (empty body, OOB URL only) must still be backfillable.
func TestMamItemBackfillableMediaOnly(t *testing.T) {
	if !mamItemBackfillable(mamResultItem{OOBURL: "https://cache.cheogram.com/x.jpg"}, nil) {
		t.Error("media-only item must be backfillable")
	}
	if mamItemBackfillable(mamResultItem{}, nil) {
		t.Error("body-less, media-less item must not be backfillable")
	}
}
