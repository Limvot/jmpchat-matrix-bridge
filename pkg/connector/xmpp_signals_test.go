package connector

import (
	"context"
	"encoding/xml"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	sigBob  = networkid.PortalID("bob@example.com")
	sigRoom = "room@muc.example.com"
)

func sigKey(id string) networkid.PortalKey {
	return networkid.PortalKey{ID: networkid.PortalID(id), Receiver: "me@example.com"}
}

func typingEvents(got []bridgev2.RemoteEvent) (out []*simplevent.Typing) {
	for _, e := range got {
		if t, ok := e.(*simplevent.Typing); ok {
			out = append(out, t)
		}
	}
	return
}

func receiptEvents(got []bridgev2.RemoteEvent) (out []*simplevent.Receipt) {
	for _, e := range got {
		if t, ok := e.(*simplevent.Receipt); ok {
			out = append(out, t)
		}
	}
	return
}

func TestInboundComposingQueuesTyping(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="s1"><composing xmlns="http://jabber.org/protocol/chatstates"/></message>`)
	ev := typingEvents(*got)
	if len(ev) != 1 || len(*got) != 1 {
		t.Fatalf("events = %v", *got)
	}
	if ev[0].Timeout != inboundTypingTimeout || ev[0].PortalKey != sigKey("bob@example.com") || ev[0].CreatePortal || ev[0].GetType() != bridgev2.RemoteEventTyping {
		t.Errorf("typing = %+v", ev[0])
	}
	if ev[0].Sender.Sender != "bob@example.com" || ev[0].Sender.IsFromMe {
		t.Errorf("sender = %+v", ev[0].Sender)
	}
	if !x.sig.isPeer("Bob@example.com") {
		t.Error("peer not recorded")
	}
}

func TestInboundPausedStopsTyping(t *testing.T) {
	for _, st := range []string{"paused", "active", "inactive", "gone"} {
		x, got := hookClient()
		runHandler(t, x, `<message type="chat" from="bob@example.com/p"><`+st+` xmlns="http://jabber.org/protocol/chatstates"/></message>`)
		ev := typingEvents(*got)
		if len(ev) != 1 || ev[0].Timeout != 0 {
			t.Errorf("%s: events = %+v", st, ev)
		}
	}
}

func TestChatStateMarksPeer(t *testing.T) {
	x, got := hookClient()
	x.conn = &JabberConnector{}
	// A state on a message with a body records the peer but queues only the message, no typing.
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="m1"><body>hi</body><active xmlns="http://jabber.org/protocol/chatstates"/></message>`)
	if !x.sig.isPeer("bob@example.com") {
		t.Error("peer not recorded from a message with <active/>")
	}
	if len(typingEvents(*got)) != 0 {
		t.Error("typing queued for a message with a body")
	}
	if x.sig.isPeer("carol@example.com") {
		t.Error("unknown peer reported")
	}
}

func TestChatStateCarbonAndMUCIgnored(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob"><composing xmlns="http://jabber.org/protocol/chatstates"/></message>`)
	runHandler(t, x, `<message from="me@example.com"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat"><composing xmlns="http://jabber.org/protocol/chatstates"/></message></forwarded></received></message>`)
	runHandler(t, x, `<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/l" to="bob@example.com" type="chat"><composing xmlns="http://jabber.org/protocol/chatstates"/></message></forwarded></sent></message>`)
	if len(*got) != 0 || x.sig.isPeer("bob@example.com") {
		t.Fatalf("events = %v peer=%v", *got, x.sig.isPeer("bob@example.com"))
	}
}

func TestInboundDisplayedDMQueuesReadReceipt(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="d1"><displayed xmlns="urn:xmpp:chat-markers:0" id="mine1"/></message>`)
	ev := receiptEvents(*got)
	if len(ev) != 1 || len(*got) != 1 {
		t.Fatalf("events = %v", *got)
	}
	if ev[0].GetType() != bridgev2.RemoteEventReadReceipt || ev[0].PortalKey != sigKey("bob@example.com") || ev[0].CreatePortal || ev[0].Sender.IsFromMe {
		t.Errorf("receipt = %+v", ev[0])
	}
}

func TestCarbonSentDisplayedIsFromMe(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/l" to="Bob@example.com/p" type="chat"><displayed xmlns="urn:xmpp:chat-markers:0" id="theirs1"/></message></forwarded></sent></message>`)
	ev := receiptEvents(*got)
	if len(ev) != 1 || !ev[0].Sender.IsFromMe || ev[0].PortalKey != sigKey("bob@example.com") || ev[0].GetType() != bridgev2.RemoteEventReadReceipt {
		t.Fatalf("events = %v", *got)
	}
	// A carbon of a marker the contact sent to another client of ours is the contact's receipt.
	x, got = hookClient()
	runHandler(t, x, `<message from="me@example.com"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="Bob@example.com/p" to="me@example.com" type="chat"><displayed xmlns="urn:xmpp:chat-markers:0" id="mine1"/></message></forwarded></received></message>`)
	ev = receiptEvents(*got)
	if len(ev) != 1 || ev[0].Sender.IsFromMe || ev[0].Sender.Sender != "bob@example.com" {
		t.Fatalf("events = %v", *got)
	}
}

func TestMUCDisplayedOwnOccupantIsFromMe(t *testing.T) {
	x, got := hookClient()
	x.mucOurOccupantJID = map[string]string{sigRoom: sigRoom + "/me"}
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="x1"><displayed xmlns="urn:xmpp:chat-markers:0" id="ROOMSID"/></message>`)
	ev := receiptEvents(*got)
	if len(ev) != 1 || !ev[0].Sender.IsFromMe || ev[0].PortalKey != sigKey(sigRoom) {
		t.Fatalf("events = %v", *got)
	}
}

func TestMUCDisplayedUnknownOccupantIgnored(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="x1"><displayed xmlns="urn:xmpp:chat-markers:0" id="ROOMSID"/></message>`)
	if len(*got) != 0 {
		t.Fatalf("a marker from an occupant without a ghost was queued: %v", *got)
	}
	x.mucAv.noteGhost("room@muc.example.com/bob")
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="x2"><displayed xmlns="urn:xmpp:chat-markers:0" id="ROOMSID"/></message>`)
	ev := receiptEvents(*got)
	if len(ev) != 1 || ev[0].Sender.Sender != "room@muc.example.com/bob" {
		t.Fatalf("events = %v", *got)
	}
}

func TestMUCDisplayedEchoDropped(t *testing.T) {
	fastMUCMarkers(t)
	c := &sigCapture{}
	x := sigClient(c)
	got := captureQueue(x)
	x.mucOurOccupantJID = map[string]string{sigRoom: sigRoom + "/me"}
	muc := &bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey(sigRoom), RoomType: database.RoomTypeGroupDM}}
	if err := x.HandleMatrixReadReceipt(context.Background(), readMsg(muc, mkRow("m1", "room@muc.example.com/bob", 5, true, "ROOMSID"))); err != nil {
		t.Fatal(err)
	}
	id := waitSent(t, c, 1)[0].ID
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="`+id+`"><displayed xmlns="urn:xmpp:chat-markers:0" id="ROOMSID"/><origin-id xmlns="urn:xmpp:sid:0" id="`+id+`"/></message>`)
	if len(*got) != 0 {
		t.Fatalf("our own marker's echo was bridged: %v", *got)
	}
}

func TestInboundDeliveryReceiptDM(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="r1"><received xmlns="urn:xmpp:receipts" id="mine1"/></message>`)
	ev := receiptEvents(*got)
	if len(ev) != 1 || ev[0].GetType() != bridgev2.RemoteEventDeliveryReceipt || ev[0].Sender.Sender != "bob@example.com" {
		t.Fatalf("events = %v", *got)
	}
}

func TestDeliveryReceiptMUCIgnored(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="r1"><received xmlns="urn:xmpp:receipts" id="mine1"/></message>`)
	if len(*got) != 0 {
		t.Fatalf("events = %v", *got)
	}
}

func TestGroupMMSSignalsIgnored(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message type="chat" from="+15551230000,+15551230001@cheogram.com/p" id="r1"><composing xmlns="http://jabber.org/protocol/chatstates"/><received xmlns="urn:xmpp:receipts" id="mine1"/></message>`)
	if len(*got) != 0 {
		t.Fatalf("events = %v", *got)
	}
}

// ---- receipt pre-handler ----

type fakeReceiptEnv struct {
	rows  map[string][]*database.Message
	err   error
	noOwn bool // the user has no double puppet
}

func (f *fakeReceiptEnv) canMarkOwnReads(context.Context) bool { return !f.noOwn }

func (f *fakeReceiptEnv) findByRef(_ context.Context, _ networkid.PortalKey, ref string, _ refKind) ([]*database.Message, error) {
	return f.rows[ref], f.err
}
func (f *fakeReceiptEnv) log(context.Context) *zerolog.Logger { l := zerolog.Nop(); return &l }

func runReceiptPre(t *testing.T, env receiptEnv, delivery, wantOwn bool, ref string) (*simplevent.Receipt, *bridgev2.EventHandlingResult) {
	t.Helper()
	evt := newMarkerReceipt(env, "me@example.com", sigKey("bob@example.com"), bridgev2.EventSender{Sender: "bob@example.com"}, ref, refReplyDM, delivery, wantOwn)
	return evt, evt.PreHandleWithResultFunc(context.Background(), nil)
}

func TestReceiptPreHandleResolvesMatrixRows(t *testing.T) {
	row := &database.Message{ID: "matrix:$evt", Room: sigKey("bob@example.com"), SenderID: "me@example.com", Metadata: &MessageMetadata{XMPPMessageID: "sent-1"}}
	env := &fakeReceiptEnv{rows: map[string][]*database.Message{"sent-1": {row}}}
	evt, res := runReceiptPre(t, env, false, true, "sent-1")
	if res != nil || evt.LastTarget != "matrix:$evt" {
		t.Fatalf("res=%v target=%q", res, evt.LastTarget)
	}
	evt, res = runReceiptPre(t, env, true, true, "sent-1")
	if res != nil || len(evt.Targets) != 1 || evt.Targets[0] != "matrix:$evt" {
		t.Fatalf("res=%v targets=%v", res, evt.Targets)
	}
}

func TestReceiptPreHandleRoomScoped(t *testing.T) {
	other := &database.Message{ID: "m", Room: sigKey("carol@example.com"), SenderID: "me@example.com"}
	env := &fakeReceiptEnv{rows: map[string][]*database.Message{"x": {other}}}
	if _, res := runReceiptPre(t, env, false, true, "x"); res == nil || *res != bridgev2.EventHandlingResultIgnored {
		t.Fatalf("res = %v", res)
	}
	if _, res := runReceiptPre(t, env, false, true, "unknown"); res == nil || *res != bridgev2.EventHandlingResultIgnored {
		t.Fatalf("unknown ref: res = %v", res)
	}
}

func TestReceiptPreHandleOwnershipRule(t *testing.T) {
	theirs := &database.Message{ID: "t1", Room: sigKey("bob@example.com"), SenderID: "bob@example.com"}
	mine := &database.Message{ID: "m1", Room: sigKey("bob@example.com"), SenderID: "me@example.com"}
	env := &fakeReceiptEnv{rows: map[string][]*database.Message{"t": {theirs}, "m": {mine}}}
	// A contact cannot mark its own message read for us, and we mark only others' messages.
	if _, res := runReceiptPre(t, env, false, true, "t"); res == nil || *res != bridgev2.EventHandlingResultIgnored {
		t.Errorf("contact marker on its own message: %v", res)
	}
	if _, res := runReceiptPre(t, env, false, false, "m"); res == nil || *res != bridgev2.EventHandlingResultIgnored {
		t.Errorf("own marker on our own message: %v", res)
	}
	if evt, res := runReceiptPre(t, env, false, false, "t"); res != nil || evt.LastTarget != "t1" {
		t.Errorf("own marker on their message: %v %q", res, evt.LastTarget)
	}
}

func TestReceiptPreHandleError(t *testing.T) {
	env := &fakeReceiptEnv{err: fmt.Errorf("db down")}
	if _, res := runReceiptPre(t, env, false, true, "x"); res == nil || *res == bridgev2.EventHandlingResultSuccess || res.Error == nil {
		t.Fatalf("res = %v", res)
	}
}

// ---- answering XEP-0184 requests ----

type sigCapture struct {
	mu   sync.Mutex
	sent []outSignal
}

func (c *sigCapture) snapshot() []outSignal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]outSignal(nil), c.sent...)
}

// waitSent polls until n signals were captured (debounced MUC markers come from a timer goroutine).
func waitSent(t *testing.T, c *sigCapture, n int) []outSignal {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := c.snapshot(); len(got) >= n {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d signals, got %+v", n, c.snapshot())
	return nil
}

// fastMUCMarkers shortens the MUC marker debounce for a test.
func fastMUCMarkers(t *testing.T) {
	t.Helper()
	old := mucMarkerDebounce
	mucMarkerDebounce = 20 * time.Millisecond
	t.Cleanup(func() { mucMarkerDebounce = old })
}

func sigClient(c *sigCapture) *xmppClient {
	x, _ := hookClient()
	x.conn = &JabberConnector{}
	x.signalHook = func(_ context.Context, s outSignal) error {
		c.mu.Lock()
		c.sent = append(c.sent, s)
		c.mu.Unlock()
		return nil
	}
	x.bridgedHook = func(networkid.PortalKey, networkid.MessageID) bool { return true }
	return x
}

func queuedMessage(t *testing.T, got []bridgev2.RemoteEvent) *jabberRemoteMessage {
	t.Helper()
	for _, e := range got {
		if m, ok := e.(*jabberRemoteMessage); ok {
			return m
		}
	}
	t.Fatalf("no message queued: %v", got)
	return nil
}

func TestReceiptRequestAnsweredAfterBridge(t *testing.T) {
	c := &sigCapture{}
	x := sigClient(c)
	got := captureQueue(x)
	runHandler(t, x, `<message type="chat" from="bob@example.com/phone" id="in1"><body>hi</body><request xmlns="urn:xmpp:receipts"/></message>`)
	m := queuedMessage(t, *got)
	if len(c.sent) != 0 {
		t.Fatal("receipt sent before the message was bridged")
	}
	m.PostHandle(context.Background(), nil)
	if len(c.sent) != 1 {
		t.Fatalf("sent = %+v", c.sent)
	}
	s := c.sent[0]
	if s.Kind != sigReceived || s.Value != "in1" || s.To.String() != "bob@example.com/phone" || s.Type != stanza.ChatMessage {
		t.Errorf("signal = %+v", s)
	}
}

func captureQueue(x *xmppClient) *[]bridgev2.RemoteEvent {
	var got []bridgev2.RemoteEvent
	x.queueHook = func(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
		got = append(got, evt)
		return bridgev2.EventHandlingResultSuccess
	}
	return &got
}

func TestNoReceiptAnswerCases(t *testing.T) {
	off := false
	for _, tc := range []struct {
		name   string
		stanza string
		cfg    *JabberConfig
	}{
		{"groupchat", `<message type="groupchat" from="room@muc.example.com/bob" id="in1"><body>hi</body><request xmlns="urn:xmpp:receipts"/></message>`, nil},
		{"carbon", `<message from="me@example.com"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat" id="in1"><body>hi</body><request xmlns="urn:xmpp:receipts"/></message></forwarded></received></message>`, nil},
		{"from me", `<message type="chat" from="me@example.com/other" id="in1"><body>hi</body><request xmlns="urn:xmpp:receipts"/></message>`, nil},
		{"config off", `<message type="chat" from="bob@example.com/p" id="in1"><body>hi</body><request xmlns="urn:xmpp:receipts"/></message>`, &JabberConfig{SendDeliveryReceipts: &off}},
		{"no id", `<message type="chat" from="bob@example.com/p"><body>hi</body><request xmlns="urn:xmpp:receipts"/></message>`, nil},
		{"no request", `<message type="chat" from="bob@example.com/p" id="in1"><body>hi</body></message>`, nil},
		{"error type", `<message type="error" from="bob@example.com/p" id="in1"><body>hi</body><request xmlns="urn:xmpp:receipts"/></message>`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &sigCapture{}
			x := sigClient(c)
			x.conn = &JabberConnector{networkConfig: tc.cfg}
			got := captureQueue(x)
			runHandler(t, x, tc.stanza)
			for _, e := range *got {
				if m, ok := e.(*jabberRemoteMessage); ok {
					m.PostHandle(context.Background(), nil)
				}
			}
			if len(c.sent) != 0 {
				t.Fatalf("receipt sent: %+v", c.sent)
			}
		})
	}
}

func TestNoReceiptAnswerNotBridged(t *testing.T) {
	c := &sigCapture{}
	x := sigClient(c)
	x.bridgedHook = func(networkid.PortalKey, networkid.MessageID) bool { return false }
	got := captureQueue(x)
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="in1"><body>hi</body><request xmlns="urn:xmpp:receipts"/></message>`)
	queuedMessage(t, *got).PostHandle(context.Background(), nil)
	if len(c.sent) != 0 {
		t.Fatalf("sent = %+v", c.sent)
	}
}

// ---- outbound ----

func TestHandleMatrixTypingCases(t *testing.T) {
	dm := &bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey("bob@example.com"), RoomType: database.RoomTypeDM}}
	run := func(x *xmppClient, p *bridgev2.Portal, typing bool) {
		if err := x.HandleMatrixTyping(context.Background(), &bridgev2.MatrixTyping{Portal: p, IsTyping: typing}); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("only known peers", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		run(x, dm, true)
		if len(c.sent) != 0 {
			t.Fatal("sent to a peer without chat state support")
		}
		x.sig.notePeer("bob@example.com")
		run(x, dm, true)
		if len(c.sent) != 1 || c.sent[0].Kind != sigChatState || c.sent[0].Value != "composing" || c.sent[0].Type != stanza.ChatMessage || c.sent[0].To.String() != "bob@example.com" {
			t.Fatalf("sent = %+v", c.sent)
		}
		run(x, dm, false)
		if len(c.sent) != 2 || c.sent[1].Value != "active" {
			t.Fatalf("sent = %+v", c.sent)
		}
	})
	t.Run("dedupe", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		x.sig.notePeer("bob@example.com")
		run(x, dm, true)
		run(x, dm, true)
		if len(c.sent) != 1 {
			t.Fatalf("sent = %+v", c.sent)
		}
	})
	t.Run("skips muc", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		x.sig.notePeer(sigRoom)
		run(x, &bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey(sigRoom), RoomType: database.RoomTypeGroupDM}}, true)
		if len(c.sent) != 0 {
			t.Fatal("typing sent to a MUC")
		}
	})
	t.Run("skips cheogram", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		id := "+15551230000@cheogram.com"
		x.sig.notePeer(id)
		run(x, &bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey(id), RoomType: database.RoomTypeDM}}, true)
		if len(c.sent) != 0 {
			t.Fatal("typing sent to a Cheogram number")
		}
	})
	t.Run("config off", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		off := false
		x.conn = &JabberConnector{networkConfig: &JabberConfig{SendTyping: &off}}
		x.sig.notePeer("bob@example.com")
		run(x, dm, true)
		if len(c.sent) != 0 {
			t.Fatal("typing sent with send_typing off")
		}
	})
}

func mkRow(id, sender string, ts int64, markable bool, stanzaID string) *database.Message {
	return &database.Message{ID: networkid.MessageID(id), SenderID: networkid.UserID(sender), Timestamp: time.Unix(ts, 0), Metadata: &MessageMetadata{Markable: markable, StanzaID: stanzaID}}
}

func TestPickDisplayedTarget(t *testing.T) {
	me := networkid.UserLoginID("me@example.com")
	exact := mkRow("e", "bob@example.com", 5, true, "")
	window := []*database.Message{
		mkRow("a", "bob@example.com", 1, true, ""),
		mkRow("own", "me@example.com", 9, true, ""),
		mkRow("nomark", "bob@example.com", 8, false, ""),
		mkRow("b", "bob@example.com", 3, true, ""),
	}
	if got := pickDisplayedTarget(exact, window, me); got != exact {
		t.Errorf("exact markable not preferred: %v", got)
	}
	if got := pickDisplayedTarget(nil, window, me); got == nil || got.ID != "b" {
		t.Errorf("newest markable of the window = %v", got)
	}
	if got := pickDisplayedTarget(mkRow("own2", "me@example.com", 2, true, ""), window, me); got == nil || got.ID != "b" {
		t.Errorf("own exact message must not be the target: %v", got)
	}
	if got := pickDisplayedTarget(nil, window[1:3], me); got != nil {
		t.Errorf("no markable foreign message: %v", got)
	}
}

func readMsg(p *bridgev2.Portal, exact *database.Message) *bridgev2.MatrixReadReceipt {
	return &bridgev2.MatrixReadReceipt{Portal: p, ExactMessage: exact, ReadUpTo: time.Unix(100, 0), LastRead: time.Unix(0, 0)}
}

func TestHandleMatrixReadReceipt(t *testing.T) {
	dm := &bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey("bob@example.com"), RoomType: database.RoomTypeDM}}
	muc := &bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey(sigRoom), RoomType: database.RoomTypeGroupDM}}

	t.Run("picks newest markable and dedupes", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		x.betweenHook = func(networkid.PortalKey, time.Time, time.Time) ([]*database.Message, error) {
			return []*database.Message{mkRow("a", "bob@example.com", 1, true, ""), mkRow("b", "bob@example.com", 3, true, ""), mkRow("own", "me@example.com", 4, true, "")}, nil
		}
		for i := 0; i < 2; i++ {
			if err := x.HandleMatrixReadReceipt(context.Background(), readMsg(dm, nil)); err != nil {
				t.Fatal(err)
			}
		}
		if len(c.sent) != 1 || c.sent[0].Kind != sigDisplayed || c.sent[0].Value != "b" || c.sent[0].Type != stanza.ChatMessage {
			t.Fatalf("sent = %+v", c.sent)
		}
	})
	t.Run("skips own and non markable", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		x.betweenHook = func(networkid.PortalKey, time.Time, time.Time) ([]*database.Message, error) {
			return []*database.Message{mkRow("own", "me@example.com", 4, true, ""), mkRow("nm", "bob@example.com", 5, false, "")}, nil
		}
		_ = x.HandleMatrixReadReceipt(context.Background(), readMsg(dm, mkRow("own", "me@example.com", 4, true, "")))
		_ = x.HandleMatrixReadReceipt(context.Background(), readMsg(dm, mkRow("nm", "bob@example.com", 5, false, "")))
		if len(c.sent) != 0 {
			t.Fatalf("sent = %+v", c.sent)
		}
	})
	t.Run("muc uses the stanza id and registers the echo", func(t *testing.T) {
		fastMUCMarkers(t)
		c := &sigCapture{}
		x := sigClient(c)
		x.mucOurOccupantJID = map[string]string{sigRoom: sigRoom + "/me"}
		if err := x.HandleMatrixReadReceipt(context.Background(), readMsg(muc, mkRow("m1", "room@muc.example.com/bob", 5, true, "ROOMSID"))); err != nil {
			t.Fatal(err)
		}
		if got := c.snapshot(); len(got) != 0 {
			t.Fatalf("marker sent before the quiet period: %+v", got)
		}
		sent := waitSent(t, c, 1)
		if len(sent) != 1 || sent[0].Value != "ROOMSID" || sent[0].Type != "groupchat" || sent[0].ID == "" {
			t.Fatalf("sent = %+v", sent)
		}
		c.sent = sent
		// The room's reflection of the marker is recognized as ours.
		res := x.mucEcho.matchEcho(sigRoom, []string{c.sent[0].ID}, "", "")
		if !res.matched {
			t.Error("marker id was not registered in the echo table")
		}
		// No room stanza-id: skipped (a legacy id would not be matched by receivers).
		c.sent = nil
		_ = x.HandleMatrixReadReceipt(context.Background(), readMsg(muc, mkRow("m2", "room@muc.example.com/bob", 6, true, "")))
		if len(c.sent) != 0 {
			t.Fatalf("sent without a stanza id: %+v", c.sent)
		}
	})
	t.Run("muc config off", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		off := false
		x.conn = &JabberConnector{networkConfig: &JabberConfig{MUCReadMarkers: &off}}
		x.mucOurOccupantJID = map[string]string{sigRoom: sigRoom + "/me"}
		_ = x.HandleMatrixReadReceipt(context.Background(), readMsg(muc, mkRow("m1", "room@muc.example.com/bob", 5, true, "ROOMSID")))
		if len(c.sent) != 0 {
			t.Fatalf("sent = %+v", c.sent)
		}
	})
	t.Run("muc not joined", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		_ = x.HandleMatrixReadReceipt(context.Background(), readMsg(muc, mkRow("m1", "room@muc.example.com/bob", 5, true, "ROOMSID")))
		if len(c.sent) != 0 {
			t.Fatalf("a read receipt must not trigger MUC traffic when not in the room: %+v", c.sent)
		}
	})
	t.Run("dm config off", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		off := false
		x.conn = &JabberConnector{networkConfig: &JabberConfig{SendReadMarkers: &off}}
		_ = x.HandleMatrixReadReceipt(context.Background(), readMsg(dm, mkRow("m1", "bob@example.com", 5, true, "")))
		if len(c.sent) != 0 {
			t.Fatalf("sent = %+v", c.sent)
		}
	})
	t.Run("cheogram", func(t *testing.T) {
		c := &sigCapture{}
		x := sigClient(c)
		id := "+15551230000@cheogram.com"
		p := &bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey(id), RoomType: database.RoomTypeDM}}
		_ = x.HandleMatrixReadReceipt(context.Background(), readMsg(p, mkRow("m1", id, 5, true, "")))
		if len(c.sent) != 0 {
			t.Fatalf("sent = %+v", c.sent)
		}
	})
}

// tokString renders the start elements of r as `<{namespace}local id="..">` (the xmlns attribute is
// skipped), which is what the tests compare against.
func tokString(t *testing.T, r xml.TokenReader) string {
	t.Helper()
	toks, err := xmlstream.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	for _, tok := range toks {
		if se, ok := tok.(xml.StartElement); ok {
			fmt.Fprintf(&sb, "<{%s}%s", se.Name.Space, se.Name.Local)
			for _, a := range se.Attr {
				if a.Name.Local != "xmlns" {
					fmt.Fprintf(&sb, " %s=%q", a.Name.Local, a.Value)
				}
			}
			sb.WriteString(">")
		}
	}
	return sb.String()
}

func payloadXML(t *testing.T, m outMessage) string {
	t.Helper()
	return tokString(t, outPayload(m, "body", "oid"))
}

func TestOutboundMessageCarriesMarkableRequestActive(t *testing.T) {
	x, _ := hookClient()
	to, _ := jid.Parse("bob@example.com")
	m := x.withSignalFlags(outMessage{To: to, Type: stanza.ChatMessage, Body: "hi"})
	if !m.Markable || !m.RequestReceipt || !m.Active {
		t.Fatalf("flags = %+v", m)
	}
	xml := payloadXML(t, m)
	for _, want := range []string{
		`<{http://jabber.org/protocol/chatstates}active>`,
		`<{urn:xmpp:chat-markers:0}markable>`,
		`<{urn:xmpp:receipts}request>`,
	} {
		if !strings.Contains(xml, want) {
			t.Errorf("payload lacks %s: %s", want, xml)
		}
	}
	if !(strings.Index(xml, "}body>") < strings.Index(xml, "}origin-id") && strings.Index(xml, "}active>") < strings.Index(xml, "}markable>") && strings.Index(xml, "}markable>") < strings.Index(xml, "}request>")) {
		t.Errorf("element order: %s", xml)
	}
	// Corrections (sent through sendOut) carry them in 1:1 too.
	var sent outMessage
	x.sendOutHook = func(_ context.Context, m outMessage) (string, error) { sent = m; return "id", nil }
	_, _ = x.sendOut(context.Background(), outMessage{To: to, Type: stanza.ChatMessage, Body: "x", Replace: "r"})
	if !sent.Markable || !sent.RequestReceipt {
		t.Errorf("correction flags = %+v", sent)
	}
}

func TestOutboundGroupchatNoRequest(t *testing.T) {
	x, _ := hookClient()
	to, _ := jid.Parse(sigRoom)
	m := x.withSignalFlags(outMessage{To: to, Type: stanza.MessageType("groupchat"), Body: "hi"})
	if !m.Markable || m.RequestReceipt || m.Active {
		t.Fatalf("flags = %+v", m)
	}
}

func TestOutboundCheogramNoActiveButRequest(t *testing.T) {
	x, _ := hookClient()
	to, _ := jid.Parse("+15551230000@cheogram.com")
	m := x.withSignalFlags(outMessage{To: to, Type: stanza.ChatMessage, Body: "hi"})
	if m.Active || !m.RequestReceipt || !m.Markable {
		t.Fatalf("flags = %+v", m)
	}
	off := false
	x.conn = &JabberConnector{networkConfig: &JabberConfig{SendTyping: &off}}
	bob, _ := jid.Parse("bob@example.com")
	if m := x.withSignalFlags(outMessage{To: bob, Type: stanza.ChatMessage}); m.Active {
		t.Error("<active/> sent with send_typing off")
	}
}

func TestSignalPayloads(t *testing.T) {
	to, _ := jid.Parse("bob@example.com")
	for _, tc := range []struct {
		s    outSignal
		want []string
		not  string
	}{
		{outSignal{To: to, Kind: sigChatState, Value: "composing"}, []string{`<{http://jabber.org/protocol/chatstates}composing>`, `<{urn:xmpp:hints}no-store>`}, ""},
		{outSignal{To: to, Kind: sigDisplayed, Value: "R1"}, []string{`<{urn:xmpp:chat-markers:0}displayed id="R1">`, `<{urn:xmpp:hints}store>`}, ""},
		{outSignal{To: to, Kind: sigReceived, Value: "R2"}, []string{`<{urn:xmpp:receipts}received id="R2">`}, "hints"},
	} {
		got := tokString(t, signalPayload(tc.s))
		for _, w := range tc.want {
			if !strings.Contains(got, w) {
				t.Errorf("payload lacks %s: %s", w, got)
			}
		}
		if tc.not != "" && strings.Contains(got, tc.not) {
			t.Errorf("payload has %s: %s", tc.not, got)
		}
	}
}

// ---- config and caps ----

func TestSignalPrefsDefaults(t *testing.T) {
	want := signalPrefs{SendTyping: true, SendReadMarkers: true, SendDeliveryReceipts: true, MUCReadMarkers: true}
	if got := (*JabberConnector)(nil).signalPrefs(); got != want {
		t.Errorf("nil connector = %+v", got)
	}
	if got := (&JabberConnector{networkConfig: &JabberConfig{}}).signalPrefs(); got != want {
		t.Errorf("empty config = %+v", got)
	}
	off := false
	got := (&JabberConnector{networkConfig: &JabberConfig{SendTyping: &off, MUCReadMarkers: &off}}).signalPrefs()
	if got.SendTyping || got.MUCReadMarkers || !got.SendReadMarkers || !got.SendDeliveryReceipts {
		t.Errorf("partial config = %+v", got)
	}
	// The example config enables all four.
	example, _, _ := (&JabberConnector{}).GetConfig()
	var cfg JabberConfig
	if err := yaml.Unmarshal([]byte(example), &cfg); err != nil {
		t.Fatal(err)
	}
	if p := (&JabberConnector{networkConfig: &cfg}).signalPrefs(); p != want || cfg.MUCReadMarkers == nil {
		t.Errorf("example config = %+v", p)
	}
}

func TestConfigUpgradeCopiesSignalKeys(t *testing.T) {
	c := &JabberConnector{}
	example, _, upgrader := c.GetConfig()
	user := "send_typing: false\nsend_read_markers: false\nsend_delivery_receipts: false\nmuc_read_markers: false\n"
	out, err := configupgrade.DoBytes([]byte(user), &configupgrade.StructUpgrader{
		SimpleUpgrader: func(h configupgrade.Helper) { upgrader.DoUpgrade(h) },
		Base:           example,
	})
	if err != nil {
		t.Fatal(err)
	}
	var cfg JabberConfig
	if err = yaml.Unmarshal(out, &cfg); err != nil {
		t.Fatal(err)
	}
	if p := (&JabberConnector{networkConfig: &cfg}).signalPrefs(); p != (signalPrefs{}) {
		t.Fatalf("user values lost: %+v\n%s", p, out)
	}
	// An absent key takes the (enabled) example value.
	out, err = configupgrade.DoBytes([]byte("cheogram: true\n"), &configupgrade.StructUpgrader{
		SimpleUpgrader: func(h configupgrade.Helper) { upgrader.DoUpgrade(h) },
		Base:           example,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg = JabberConfig{}
	if err = yaml.Unmarshal(out, &cfg); err != nil {
		t.Fatal(err)
	}
	if p := (&JabberConnector{networkConfig: &cfg}).signalPrefs(); !p.SendTyping || !p.MUCReadMarkers {
		t.Fatalf("defaults lost: %+v\n%s", p, out)
	}
}

func TestSignalCapabilities(t *testing.T) {
	if !jabberCaps.ReadReceipts || !jabberCaps.TypingNotifications {
		t.Errorf("caps = %+v", jabberCaps)
	}
}
