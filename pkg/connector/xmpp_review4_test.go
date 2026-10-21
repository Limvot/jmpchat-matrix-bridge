package connector

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const delayEl = `<delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z" from="room@muc.example.com"/>`

// ---- 1: delayed (history) signals ----

func TestDelayedBodylessSignalsDropped(t *testing.T) {
	// MUC history replay of a marker from a known occupant.
	x, got := hookClient()
	x.mucAv.noteGhost("room@muc.example.com/bob")
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="x1"><displayed xmlns="urn:xmpp:chat-markers:0" id="ROOMSID"/>`+delayEl+`</message>`)
	if len(*got) != 0 {
		t.Fatalf("delayed MUC marker bridged: %v", *got)
	}
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="x2"><displayed xmlns="urn:xmpp:chat-markers:0" id="ROOMSID"/></message>`)
	if len(receiptEvents(*got)) != 1 {
		t.Fatalf("live MUC marker must still be bridged: %v", *got)
	}

	// Delayed DM chat state / marker / receipt, live and as a carbon.
	x, got = hookClient()
	for _, s := range []string{
		`<message type="chat" from="bob@example.com/p"><composing xmlns="http://jabber.org/protocol/chatstates"/>` + delayEl + `</message>`,
		`<message type="chat" from="bob@example.com/p"><displayed xmlns="urn:xmpp:chat-markers:0" id="m1"/>` + delayEl + `</message>`,
		`<message type="chat" from="bob@example.com/p"><received xmlns="urn:xmpp:receipts" id="m1"/>` + delayEl + `</message>`,
		`<message from="me@example.com"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat"><displayed xmlns="urn:xmpp:chat-markers:0" id="m1"/>` + delayEl + `</message></forwarded></received></message>`,
		`<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/l" to="bob@example.com" type="chat"><displayed xmlns="urn:xmpp:chat-markers:0" id="m1"/>` + delayEl + `</message></forwarded></sent></message>`,
	} {
		runHandler(t, x, s)
	}
	if len(*got) != 0 || x.sig.isPeer("bob@example.com") {
		t.Fatalf("delayed DM signal bridged: %v", *got)
	}
}

// ---- 2: MUC marker debounce ----

func TestMUCMarkerDebounceSendsOnlyNewest(t *testing.T) {
	fastMUCMarkers(t)
	c := &sigCapture{}
	x := sigClient(c)
	x.mucOurOccupantJID = map[string]string{sigRoom: sigRoom + "/me"}
	muc := &bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey(sigRoom), RoomType: database.RoomTypeGroupDM}}
	for i, ref := range []string{"S1", "S2", "S3"} {
		if err := x.HandleMatrixReadReceipt(context.Background(), readMsg(muc, mkRow("m"+ref, "room@muc.example.com/bob", int64(i+1), true, ref))); err != nil {
			t.Fatal(err)
		}
	}
	sent := waitSent(t, c, 1)
	time.Sleep(80 * time.Millisecond)
	sent = c.snapshot()
	if len(sent) != 1 || sent[0].Value != "S3" {
		t.Fatalf("sent = %+v, want only the newest ref", sent)
	}
	x.sig.mu.Lock()
	pending := len(x.sig.mucPending)
	x.sig.mu.Unlock()
	if pending != 0 {
		t.Errorf("%d timers left", pending)
	}
}

func TestMUCMarkerTimerStoppedOnReset(t *testing.T) {
	old := mucMarkerDebounce
	mucMarkerDebounce = 50 * time.Millisecond
	defer func() { mucMarkerDebounce = old }()
	c := &sigCapture{}
	x := sigClient(c)
	x.mucOurOccupantJID = map[string]string{sigRoom: sigRoom + "/me"}
	muc := &bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey(sigRoom), RoomType: database.RoomTypeGroupDM}}
	_ = x.HandleMatrixReadReceipt(context.Background(), readMsg(muc, mkRow("m1", "room@muc.example.com/bob", 1, true, "S1")))
	x.sig.reset()
	time.Sleep(150 * time.Millisecond)
	if got := c.snapshot(); len(got) != 0 {
		t.Fatalf("a reset must cancel the pending marker: %+v", got)
	}
}

func TestDisplayedMarkerStoreHint(t *testing.T) {
	to, _ := jid.Parse(sigRoom)
	muc := tokString(t, signalPayload(outSignal{To: to, Type: stanza.MessageType("groupchat"), Kind: sigDisplayed, Value: "S1"}))
	if strings.Contains(muc, "hints") {
		t.Errorf("groupchat marker must carry no hint: %s", muc)
	}
	dm := tokString(t, signalPayload(outSignal{To: to, Type: stanza.ChatMessage, Kind: sigDisplayed, Value: "S1"}))
	if !strings.Contains(dm, "store") {
		t.Errorf("DM marker keeps <store/>: %s", dm)
	}
}

// ---- 3: MUC private messages ----

func TestMUCPrivateMessageSignalsIgnored(t *testing.T) {
	x, got := hookClient()
	x.mucOurOccupantJID = map[string]string{sigRoom: sigRoom + "/me"}
	runHandler(t, x, `<message type="chat" from="room@muc.example.com/bob" id="p1"><composing xmlns="http://jabber.org/protocol/chatstates"/><displayed xmlns="urn:xmpp:chat-markers:0" id="m1"/><received xmlns="urn:xmpp:receipts" id="m2"/></message>`)
	runHandler(t, x, `<message from="me@example.com"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="room@muc.example.com/bob" to="me@example.com" type="chat"><displayed xmlns="urn:xmpp:chat-markers:0" id="m1"/></message></forwarded></received></message>`)
	runHandler(t, x, `<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/l" to="room@muc.example.com/bob" type="chat"><displayed xmlns="urn:xmpp:chat-markers:0" id="m1"/></message></forwarded></sent></message>`)
	if len(*got) != 0 || x.sig.isPeer(sigRoom) {
		t.Fatalf("MUC PM produced DM signals: %v", *got)
	}
	// A room we did not join is not special-cased.
	x, got = hookClient()
	runHandler(t, x, `<message type="chat" from="bob@example.com/p"><displayed xmlns="urn:xmpp:chat-markers:0" id="m1"/></message>`)
	if len(receiptEvents(*got)) != 1 {
		t.Fatalf("events = %v", *got)
	}
}

// ---- 4: own read receipts without double puppet ----

func TestOwnReadReceiptIgnoredWithoutDoublePuppet(t *testing.T) {
	row := &database.Message{ID: "t1", Room: sigKey("bob@example.com"), SenderID: "bob@example.com"}
	env := &fakeReceiptEnv{rows: map[string][]*database.Message{"t": {row}}, noOwn: true}
	me := bridgev2.EventSender{IsFromMe: true, Sender: "me@example.com"}
	run := func(env receiptEnv, sender bridgev2.EventSender) *bridgev2.EventHandlingResult {
		evt := newMarkerReceipt(env, "me@example.com", sigKey("bob@example.com"), sender, "t", refReplyDM, false, false)
		return evt.PreHandleWithResultFunc(context.Background(), nil)
	}
	if res := run(env, me); res == nil || *res != bridgev2.EventHandlingResultIgnored {
		t.Fatalf("res = %v, want ignored", res)
	}
	env.noOwn = false
	if res := run(env, me); res != nil {
		t.Fatalf("res = %v, want the receipt to proceed", res)
	}
	// The real environment consults the seam.
	x := &xmppClient{loginID: "me@example.com", log: zerolog.Nop()}
	x.doublePuppetHook = func() bool { return false }
	if (xmppRetractionEnv{x}).canMarkOwnReads(context.Background()) {
		t.Error("hook false must disable own receipts")
	}
}

// ---- 5: typing record after a send ----

func TestSendWithActiveForgetsTyping(t *testing.T) {
	x, _ := hookClient()
	bob, _ := jid.Parse("bob@example.com")
	x.sendOutHook = func(context.Context, outMessage) (string, error) { return "id", nil }
	now := time.Now()
	if !x.sig.shouldSendTyping("bob@example.com", "composing", now) {
		t.Fatal("first composing must send")
	}
	if x.sig.shouldSendTyping("bob@example.com", "composing", now) {
		t.Fatal("duplicate composing must be suppressed")
	}
	if _, err := x.sendOut(context.Background(), outMessage{To: bob, Type: stanza.ChatMessage, Body: "hi"}); err != nil {
		t.Fatal(err)
	}
	if !x.sig.shouldSendTyping("bob@example.com", "composing", now) {
		t.Error("the message carried <active/>: the next composing must be sent")
	}
}

// ---- 8: encrypted corrections ----

func TestEncryptedCorrectionIgnored(t *testing.T) {
	const enc = `<encrypted xmlns="eu.siacs.conversations.axolotl"/>`
	const rep = `<replace xmlns="urn:xmpp:message-correct:0" id="m1"/>`
	x, got := hookClient()
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="c1"><body>I sent you an OMEMO encrypted message</body>`+rep+enc+`</message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="c2"><body>fallback</body>`+rep+enc+`</message>`)
	runHandler(t, x, `<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/l" to="bob@example.com" type="chat" id="c3"><body>fallback</body>`+rep+enc+`</message></forwarded></sent></message>`)
	runHandler(t, x, `<message from="me@example.com"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat" id="c4"><body>fallback</body>`+rep+enc+`</message></forwarded></received></message>`)
	if len(*got) != 0 {
		t.Fatalf("encrypted correction produced events: %v", *got)
	}
	// MAM: consumed (never a message), no event, never folded.
	it := corrItem("c5", "bob@example.com/p", "m1", "fallback", 5)
	it.Extras.Encryption = "omemo"
	pk := networkid.PortalKey{ID: "bob@example.com", Receiver: retractTestLogin}
	if !x.queueMAMCorrection(context.Background(), pk, it) || len(*got) != 0 {
		t.Fatal("encrypted MAM correction must be consumed without an event")
	}
	orig := corrItem("m1", "bob@example.com/p", "", "original", 1)
	out, folded := foldBatchCorrectionsDetailed([]mamResultItem{orig, it})
	if len(folded) != 0 || len(out) != 1 || out[0].Body != "original" {
		t.Fatalf("encrypted correction folded: out=%+v folded=%+v", out, folded)
	}
	// A plain correction still applies.
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="c6"><body>fixed</body>`+rep+`</message>`)
	if len(*got) != 1 {
		t.Fatalf("plain correction events = %v", *got)
	}
}

// ---- 9: session start recorded by Connect ----

func TestReconnectLoopBacksOffAfterExternallyStartedShortSession(t *testing.T) {
	oldB, oldM := reconnectBackoffStart, minHealthySession
	reconnectBackoffStart, minHealthySession = 150*time.Millisecond, time.Hour
	defer func() { reconnectBackoffStart, minHealthySession = oldB, oldM }()
	x := &xmppClient{loginID: "me@example.com", log: zerolog.Nop()}
	// A session started by bridgev2's own Connect call: live, with the start recorded by Connect.
	x.session = &xmpp.Session{}
	x.sessionStartedAt.Store(time.Now().UnixNano())
	var connectedAt atomic.Int64
	x.connectHook = func(context.Context) { connectedAt.CompareAndSwap(0, time.Now().UnixNano()) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { x.ReconnectLoop(ctx); close(done) }()
	time.Sleep(20 * time.Millisecond)
	killed := time.Now()
	x.mu.Lock()
	x.session = nil
	x.mu.Unlock()
	x.wakeReconnect()
	time.Sleep(400 * time.Millisecond)
	cancel()
	<-done
	at := connectedAt.Load()
	if at == 0 {
		t.Fatal("the loop never reconnected")
	}
	if d := time.Unix(0, at).Sub(killed); d < 140*time.Millisecond {
		t.Errorf("reconnected %v after a short session, want the backoff first", d)
	}
}

// ---- 10: folded keys only for kept originals ----

func TestFoldedWithin(t *testing.T) {
	m1 := corrItem("m1", "bob@example.com/p", "", "first", 1)
	m2 := corrItem("m2", "bob@example.com/p", "", "second", 2)
	c1 := corrItem("c1", "bob@example.com/p", "m1", "first fixed", 3)
	c2 := corrItem("c2", "bob@example.com/p", "m2", "second fixed", 4)
	kept, folded := foldBatchCorrectionsDetailed([]mamResultItem{m1, m2, c1, c2})
	if len(kept) != 2 || len(folded) != 2 {
		t.Fatalf("kept=%d folded=%d", len(kept), len(folded))
	}
	within := foldedWithin(folded, kept[:1])
	if len(within) != 1 || within[0].corr.StanzaID != "c1" {
		t.Fatalf("within = %+v, want only the correction of the kept original", within)
	}
	if len(foldedWithin(folded, nil)) != 0 {
		t.Error("nothing kept, nothing recorded")
	}
}
