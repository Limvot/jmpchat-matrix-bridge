package connector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/database"
)

const healRoom = "discuss@conference.example.com"

func newHealClient() (*xmppClient, func() int) {
	x := &xmppClient{loginID: "alice@example.com", log: zerolog.Nop()}
	var mu sync.Mutex
	n := 0
	x.mucSendJoinHook = func(room, nick string) error {
		mu.Lock()
		n++
		mu.Unlock()
		return nil
	}
	x.mucRejoinPortalsHook = func(context.Context) ([]*database.Portal, error) { return nil, nil }
	return x, func() int { mu.Lock(); defer mu.Unlock(); return n }
}

func waitCount(t *testing.T, count func() int, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for count() != want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := count(); got != want {
		t.Fatalf("join presences = %d, want %d", got, want)
	}
}

func TestJoinMUCForPortalAlreadyJoinedSendsNothing(t *testing.T) {
	x, count := newHealClient()
	x.mucOurOccupantJID = map[string]string{healRoom: healRoom + "/alice"}
	if err := x.joinMUCForPortal(context.Background(), healRoom); err != nil {
		t.Fatal(err)
	}
	if count() != 0 {
		t.Fatalf("sent %d join presences for a joined room", count())
	}
}

func TestJoinMUCForPortalNotJoinedJoins(t *testing.T) {
	x, count := newHealClient()
	if err := x.joinMUCForPortal(context.Background(), healRoom); err != nil {
		t.Fatal(err)
	}
	if count() != 1 {
		t.Fatalf("sent %d join presences, want 1", count())
	}
}

func errStanza(from, cond string) string {
	return `<message xmlns="jabber:client" type="error" id="m1" from="` + from + `" to="alice@example.com/r"><error type="cancel"><` + cond + ` xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></message>`
}

func TestNotAcceptableFromRoomRejoinsOncePerWindow(t *testing.T) {
	x, count := newHealClient()
	x.mucOurOccupantJID = map[string]string{healRoom: healRoom + "/alice"}
	serveStanza(t, x, errStanza(healRoom, "not-acceptable"))
	waitCount(t, count, 1)
	if x.getMUCOurOccupantJID(healRoom) != "" {
		t.Error("occupant JID not cleared")
	}
	serveStanza(t, x, errStanza(healRoom, "not-acceptable"))
	serveStanza(t, x, errStanza(healRoom, "item-not-found"))
	waitCount(t, count, 1)
	// After the window a new heal is allowed.
	x.mucHealMu.Lock()
	x.mucHealAt[healRoom] = time.Now().Add(-mucHealInterval - time.Second)
	x.mucHealMu.Unlock()
	serveStanza(t, x, errStanza(healRoom, "recipient-unavailable"))
	waitCount(t, count, 2)
}

func TestOccupantForwardedErrorsDoNotRejoin(t *testing.T) {
	x, count := newHealClient()
	x.mucOurOccupantJID = map[string]string{healRoom: healRoom + "/alice"}
	serveStanza(t, x, errStanza(healRoom+"/mallory", "not-acceptable"))
	serveStanza(t, x, errStanza(healRoom, "forbidden"))
	waitCount(t, count, 0)
	if x.getMUCOurOccupantJID(healRoom) == "" {
		t.Error("occupant JID cleared by a non-qualifying error")
	}
}
