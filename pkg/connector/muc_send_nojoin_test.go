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

var (
	healNickMu   sync.Mutex
	healLastNick string
)

func newHealClient() (*xmppClient, func() int) {
	x := &xmppClient{loginID: "alice@example.com", log: zerolog.Nop()}
	var mu sync.Mutex
	n := 0
	x.mucSendJoinHook = func(room, nick string) error {
		mu.Lock()
		n++
		mu.Unlock()
		healNickMu.Lock()
		healLastNick = nick
		healNickMu.Unlock()
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
	x.mucOurOccupantJID = map[string]string{healRoom: healRoom + "/renamed"}
	serveStanza(t, x, errStanza(healRoom, "not-acceptable"))
	waitCount(t, count, 1)
	healNickMu.Lock()
	if healLastNick != "renamed" {
		t.Errorf("heal joined as %q, want the held nick", healLastNick)
	}
	healNickMu.Unlock()
	if x.getMUCOurOccupantJID(healRoom) != "" {
		t.Error("occupant JID not cleared")
	}
	// Joined again (confirmation not simulated by the hook): further errors inside the window do nothing.
	x.mucOurOccupantJIDMu.Lock()
	x.mucOurOccupantJID[healRoom] = healRoom + "/alice"
	x.mucOurOccupantJIDMu.Unlock()
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

func TestHealSkippedWhenNotJoined(t *testing.T) {
	x, count := newHealClient()
	serveStanza(t, x, errStanza(healRoom, "not-acceptable"))
	waitCount(t, count, 0)
}

const mucUserX = `<x xmlns="http://jabber.org/protocol/muc#user">`

func TestOwnExitClearsOccupantJID(t *testing.T) {
	ours := healRoom + "/alice"
	cases := map[string]string{
		"kick":    `<presence xmlns="jabber:client" type="unavailable" from="` + ours + `">` + mucUserX + `<item affiliation="none" role="none"/><status code="307"/><status code="110"/></x></presence>`,
		"ban":     `<presence xmlns="jabber:client" type="unavailable" from="` + ours + `">` + mucUserX + `<item affiliation="outcast" role="none"/><status code="301"/></x></presence>`,
		"destroy": `<presence xmlns="jabber:client" type="unavailable" from="` + ours + `">` + mucUserX + `<item affiliation="none" role="none"/><destroy/></x></presence>`,
		"error":   `<presence xmlns="jabber:client" type="error" from="` + ours + `"><error type="modify"><not-acceptable xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></presence>`,
	}
	for name, st := range cases {
		t.Run(name, func(t *testing.T) {
			x, count := newHealClient()
			x.mucOurOccupantJID = map[string]string{healRoom: ours}
			serveStanza(t, x, st)
			if x.getMUCOurOccupantJID(healRoom) != "" {
				t.Fatal("occupant JID kept after our exit")
			}
			if err := x.joinMUCForPortal(context.Background(), healRoom); err != nil {
				t.Fatal(err)
			}
			if count() != 1 {
				t.Fatalf("next send sent %d join presences, want 1", count())
			}
		})
	}
}

func TestOtherExitsKeepOccupantJID(t *testing.T) {
	ours := healRoom + "/alice"
	for name, st := range map[string]string{
		"other kicked": `<presence xmlns="jabber:client" type="unavailable" from="` + healRoom + `/bob">` + mucUserX + `<item role="none"/><status code="307"/></x></presence>`,
		"own rename":   `<presence xmlns="jabber:client" type="unavailable" from="` + ours + `">` + mucUserX + `<item nick="alice2"/><status code="303"/><status code="110"/></x></presence>`,
		"conflict":     `<presence xmlns="jabber:client" type="error" from="` + ours + `"><error type="cancel"><conflict xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></presence>`,
	} {
		t.Run(name, func(t *testing.T) {
			x, _ := newHealClient()
			x.mucOurOccupantJID = map[string]string{healRoom: ours}
			serveStanza(t, x, st)
			if x.getMUCOurOccupantJID(healRoom) == "" {
				t.Fatal("occupant JID cleared")
			}
		})
	}
}
