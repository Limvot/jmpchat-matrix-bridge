package connector

import (
	"context"
	"encoding/xml"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

const nickRoom = "discuss@conference.example.com"

type nickRig struct {
	x       *xmppClient
	mu      sync.Mutex
	sent    []string
	notices []string
	saved   [][2]string
	done    chan struct{}
}

func newNickRig() *nickRig {
	r := &nickRig{done: make(chan struct{}, 16)}
	r.x = &xmppClient{loginID: "alice@example.com", log: zerolog.Nop()}
	r.x.mucSendJoinHook = func(room, nick string) error {
		r.mu.Lock()
		r.sent = append(r.sent, nick)
		r.mu.Unlock()
		r.done <- struct{}{}
		return nil
	}
	r.x.mucNoticeHook = func(room, text string) {
		r.mu.Lock()
		r.notices = append(r.notices, text)
		r.mu.Unlock()
		r.done <- struct{}{}
	}
	r.x.mucNickSaveHook = func(room, base, nick string) error {
		r.mu.Lock()
		r.saved = append(r.saved, [2]string{base, nick})
		r.mu.Unlock()
		return nil
	}
	return r
}

func (r *nickRig) wait(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async work")
	}
}

func (r *nickRig) settle() { time.Sleep(50 * time.Millisecond) }

func presenceStanza(t *testing.T, x *xmppClient, typ, from, body string) {
	t.Helper()
	xmlStr := `<presence from="` + from + `"`
	if typ != "" {
		xmlStr += ` type="` + typ + `"`
	}
	xmlStr += `>` + body + `</presence>`
	d := xml.NewDecoder(strings.NewReader(xmlStr))
	tok, err := d.Token()
	if err != nil {
		t.Fatal(err)
	}
	start := tok.(xml.StartElement)
	if err := x.handlePresence(&decoderRW{Decoder: d, Encoder: xml.NewEncoder(&strings.Builder{})}, &start); err != nil {
		t.Fatal(err)
	}
}

const conflictBody = `<error type="cancel" code="409"><conflict xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error>`
const selfBody = `<x xmlns="http://jabber.org/protocol/muc#user"><status code="110"/></x>`

func (r *nickRig) conflict(t *testing.T, nick string) {
	t.Helper()
	presenceStanza(t, r.x, "error", nickRoom+"/"+nick, conflictBody)
}

func TestMUCNickRetrySequenceThenSuccessNotifiesOnce(t *testing.T) {
	r := newNickRig()
	ctx := context.Background()
	if err := r.x.joinMUC(ctx, nickRoom, nil); err != nil {
		t.Fatal(err)
	}
	r.wait(t) // not async: joinMUC sent synchronously (drain the signal)
	r.conflict(t, "alice")
	r.wait(t) // _1 sent
	r.conflict(t, "alice_1")
	r.wait(t) // _2 sent
	if got := strings.Join(r.sent, ","); got != "alice,alice_1,alice_2" {
		t.Fatalf("sent = %s", got)
	}
	r.settle()
	if len(r.notices) != 0 {
		t.Fatalf("notice before confirmation: %v", r.notices)
	}
	presenceStanza(t, r.x, "", nickRoom+"/alice_2", selfBody)
	r.wait(t)
	r.settle()
	if len(r.notices) != 1 {
		t.Fatalf("notices = %v", r.notices)
	}
	want := "`alice` is taken in " + nickRoom + ", joined as `alice_2`. Use `!jabber muc-nick " + nickRoom + " <nick>` to pick another."
	if r.notices[0] != want {
		t.Fatalf("notice = %q", r.notices[0])
	}
	if len(r.saved) != 1 || r.saved[0] != [2]string{"alice", "alice_2"} {
		t.Fatalf("saved = %v", r.saved)
	}
	// A repeated self presence must not notify again.
	presenceStanza(t, r.x, "", nickRoom+"/alice_2", selfBody)
	r.settle()
	if len(r.notices) != 1 {
		t.Fatalf("duplicate notice: %v", r.notices)
	}
}

func TestMUCNickAllTakenNotice(t *testing.T) {
	r := newNickRig()
	if err := r.x.joinMUC(context.Background(), nickRoom, nil); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	for _, n := range []string{"alice", "alice_1", "alice_2"} {
		r.conflict(t, n)
		r.wait(t)
	}
	r.conflict(t, "alice_3")
	r.wait(t) // notice
	r.settle()
	if got := strings.Join(r.sent, ","); got != "alice,alice_1,alice_2,alice_3" {
		t.Fatalf("sent = %s", got)
	}
	want := "Could not join " + nickRoom + ": nicks `alice`, `alice_1`, `alice_2`, `alice_3` are all taken. Use `!jabber muc-nick " +
		nickRoom + " <nick>` and `!jabber join " + nickRoom + "` to try again."
	if len(r.notices) != 1 || r.notices[0] != want {
		t.Fatalf("notices = %q", r.notices)
	}
	if len(r.saved) != 0 {
		t.Fatalf("saved on failure: %v", r.saved)
	}
	// Chain is over: a late 409 is ignored.
	r.conflict(t, "alice_3")
	r.settle()
	if len(r.sent) != 4 || len(r.notices) != 1 {
		t.Fatalf("stale 409 acted on: %v %v", r.sent, r.notices)
	}
}

func TestMUCNickNoNoticeWhenPreferredSucceeds(t *testing.T) {
	r := newNickRig()
	if err := r.x.joinMUC(context.Background(), nickRoom, nil); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	presenceStanza(t, r.x, "", nickRoom+"/alice", selfBody)
	r.settle()
	if len(r.notices) != 0 || len(r.saved) != 0 {
		t.Fatalf("unexpected notice/save: %v %v", r.notices, r.saved)
	}
}

func TestMUCNickStale409Ignored(t *testing.T) {
	r := newNickRig()
	if err := r.x.joinMUC(context.Background(), nickRoom, nil); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	r.conflict(t, "someoneelse")
	r.settle()
	if len(r.sent) != 1 {
		t.Fatalf("retried on unrelated 409: %v", r.sent)
	}
}

func TestChooseMUCNick(t *testing.T) {
	cases := []struct {
		name                          string
		explicit, def, auto, autoBase string
		wantBase, wantNick            string
	}{
		{"login localpart", "", "", "", "", "alice", "alice"},
		{"default", "", "dflt", "", "", "dflt", "dflt"},
		{"explicit beats default", "mine", "dflt", "", "", "mine", "mine"},
		{"remembered auto reused on rejoin", "", "", "alice_1", "alice", "alice", "alice_1"},
		{"auto for explicit base", "mine", "", "mine_2", "mine", "mine", "mine_2"},
		{"default changed: auto dropped", "", "new", "alice_1", "alice", "new", "new"},
		{"explicit set: auto dropped", "mine", "", "alice_1", "alice", "mine", "mine"},
	}
	for _, c := range cases {
		base, nick := chooseMUCNick(c.explicit, c.def, "alice@example.com", c.auto, c.autoBase)
		if base != c.wantBase || nick != c.wantNick {
			t.Errorf("%s: got (%s,%s) want (%s,%s)", c.name, base, nick, c.wantBase, c.wantNick)
		}
	}
}

func TestExplicitMUCNickResetsAuto(t *testing.T) {
	m := &PortalMetadata{MUCAutoNick: "alice_1", MUCAutoNickBase: "alice"}
	setExplicitMUCNick(m, "mine")
	if m.MUCPreferredNick != "mine" || m.MUCAutoNick != "" || m.MUCAutoNickBase != "" {
		t.Fatalf("%+v", m)
	}
}

func TestRememberedNickUsedOnRejoin(t *testing.T) {
	// Without a bridge the portal lookup is skipped, so exercise the join path through the hooks:
	// the base/nick pair that mucNickChoice would return comes from chooseMUCNick (tested above);
	// here verify joinMUC sends exactly that nick and tracks base separately.
	r := newNickRig()
	r.x.login = nil
	base, nick := chooseMUCNick("", "", string(r.x.loginID), "alice_1", "alice")
	r.x.startMUCJoinTrack(nickRoom, base, nick, nil)
	if err := r.x.sendMUCJoin(context.Background(), nickRoom, nick, nil); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	presenceStanza(t, r.x, "", nickRoom+"/alice_1", selfBody)
	r.settle()
	if r.sent[0] != "alice_1" || len(r.notices) != 0 {
		t.Fatalf("sent=%v notices=%v", r.sent, r.notices)
	}
	// If the remembered nick now conflicts, the next try is alice_2 (alice_1 counts as tried).
	r.x.startMUCJoinTrack(nickRoom, base, nick, nil)
	r.conflict(t, "alice_1")
	r.wait(t)
	if r.sent[len(r.sent)-1] != "alice_2" {
		t.Fatalf("sent=%v", r.sent)
	}
}

func TestMUCNickNoticeNamesFirstTriedNick(t *testing.T) {
	r := newNickRig()
	base, nick := chooseMUCNick("", "", string(r.x.loginID), "alice_1", "alice")
	r.x.startMUCJoinTrack(nickRoom, base, nick, nil)
	if err := r.x.sendMUCJoin(context.Background(), nickRoom, nick, nil); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	r.conflict(t, "alice_1")
	r.wait(t) // alice_2 sent
	presenceStanza(t, r.x, "", nickRoom+"/alice_2", selfBody)
	r.wait(t)
	r.settle()
	want := "`alice_1` is taken in " + nickRoom + ", joined as `alice_2`. Use `!jabber muc-nick " + nickRoom + " <nick>` to pick another."
	if len(r.notices) != 1 || r.notices[0] != want {
		t.Fatalf("notices = %q", r.notices)
	}
	if len(r.saved) != 1 || r.saved[0] != [2]string{"alice", "alice_2"} {
		t.Fatalf("saved = %v", r.saved)
	}
}

func TestMUCJoinStateResetStopsStaleRetry(t *testing.T) {
	r := newNickRig()
	if err := r.x.joinMUC(context.Background(), nickRoom, nil); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	next, tried, gen, handled := r.x.onMUCJoinConflict(nickRoom, "alice")
	if !handled || next != "alice_1" {
		t.Fatalf("next=%q handled=%v", next, handled)
	}
	// New session before the retry goroutine runs.
	r.x.resetMUCJoinState()
	if len(r.x.mucPendingJoin) != 0 || len(r.x.mucJoinTracks) != 0 {
		t.Fatal("reset must clear join state")
	}
	r.x.continueMUCJoinAfterConflict(nickRoom, next, tried, gen)
	r.settle()
	if len(r.sent) != 1 || len(r.notices) != 0 {
		t.Fatalf("stale chain acted: sent=%v notices=%v", r.sent, r.notices)
	}
	// A stale all-taken notice is dropped too.
	r.x.continueMUCJoinAfterConflict(nickRoom, "", []string{"alice"}, gen)
	r.settle()
	if len(r.notices) != 0 {
		t.Fatalf("stale notice: %v", r.notices)
	}
	// A chain in the new session works.
	if err := r.x.joinMUC(context.Background(), nickRoom, nil); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	r.conflict(t, "alice")
	r.wait(t)
	if r.sent[len(r.sent)-1] != "alice_1" {
		t.Fatalf("sent=%v", r.sent)
	}
}

// A 409 retry repeats the history request of the join that started the chain (auto-rejoin since=).
func TestMUCJoinRetryKeepsHistory(t *testing.T) {
	r := newNickRig()
	var hists []*mucHistory
	r.x.mucJoinHistHook = func(_ string, h *mucHistory) {
		r.mu.Lock()
		hists = append(hists, h)
		r.mu.Unlock()
	}
	want := &mucHistory{Since: time.Now().Add(-time.Hour), MaxStanzas: mucRejoinMaxStanzas}
	if err := r.x.joinMUC(context.Background(), nickRoom, want); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	next, tried, gen, handled := r.x.onMUCJoinConflict(nickRoom, "alice")
	if !handled || next != "alice_1" {
		t.Fatalf("next=%q handled=%v", next, handled)
	}
	r.x.continueMUCJoinAfterConflict(nickRoom, next, tried, gen)
	r.wait(t)
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(hists) != 2 || hists[0] != want || hists[1] != want {
		t.Fatalf("history requests = %v, want the first join's on both", hists)
	}
}
