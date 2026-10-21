package connector

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/simplevent"
)

func eventRig(t *testing.T, server ...bookmarkConf) *bmRig {
	t.Helper()
	fastRejoin(t, time.Millisecond)
	r := newBMRig(t, true, server...)
	return r
}

func pub(id string, autojoin bool, nick string) bookmarkChange {
	c := bm(id, autojoin, nick)
	return bookmarkChange{ID: id, Conf: &c}
}

func apply(r *bmRig, changes ...bookmarkChange) {
	r.x.applyBookmarkBatch(context.Background(), bookmarkBatch{changes: changes})
}

func (r *bmRig) createdPortals() (out []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if cr, ok := e.(*simplevent.ChatResync); ok && cr.CreatePortal {
			out = append(out, string(cr.PortalKey.ID))
		}
	}
	return out
}

func TestEventAutojoinOnNewRoomJoinsAndCreatesPortal(t *testing.T) {
	r := eventRig(t)
	apply(r, pub(roomC, true, "c-nick"))
	// The cached bookmark nick is already in effect for the join (cache is updated before joining).
	if got := strings.Join(r.joins(), ","); got != roomC+"|c-nick" {
		t.Fatalf("joins = %s", got)
	}
	if got := r.createdPortals(); len(got) != 1 || got[0] != roomC {
		t.Fatalf("created = %v", got)
	}
	if r.x.bm.importBudgetLeft() != bookmarkImportMaxRooms-1 {
		t.Fatalf("budget left = %d", r.x.bm.importBudgetLeft())
	}
	// Invalid ids and Cheogram numbers are ignored.
	apply(r, pub("+15551234567@cheogram.com", true, ""), pub("not a jid", true, ""), pub(roomC+"/res", true, ""))
	if len(r.joins()) != 1 {
		t.Fatalf("joins = %v", r.joins())
	}
}

func TestEventAutojoinOnClearsLeftFlagAndRejoinsExistingPortal(t *testing.T) {
	r := eventRig(t, bm(roomE, false, ""))
	r.x.bm.setCache([]bookmarkConf{bm(roomE, false, "")})
	r.addPortal(roomE, true)
	apply(r, pub(roomE, true, ""))
	if r.left[roomE] {
		t.Fatalf("left flag not cleared: %v", r.left)
	}
	if got := strings.Join(r.joins(), ","); got != roomE+"|alice" {
		t.Fatalf("joins = %s", got)
	}
	if len(r.createdPortals()) != 0 || r.x.bm.importBudgetLeft() != bookmarkImportMaxRooms {
		t.Fatal("existing portal consumed budget or was re-created")
	}
	// Already in the room: only the flag logic runs.
	r2 := eventRig(t)
	r2.addPortal(roomA, false)
	r2.joinedRoom(roomA, "alice")
	apply(r2, pub(roomA, true, ""))
	if len(r2.joins()) != 0 {
		t.Fatalf("joined a room we are in: %v", r2.joins())
	}
}

func TestEventEchoAndEditsOfOnBookmarksAreNotJoinRequests(t *testing.T) {
	r := eventRig(t)
	r.addPortal(roomE, true) // left locally (e.g. the leave publish failed); bookmark still on
	r.x.bm.setCache([]bookmarkConf{bm(roomE, true, "n")})
	// Our own echo: identical to the cache.
	apply(r, pub(roomE, true, "n"))
	// Another client edits the name only: autojoin was already on.
	edited := bm(roomE, true, "n")
	edited.Name = "renamed"
	apply(r, bookmarkChange{ID: roomE, Conf: &edited})
	if len(r.joins()) != 0 || len(r.left) != 0 {
		t.Fatalf("joins = %v left = %v", r.joins(), r.left)
	}
	if c, _, _ := r.x.bm.lookup(roomE); c.Name != "renamed" {
		t.Fatalf("cache not updated: %+v", c)
	}
	// Our own publish is echoed back: ignored.
	r2 := eventRig(t, bm(roomA, false, "n"))
	r2.addPortal(roomA, false)
	r2.joinedRoom(roomA, "n")
	r2.x.publishBookmarkFor(context.Background(), roomA, true, "", true)
	echo := r2.published()[0]
	apply(r2, bookmarkChange{ID: roomA, Conf: &echo})
	if len(r2.joins()) != 0 || len(r2.leaves) != 0 {
		t.Fatalf("own echo acted on: joins=%v leaves=%v", r2.joins(), r2.leaves)
	}
}

func TestEventAutojoinOffLeavesKeepsPortalNotifiesAndViewDoesNotRejoin(t *testing.T) {
	for name, change := range map[string]bookmarkChange{
		"autojoin off": pub(roomA, false, "n"),
		"retract":      {ID: "A@Groups.Example.com", Retract: true},
	} {
		t.Run(name, func(t *testing.T) {
			r := eventRig(t)
			r.x.bm.setCache([]bookmarkConf{bm(roomA, true, "n")})
			r.addPortal(roomA, false)
			r.joinedRoom(roomA, "n")
			apply(r, change)
			if len(r.leaves) != 1 || r.leaves[0] != roomA {
				t.Fatalf("leaves = %v", r.leaves)
			}
			if !r.left[roomA] {
				t.Fatalf("MUCLeft not set: %v", r.left)
			}
			wantNotice := roomA + "|Left " + roomA + ": its bookmark was set to not auto-join (or was deleted) on another client. Use `!jabber join " + roomA + "` to return."
			if n := r.noticeList(); len(n) != 1 || n[0] != wantNotice {
				t.Fatalf("notices = %q", n)
			}
			if len(r.createdPortals()) != 0 || len(r.published()) != 0 {
				t.Fatal("portal touched or bookmark written")
			}
			// Viewing/sending does not bring the room back; only the join command does.
			if err := r.x.joinMUCForPortal(context.Background(), roomA); err == nil {
				t.Fatal("joinMUCForPortal rejoined a left room")
			}
			if len(r.joins()) != 0 {
				t.Fatalf("rejoined: %v", r.joins())
			}
			// Repeating the event is idempotent: no second leave, no second notice.
			apply(r, change)
			if len(r.leaves) != 1 || len(r.noticeList()) != 1 {
				t.Fatalf("not idempotent: leaves=%v notices=%v", r.leaves, r.noticeList())
			}
			if err := r.x.JoinMUC(context.Background(), roomA); err != nil || r.left[roomA] {
				t.Fatalf("explicit join: err=%v left=%v", err, r.left)
			}
		})
	}
}

func TestEventAutojoinOffForRoomWeAreNotInIsNoop(t *testing.T) {
	r := eventRig(t)
	apply(r, pub(roomD, false, ""), bookmarkChange{ID: roomD, Retract: true}, bookmarkChange{Purge: true})
	if len(r.leaves) != 0 || len(r.left) != 0 || len(r.noticeList()) != 0 || len(r.createdPortals()) != 0 || len(r.joins()) != 0 {
		t.Fatalf("not a no-op: leaves=%v left=%v notices=%v", r.leaves, r.left, r.noticeList())
	}
	// A bridged portal that was never joined this session (join pending) is marked left too.
	r = eventRig(t)
	r.addPortal(roomB, false)
	apply(r, pub(roomB, false, ""))
	if !r.left[roomB] || len(r.leaves) != 0 {
		t.Fatalf("active portal not marked left: left=%v leaves=%v", r.left, r.leaves)
	}
}

func TestEventSharedImportBudget(t *testing.T) {
	r := eventRig(t)
	r.x.bm.takeImportBudget(bookmarkImportMaxRooms - 1)
	apply(r, pub("one@groups.example.com", true, ""), pub("two@groups.example.com", true, ""), pub("three@groups.example.com", true, ""))
	if got := r.createdPortals(); len(got) != 1 || got[0] != "one@groups.example.com" {
		t.Fatalf("created = %v", got)
	}
	var notices int
	for _, n := range r.noticeList() {
		if strings.HasPrefix(n, "|Imported the maximum of 50 bookmarked rooms this session; more bookmarks were not imported. Run `!jabber bookmarks-sync` or reconnect to continue.") {
			notices++
		}
	}
	if notices != 1 {
		t.Fatalf("budget notices = %d (%q)", notices, r.noticeList())
	}
}

func TestEventWorkerAppliesQueuedEventsAndStopsWithSession(t *testing.T) {
	r := eventRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.x.runBookmarkEventWorker(ctx, r.x.bm.eventChan()); close(done) }()
	if !r.x.bm.offer(bookmarkBatch{changes: []bookmarkChange{pub(roomC, true, "c-nick")}}) {
		t.Fatal("offer failed")
	}
	bmWaitFor(t, "join from event", func() bool { return len(r.joins()) == 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestEventHandlerOnServeDoesNoIO(t *testing.T) {
	r := eventRig(t)
	fail := func(what string) { t.Errorf("Serve path did IO: %s", what) }
	r.x.bm.fetchHook = func(context.Context) ([]bookmarkConf, bookmarkFetchInfo, error) {
		fail("fetch")
		return nil, bookmarkFetchInfo{}, nil
	}
	r.x.bm.publishHook = func(context.Context, bookmarkConf) error { fail("publish"); return nil }
	r.x.mucSendJoinHook = func(string, string) error { fail("join"); return nil }
	r.x.mucLeaveHook = func(string) error { fail("leave"); return nil }
	r.x.mucLeftHook = func(string, bool) error { fail("left flag"); return nil }
	r.x.queueHook = nil // a queued remote event would dereference the nil login
	serveStanza(t, r.x, strings.Replace(pepEvent, "%s", "alice@example.com", 1))
	select {
	case b := <-r.x.bm.eventChan():
		if len(b.changes) != 2 || b.changes[0].Conf == nil || !b.changes[1].Retract {
			t.Fatalf("batch = %+v", b)
		}
	default:
		t.Fatal("event not queued")
	}
	// Spoofed sender: nothing queued.
	serveStanza(t, r.x, strings.Replace(pepEvent, "%s", "evil@example.com", 1))
	select {
	case <-r.x.bm.eventChan():
		t.Fatal("spoofed event queued")
	default:
	}
}

func TestEventOverflowDropsAndHandlerSurvives(t *testing.T) {
	r := eventRig(t)
	buf := &bmLogBuf{}
	r.x.log = zerolog.New(buf)
	for i := 0; i < bookmarkEventBuffer; i++ {
		if !r.x.bm.offer(bookmarkBatch{}) {
			t.Fatalf("offer %d failed early", i)
		}
	}
	serveStanza(t, r.x, strings.Replace(pepEvent, "%s", "alice@example.com", 1))
	if !strings.Contains(buf.String(), "event queue full") {
		t.Fatalf("no overflow warning: %s", buf.String())
	}
	// Without a session (channel nil) the handler also survives.
	x := &xmppClient{loginID: "alice@example.com", log: zerolog.Nop()}
	serveStanza(t, x, strings.Replace(pepEvent, "%s", "alice@example.com", 1))
}
