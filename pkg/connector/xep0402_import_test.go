package connector

import (
	"bytes"
	"context"
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"mellium.im/xmpp/disco"
)

const (
	roomA = "a@groups.example.com" // bridged, bookmark autojoin on
	roomB = "b@groups.example.com" // bridged, bookmark autojoin off
	roomC = "c@groups.example.com" // not bridged, bookmark autojoin on
	roomD = "d@groups.example.com" // not bridged, bookmark autojoin off
	roomE = "e@groups.example.com" // bridged and left locally, bookmark autojoin on
	roomF = "f@groups.example.com" // bridged, no bookmark
)

func startupRig(t *testing.T) *bmRig {
	t.Helper()
	fastRejoin(t, time.Millisecond)
	od := bookmarkBackfillSettle
	bookmarkBackfillSettle = 0
	t.Cleanup(func() { bookmarkBackfillSettle = od })
	r := newBMRig(t, true, bm(roomA, true, "a-nick"), bm(roomB, false, "b-nick"), bm(roomC, true, "c-nick"), bm(roomD, false, ""), bm(roomE, true, ""),
		bm("+15551234567@cheogram.com", true, ""))
	r.addPortal(roomA, false)
	r.addPortal(roomB, false)
	r.addPortal(roomE, true)
	r.addPortal(roomF, false)
	r.x.login = backfillLogin(t)
	return r
}

func TestStartupCoordinatorAppliesBookmarks(t *testing.T) {
	r := startupRig(t)
	r.x.runMUCAutoRejoin(context.Background())

	// Joins: bridged rooms first (A, F), B skipped (bookmark off), E skipped (left locally), then the import C.
	if got := strings.Join(r.joins(), ","); got != roomA+"|a-nick,"+roomF+"|alice,"+roomC+"|c-nick" {
		t.Fatalf("joins = %s", got)
	}
	if !r.left[roomB] {
		t.Fatalf("B must be marked left: %v", r.left)
	}
	if _, touched := r.left[roomE]; touched || r.left[roomA] || r.left[roomF] {
		t.Fatalf("left flags = %v", r.left)
	}
	want := roomB + "|Not re-joining " + roomB + ": its bookmark is set to not auto-join. Use `!jabber join " + roomB + "` to return."
	var found bool
	for _, n := range r.noticeList() {
		if n == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("notices = %q, want %q", r.noticeList(), want)
	}
	// The import queues a portal creation with the MUC-forced chat info; nothing for existing rooms.
	var creates []string
	r.mu.Lock()
	for _, e := range r.events {
		if cr, ok := e.(*simplevent.ChatResync); ok && cr.CreatePortal {
			creates = append(creates, string(cr.PortalKey.ID))
			if cr.PortalKey.Receiver != r.x.loginID || cr.GetChatInfoFunc == nil {
				t.Errorf("bad import event %+v", cr)
			}
		}
	}
	r.mu.Unlock()
	if len(creates) != 1 || creates[0] != roomC {
		t.Fatalf("created portals = %v", creates)
	}
	// Reading imports never writes to a bookmark that exists; only the backfill created F's.
	for _, p := range r.published() {
		if bookmarkKey(p.ID) != roomF {
			t.Fatalf("unexpected bookmark write: %+v", p)
		}
	}
}

func TestStartupCoordinatorWithoutBookmarksBehavesAsBefore(t *testing.T) {
	for name, setup := range map[string]func(r *bmRig){
		"fetch fails": func(r *bmRig) { r.fetchErr = context.DeadlineExceeded },
		"no read support": func(r *bmRig) {
			r.x.bm.discoHook = func(context.Context) (disco.Info, error) { return disco.Info{}, nil }
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := startupRig(t)
			setup(r)
			r.x.runMUCAutoRejoin(context.Background())
			// B (bookmark autojoin=false) is joined like before, no import, no flag, no write.
			if got := strings.Join(r.joins(), ","); got != roomA+"|alice,"+roomB+"|alice,"+roomF+"|alice" {
				t.Fatalf("joins = %s", got)
			}
			if len(r.left) != 0 || len(r.published()) != 0 || len(r.events) != 0 {
				t.Fatalf("left=%v published=%d events=%d", r.left, len(r.published()), len(r.events))
			}
		})
	}
}

func TestStartupImportBudgetIsSharedAndNoticed(t *testing.T) {
	r := startupRig(t)
	r.x.bm.takeImportBudget(bookmarkImportMaxRooms - 0) // used up by live events earlier this session
	r.x.runMUCAutoRejoin(context.Background())
	for _, j := range r.joins() {
		if strings.HasPrefix(j, roomC) {
			t.Fatalf("imported past the budget: %v", r.joins())
		}
	}
	var noticed bool
	for _, n := range r.noticeList() {
		if strings.HasPrefix(n, "|Imported the maximum of 50 bookmarked rooms") {
			noticed = true
		}
	}
	if !noticed {
		t.Fatalf("budget notice missing: %q", r.noticeList())
	}
}

func TestStartupCancelBeforeImports(t *testing.T) {
	r := startupRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	r.x.mucSendJoinHook = func(room, nick string) error {
		r.mu.Lock()
		r.sentJoins = append(r.sentJoins, room+"|"+nick)
		r.mu.Unlock()
		cancel() // the session ends after the first join
		return nil
	}
	r.x.runMUCAutoRejoin(ctx)
	if n := len(r.joins()); n != 1 {
		t.Fatalf("joins after cancel = %v", r.joins())
	}
	if len(r.events) != 0 {
		t.Fatal("portal created after cancel")
	}
}

func TestBookmarkNickAndPasswordUsedForJoin(t *testing.T) {
	r := newBMRig(t, true)
	buf := &bmLogBuf{}
	r.x.log = zerolog.New(buf)
	c := bm(roomC, true, "bm-nick")
	c.Password = "s3cret-pw"
	r.x.bm.setCache([]bookmarkConf{c})
	var payloads [][]xml.Token
	r.x.mucJoinTokensHook = func(_ string, toks []xml.Token) { payloads = append(payloads, toks) }
	if err := r.x.joinMUC(context.Background(), roomC, &mucHistory{MaxStanzas: 20}); err != nil {
		t.Fatal(err)
	}
	// 409 on the bookmark nick: the alternative derives from it, the password rides on the retry too.
	presenceStanza(t, r.x, "error", roomC+"/bm-nick", conflictBody)
	bmWaitFor(t, "retry join", func() bool { return len(r.joins()) == 2 })
	if got := strings.Join(r.joins(), ","); got != roomC+"|bm-nick,"+roomC+"|bm-nick_1" {
		t.Fatalf("joins = %s", got)
	}
	bmWaitFor(t, "retry payload", func() bool { return len(payloads) == 2 })
	for i, toks := range payloads {
		var out bytes.Buffer
		enc := xml.NewEncoder(&out)
		for _, tok := range toks {
			if err := enc.EncodeToken(tok); err != nil {
				t.Fatal(err)
			}
		}
		enc.Flush()
		if !strings.Contains(out.String(), "<password>s3cret-pw</password>") || !strings.Contains(out.String(), `<history maxstanzas="20">`) {
			t.Errorf("payload %d = %s", i, out.String())
		}
	}
	if strings.Contains(buf.String(), "s3cret-pw") || strings.Contains(buf.String(), "Name of") {
		t.Fatalf("logs leak bookmark secrets: %s", buf.String())
	}
	// No bookmark password: no <password> element.
	toks := mucJoinTokens(nil, "")
	if len(toks) != 2 {
		t.Fatalf("plain join payload = %v", toks)
	}
}

func TestExplicitNickBeatsBookmarkNick(t *testing.T) {
	r := newBMRig(t, true)
	r.x.bm.setCache([]bookmarkConf{bm(roomC, true, "bm-nick")})
	if base, nick := r.x.mucNickChoice(context.Background(), roomC); base != "bm-nick" || nick != "bm-nick" {
		t.Fatalf("bookmark nick = %s/%s", base, nick)
	}
	// Bookmark nick lookups are case-insensitive on the room.
	if r.x.bookmarkNick(strings.ToUpper(roomC)) != "bm-nick" {
		t.Fatal("mixed-case lookup failed")
	}
}

func TestSyncBookmarksImportsOnlyAndNeverWrites(t *testing.T) {
	r := startupRig(t)
	res, err := r.x.SyncBookmarks(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !res.ReadSupported || !res.WriteSupported || res.Total != 6 || res.Imported != 1 || res.Existing != 2 /* A and E */ || res.Invalid != 1 /* cheogram */ {
		t.Fatalf("result = %+v", res)
	}
	if got := strings.Join(r.joins(), ","); got != roomC+"|c-nick" {
		t.Fatalf("joins = %s", got)
	}
	if len(r.published()) != 0 || len(r.left) != 0 || len(r.leaves) != 0 {
		t.Fatalf("sync wrote or left: pub=%d left=%v leaves=%v", len(r.published()), r.left, r.leaves)
	}
	// Running it again in the same session: C is no longer imported twice only because it now has a portal;
	// here (portal creation is asynchronous) the budget still counts it.
	if r.x.bm.importBudgetLeft() != bookmarkImportMaxRooms-1 {
		t.Fatalf("budget left = %d", r.x.bm.importBudgetLeft())
	}
	// Unsupported server.
	r = startupRig(t)
	r.x.bm.discoHook = func(context.Context) (disco.Info, error) { return disco.Info{}, nil }
	res, err = r.x.SyncBookmarks(context.Background(), false)
	if err != nil || res.ReadSupported || len(r.joins()) != 0 {
		t.Fatalf("unsupported: %+v err=%v", res, err)
	}
}
