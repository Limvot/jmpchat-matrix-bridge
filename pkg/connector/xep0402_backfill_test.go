package connector

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"mellium.im/xmpp/stanza"
)

// backfillLogin is a login whose metadata is saved in memory only.
func backfillLogin(t *testing.T) *bridgev2.UserLogin {
	t.Helper()
	old := saveUserLogin
	saveUserLogin = func(context.Context, *bridgev2.UserLogin) error { return nil }
	t.Cleanup(func() { saveUserLogin = old })
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "alice@example.com", Metadata: &JabberLoginMeta{}}}
}

func TestBackfillCreatesOnlyMissingBookmarksOnce(t *testing.T) {
	existing := bm(roomB, false, "")
	r := newBMRig(t, true, existing)
	r.x.login = backfillLogin(t)
	r.joinedRoom(roomA, "joined-nick") // the nick the bridge uses there (here: a 409 alternative would be the same)
	res := r.x.runBookmarkBackfill(context.Background(), []string{roomA, roomB, roomF}, false)
	if !res.Ran || res.Created != 2 || res.AlreadyHad != 1 || res.Failed != 0 {
		t.Fatalf("result = %+v", res)
	}
	byRoom := map[string]bookmarkConf{}
	for _, p := range r.published() {
		byRoom[p.ID] = p
	}
	if len(byRoom) != 2 || byRoom[roomA].Nick != "joined-nick" || !byRoom[roomA].Autojoin || byRoom[roomF].Nick != "alice" {
		t.Fatalf("published = %+v", byRoom)
	}
	// The existing bookmark (autojoin off, no nick) was not touched.
	for _, s := range r.server {
		if s.ID == roomB && (s.Autojoin || s.Nick != "") {
			t.Fatalf("existing bookmark modified: %+v", s)
		}
	}
	if !r.x.backfillDone() {
		t.Fatal("marker not set")
	}
	if _, ok := r.x.login.Metadata.(*JabberLoginMeta).Extra[bookmarkBackfillKey]; !ok {
		t.Fatal("marker not in login metadata")
	}
	var m backfillMarker
	_ = json.Unmarshal(r.x.login.Metadata.(*JabberLoginMeta).Extra[bookmarkBackfillKey], &m)
	if m.V != 1 || m.At == "" {
		t.Fatalf("marker = %+v", m)
	}
	// Notice for the management room only.
	if n := r.noticeList(); len(n) != 1 || n[0] != "|Created bookmarks (auto-join on) for 2 room(s) this bridge is already in, so your other clients see them too. Existing bookmarks were not changed." {
		t.Fatalf("notices = %q", n)
	}
	// Second run: marker present, nothing happens (no fetch either).
	fetches := r.fetchCount()
	if res := r.x.runBookmarkBackfill(context.Background(), []string{"new@groups.example.com"}, false); res.Ran || r.fetchCount() != fetches {
		t.Fatalf("second run: %+v", res)
	}
	// push (force) runs again.
	if res := r.x.runBookmarkBackfill(context.Background(), []string{"new@groups.example.com"}, true); !res.Ran || res.Created != 1 {
		t.Fatalf("forced run: %+v", res)
	}
}

func TestBackfillNeedsWriteSupportAndRetriesAfterFailure(t *testing.T) {
	// Read-only server: nothing, no marker.
	r := newBMRig(t, false)
	r.x.login = backfillLogin(t)
	if res := r.x.runBookmarkBackfill(context.Background(), []string{roomA}, false); res.Ran || r.x.backfillDone() || len(r.published()) != 0 {
		t.Fatalf("read-only: %+v", res)
	}
	// A failed publish leaves no marker (retried next session); the refusal disables writes.
	r = newBMRig(t, true)
	r.x.login = backfillLogin(t)
	r.publishErr = errors.New("timeout")
	res := r.x.runBookmarkBackfill(context.Background(), []string{roomA}, false)
	if res.Failed != 1 || r.x.backfillDone() {
		t.Fatalf("transient failure: %+v done=%v", res, r.x.backfillDone())
	}
	r.publishErr = stanza.Error{Condition: stanza.Conflict}
	res = r.x.runBookmarkBackfill(context.Background(), []string{roomA, roomF}, false)
	if res.Failed != 1 || !r.x.bm.writesDisabled() || r.x.backfillDone() {
		t.Fatalf("refused: %+v", res)
	}
	// Fetch failure: nothing, no marker.
	r = newBMRig(t, true)
	r.x.login = backfillLogin(t)
	r.fetchErr = errors.New("boom")
	if res := r.x.runBookmarkBackfill(context.Background(), []string{roomA}, false); res.Ran || r.x.backfillDone() {
		t.Fatalf("fetch failure: %+v", res)
	}
	// No rooms: the run completes and sets the marker (rooms bridged later get bookmarks via the join command).
	r = newBMRig(t, true)
	r.x.login = backfillLogin(t)
	if res := r.x.runBookmarkBackfill(context.Background(), nil, false); !res.Ran || !r.x.backfillDone() {
		t.Fatalf("empty run: %+v", res)
	}
}

func TestSyncBookmarksPushRunsBackfillRegardlessOfMarker(t *testing.T) {
	r := startupRig(t)
	if err := r.x.markBackfilled(context.Background()); err != nil {
		t.Fatal(err)
	}
	res, err := r.x.SyncBookmarks(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	// Candidates: bridged, not left, MUC portals: A, B, F; A and B have bookmarks, F gets one.
	if !res.BackfillRan || res.Created != 1 || res.AlreadyHad != 2 {
		t.Fatalf("push result = %+v", res)
	}
	p := r.published()
	if len(p) != 1 || bookmarkKey(p[0].ID) != roomF {
		t.Fatalf("published = %+v", p)
	}
}
