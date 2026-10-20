package connector

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

// contactHarness adds a contact-ghost seam and records refresh requests.
type contactHarness struct {
	*harness
	mu        sync.Mutex
	contacts  map[string]contactAvatar
	lookups   int
	refreshes []string
}

func newContactHarness(t *testing.T) *contactHarness {
	ch := &contactHarness{harness: newHarness(), contacts: map[string]contactAvatar{}}
	noFetch(t, ch.harness)
	ch.s.getContact = func(ctx context.Context, real string) (contactAvatar, error) {
		ch.mu.Lock()
		defer ch.mu.Unlock()
		ch.lookups++
		return ch.contacts[real], nil
	}
	ch.s.refreshContact = func(cid string) {
		ch.mu.Lock()
		ch.refreshes = append(ch.refreshes, cid)
		ch.mu.Unlock()
	}
	return ch
}

func (ch *contactHarness) setContact(n int, fetchedAgo time.Duration) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	c := contactAvatar{state: contactHas, id: testReal, fetchedAt: ch.now.Add(-fetchedAgo)}
	if n == 0 {
		c.state = contactNone
	} else {
		c.mxc = *propMXC(n)
	}
	ch.contacts[testReal] = c
}

func (ch *contactHarness) refreshCount() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return len(ch.refreshes)
}

func storedHash(g *fakeGhost) string { return g.g.Metadata.(*GhostMetadata).AvatarHash }

// The live failure: the contact changed avatar while the occupant was unknown; after (re)join the
// room still advertises the old hash. The occupant gets the contact avatar without a fetch, the old
// room hash does not override it, and the contact is refreshed once (room/contact mismatch).
func TestContactRuleOccupantKnownAfterChange(t *testing.T) {
	ch := newContactHarness(t)
	ch.setContact(5, time.Hour)
	g := newFakeGhost(hashN(1), true) // occupant ghost still has the old avatar
	ch.ghosts[testOcc] = g
	queueOne(ch.harness, testOcc, realPresence(hashN(1), testReal))
	for ch.s.runOnce(context.Background()) {
	}
	if len(g.updates) != 1 {
		t.Fatalf("updates %d", len(g.updates))
	}
	av := g.updates[0].Avatar
	if av.MXC != "mxc://example.com/prop" || av.Get != nil || av.Hash != [32]byte{5} || string(av.ID) != "sha1:"+hashN(5) {
		t.Fatalf("avatar %+v", av)
	}
	if storedHash(g) != hashN(5) {
		t.Fatal("hash not stored")
	}
	if ch.refreshCount() != 1 || ch.refreshes[0] != testReal {
		t.Fatalf("refreshes %v", ch.refreshes)
	}
	// The room keeps advertising the old hash: nothing enqueued, nothing reverted.
	ch.now = ch.now.Add(2 * time.Hour)
	ch.s.notePresence(testOcc, testRoom, realPresence(hashN(1), testReal), false, false)
	if ch.pendingLen() != 0 {
		t.Fatal("stale room hash must not enqueue")
	}
	// A new session (rejoin) with the same stale hash: contact rule again, no revert, no fetch,
	// and no second refresh for the same advertised hash.
	ch.s.reset()
	queueOne(ch.harness, testOcc, realPresence(hashN(1), testReal))
	for ch.s.runOnce(context.Background()) {
	}
	if len(g.updates) != 1 || storedHash(g) != hashN(5) {
		t.Fatal("ghost reverted or updated again")
	}
	if ch.refreshCount() != 1 {
		t.Fatalf("refresh repeated: %v", ch.refreshes)
	}
	if len(ch.calls) != 0 {
		t.Fatal("fetch happened")
	}
}

// Ghost seen first (it spoke), presence later: the presence triggers the contact rule. A presence
// without a usable photo element still gets one contact-rule job per session.
func TestContactRuleFirstMessageAndUnknownPhoto(t *testing.T) {
	ch := newContactHarness(t)
	ch.setContact(5, time.Hour)
	ch.ghosts[testOcc] = newFakeGhost("", true)
	ch.s.noteGhost(testOcc)
	if ch.pendingLen() != 0 {
		t.Fatal("history-only entry must not enqueue")
	}
	ch.s.notePresence(testOcc, testRoom, presenceInfo{isMUC: true, itemJID: testReal}, false, false)
	if ch.pendingLen() != 1 {
		t.Fatal("unknown photo with real JID must enqueue one contact job")
	}
	ch.s.runOnce(context.Background())
	if storedHash(ch.ghosts[testOcc]) != hashN(5) {
		t.Fatal("contact avatar not applied")
	}
	if ch.refreshCount() != 0 {
		t.Fatal("unknown advertised state must not trigger a refresh")
	}
	ch.now = ch.now.Add(time.Hour)
	ch.s.notePresence(testOcc, testRoom, presenceInfo{isMUC: true, itemJID: testReal}, false, false)
	if ch.pendingLen() != 0 {
		t.Fatal("contact job must run once per session for unknown photo")
	}
	// Without the contact seam an unknown photo still never enqueues.
	h := newHarness()
	h.s.notePresence(testOcc, testRoom, presenceInfo{isMUC: true, itemJID: testReal}, false, false)
	h.s.noteGhost(testOcc)
	if h.pendingLen() != 0 {
		t.Fatal("unknown photo without contact rule must not enqueue")
	}
}

func TestContactRuleNoContactFallsBackToRoomPath(t *testing.T) {
	ch := newContactHarness(t)
	ch.ghosts[testOcc] = newFakeGhost("", true)
	ch.fetchFn = func(string) ([]byte, string, error) { return pngBytes, hashN(7), nil }
	queueOne(ch.harness, testOcc, realPresence(hashN(7), testReal))
	ch.s.runOnce(context.Background())
	if ch.lookups != 1 || len(ch.calls) != 1 || ch.calls[0].to != testOcc {
		t.Fatalf("lookups %d calls %+v", ch.lookups, ch.calls)
	}
	if storedHash(ch.ghosts[testOcc]) != hashN(7) || ch.refreshCount() != 0 {
		t.Fatal("room path result wrong")
	}
	// A contact ghost whose avatar is not known (unknown state) also falls back.
	ch.contacts[testReal] = contactAvatar{state: contactUnknown, id: testReal}
	ch.s.notePresence(otherOcc, testRoom, realPresence(hashN(8), testReal), false, false)
	ch.ghosts[otherOcc] = newFakeGhost("", true)
	ch.fetchFn = func(string) ([]byte, string, error) { return pngBytes, hashN(8), nil }
	ch.s.noteGhost(otherOcc)
	ch.s.runOnce(context.Background())
	if storedHash(ch.ghosts[otherOcc]) != hashN(8) {
		t.Fatal("unknown contact must fall back to the room path")
	}
}

func TestContactRuleAnonymousRoomUnchanged(t *testing.T) {
	ch := newContactHarness(t)
	ch.setContact(5, time.Hour)
	ch.ghosts[testOcc] = newFakeGhost("", true)
	ch.fetchFn = func(string) ([]byte, string, error) { return pngBytes, hashN(7), nil }
	queueOne(ch.harness, testOcc, hashPresence(hashN(7))) // no real JID
	ch.s.runOnce(context.Background())
	if ch.lookups != 0 {
		t.Fatal("no contact lookup without a real JID")
	}
	if len(ch.calls) != 1 || storedHash(ch.ghosts[testOcc]) != hashN(7) {
		t.Fatal("room path must run")
	}
}

func TestContactRuleRemoveWhenContactHasNone(t *testing.T) {
	ch := newContactHarness(t)
	ch.setContact(0, time.Hour)
	ch.ghosts[testOcc] = newFakeGhost(hashN(1), true)
	queueOne(ch.harness, testOcc, realPresence(hashN(1), testReal))
	ch.s.runOnce(context.Background())
	u := ch.ghosts[testOcc].updates
	if len(u) != 1 || !u[0].Avatar.Remove || storedHash(ch.ghosts[testOcc]) != "" {
		t.Fatalf("removal: %+v", u)
	}
	// Already none: no update.
	ch.ghosts[otherOcc] = newFakeGhost("", true)
	queueOne(ch.harness, otherOcc, presenceInfo{isMUC: true, photo: photoNone, itemJID: testReal})
	ch.s.runOnce(context.Background())
	if len(ch.ghosts[otherOcc].updates) != 0 {
		t.Fatal("no-op expected")
	}
}

// The contact refresh is rate limited and cannot loop: a recently fetched contact is not
// refreshed (the job stays unhandled and a later presence after the minimum interval re-checks),
// a refresh happens once the contact is old enough, and its propagation enqueues nothing more.
func TestContactRuleRefreshRateLimitedNoLoop(t *testing.T) {
	ch := newContactHarness(t)
	ch.setContact(5, 2*time.Minute)
	g := newFakeGhost(hashN(5), true)
	ch.ghosts[testOcc] = g
	queueOne(ch.harness, testOcc, realPresence(hashN(1), testReal))
	ch.s.runOnce(context.Background())
	if ch.refreshCount() != 0 || len(g.updates) != 0 {
		t.Fatal("recently fetched contact must not be refreshed")
	}
	// Within the minimum interval: nothing.
	ch.s.notePresence(testOcc, testRoom, realPresence(hashN(1), testReal), false, false)
	if ch.pendingLen() != 0 {
		t.Fatal("must respect the minimum interval")
	}
	// After the interval but contact still fresh: re-checked, no refresh.
	ch.now = ch.now.Add(61 * time.Second)
	ch.s.notePresence(testOcc, testRoom, realPresence(hashN(1), testReal), false, false)
	if ch.pendingLen() != 1 {
		t.Fatal("unsettled state must be re-checked")
	}
	ch.s.runOnce(context.Background())
	if ch.refreshCount() != 0 {
		t.Fatal("still too fresh")
	}
	// Contact old enough now: exactly one refresh.
	ch.now = ch.now.Add(10 * time.Minute)
	// The refresh finds the same avatar and (as doAvatarRefresh does on a change) propagates.
	ch.s.refreshContact = func(cid string) {
		ch.mu.Lock()
		ch.refreshes = append(ch.refreshes, cid)
		ch.mu.Unlock()
		ch.s.propagateContact(cid, propMXC(5))
	}
	ch.s.notePresence(testOcc, testRoom, realPresence(hashN(1), testReal), false, false)
	for i := 0; i < 10 && ch.s.runOnce(context.Background()); i++ {
	}
	if ch.refreshCount() != 1 {
		t.Fatalf("refreshes %d", ch.refreshCount())
	}
	if ch.pendingLen() != 0 {
		t.Fatal("propagation must not loop")
	}
	// Further stale presences: handled, no refresh.
	for i := 0; i < 5; i++ {
		ch.now = ch.now.Add(time.Hour)
		ch.s.notePresence(testOcc, testRoom, realPresence(hashN(1), testReal), false, false)
		for ch.s.runOnce(context.Background()) {
		}
	}
	if ch.refreshCount() != 1 {
		t.Fatalf("refresh repeated: %d", ch.refreshCount())
	}
	// A different advertised hash within 10 min of the last request: no refresh yet.
	last := ch.now
	ch.s.mu.Lock()
	ch.s.contactReq[testReal] = mucContactReq{adv: hashN(1), at: last}
	ch.s.mu.Unlock()
	ch.s.notePresence(testOcc, testRoom, realPresence(hashN(2), testReal), false, false)
	ch.s.runOnce(context.Background())
	if ch.refreshCount() != 1 {
		t.Fatal("per-contact refresh interval not honoured")
	}
	if len(g.updates) != 0 || len(ch.calls) != 0 {
		t.Fatal("no update or fetch expected")
	}
}

func TestClassifyContactGhost(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	h := hashN(3)
	sum := [32]byte{1}
	meta := func(hash string, fetched bool) *GhostMetadata {
		m := &GhostMetadata{AvatarHash: hash}
		if fetched {
			m.AvatarFetchedAt = jsontime.UM(now)
		}
		return m
	}
	aid := avatarIDFromHash(h)
	mxc := id.ContentURIString("mxc://example.com/c")
	tests := []struct {
		name string
		aid  networkid.AvatarID
		mxc  id.ContentURIString
		sum  [32]byte
		set  bool
		meta *GhostMetadata
		want contactAvatarState
	}{
		{"has", aid, mxc, sum, true, meta(h, true), contactHas},
		{"no meta", aid, mxc, sum, true, nil, contactUnknown},
		{"never fetched", aid, mxc, sum, true, meta(h, false), contactUnknown},
		{"not set on matrix", aid, mxc, sum, false, meta(h, true), contactUnknown},
		{"upload failed (id/hash mismatch)", avatarIDFromHash(hashN(4)), mxc, sum, true, meta(h, true), contactUnknown},
		{"no mxc", aid, "", sum, true, meta(h, true), contactUnknown},
		{"no sha256", aid, mxc, [32]byte{}, true, meta(h, true), contactUnknown},
		{"none", "", "", [32]byte{}, true, meta("", true), contactNone},
		{"none but avatar left on ghost", aid, mxc, sum, true, meta("", true), contactUnknown},
		{"none not set", "", "", [32]byte{}, false, meta("", true), contactUnknown},
	}
	for _, tc := range tests {
		c := classifyContactGhost(testReal, tc.aid, tc.mxc, tc.sum, tc.set, tc.meta)
		if c.state != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, c.state, tc.want)
		}
		if c.state == contactHas && (c.mxc.realHash != h || c.mxc.mxc != string(mxc) || c.mxc.sha256 != sum) {
			t.Errorf("%s: mxc %+v", tc.name, c.mxc)
		}
	}
}

func TestContactRuleRace(t *testing.T) {
	ch := newContactHarness(t)
	ch.setContact(5, time.Hour)
	ch.ghosts[testOcc] = newFakeGhost(hashN(1), true)
	queueOne(ch.harness, testOcc, realPresence(hashN(1), testReal))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				ch.s.propagateContact(testReal, propMXC(5))
				ch.s.notePresence(testOcc, testRoom, realPresence(hashN(1+j%3), testReal), false, false)
				ch.s.noteGhost(testOcc)
			}
		}(i)
	}
	for i := 0; i < 200; i++ {
		ch.s.runOnce(context.Background())
	}
	wg.Wait()
	for ch.s.runOnce(context.Background()) {
	}
	if storedHash(ch.ghosts[testOcc]) != hashN(5) {
		t.Fatal("final avatar must be the contact avatar")
	}
}
