package connector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/id"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	testRoom = "room@muc.example.com"
	testOcc  = "room@muc.example.com/alice"
)

func hashN(n int) string { return fmt.Sprintf("%040x", n) }

func hashPresence(h string) presenceInfo {
	return presenceInfo{isMUC: true, photo: photoHash, hash: h}
}

// fakeGhost implements avatarGhost on top of a real bridgev2.Ghost struct so ExtraUpdates
// closures can run against real GhostMetadata.
type fakeGhost struct {
	g         *bridgev2.Ghost
	updates   []*bridgev2.UserInfo
	setOnLoad bool // AvatarSet value after an update with an avatar
	failSet   bool
}

func newFakeGhost(storedHash string, applied bool) *fakeGhost {
	return &fakeGhost{
		g: &bridgev2.Ghost{Ghost: &database.Ghost{
			Metadata:  &GhostMetadata{AvatarHash: storedHash},
			AvatarSet: applied,
		}},
	}
}

func (f *fakeGhost) UpdateInfo(ctx context.Context, info *bridgev2.UserInfo) {
	f.updates = append(f.updates, info)
	if info.Avatar != nil && !info.Avatar.Remove && info.Avatar.MXC == "" {
		f.g.AvatarMXC = id.ContentURIString("mxc://example.com/" + string(info.Avatar.ID))
		f.g.AvatarHash = [32]byte{1}
	}
	if info.Avatar != nil {
		f.g.AvatarSet = !f.failSet
	}
	if info.ExtraUpdates != nil {
		info.ExtraUpdates(ctx, f.g)
	}
}
func (f *fakeGhost) StoredAvatarHash() string { return f.g.Metadata.(*GhostMetadata).AvatarHash }
func (f *fakeGhost) AvatarApplied() bool      { return f.g.AvatarSet }

type fetchCall struct {
	to       string
	spanPeer string
}

type harness struct {
	s      *mucAvatarState
	now    time.Time
	ghosts map[string]*fakeGhost
	calls  []fetchCall
	// fetchFn decides the fetch result per target JID.
	fetchFn func(to string) ([]byte, string, error)
	slept   []time.Duration
}

func newHarness() *harness {
	h := &harness{now: time.Unix(1_700_000_000, 0), ghosts: map[string]*fakeGhost{}}
	h.s = &mucAvatarState{
		now: func() time.Time { return h.now },
		fetch: func(ctx context.Context, to jid.JID, spanPeer string) ([]byte, string, error) {
			h.calls = append(h.calls, fetchCall{to.String(), spanPeer})
			return h.fetchFn(to.String())
		},
		getGhost: func(ctx context.Context, occ string) (avatarGhost, error) {
			if g, ok := h.ghosts[occ]; ok {
				return g, nil
			}
			return nil, nil
		},
		pace:  func(context.Context) error { return nil },
		sleep: func(ctx context.Context, d time.Duration) error { h.slept = append(h.slept, d); return nil },
	}
	h.s.reset()
	return h
}

func (h *harness) pendingLen() int {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return len(h.s.pending)
}

func (h *harness) rec(occ string) *mucOccupant {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	return h.s.occ[occ]
}

var pngBytes = []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")

func TestDecideMUCAvatar(t *testing.T) {
	cache := &mucMXC{realHash: hashN(2)}
	tests := []struct {
		name    string
		snap    mucOccSnapshot
		ghost   string
		applied bool
		cached  *mucMXC
		want    mucAvatarAction
	}{
		{"unknown", mucOccSnapshot{photo: photoUnknown}, hashN(1), true, nil, mucActSkipUnset},
		{"none with stored hash", mucOccSnapshot{photo: photoNone}, hashN(1), true, nil, mucActRemove},
		{"none without stored hash", mucOccSnapshot{photo: photoNone}, "", true, nil, mucActDone},
		{"same hash", mucOccSnapshot{photo: photoHash, hash: hashN(1)}, hashN(1), true, nil, mucActDone},
		{"same hash but not applied", mucOccSnapshot{photo: photoHash, hash: hashN(1)}, hashN(1), false, nil, mucActFetch},
		{"new hash", mucOccSnapshot{photo: photoHash, hash: hashN(2)}, hashN(1), true, nil, mucActFetch},
		{"cache hit", mucOccSnapshot{photo: photoHash, hash: hashN(2)}, hashN(1), true, cache, mucActApplyMXC},
	}
	for _, tc := range tests {
		if got := decideMUCAvatar(tc.snap, tc.ghost, tc.applied, tc.cached); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestMUCTriggers(t *testing.T) {
	h := newHarness()
	s := h.s
	// Presence without a known ghost: no enqueue.
	s.notePresence(testOcc, testRoom, hashPresence(hashN(1)), false, false)
	if h.pendingLen() != 0 {
		t.Fatal("presence without ghost must not enqueue")
	}
	// First ghost sighting flips and enqueues once.
	s.noteGhost(testOcc)
	if h.pendingLen() != 1 {
		t.Fatalf("pending %d", h.pendingLen())
	}
	s.noteGhost(testOcc)
	s.notePresence(testOcc, testRoom, hashPresence(hashN(1)), false, false)
	if h.pendingLen() != 1 || s.stats.enqueued != 1 {
		t.Fatalf("coalescing failed: %+v", s.stats)
	}
	// Presence without x:update keeps the known hash and does not enqueue anything new.
	s.notePresence(testOcc, testRoom, presenceInfo{isMUC: true}, false, false)
	if r := h.rec(testOcc); r.photo != photoHash || r.hash != hashN(1) {
		t.Fatalf("unknown presence clobbered state: %+v", r)
	}
	// doneHash suppresses; a new hash re-enqueues.
	key, _, _ := s.dequeue()
	s.markDone(key, hashN(1))
	s.notePresence(testOcc, testRoom, hashPresence(hashN(1)), false, false)
	if h.pendingLen() != 0 {
		t.Fatal("done hash must not re-enqueue")
	}
	s.notePresence(testOcc, testRoom, hashPresence(hashN(2)), false, false)
	if h.pendingLen() != 1 {
		t.Fatal("new hash must enqueue")
	}
	// failHash suppression for 6 h, then retry; a new hash retries immediately.
	key, _, _ = s.dequeue()
	s.markFail(key, hashN(2))
	s.notePresence(testOcc, testRoom, hashPresence(hashN(2)), false, false)
	if h.pendingLen() != 0 {
		t.Fatal("failed hash must back off")
	}
	h.now = h.now.Add(5 * time.Hour)
	s.notePresence(testOcc, testRoom, hashPresence(hashN(2)), false, false)
	if h.pendingLen() != 0 {
		t.Fatal("still within 6h")
	}
	h.now = h.now.Add(2 * time.Hour)
	s.notePresence(testOcc, testRoom, hashPresence(hashN(2)), false, false)
	if h.pendingLen() != 1 {
		t.Fatal("must retry after 6h")
	}
	key, _, _ = s.dequeue()
	s.markFail(key, hashN(2))
	s.notePresence(testOcc, testRoom, hashPresence(hashN(3)), false, false)
	if h.pendingLen() != 1 {
		t.Fatal("different hash retries immediately")
	}
}

func TestMUCGhostBeforePresence(t *testing.T) {
	h := newHarness()
	h.s.noteGhost(testOcc) // spoke before we saw its presence: no photo info yet
	if h.pendingLen() != 0 {
		t.Fatal("unknown photo must not enqueue")
	}
	h.s.notePresence(testOcc, testRoom, hashPresence(hashN(1)), false, false)
	if h.pendingLen() != 1 {
		t.Fatal("presence after ghost must enqueue")
	}
}

func TestMUCUnavailableAndLeave(t *testing.T) {
	h := newHarness()
	s := h.s
	s.notePresence(testOcc, testRoom, hashPresence(hashN(1)), false, false)
	s.noteGhost(testOcc)
	if h.pendingLen() != 1 {
		t.Fatal("setup")
	}
	// unavailable (also what a 303 nick change looks like) deletes record and pending job.
	s.notePresence(testOcc, testRoom, presenceInfo{isMUC: true, newNick: "al", statusCodes: []string{"303"}}, true, false)
	if h.rec(testOcc) != nil || h.pendingLen() != 0 {
		t.Fatal("unavailable must delete record and pending job")
	}
	if _, _, ok := s.dequeue(); ok {
		t.Fatal("dropped job must not dequeue")
	}
	// LeaveMUC clears the room only.
	other := "other@muc.example.com/bob"
	s.notePresence(testOcc, testRoom, hashPresence(hashN(1)), false, false)
	s.notePresence(other, "other@muc.example.com", hashPresence(hashN(1)), false, false)
	s.clearRoom(testRoom)
	if h.rec(testOcc) != nil || h.rec(other) == nil {
		t.Fatal("clearRoom scope wrong")
	}
}

func TestMUCOccupantCap(t *testing.T) {
	h := newHarness()
	s := h.s
	for i := 0; i < mucOccupantCap; i++ {
		s.notePresence(fmt.Sprintf("room@muc.example.com/n%d", i), testRoom, presenceInfo{photo: photoNone}, false, false)
	}
	if !s.notePresence("room@muc.example.com/extra", testRoom, presenceInfo{}, false, false) {
		t.Error("first overflow must report capReached")
	}
	if s.notePresence("room@muc.example.com/extra2", testRoom, presenceInfo{}, false, false) {
		t.Error("only the first overflow reports")
	}
	if h.rec("room@muc.example.com/extra") != nil {
		t.Error("over-cap occupant must be ignored")
	}
	if s.noteGhost("room@muc.example.com/extra3") {
		t.Error("overflow already logged")
	}
}

func TestMUCOwnSkipped(t *testing.T) {
	x := &xmppClient{}
	x.mucOurOccupantJID = map[string]string{testRoom: "room@muc.example.com/me"}
	x.noteMUCGhost("room@muc.example.com/me") // would be hasGhost; irrelevant for own presence
	x.mucAv.reset()
	x.noteMUCPresence("room@muc.example.com/me", testRoom, "", hashPresence(hashN(1)))
	x.noteMUCPresence("room@muc.example.com/other", testRoom, "", presenceInfo{isMUC: true, photo: photoHash, hash: hashN(1), statusCodes: []string{"110"}})
	x.noteMUCPresence(testOcc, testRoom, "", hashPresence(hashN(1)))
	x.mucAv.mu.Lock()
	defer x.mucAv.mu.Unlock()
	if len(x.mucAv.occ) != 1 || x.mucAv.occ[testOcc] == nil {
		t.Fatalf("own presence leaked into the table: %d entries", len(x.mucAv.occ))
	}
}

func TestMUCJoinStormNoJobs(t *testing.T) {
	h := newHarness()
	for i := 0; i < 500; i++ {
		h.s.notePresence(fmt.Sprintf("room@muc.example.com/n%d", i), testRoom, hashPresence(hashN(i+1)), false, false)
	}
	if h.pendingLen() != 0 || h.s.runOnce(context.Background()) || len(h.calls) != 0 {
		t.Fatal("join storm must cost nothing beyond table entries")
	}
}

func TestMUCQueueCapAndCoalesce(t *testing.T) {
	h := newHarness()
	s := h.s
	for i := 0; i < mucAvatarMaxPending+10; i++ {
		occ := fmt.Sprintf("room@muc.example.com/n%d", i)
		s.notePresence(occ, testRoom, hashPresence(hashN(1)), false, false)
		s.noteGhost(occ)
	}
	if h.pendingLen() != mucAvatarMaxPending || s.stats.droppedFull != 10 {
		t.Fatalf("pending %d stats %+v", h.pendingLen(), s.stats)
	}
	// Latest record state wins at dequeue time.
	h2 := newHarness()
	h2.s.noteGhost(testOcc)
	h2.s.notePresence(testOcc, testRoom, hashPresence(hashN(1)), false, false)
	h2.s.notePresence(testOcc, testRoom, hashPresence(hashN(2)), false, false)
	_, snap, ok := h2.s.dequeue()
	if !ok || snap.hash != hashN(2) || h2.s.stats.coalesced != 1 {
		t.Fatalf("snap %+v stats %+v", snap, h2.s.stats)
	}
}

func TestMUCQueueCompaction(t *testing.T) {
	h := newHarness()
	s := h.s
	for i := 0; i < 5*mucAvatarMaxPending; i++ {
		occ := testOcc
		s.notePresence(occ, testRoom, hashPresence(hashN(i+1)), false, false)
		s.noteGhost(occ)
		s.notePresence(occ, testRoom, presenceInfo{}, true, false) // delete again
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) > 2*mucAvatarMaxPending {
		t.Fatalf("queue grew to %d", len(s.queue))
	}
}

func queueOne(h *harness, occ string, p presenceInfo) {
	h.s.notePresence(occ, bareOf(occ), p, false, false)
	h.s.noteGhost(occ)
}

func TestWorkerFetchApply(t *testing.T) {
	h := newHarness()
	adv := hashN(1)
	real := hashN(9) // server re-encodes: real hash differs from the advertised one
	h.ghosts[testOcc] = newFakeGhost("", true)
	h.fetchFn = func(string) ([]byte, string, error) { return pngBytes, real, nil }
	queueOne(h, testOcc, hashPresence(adv))
	if !h.s.runOnce(context.Background()) {
		t.Fatal("no job")
	}
	if len(h.calls) != 1 || h.calls[0].to != testOcc || h.calls[0].spanPeer != testOcc {
		t.Fatalf("calls %+v", h.calls)
	}
	g := h.ghosts[testOcc]
	if len(g.updates) != 1 {
		t.Fatalf("updates %d", len(g.updates))
	}
	av := g.updates[0].Avatar
	if string(av.ID) != "sha1:"+real || av.Remove {
		t.Fatalf("avatar %+v", av)
	}
	// Get returns captured bytes and never calls the fetcher.
	before := len(h.calls)
	data, err := av.Get(context.Background())
	if err != nil || string(data) != string(pngBytes) || len(h.calls) != before {
		t.Fatal("Get must return captured bytes without I/O")
	}
	m := g.g.Metadata.(*GhostMetadata)
	if m.AvatarHash != real || m.AvatarFetchedAt.Time.IsZero() {
		t.Fatalf("metadata %+v", m)
	}
	if e, ok := h.s.lookupMXC(adv); !ok || e.realHash != real {
		t.Fatalf("mxc cache missing advertised key: %+v", e)
	}
	if _, ok := h.s.lookupMXC(real); !ok {
		t.Fatal("mxc cache missing real key")
	}
	// Mismatch does not loop: same advertised hash again enqueues nothing.
	h.s.notePresence(testOcc, testRoom, hashPresence(adv), false, false)
	if h.pendingLen() != 0 {
		t.Fatal("hash mismatch must not loop")
	}
}

func TestWorkerCacheHitAndSameHash(t *testing.T) {
	h := newHarness()
	h.s.putMXC(mucMXC{realHash: hashN(5), mxc: "mxc://example.com/x", sha256: [32]byte{7}}, hashN(5))
	h.ghosts[testOcc] = newFakeGhost("", true)
	h.fetchFn = func(string) ([]byte, string, error) { t.Fatal("no IQ expected"); return nil, "", nil }
	queueOne(h, testOcc, hashPresence(hashN(5)))
	h.s.runOnce(context.Background())
	u := h.ghosts[testOcc].updates
	if len(u) != 1 || u[0].Avatar.MXC != "mxc://example.com/x" || u[0].Avatar.Hash != [32]byte{7} || u[0].Avatar.Get != nil || string(u[0].Avatar.ID) != "sha1:"+hashN(5) {
		t.Fatalf("cache hit avatar wrong: %+v", u)
	}
	// Same hash as the stored one: no IQ and no update.
	other := "room@muc.example.com/bob"
	h.ghosts[other] = newFakeGhost(hashN(6), true)
	queueOne(h, other, hashPresence(hashN(6)))
	h.s.runOnce(context.Background())
	if len(h.ghosts[other].updates) != 0 {
		t.Fatal("unchanged hash must not update")
	}
	if r := h.rec(other); r.doneHash != hashN(6) {
		t.Fatal("unchanged must mark done")
	}
}

func TestWorkerRemove(t *testing.T) {
	h := newHarness()
	h.ghosts[testOcc] = newFakeGhost(hashN(1), true)
	h.fetchFn = func(string) ([]byte, string, error) { t.Fatal("no IQ expected"); return nil, "", nil }
	queueOne(h, testOcc, presenceInfo{isMUC: true, photo: photoNone})
	h.s.runOnce(context.Background())
	u := h.ghosts[testOcc].updates
	if len(u) != 1 || !u[0].Avatar.Remove || u[0].Avatar.ID != "" {
		t.Fatalf("remove avatar wrong: %+v", u)
	}
	if h.ghosts[testOcc].g.Metadata.(*GhostMetadata).AvatarHash != "" {
		t.Fatal("hash must clear")
	}
}

func TestWorkerFallbackToRealJID(t *testing.T) {
	h := newHarness()
	h.ghosts[testOcc] = newFakeGhost("", true)
	h.fetchFn = func(to string) ([]byte, string, error) {
		if to == testOcc {
			return nil, "", stanza.Error{Type: stanza.Cancel, Condition: stanza.Forbidden}
		}
		return pngBytes, hashN(4), nil
	}
	p := hashPresence(hashN(4))
	p.itemJID = "alice@example.com"
	queueOne(h, testOcc, p)
	h.s.runOnce(context.Background())
	if len(h.calls) != 2 || h.calls[0].to != testOcc || h.calls[1].to != "alice@example.com" {
		t.Fatalf("calls %+v", h.calls)
	}
	if h.calls[1].spanPeer != testOcc {
		t.Fatal("real JID must stay off the span")
	}
	if len(h.ghosts[testOcc].updates) != 1 {
		t.Fatal("fallback success must apply")
	}
	// No real JID known: no fallback.
	h2 := newHarness()
	h2.ghosts[testOcc] = newFakeGhost("", true)
	h2.fetchFn = func(string) ([]byte, string, error) { return nil, "", errors.New("boom") }
	queueOne(h2, testOcc, hashPresence(hashN(4)))
	h2.s.runOnce(context.Background())
	if len(h2.calls) != 1 {
		t.Fatalf("calls %+v", h2.calls)
	}
}

func TestWorkerRejectedIsDone(t *testing.T) {
	h := newHarness()
	h.ghosts[testOcc] = newFakeGhost("", true)
	h.fetchFn = func(string) ([]byte, string, error) {
		return nil, "", fmt.Errorf("%w: type text/plain", errAvatarRejected)
	}
	p := hashPresence(hashN(4))
	p.itemJID = "alice@example.com"
	queueOne(h, testOcc, p)
	h.s.runOnce(context.Background())
	if len(h.calls) != 1 {
		t.Fatal("rejection must not fall back")
	}
	h.s.notePresence(testOcc, testRoom, hashPresence(hashN(4)), false, false)
	if h.pendingLen() != 0 || h.s.stats.rejected != 1 {
		t.Fatal("rejected hash must be marked done")
	}
}

func TestWorkerWaitErrorPauses(t *testing.T) {
	h := newHarness()
	h.ghosts[testOcc] = newFakeGhost("", true)
	other := "room@muc.example.com/bob"
	h.ghosts[other] = newFakeGhost("", true)
	h.fetchFn = func(to string) ([]byte, string, error) {
		if to == testOcc {
			return nil, "", stanza.Error{Type: stanza.Wait, Condition: stanza.ResourceConstraint}
		}
		return pngBytes, hashN(8), nil
	}
	queueOne(h, testOcc, hashPresence(hashN(4)))
	queueOne(h, other, hashPresence(hashN(8)))
	h.s.runOnce(context.Background())
	if len(h.slept) != 0 {
		t.Fatal("no pause before the error")
	}
	h.s.runOnce(context.Background())
	if len(h.slept) != 1 || h.slept[0] != mucAvatarWaitPause {
		t.Fatalf("slept %v", h.slept)
	}
}

func TestWorkerCancelErrorsDisableRoom(t *testing.T) {
	h := newHarness()
	h.fetchFn = func(string) ([]byte, string, error) {
		return nil, "", stanza.Error{Type: stanza.Cancel, Condition: stanza.ServiceUnavailable}
	}
	for i := 0; i < mucAvatarRoomErrMax; i++ {
		occ := fmt.Sprintf("room@muc.example.com/n%d", i)
		h.ghosts[occ] = newFakeGhost("", true)
		queueOne(h, occ, hashPresence(hashN(i+1)))
		h.s.runOnce(context.Background())
	}
	if len(h.calls) != mucAvatarRoomErrMax {
		t.Fatalf("calls %d", len(h.calls))
	}
	occ := "room@muc.example.com/late"
	h.ghosts[occ] = newFakeGhost("", true)
	queueOne(h, occ, hashPresence(hashN(99)))
	if h.pendingLen() != 0 {
		t.Fatal("disabled room must not enqueue")
	}
	// A different room is unaffected.
	occ2 := "other@muc.example.com/x"
	h.ghosts[occ2] = newFakeGhost("", true)
	queueOne(h, occ2, hashPresence(hashN(98)))
	if h.pendingLen() != 1 {
		t.Fatal("other room must still work")
	}
}

func TestWorkerNilGhostClearsFlag(t *testing.T) {
	h := newHarness()
	h.fetchFn = func(string) ([]byte, string, error) { t.Fatal("no IQ expected"); return nil, "", nil }
	queueOne(h, testOcc, hashPresence(hashN(1)))
	h.s.runOnce(context.Background())
	if h.rec(testOcc).hasGhost {
		t.Fatal("deleted ghost must clear hasGhost")
	}
}

func TestWorkerAvatarNotSetNotDone(t *testing.T) {
	h := newHarness()
	g := newFakeGhost("", true)
	g.failSet = true
	h.ghosts[testOcc] = g
	h.fetchFn = func(string) ([]byte, string, error) { return pngBytes, hashN(1), nil }
	queueOne(h, testOcc, hashPresence(hashN(1)))
	h.s.runOnce(context.Background())
	if h.rec(testOcc).doneHash != "" {
		t.Fatal("failed SetAvatarURL must not mark done")
	}
}

func TestWorkerRace(t *testing.T) {
	h := newHarness()
	var mu sync.Mutex
	h.fetchFn = func(string) ([]byte, string, error) { return pngBytes, hashN(1), nil }
	h.s.fetch = func(ctx context.Context, to jid.JID, spanPeer string) ([]byte, string, error) {
		mu.Lock()
		defer mu.Unlock()
		return pngBytes, hashN(1), nil
	}
	for i := 0; i < 50; i++ {
		occ := fmt.Sprintf("room@muc.example.com/n%d", i)
		h.ghosts[occ] = newFakeGhost("", true)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.s.runWorker(ctx); close(done) }()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			occ := fmt.Sprintf("room@muc.example.com/n%d", i)
			h.s.notePresence(occ, testRoom, hashPresence(hashN(1)), false, false)
			h.s.noteGhost(occ)
			h.s.notePresence(occ, testRoom, hashPresence(hashN(2)), false, false)
		}(i)
	}
	wg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.s.mu.Lock()
		n := len(h.s.pending)
		h.s.mu.Unlock()
		if n == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}

func TestGetUserInfoMUCOccupantHasNoAvatar(t *testing.T) {
	x := &xmppClient{}
	g := &bridgev2.Ghost{Ghost: &database.Ghost{
		ID:       "room@muc.example.com/alice",
		Metadata: &GhostMetadata{AvatarHash: hashN(1)},
	}}
	info, err := x.getUserInfo(context.Background(), g)
	if err != nil || info == nil || info.Name == nil || *info.Name != "alice" {
		t.Fatalf("info %+v err %v", info, err)
	}
	if info.Avatar != nil {
		t.Fatal("MUC occupants must keep a nil Avatar (the worker is the sole writer)")
	}
}
