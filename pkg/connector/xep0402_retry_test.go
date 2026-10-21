package connector

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"mellium.im/xmpp/disco"
)

// failFirst returns a disco hook that fails the first n calls with a timeout, then reports PEP with
// publish-options; calls counts every call.
func failFirst(n int, calls *int, mu *sync.Mutex) func(context.Context) (disco.Info, error) {
	return func(context.Context) (disco.Info, error) {
		mu.Lock()
		defer mu.Unlock()
		*calls++
		if *calls <= n {
			return disco.Info{}, context.DeadlineExceeded
		}
		return pepDisco(true), nil
	}
}

func countLines(s, msg string) int { return strings.Count(s, msg) }

func TestStartupBookmarkRetryAppliesSkippedWorkOnce(t *testing.T) {
	for name, setup := range map[string]func(r *bmRig) (check func(t *testing.T)){
		"gate times out once": func(r *bmRig) func(t *testing.T) {
			var mu sync.Mutex
			calls := 0
			r.x.bm.discoHook = failFirst(1, &calls, &mu)
			return func(t *testing.T) {
				mu.Lock()
				defer mu.Unlock()
				if calls != 2 {
					t.Errorf("disco calls = %d, want 2 (startup + one retry; the gate is cached afterwards)", calls)
				}
			}
		},
		"fetch fails once": func(r *bmRig) func(t *testing.T) {
			inner := r.x.bm.fetchHook
			failed := false
			r.x.bm.fetchHook = func(ctx context.Context) ([]bookmarkConf, bookmarkFetchInfo, error) {
				r.mu.Lock()
				first := !failed
				failed = true
				r.mu.Unlock()
				if first {
					return nil, bookmarkFetchInfo{}, context.DeadlineExceeded
				}
				return inner(ctx)
			}
			return func(*testing.T) {}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := startupRig(t)
			var logs bmLogBuf
			r.x.log = zerolog.New(&logs)
			check := setup(r)
			r.x.runMUCAutoRejoin(context.Background())
			check(t)

			// The batch ran without bookmarks (B joined, no import), then the late fetch applied them:
			// B left again and marked left, C imported, F backfilled.
			if got := strings.Join(r.joins(), ","); got != roomA+"|alice,"+roomB+"|alice,"+roomF+"|alice,"+roomC+"|c-nick" {
				t.Fatalf("joins = %s", got)
			}
			r.mu.Lock()
			leaves := strings.Join(r.leaves, ",")
			leftB, leftA, leftF := r.left[roomB], r.left[roomA], r.left[roomF]
			r.mu.Unlock()
			if leaves != roomB {
				t.Fatalf("leaves = %q, want only B", leaves)
			}
			if !leftB || leftA || leftF {
				t.Fatalf("left flags: B=%v A=%v F=%v", leftB, leftA, leftF)
			}
			want := roomB + "|" + bookmarkLeftLateNotice("!jabber", roomB)
			var found bool
			for _, n := range r.noticeList() {
				if n == want {
					found = true
				}
			}
			if !found {
				t.Fatalf("notices = %q, want %q", r.noticeList(), want)
			}
			pubs := r.published()
			if len(pubs) != 1 || bookmarkKey(pubs[0].ID) != roomF {
				t.Fatalf("published = %+v, want only F's backfill", pubs)
			}
			out := logs.String()
			if n := countLines(out, `"level":"warn"`); n != 1 {
				t.Errorf("warn lines = %d, want 1 (the failed attempt):\n%s", n, out)
			}
			if n := countLines(out, "succeeded after retry"); n != 1 {
				t.Errorf("success-after-retry lines = %d, want 1:\n%s", n, out)
			}
		})
	}
}

func TestStartupBookmarkRetryGivesUp(t *testing.T) {
	r := startupRig(t)
	var logs bmLogBuf
	r.x.log = zerolog.New(&logs)
	var mu sync.Mutex
	calls := 0
	r.x.bm.discoHook = failFirst(1000, &calls, &mu)
	r.x.runMUCAutoRejoin(context.Background())

	mu.Lock()
	got := calls
	mu.Unlock()
	if want := 1 + len(bookmarkStartupRetryDelays); got != want {
		t.Fatalf("disco calls = %d, want %d", got, want)
	}
	if strings.Join(r.joins(), ",") != roomA+"|alice,"+roomB+"|alice,"+roomF+"|alice" {
		t.Fatalf("joins = %v", r.joins())
	}
	if len(r.left) != 0 || len(r.published()) != 0 || r.fetchCount() != 0 {
		t.Fatalf("left=%v published=%d fetches=%d", r.left, len(r.published()), r.fetchCount())
	}
	out := logs.String()
	if n := countLines(out, `"level":"warn"`); n != got {
		t.Errorf("warn lines = %d, want one per attempt (%d):\n%s", n, got, out)
	}
	if n := countLines(out, "kept failing"); n != 1 {
		t.Errorf("give-up lines = %d, want 1", n)
	}
}

func TestStartupBookmarkRetryStopsWithSession(t *testing.T) {
	r := startupRig(t)
	bookmarkStartupRetryDelays = []time.Duration{time.Hour}
	var mu sync.Mutex
	calls := 0
	r.x.bm.discoHook = failFirst(1000, &calls, &mu)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.x.runMUCAutoRejoin(ctx); close(done) }()
	bmWaitFor(t, "the rejoin batch", func() bool { return len(r.joins()) == 3 })
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator did not stop with the session while waiting to retry")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("disco calls = %d, want 1", calls)
	}
}

func TestStartupBookmarkRetrySkipsRoomLeftMeanwhile(t *testing.T) {
	r := startupRig(t)
	var mu sync.Mutex
	calls := 0
	r.x.bm.discoHook = failFirst(1, &calls, &mu)
	// The user leaves B explicitly while the retry is pending (first retry delay).
	inner := r.x.bm.discoHook
	r.x.bm.discoHook = func(ctx context.Context) (disco.Info, error) {
		mu.Lock()
		retry := calls >= 1
		mu.Unlock()
		if retry {
			_ = r.x.mucLeftHook(roomB, true)
		}
		return inner(ctx)
	}
	r.x.runMUCAutoRejoin(context.Background())
	for _, n := range r.noticeList() {
		if strings.HasPrefix(n, roomB+"|") {
			t.Fatalf("notice for a room the user already left: %q", n)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.leaves) != 0 {
		t.Fatalf("leaves = %v", r.leaves)
	}
}

func TestStartupBookmarkNoRetryWhenUnsupportedOrNoSession(t *testing.T) {
	for name, hook := range map[string]func(context.Context) (disco.Info, error){
		"unsupported": func(context.Context) (disco.Info, error) { return disco.Info{}, nil },
		"no session":  func(context.Context) (disco.Info, error) { return disco.Info{}, errBookmarksNotConnected },
	} {
		t.Run(name, func(t *testing.T) {
			r := startupRig(t)
			calls := 0
			r.x.bm.discoHook = func(ctx context.Context) (disco.Info, error) { calls++; return hook(ctx) }
			r.x.runMUCAutoRejoin(context.Background())
			if calls != 1 {
				t.Fatalf("disco calls = %d, want 1 (no retry)", calls)
			}
		})
	}
}
