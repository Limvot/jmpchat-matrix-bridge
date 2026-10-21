package connector

import (
	"context"
	"errors"
	"sync"

	"mellium.im/xmpp/disco"
	"mellium.im/xmpp/jid"
)

// Per-session bookmark state of one login: the capability gate, the bookmark cache and the budgets.
// Everything is reset by Connect (bookmarkState.reset); PEP is per account, so each xmppClient has its own.

// bookmarkState is the zero-value-usable state. mu guards every field except writeMu and the hooks.
type bookmarkState struct {
	mu sync.Mutex
	// gateKnown/support: the outcome of the disco#info check of our own account (once per session).
	gateKnown  bool
	support    bookmarkSupport
	gateLogged bool
	// writeOff: a publish failed in a way that means "do not write again this session" (plan 0.2).
	writeOff bool
	// cache holds the server's bookmarks keyed by bookmarkKey. It is NEVER the base of a write (a fresh
	// fetch always is); fetched is true only after a successful fetch this session.
	cache   map[string]bookmarkConf
	fetched bool
	// importUsed counts the rooms imported from bookmarks this session (budget bookmarkImportMaxRooms,
	// shared by connect import, live events and bookmarks-sync).
	importUsed int
	// budgetNoticed: the "budget exhausted" notice was posted this session.
	budgetNoticed bool
	// events carries parsed PEP events from the Serve goroutine to the worker (nil until reset).
	events chan bookmarkBatch

	// writeMu serializes fetch+publish read-modify-writes (never held on the Serve goroutine).
	writeMu sync.Mutex

	// Tests only.
	discoHook   func(ctx context.Context) (disco.Info, error)
	fetchHook   func(ctx context.Context) ([]bookmarkConf, bookmarkFetchInfo, error)
	publishHook func(ctx context.Context, c bookmarkConf) error
}

// bookmarkBatch is the changes of one PEP event.
type bookmarkBatch struct {
	changes   []bookmarkChange
	truncated bool
}

// bookmarkEventBuffer is the capacity of the event channel.
const bookmarkEventBuffer = 64

// reset starts a new session.
func (s *bookmarkState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gateKnown, s.support, s.gateLogged = false, bookmarkSupport{}, false
	s.writeOff = false
	s.cache, s.fetched = nil, false
	s.importUsed, s.budgetNoticed = 0, false
	s.events = make(chan bookmarkBatch, bookmarkEventBuffer)
}

func (s *bookmarkState) eventChan() chan bookmarkBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events
}

// offer queues a batch without blocking; false when the buffer is full (or there is no session).
func (s *bookmarkState) offer(b bookmarkBatch) bool {
	s.mu.Lock()
	ch := s.events
	s.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- b:
		return true
	default:
		return false
	}
}

func (s *bookmarkState) writesDisabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeOff
}

func (s *bookmarkState) disableWrites() (first bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	first = !s.writeOff
	s.writeOff = true
	return first
}

// lookup returns the cached bookmark of a room and whether the cache is authoritative (fetched).
func (s *bookmarkState) lookup(key string) (c bookmarkConf, hit, fetched bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, hit = s.cache[key]
	return c, hit, s.fetched
}

// setCache replaces the cache with a successful fetch.
func (s *bookmarkState) setCache(bms []bookmarkConf) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache = make(map[string]bookmarkConf, len(bms))
	for _, b := range bms {
		s.cache[bookmarkKey(b.ID)] = b
	}
	s.fetched = true
}

func (s *bookmarkState) cacheSet(key string, c bookmarkConf) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache == nil {
		s.cache = map[string]bookmarkConf{}
	}
	s.cache[key] = c
}

// cacheRestore puts back the item a failed publish had replaced (nil: there was none).
func (s *bookmarkState) cacheRestore(key string, prev *bookmarkConf) {
	if prev == nil {
		s.cacheDelete(key)
		return
	}
	s.cacheSet(key, *prev)
}

func (s *bookmarkState) cacheDelete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cache, key)
}

// takeImportBudget reserves up to n rooms of the per-session import budget and returns how many it got.
func (s *bookmarkState) takeImportBudget(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	left := bookmarkImportMaxRooms - s.importUsed
	if left < 0 {
		left = 0
	}
	if n > left {
		n = left
	}
	s.importUsed += n
	return n
}

func (s *bookmarkState) importBudgetLeft() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if left := bookmarkImportMaxRooms - s.importUsed; left > 0 {
		return left
	}
	return 0
}

// markBudgetNoticed reports true the first time it is called in a session.
func (s *bookmarkState) markBudgetNoticed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := !s.budgetNoticed
	s.budgetNoticed = true
	return first
}

// bookmarkNick is the nick of the cached bookmark of a room ("" when none). Memory only.
func (x *xmppClient) bookmarkNick(room string) string {
	c, _, _ := x.bm.lookup(bookmarkKey(room))
	return c.Nick
}

// bookmarkPassword is the room password of the cached bookmark ("" when none). Never log it.
func (x *xmppClient) bookmarkPassword(room string) string {
	c, _, _ := x.bm.lookup(bookmarkKey(room))
	return c.Password
}

var errBookmarksNotConnected = errors.New("not connected to XMPP")

// ownBareJID is our own account's bare JID.
func (x *xmppClient) ownBareJID() (jid.JID, error) {
	j, err := jid.Parse(string(x.loginID))
	if err != nil {
		return jid.JID{}, err
	}
	return j.Bare(), nil
}

// bookmarkDisco is the disco#info of our own account (the PEP service).
func (x *xmppClient) bookmarkDisco(ctx context.Context) (disco.Info, error) {
	if x.bm.discoHook != nil {
		return x.bm.discoHook(ctx)
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	own, err := x.ownBareJID()
	if sess == nil || err != nil {
		return disco.Info{}, errBookmarksNotConnected
	}
	return x.discoInfoTraced(ctx, sess, own)
}

// bookmarkFetch reads the whole bookmarks node (see fetchBookmarks).
func (x *xmppClient) bookmarkFetch(ctx context.Context) ([]bookmarkConf, bookmarkFetchInfo, error) {
	if x.bm.fetchHook != nil {
		return x.bm.fetchHook(ctx)
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, bookmarkFetchInfo{}, errBookmarksNotConnected
	}
	return x.fetchBookmarks(ctx, sess)
}

// ensureBookmarkGate returns the capability gate of this session, running the disco#info check the
// first time. ok is false when the check itself failed (state stays unknown, so the next call retries;
// joins and leaves are never blocked by it).
func (x *xmppClient) ensureBookmarkGate(ctx context.Context) (s bookmarkSupport, ok bool) {
	s, err := x.ensureBookmarkGateErr(ctx)
	return s, err == nil
}

// ensureBookmarkGateErr is ensureBookmarkGate returning the error of a failed check.
func (x *xmppClient) ensureBookmarkGateErr(ctx context.Context) (bookmarkSupport, error) {
	x.bm.mu.Lock()
	if x.bm.gateKnown {
		s := x.bm.support
		x.bm.mu.Unlock()
		return s, nil
	}
	x.bm.mu.Unlock()
	info, err := x.bookmarkDisco(ctx)
	if err != nil {
		if !errors.Is(err, errBookmarksNotConnected) {
			x.logFor(ctx).Warn().Err(err).Msg("Bookmarks: capability check failed (disco#info of own account); will retry")
		}
		return bookmarkSupport{}, err
	}
	s := bookmarkSupportFromInfo(info)
	x.bm.mu.Lock()
	x.bm.gateKnown, x.bm.support = true, s
	first := !x.bm.gateLogged
	x.bm.gateLogged = true
	x.bm.mu.Unlock()
	if first {
		x.logBookmarkGate(ctx, s)
	}
	return s, nil
}

// bookmarkStartupStatus is the outcome of bookmarkStartup.
type bookmarkStartupStatus int

const (
	// bookmarkStartupFetched: the bookmarks were read; the cache is filled.
	bookmarkStartupFetched bookmarkStartupStatus = iota
	// bookmarkStartupUnsupported: the server does not offer bookmark reading (final for this session).
	bookmarkStartupUnsupported
	// bookmarkStartupFailed: the gate check or the fetch failed (timeout, transient error); worth retrying.
	bookmarkStartupFailed
	// bookmarkStartupNoSession: there is no session (it ended); nothing to retry.
	bookmarkStartupNoSession
)

// bookmarkStartup is the first step of the startup coordinator (runMUCAutoRejoin): the capability gate and
// one fetch that fills the cache. Unless the status is bookmarkStartupFetched the caller behaves as before
// bookmarks existed (no skip set, no imports); on bookmarkStartupFailed it retries later (see
// retryBookmarkStartup). Each failure logs one warning.
func (x *xmppClient) bookmarkStartup(ctx context.Context) ([]bookmarkConf, bookmarkStartupStatus) {
	s, err := x.ensureBookmarkGateErr(ctx)
	if err != nil {
		if errors.Is(err, errBookmarksNotConnected) {
			return nil, bookmarkStartupNoSession
		}
		return nil, bookmarkStartupFailed
	}
	if !s.Read {
		return nil, bookmarkStartupUnsupported
	}
	bms, info, err := x.bookmarkFetch(ctx)
	if err != nil {
		if errors.Is(err, errBookmarksNotConnected) {
			return nil, bookmarkStartupNoSession
		}
		x.logFor(ctx).Warn().Err(err).Msg("Bookmarks: fetch failed; will retry")
		return nil, bookmarkStartupFailed
	}
	x.bm.setCache(bms)
	log := x.logFor(ctx)
	log.Debug().Int("count", len(bms)).Int("skipped", info.Skipped).Bool("truncated", info.Truncated).Msg("Bookmarks: fetched")
	for _, b := range bms {
		// Room JID, autojoin and nick only: never the password, name or extensions.
		log.Debug().Str("room", b.ID).Bool("autojoin", b.Autojoin).Str("nick", b.Nick).Msg("Bookmark")
	}
	return bms, bookmarkStartupFetched
}
