// MUC occupant avatars (XMPP -> Matrix only), see the avatars section of the README.
//
// Signal: the XEP-0153 vcard-temp:x:update hash in an occupant's MUC presence. Presence handling
// on the mellium Serve goroutine only updates an in-memory occupant table and does a non-blocking
// enqueue (no DB, no ghost lookup, no IQ, no HTTP, no spans). A single worker goroutine per session
// does all ghost lookups, vcard-temp IQs and ghost updates. An occupant is only considered once a
// ghost for it was seen this session (it spoke), so a join storm costs table entries only.
// The worker is the sole writer of MUC occupant avatars; getUserInfo keeps returning a nil Avatar
// for MUC occupants (a stale Avatar from the Serve side could revert or corrupt the applied one).

package connector

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

const (
	mucOccupantCap       = 20000            // global cap on occupant table entries
	mucHistoryCap        = 1024             // cap on history-only entries (ghost seen, no presence seen)
	mucAvatarMinInterval = 60 * time.Second // minimum time between jobs doing work for one occupant
	mucAvatarMaxPending  = 1024             // cap on queued jobs
	mucAvatarMXCCap      = 512              // cap on the advertised/real hash -> MXC cache
	mucAvatarPace        = 250 * time.Millisecond
	mucAvatarWaitPause   = 60 * time.Second
	mucAvatarFailRetry   = 6 * time.Hour
	mucAvatarRoomErrMax  = 5
	mucAvatarStatsPeriod = 60 * time.Second
)

// mucOccupant is the per-occupant avatar record (key: occupant full JID room@muc/nick).
type mucOccupant struct {
	room     string
	photo    photoKind
	hash     string    // advertised (presence) hash, lowercased
	realJID  string    // bare, optional, never logged at info
	hasGhost bool      // a ghost for this occupant was seen this session
	live     bool      // a presence for this occupant was seen this session (else history-only)
	lastWork time.Time // when the worker last started real work (fetch/apply/remove) for it
	doneHash string    // advertised state ("none" or hash) last fully handled
	failHash string    // advertised state whose fetch failed
	failAt   time.Time
	prop     *mucProp // pending propagation from the 1:1 path (latest wins)
}

// mucProp is an avatar the 1:1 path just applied for the occupant's real JID: remove, or apply the
// already-uploaded mxc (no IQ, no upload).
type mucProp struct {
	remove bool
	mxc    mucMXC
}

// adv is the advertised state: "none", the hash, or "" when unknown.
func (o *mucOccupant) adv() string {
	switch o.photo {
	case photoNone:
		return "none"
	case photoHash:
		return o.hash
	}
	return ""
}

// mucOccSnapshot is a copy of an occupant record taken under the lock for the worker.
type mucOccSnapshot struct {
	room    string
	photo   photoKind
	hash    string
	realJID string
	prop    *mucProp
}

func (s mucOccSnapshot) adv() string { return (&mucOccupant{photo: s.photo, hash: s.hash}).adv() }

// mucMXC is a pre-uploaded avatar: the SHA-1 of the photo we hashed, and the MXC/SHA-256 the
// bridge produced when it first uploaded it.
type mucMXC struct {
	realHash string
	mxc      string
	sha256   [32]byte
}

type mucAvatarStats struct {
	enqueued, coalesced, droppedFull, fetched, applied, removed, failed, rejected int
}

// avatarGhost is the slice of *bridgev2.Ghost the worker uses (fakeable in tests).
type avatarGhost interface {
	UpdateInfo(ctx context.Context, info *bridgev2.UserInfo)
	StoredAvatarHash() string // GhostMetadata.AvatarHash (SHA-1 hex of the last applied photo)
	AvatarApplied() bool      // ghost.AvatarSet: the avatar is set on Matrix
}

type realAvatarGhost struct{ g *bridgev2.Ghost }

func (r realAvatarGhost) UpdateInfo(ctx context.Context, info *bridgev2.UserInfo) {
	r.g.UpdateInfo(ctx, info)
}

func (r realAvatarGhost) StoredAvatarHash() string {
	if m, ok := r.g.Metadata.(*GhostMetadata); ok && m != nil {
		return m.AvatarHash
	}
	return ""
}

func (r realAvatarGhost) AvatarApplied() bool { return r.g.AvatarSet }

// mucAvatarState holds the occupant table, the job queue, and the worker's state. The zero value
// is usable; reset starts a fresh session.
type mucAvatarState struct {
	mu             sync.Mutex
	occ            map[string]*mucOccupant
	queue          []string
	pending        map[string]struct{}
	wake           chan struct{}
	roomDisabled   map[string]bool
	roomCancelErrs map[string]int
	mxcByHash      map[string]mucMXC // kept across sessions
	pausedUntil    time.Time
	capLogged      bool
	histOrder      []string // keys of history-only entries, oldest first (may hold stale keys)
	histN          int      // number of live table entries that are history-only
	stats          mucAvatarStats
	lastStats      mucAvatarStats
	lastStatsAt    time.Time

	// Seams (defaults installed by the xmppClient; tests set their own).
	now      func() time.Time
	fetch    func(ctx context.Context, to jid.JID, spanPeer string) (data []byte, sha1Hex string, err error)
	getGhost func(ctx context.Context, occupant string) (avatarGhost, error)
	pace     func(ctx context.Context) error
	paceC    <-chan time.Time // current session's pacing ticker (production pace), guarded by mu
	sleep    func(ctx context.Context, d time.Duration) error
	logger   func(ctx context.Context) *zerolog.Logger
}

func (s *mucAvatarState) nowT() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *mucAvatarState) log(ctx context.Context) *zerolog.Logger {
	if s.logger != nil {
		return s.logger(ctx)
	}
	nop := zerolog.Nop()
	return &nop
}

// initLocked lazily creates the maps. Requires s.mu.
func (s *mucAvatarState) initLocked() {
	if s.occ == nil {
		s.occ = make(map[string]*mucOccupant)
		s.pending = make(map[string]struct{})
		s.roomDisabled = make(map[string]bool)
		s.roomCancelErrs = make(map[string]int)
	}
	if s.wake == nil {
		s.wake = make(chan struct{}, 1)
	}
	if s.mxcByHash == nil {
		s.mxcByHash = make(map[string]mucMXC)
	}
}

// reset starts a fresh session: a rejoin re-sends every presence, so the table, queue and
// per-room state are dropped. The MXC cache survives (MXC URIs stay valid).
func (s *mucAvatarState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.occ = nil
	s.queue = nil
	s.pending = nil
	s.roomDisabled = nil
	s.roomCancelErrs = nil
	s.pausedUntil = time.Time{}
	s.capLogged = false
	s.histOrder = nil
	s.histN = 0
	s.initLocked()
}

// shouldEnqueueLocked is the single enqueue condition (see the plan, section 2.2).
func (s *mucAvatarState) shouldEnqueueLocked(rec *mucOccupant) bool {
	if !rec.hasGhost || rec.photo == photoUnknown || s.roomDisabled[rec.room] {
		return false
	}
	adv := rec.adv()
	if adv == rec.doneHash {
		return false
	}
	if !rec.lastWork.IsZero() && s.nowT().Sub(rec.lastWork) < mucAvatarMinInterval {
		return false // a later presence or message after the interval re-triggers
	}
	if adv == rec.failHash && s.nowT().Sub(rec.failAt) < mucAvatarFailRetry {
		return false
	}
	return true
}

// enqueueLocked adds key to the queue (coalescing). Never blocks.
func (s *mucAvatarState) enqueueLocked(key string) {
	if _, ok := s.pending[key]; ok {
		s.stats.coalesced++
		return
	}
	if len(s.pending) >= mucAvatarMaxPending {
		s.stats.droppedFull++
		return
	}
	if len(s.queue) >= 2*mucAvatarMaxPending { // compact stale keys left by deletions
		q := s.queue[:0]
		for _, k := range s.queue {
			if _, ok := s.pending[k]; ok {
				q = append(q, k)
			}
		}
		s.queue = q
	}
	s.pending[key] = struct{}{}
	s.queue = append(s.queue, key)
	s.stats.enqueued++
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// deleteOccLocked removes an occupant record and its pending job. Requires s.mu.
func (s *mucAvatarState) deleteOccLocked(key string) {
	if rec := s.occ[key]; rec != nil && !rec.live {
		s.histN--
	}
	delete(s.occ, key)
	delete(s.pending, key)
}

// evictHistoryLocked drops the oldest history-only entry (a sender that spoke without a presence
// we saw, e.g. MAM/history replay). It reports whether one was dropped. Requires s.mu.
func (s *mucAvatarState) evictHistoryLocked() bool {
	for len(s.histOrder) > 0 {
		k := s.histOrder[0]
		s.histOrder = s.histOrder[1:]
		if rec := s.occ[k]; rec != nil && !rec.live {
			s.deleteOccLocked(k)
			return true
		}
	}
	return false
}

// notePresence records an occupant's MUC presence. unavailable covers leave, kick, ban and
// nick change (303): the record and its pending job are dropped. own is our own occupant.
// Returns true when the occupant table is full and the entry was ignored for the first time.
func (s *mucAvatarState) notePresence(occ, room string, info presenceInfo, unavailable, own bool) (capReached bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	if own {
		if unavailable {
			s.clearRoomLocked(room)
		}
		return false
	}
	if unavailable {
		s.deleteOccLocked(occ)
		return false
	}
	rec := s.occ[occ]
	if rec == nil {
		// Live occupants win over history-only entries when the table is full.
		if len(s.occ) >= mucOccupantCap && !s.evictHistoryLocked() {
			first := !s.capLogged
			s.capLogged = true
			return first
		}
		rec = &mucOccupant{room: room, live: true}
		s.occ[occ] = rec
	} else if !rec.live {
		rec.live = true
		s.histN--
	}
	// A presence without a usable photo element (unknown) says nothing: keep what we knew.
	if info.photo != photoUnknown {
		rec.photo = info.photo
		rec.hash = info.hash
	}
	if info.itemJID != "" {
		rec.realJID = info.itemJID
	}
	if s.shouldEnqueueLocked(rec) {
		s.enqueueLocked(occ)
	}
	return false
}

// noteGhost records that a ghost for the occupant was seen this session (it spoke) and, if that
// flips the flag, applies the enqueue condition. Memory only.
func (s *mucAvatarState) noteGhost(occ string) (capReached bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	rec := s.occ[occ]
	if rec == nil {
		// History-only entries are bounded: evict the oldest one instead of growing.
		if s.histN >= mucHistoryCap {
			s.evictHistoryLocked()
		}
		if len(s.occ) >= mucOccupantCap && !s.evictHistoryLocked() {
			first := !s.capLogged
			s.capLogged = true
			return first
		}
		rec = &mucOccupant{room: bareOf(occ)}
		s.occ[occ] = rec
		s.histN++
		if len(s.histOrder) >= 4*mucHistoryCap { // drop stale keys
			q := s.histOrder[:0]
			for _, k := range s.histOrder {
				if r := s.occ[k]; r != nil && !r.live {
					q = append(q, k)
				}
			}
			s.histOrder = q
		}
		s.histOrder = append(s.histOrder, occ)
	}
	if rec.hasGhost {
		return false
	}
	rec.hasGhost = true
	if s.shouldEnqueueLocked(rec) {
		s.enqueueLocked(occ)
	}
	return false
}

// clearRoom drops every occupant record and pending job of a room (LeaveMUC).
func (s *mucAvatarState) clearRoom(room string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearRoomLocked(room)
}

func (s *mucAvatarState) clearRoomLocked(room string) {
	for k, rec := range s.occ {
		if rec.room == room {
			s.deleteOccLocked(k)
		}
	}
}

// dequeue pops the next live job and snapshots its record; ok is false when the queue is empty.
func (s *mucAvatarState) dequeue() (key string, snap mucOccSnapshot, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) > 0 {
		k := s.queue[0]
		s.queue = s.queue[1:]
		if _, live := s.pending[k]; !live {
			continue // dropped (unavailable) or duplicate
		}
		delete(s.pending, k)
		rec := s.occ[k]
		if rec == nil {
			continue
		}
		snap = mucOccSnapshot{room: rec.room, photo: rec.photo, hash: rec.hash, realJID: rec.realJID, prop: rec.prop}
		rec.prop = nil
		return k, snap, true
	}
	return "", mucOccSnapshot{}, false
}

func (s *mucAvatarState) markDone(key, adv string) {
	s.mu.Lock()
	if rec := s.occ[key]; rec != nil {
		rec.doneHash = adv
	}
	s.mu.Unlock()
}

// markWork stamps the start of real work (fetch/apply/remove) for the per-occupant minimum interval.
func (s *mucAvatarState) markWork(key string) {
	s.mu.Lock()
	if rec := s.occ[key]; rec != nil {
		rec.lastWork = s.nowT()
	}
	s.mu.Unlock()
}

func (s *mucAvatarState) markFail(key, adv string) {
	s.mu.Lock()
	if rec := s.occ[key]; rec != nil {
		rec.failHash = adv
		rec.failAt = s.nowT()
	}
	s.mu.Unlock()
}

func (s *mucAvatarState) clearHasGhost(key string) {
	s.mu.Lock()
	if rec := s.occ[key]; rec != nil {
		rec.hasGhost = false
	}
	s.mu.Unlock()
}

func (s *mucAvatarState) lookupMXC(adv string) (mucMXC, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.mxcByHash[adv]
	return e, ok
}

func (s *mucAvatarState) putMXC(e mucMXC, keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	if len(s.mxcByHash)+len(keys) > mucAvatarMXCCap {
		s.mxcByHash = make(map[string]mucMXC)
	}
	for _, k := range keys {
		s.mxcByHash[k] = e
	}
}

func (s *mucAvatarState) bump(f func(*mucAvatarStats)) {
	s.mu.Lock()
	f(&s.stats)
	s.mu.Unlock()
}

// mucAvatarAction is the worker's decision for one job.
type mucAvatarAction int

const (
	mucActDone      mucAvatarAction = iota // nothing to do (mark handled)
	mucActRemove                           // remove the ghost avatar, no IQ
	mucActApplyMXC                         // apply a pre-uploaded avatar, no IQ
	mucActFetch                            // fetch the vCard photo
	mucActSkipUnset                        // photo unknown: nothing, never remove
)

// decideMUCAvatar is the pure decision: ghostHash is the SHA-1 stored for the ghost's current
// avatar, ghostApplied whether that avatar is actually set on Matrix, cached the pre-uploaded
// avatar for the advertised hash (nil if none).
func decideMUCAvatar(snap mucOccSnapshot, ghostHash string, ghostApplied bool, cached *mucMXC) mucAvatarAction {
	switch snap.photo {
	case photoNone:
		if ghostHash != "" {
			return mucActRemove
		}
		return mucActDone
	case photoHash:
		if ghostApplied && snap.hash == ghostHash {
			return mucActDone
		}
		if cached != nil {
			return mucActApplyMXC
		}
		return mucActFetch
	}
	return mucActSkipUnset
}

// runOnce processes at most one job. It reports whether a job was dequeued.
func (s *mucAvatarState) runOnce(ctx context.Context) bool {
	key, snap, ok := s.dequeue()
	if !ok {
		return false
	}
	s.process(ctx, key, snap)
	return true
}

// runWorker is the single per-session worker; it exits when ctx (the session context) ends.
func (s *mucAvatarState) runWorker(ctx context.Context) {
	s.mu.Lock()
	s.initLocked()
	wake := s.wake
	s.mu.Unlock()
	for ctx.Err() == nil {
		if s.runOnce(ctx) {
			s.flushStats(ctx)
			continue
		}
		s.flushStats(ctx)
		t := time.NewTimer(mucAvatarStatsPeriod)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-wake:
			t.Stop()
		case <-t.C:
		}
	}
}

// flushStats logs the counters at most once per mucAvatarStatsPeriod and only if they changed.
func (s *mucAvatarState) flushStats(ctx context.Context) {
	s.mu.Lock()
	now := s.nowT()
	if now.Sub(s.lastStatsAt) < mucAvatarStatsPeriod || s.stats == s.lastStats {
		s.mu.Unlock()
		return
	}
	st := s.stats
	s.lastStats = st
	s.lastStatsAt = now
	s.mu.Unlock()
	s.log(ctx).Debug().
		Int("enqueued", st.enqueued).Int("coalesced", st.coalesced).Int("dropped_full", st.droppedFull).
		Int("fetched", st.fetched).Int("applied", st.applied).Int("removed", st.removed).
		Int("failed", st.failed).Int("rejected", st.rejected).
		Msg("MUC avatar stats")
}

func (s *mucAvatarState) doPace(ctx context.Context) error {
	if s.pace != nil {
		return s.pace(ctx)
	}
	return nil
}

func (s *mucAvatarState) doSleep(ctx context.Context, d time.Duration) error {
	if s.sleep != nil {
		return s.sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// avatarErrInfo extracts the stanza error type and condition from a fetch error.
func avatarErrInfo(err error) (typ, cond string) {
	var se stanza.Error
	if errors.As(err, &se) {
		return string(se.Type), string(se.Condition)
	}
	return "", ""
}

// process handles one job on the worker goroutine (never on Serve).
func (s *mucAvatarState) process(ctx context.Context, key string, snap mucOccSnapshot) {
	if snap.prop != nil {
		s.processProp(ctx, key, snap)
		return
	}
	if snap.photo == photoUnknown {
		return
	}
	adv := snap.adv()
	log := s.log(ctx)
	ghost, err := s.getGhost(ctx, key)
	if err != nil {
		log.Debug().Err(err).Str("occupant", key).Msg("MUC avatar: ghost lookup failed")
		return
	}
	if ghost == nil {
		s.clearHasGhost(key)
		log.Debug().Str("occupant", key).Msg("MUC avatar: no ghost for occupant, skipped")
		return
	}
	ghostHash := ghost.StoredAvatarHash()
	var cached *mucMXC
	if snap.photo == photoHash {
		if e, ok := s.lookupMXC(snap.hash); ok {
			cached = &e
		}
	}
	act := decideMUCAvatar(snap, ghostHash, ghost.AvatarApplied(), cached)
	switch act {
	case mucActSkipUnset:
		return
	case mucActDone:
		s.markDone(key, adv)
		// Unchanged since an earlier session or job: no IQ and no span, but say so, otherwise a
		// processed job is indistinguishable from a stuck worker in the logs.
		log.Debug().Str("occupant", key).Str("photo", snap.photo.String()).Msg("MUC avatar unchanged, nothing to do")
		return
	}
	s.markWork(key)
	if act == mucActFetch {
		s.mu.Lock()
		disabled := s.roomDisabled[snap.room]
		s.mu.Unlock()
		if disabled {
			return
		}
	}

	// A job that does real work gets its own root span; ctx is the session context (no span).
	ctx, span := telemetry.Start(ctx, "xmpp.muc_avatar.refresh")
	defer span.End()
	log = s.log(ctx)
	setOutcome := func(source, outcome string) {
		if span.IsRecording() {
			span.SetAttributes(attribute.String("xmpp.avatar.source", source), attribute.String("xmpp.avatar.outcome", outcome))
		}
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", snap.room), attribute.String("xmpp.peer.jid", key))
	}

	switch act {
	case mucActRemove:
		applyGhostAvatar(ctx, ghost, &bridgev2.Avatar{Remove: true}, "", nil)
		s.bump(func(st *mucAvatarStats) { st.removed++ })
		setOutcome("occupant", "removed")
		log.Debug().Str("occupant", key).Msg("MUC occupant has no avatar, removed ghost avatar")
		if ghost.AvatarApplied() {
			s.markDone(key, adv)
		}
	case mucActApplyMXC:
		avatar := &bridgev2.Avatar{ID: avatarIDFromHash(cached.realHash), MXC: id.ContentURIString(cached.mxc), Hash: cached.sha256}
		applyGhostAvatar(ctx, ghost, avatar, cached.realHash, nil)
		s.bump(func(st *mucAvatarStats) { st.applied++ })
		setOutcome("cache", "changed")
		log.Debug().Str("occupant", key).Str("photo_hash", cached.realHash).Msg("Applied cached MUC avatar")
		if ghost.AvatarApplied() {
			s.markDone(key, adv)
		}
	case mucActFetch:
		s.fetchAndApply(ctx, key, snap, adv, ghost, ghostHash, setOutcome)
	}
}

// propagateContact is called (from any goroutine, memory only, no IQ) after the 1:1 path applied a
// new avatar (mxc != nil, SHA-1 realHash) or a removal (mxc == nil) for the real bare JID. Every
// occupant of this session whose room-reported real JID matches and which has a ghost gets a
// propagation job that applies the cached avatar. The real JID comes from the room's item jid (the
// room sets it; occupants cannot claim one). The 60 s minimum interval does not apply: no IQ is
// made. The occupant's advertised state is marked handled so a later stale room presence (the
// client did not resend room presence) enqueues nothing and cannot revert the ghost; a different
// advertised hash later still goes through the normal path.
func (s *mucAvatarState) propagateContact(realBare string, mxc *mucMXC) (matched int) {
	if realBare == "" {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	if mxc != nil {
		if len(s.mxcByHash)+1 > mucAvatarMXCCap {
			s.mxcByHash = make(map[string]mucMXC)
		}
		s.mxcByHash[mxc.realHash] = *mxc
	}
	for key, rec := range s.occ {
		if !rec.hasGhost || rec.realJID == "" || !strings.EqualFold(rec.realJID, realBare) {
			continue
		}
		p := &mucProp{remove: mxc == nil}
		if mxc != nil {
			p.mxc = *mxc
		}
		rec.prop = p
		s.enqueueLocked(key)
		if _, ok := s.pending[key]; !ok {
			rec.prop = nil // queue full: do not leave a stale propagation behind
			continue
		}
		rec.doneHash = rec.adv()
		matched++
	}
	return matched
}

// processProp applies a propagation job: no IQ, no fetch, no upload. A room presence with a new
// advertised state that coalesced into this job is requeued for the normal path afterwards.
func (s *mucAvatarState) processProp(ctx context.Context, key string, snap mucOccSnapshot) {
	defer s.requeueIfChanged(key)
	log := s.log(ctx)
	ghost, err := s.getGhost(ctx, key)
	if err != nil {
		log.Debug().Err(err).Str("occupant", key).Msg("MUC avatar: ghost lookup failed")
		return
	}
	if ghost == nil {
		s.clearHasGhost(key)
		return
	}
	p := snap.prop
	ghostHash := ghost.StoredAvatarHash()
	if p.remove {
		if ghostHash == "" {
			return
		}
	} else if ghostHash == p.mxc.realHash && ghost.AvatarApplied() {
		return
	}
	ctx, span := telemetry.Start(ctx, "xmpp.muc_avatar.refresh")
	defer span.End()
	log = s.log(ctx)
	outcome := "changed"
	if p.remove {
		outcome = "removed"
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", snap.room), attribute.String("xmpp.peer.jid", key),
			attribute.String("xmpp.avatar.source", "contact"), attribute.String("xmpp.avatar.outcome", outcome))
	}
	if p.remove {
		applyGhostAvatar(ctx, ghost, &bridgev2.Avatar{Remove: true}, "", nil)
		s.bump(func(st *mucAvatarStats) { st.removed++ })
		log.Debug().Str("occupant", key).Msg("MUC avatar removed (propagated from contact)")
		return
	}
	avatar := &bridgev2.Avatar{ID: avatarIDFromHash(p.mxc.realHash), MXC: id.ContentURIString(p.mxc.mxc), Hash: p.mxc.sha256}
	applyGhostAvatar(ctx, ghost, avatar, p.mxc.realHash, nil)
	s.bump(func(st *mucAvatarStats) { st.applied++ })
	log.Debug().Str("occupant", key).Str("photo_hash", p.mxc.realHash).Msg("Applied cached MUC avatar (propagated from contact)")
}

// requeueIfChanged enqueues key when its advertised state differs from the handled one.
func (s *mucAvatarState) requeueIfChanged(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec := s.occ[key]; rec != nil && rec.prop == nil && s.shouldEnqueueLocked(rec) {
		s.enqueueLocked(key)
	}
}

func (s *mucAvatarState) fetchAndApply(ctx context.Context, key string, snap mucOccSnapshot, adv string, ghost avatarGhost, ghostHash string, setOutcome func(source, outcome string)) {
	log := s.log(ctx)
	// Honour a pause requested by an earlier resource-constraint style error.
	s.mu.Lock()
	until := s.pausedUntil
	s.mu.Unlock()
	if d := until.Sub(s.nowT()); d > 0 {
		if s.doSleep(ctx, d) != nil {
			return
		}
	}
	occJID, perr := jid.Parse(key)
	if perr != nil {
		s.markDone(key, adv)
		return
	}
	if s.doPace(ctx) != nil {
		return
	}
	source := "occupant"
	data, hash, err := s.fetch(ctx, occJID, key)
	var primaryType, primaryCond string
	if err != nil && !errors.Is(err, errAvatarRejected) && snap.realJID != "" {
		if realJID, rerr := jid.Parse(snap.realJID); rerr == nil {
			primaryType, primaryCond = avatarErrInfo(err)
			if s.doPace(ctx) != nil {
				return
			}
			source = "real_jid"
			var err2 error
			data, hash, err2 = s.fetch(ctx, realJID, key) // span peer stays the occupant JID
			log.Debug().Str("occupant", key).Str("real_jid", snap.realJID).Bool("ok", err2 == nil).
				Msg("MUC avatar: occupant fetch failed, tried real JID")
			err = err2
		}
	}
	if err != nil && ctx.Err() != nil {
		return // session ended mid-fetch: not the occupant's fault, record nothing
	}
	if err != nil {
		if errors.Is(err, errAvatarRejected) {
			s.bump(func(st *mucAvatarStats) { st.rejected++ })
			s.markDone(key, adv)
			setOutcome(source, "rejected")
			log.Debug().Err(err).Str("occupant", key).Str("source", source).Msg("MUC avatar rejected")
			return
		}
		typ, cond := avatarErrInfo(err)
		s.markFail(key, adv)
		s.bump(func(st *mucAvatarStats) { st.failed++ })
		telemetry.Fail(trace.SpanFromContext(ctx), err)
		setOutcome(source, "error")
		ev := log.Debug().Err(err).Str("occupant", key).Str("source", source).
			Str("error_type", typ).Str("condition", cond).Str("photo_hash", adv)
		if primaryType != "" || primaryCond != "" {
			ev = ev.Str("primary_error_type", primaryType).Str("primary_condition", primaryCond)
		}
		ev.Msg("MUC avatar fetch failed")
		switch typ {
		case "wait":
			s.mu.Lock()
			s.pausedUntil = s.nowT().Add(mucAvatarWaitPause)
			s.mu.Unlock()
		case "cancel":
			s.mu.Lock()
			s.roomCancelErrs[snap.room]++
			disable := s.roomCancelErrs[snap.room] >= mucAvatarRoomErrMax && !s.roomDisabled[snap.room]
			if disable {
				s.roomDisabled[snap.room] = true
			}
			s.mu.Unlock()
			if disable {
				log.Debug().Str("room", snap.room).Msg("MUC avatar fetching disabled for room")
			}
		}
		return
	}
	// The IQ was answered: the room is not refusing us.
	s.mu.Lock()
	if s.roomCancelErrs != nil {
		delete(s.roomCancelErrs, snap.room)
	}
	s.mu.Unlock()
	if hash == "" {
		s.bump(func(st *mucAvatarStats) { st.fetched++ })
		s.markDone(key, adv)
		setOutcome(source, "no_photo")
		log.Debug().Str("occupant", key).Str("source", source).Msg("MUC occupant has no avatar")
		return
	}
	s.bump(func(st *mucAvatarStats) { st.fetched++ })
	if hash != snap.hash {
		log.Debug().Str("occupant", key).Str("advertised_hash", snap.hash).Str("photo_hash", hash).
			Msg("MUC avatar hash differs from the advertised hash")
	}
	if hash == ghostHash && ghost.AvatarApplied() {
		s.markDone(key, adv)
		setOutcome(source, "unchanged")
		return
	}
	avatarData := data
	avatar := &bridgev2.Avatar{
		ID:  avatarIDFromHash(hash),
		Get: func(context.Context) ([]byte, error) { return avatarData, nil },
	}
	dataSHA256 := sha256.Sum256(data)
	applyGhostAvatar(ctx, ghost, avatar, hash, func(g *bridgev2.Ghost) {
		// Cache only an MXC that provably holds these bytes (a failed upload leaves the previous
		// AvatarMXC/AvatarHash in place), and only under the hash we computed ourselves: keying by
		// the advertised hash would let an occupant advertise someone else's hash while serving its
		// own photo, poisoning the cache for that other occupant.
		if g.AvatarMXC != "" && g.AvatarHash == dataSHA256 {
			s.putMXC(mucMXC{realHash: hash, mxc: string(g.AvatarMXC), sha256: g.AvatarHash}, hash)
		}
	})
	s.bump(func(st *mucAvatarStats) { st.applied++ })
	setOutcome(source, "changed")
	log.Debug().Str("occupant", key).Str("photo_hash", hash).Int("bytes", len(data)).Str("source", source).
		Msg("Fetched MUC avatar (vCard)")
	if ghost.AvatarApplied() {
		s.markDone(key, adv)
	}
}

// --- xmppClient glue ---

// startMUCAvatars resets the per-session state, installs the production seams and starts the
// worker on the session context. Called next to the session context creation in Connect.
func (x *xmppClient) startMUCAvatars(sessCtx context.Context) {
	s := &x.mucAv
	s.reset()
	s.mu.Lock()
	if s.fetch == nil {
		s.fetch = func(ctx context.Context, to jid.JID, spanPeer string) ([]byte, string, error) {
			data, _, sum, err := x.fetchVCardPhoto(ctx, to, spanPeer)
			return data, sum, err
		}
	}
	if s.getGhost == nil {
		s.getGhost = func(ctx context.Context, occupant string) (avatarGhost, error) {
			if x.conn == nil || x.conn.br == nil {
				return nil, nil
			}
			g, err := x.conn.br.GetExistingGhostByID(ctx, networkid.UserID(occupant))
			if err != nil || g == nil {
				return nil, err
			}
			return realAvatarGhost{g}, nil
		}
	}
	// The pacing ticker belongs to this session, so it is always replaced; the pace closure reads
	// it under s.mu (a previous session's worker may still be finishing a job).
	ticker := time.NewTicker(mucAvatarPace)
	go func() { <-sessCtx.Done(); ticker.Stop() }()
	s.paceC = ticker.C
	if s.pace == nil {
		s.pace = func(ctx context.Context) error {
			s.mu.Lock()
			c := s.paceC
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c:
				return nil
			}
		}
	}
	if s.logger == nil {
		s.logger = x.logFor
	}
	s.mu.Unlock()
	go s.runWorker(sessCtx)
}

// noteMUCPresence is called on the Serve goroutine for MUC occupant presence (memory only).
func (x *xmppClient) noteMUCPresence(occupantFull, room, typ string, info presenceInfo) {
	own := info.hasStatus("110") || x.getMUCOurOccupantJID(room) == occupantFull
	if x.mucAv.notePresence(occupantFull, room, info, typ == "unavailable", own) {
		x.log.Debug().Int("cap", mucOccupantCap).Msg("MUC avatar occupant table full, ignoring new occupants this session")
	}
}

// propagateContactAvatar is called by the 1:1 path after it applied a new avatar (mxc != nil) or
// removal (mxc == nil) for bare JID. Memory only; safe from any goroutine.
func (x *xmppClient) propagateContactAvatar(bareJID string, mxc *mucMXC) {
	x.mucAv.propagateContact(bareJID, mxc)
}

// noteMUCGhost is called (Serve, memory only) once ensureMUCGhostDisplayName holds the ghost.
func (x *xmppClient) noteMUCGhost(occupantFull string) {
	if x.mucAv.noteGhost(occupantFull) {
		x.log.Debug().Int("cap", mucOccupantCap).Msg("MUC avatar occupant table full, ignoring new occupants this session")
	}
}
