package connector

import (
	"context"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/id"
)

// MUC echo rendezvous. A MUC retraction must name the ROOM-assigned stanza-id of the original, which
// we only learn from the room's echo of our own message. The echo (Serve goroutine) and the
// bridgev2 row insert (portal event loop, signalled through MatrixMessageResponse.PostSave) happen in
// either order, so both sides meet in mucEchoTable under one lock: whoever comes second finishes the
// handoff. No sleeps or DB polling. A Matrix redaction that arrives before the echo waits on the
// entry's done channel (bounded, see mucEchoWait).
//
// Matching is by exact id: the origin-id (preferred, rooms keep it) or the `id` attribute of a
// message WE sent and registered before sending. Issue #11 (messages from our own nick sent by other
// clients are dropped as self-echo) is fixed on top of this: only a message that matches a registered
// id is dropped as our echo; any other message from our own occupant JID was sent by another client
// of the user and is bridged as sent by the user (mucMessageSender). Every bridge MUC send path
// (text/media, reactions, retractions) must register its stanza id before sending.

const (
	// mucEchoTTL is how long an entry is kept (it also serves redactions that come after the echo).
	mucEchoTTL = 5 * time.Minute
	// mucEchoWait bounds how long a redaction waits for a pending echo.
	mucEchoWait = 3 * time.Second
)

type mucEchoKey struct{ room, id string }

func newMUCEchoKey(room, id string) mucEchoKey {
	return mucEchoKey{room: strings.ToLower(room), id: id}
}

type mucEchoEntry struct {
	created    time.Time
	done       chan struct{} // closed when the echo was seen
	echoed     bool
	stanzaID   string // room-assigned; "" when the room stamps none
	occupantID string
	saved      bool // PostSave ran and the echo was not there yet
	mxid       id.EventID
}

// mucEchoTable is usable as a zero value.
type mucEchoTable struct {
	mu      sync.Mutex
	entries map[mucEchoKey]*mucEchoEntry
	now     func() time.Time // tests only
}

func (t *mucEchoTable) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// register creates the entry for a message about to be sent (must happen BEFORE the send so that a
// fast echo cannot be missed). Expired entries are swept here.
func (t *mucEchoTable) register(room, msgID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	if t.entries == nil {
		t.entries = map[mucEchoKey]*mucEchoEntry{}
	}
	for k, e := range t.entries {
		if now.Sub(e.created) > mucEchoTTL {
			delete(t.entries, k)
		}
	}
	k := newMUCEchoKey(room, msgID)
	if _, ok := t.entries[k]; !ok {
		t.entries[k] = &mucEchoEntry{created: now, done: make(chan struct{})}
	}
}

// discard drops an entry (the send failed).
func (t *mucEchoTable) discard(room, msgID string) {
	t.mu.Lock()
	delete(t.entries, newMUCEchoKey(room, msgID))
	t.mu.Unlock()
}

// echoResult is what the echo side learns.
type echoResult struct {
	matched bool
	persist bool       // the row is already saved: write the ids to it
	mxid    id.EventID // row to update when persist
}

// matchEcho records the echo of a registered message. ids are the candidates the echo carries
// (origin-id first, then the `id` attribute). stanzaID must already be the room's trusted stanza-id.
// A second echo of the same message is matched but changes nothing.
func (t *mucEchoTable) matchEcho(room string, ids []string, stanzaID, occupantID string) echoResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	for _, cand := range ids {
		if cand == "" {
			continue
		}
		e, ok := t.entries[newMUCEchoKey(room, cand)]
		if !ok || now.Sub(e.created) > mucEchoTTL {
			continue
		}
		if e.echoed {
			return echoResult{matched: true}
		}
		e.echoed = true
		e.stanzaID = stanzaID
		e.occupantID = occupantID
		close(e.done)
		res := echoResult{matched: true}
		if e.saved {
			res.persist = true
			res.mxid = e.mxid
		}
		return res
	}
	return echoResult{}
}

// postSave is the row-insert side. When the echo already arrived it returns its ids (the caller
// writes them to the saved row); otherwise it remembers the row for the echo side.
func (t *mucEchoTable) postSave(room, msgID string, mxid id.EventID) (stanzaID, occupantID string, haveEcho bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[newMUCEchoKey(room, msgID)]
	if !ok {
		return "", "", false
	}
	if e.echoed {
		return e.stanzaID, e.occupantID, true
	}
	e.saved = true
	e.mxid = mxid
	return "", "", false
}

// wait returns the room stanza-id recorded for the message. found=false means no entry exists
// (expired or never registered). If the echo is still pending it waits up to timeout (or ctx);
// timedOut=true when it did not arrive. A confirmed echo without a stanza-id returns "" with found.
func (t *mucEchoTable) wait(ctx context.Context, room, msgID string, timeout time.Duration) (stanzaID string, found, timedOut bool) {
	t.mu.Lock()
	e, ok := t.entries[newMUCEchoKey(room, msgID)]
	t.mu.Unlock()
	if !ok {
		return "", false, false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-e.done:
	case <-timer.C:
		return "", true, true
	case <-ctx.Done():
		return "", true, true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return e.stanzaID, true, false
}

// applyEchoIDs writes the echo's ids into the row metadata; false when nothing changed.
func applyEchoIDs(meta *MessageMetadata, stanzaID, occupantID string) bool {
	if meta == nil {
		return false
	}
	changed := false
	if stanzaID != "" && meta.StanzaID != stanzaID {
		meta.StanzaID = stanzaID
		changed = true
	}
	if occupantID != "" && meta.OccupantID != occupantID {
		meta.OccupantID = occupantID
		changed = true
	}
	return changed
}

// mucEchoPostSave returns the PostSave callback for a registered MUC message (portal event loop;
// DB access allowed).
func (x *xmppClient) mucEchoPostSave(room, msgID string) func(context.Context, *database.Message) {
	return func(ctx context.Context, saved *database.Message) {
		stanzaID, occ, have := x.mucEcho.postSave(room, msgID, saved.MXID)
		if !have {
			return
		}
		meta, _ := saved.Metadata.(*MessageMetadata)
		if !applyEchoIDs(meta, stanzaID, occ) {
			return
		}
		if err := x.conn.br.DB.Message.Update(ctx, saved); err != nil {
			x.logFor(ctx).Warn().Err(err).Msg("Failed to store MUC echo ids on sent message")
		}
	}
}

// isOwnMUCOccupant reports whether occupantFull is our own occupant JID in the room (memory only).
func (x *xmppClient) isOwnMUCOccupant(room, occupantFull string) bool {
	ours := x.getMUCOurOccupantJID(room)
	return ours != "" && occupantFull == ours
}

// mucMessageSender is the bridged sender of a groupchat message: the user themselves (is_from_me,
// double puppet, like 1:1 carbons) when it comes from our own occupant JID, else the occupant's ghost.
// Only called for messages that are not registered echoes (issue #11).
func (x *xmppClient) mucMessageSender(room, occupantFull string) bridgev2.EventSender {
	if x.isOwnMUCOccupant(room, occupantFull) {
		return x.makeEventSender(string(x.loginID))
	}
	return x.makeEventSender(occupantFull)
}

// handleOwnMUCEcho is called on the Serve goroutine for every inbound groupchat message (memory
// only). It returns true when the message is the echo of a message this bridge sent, which must not
// be bridged back. The DB write (echo arrived after the row was saved) runs in a goroutine without a
// span.
func (x *xmppClient) handleOwnMUCEcho(room, occupantFull, idAttr string, ids messageIDs) bool {
	// An echo comes from our own occupant JID; when we do not know it (yet), the registered random id
	// is the only evidence. A known, different occupant is never ours.
	if ours := x.getMUCOurOccupantJID(room); ours != "" && occupantFull != ours {
		return false
	}
	res := x.mucEcho.matchEcho(room, []string{ids.OriginID, idAttr}, ids.stanzaIDBy(room), ids.OccupantID)
	if !res.matched {
		return false
	}
	if res.persist {
		stanzaID, occ := ids.stanzaIDBy(room), ids.OccupantID
		mxid := res.mxid
		go x.persistEchoIDs(mxid, stanzaID, occ)
	}
	x.log.Debug().Str("room", room).Msg("Recognized MUC echo of our own sent message")
	return true
}

// persistEchoIDs re-reads the row by event ID (so a stale struct never clobbers other columns) and
// stores the echo ids.
func (x *xmppClient) persistEchoIDs(mxid id.EventID, stanzaID, occupantID string) {
	ctx := x.log.WithContext(context.Background())
	if x.conn == nil || x.conn.br == nil || mxid == "" {
		return
	}
	row, err := x.conn.br.DB.Message.GetPartByMXID(ctx, mxid)
	if err != nil || row == nil {
		x.log.Warn().Err(err).Msg("MUC echo ids: sent message row not found")
		return
	}
	meta, _ := row.Metadata.(*MessageMetadata)
	if !applyEchoIDs(meta, stanzaID, occupantID) {
		return
	}
	if err := x.conn.br.DB.Message.Update(ctx, row); err != nil {
		x.log.Warn().Err(err).Msg("Failed to store MUC echo ids on sent message")
	}
}
