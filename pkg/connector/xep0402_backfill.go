package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// One-time backfill of bookmarks for rooms that are already bridged (MUC portals with a Matrix room, not
// left) and have none, so the user's other clients see the rooms the bridge is in. Decision D5: automatic,
// once per login (persisted marker in the login metadata Extra), only when the server supports private
// writes (publish-options). A bookmark that already exists is NEVER modified by the backfill (not even
// autojoin=false or an empty nick). `bookmarks-sync push` runs it again on demand.

// bookmarkBackfillKey is the JabberLoginMeta.Extra key of the "backfilled" marker.
const bookmarkBackfillKey = "bookmarks_backfill"

// bookmarkBackfillInterval paces the publishes of one backfill (variable so tests run without delays).
var bookmarkBackfillInterval = 200 * time.Millisecond

// bookmarkBackfillSettle is how long the startup coordinator waits after the last join before the backfill,
// so the confirmed occupant nicks are known (variable for tests).
var bookmarkBackfillSettle = 5 * time.Second

type backfillMarker struct {
	V  int    `json:"v"`
	At string `json:"at"`
}

// backfillDone reports whether this login already ran the backfill.
func (x *xmppClient) backfillDone() bool {
	meta := loginMeta(x.login)
	if meta == nil {
		return false
	}
	_, ok := meta.Extra[bookmarkBackfillKey]
	return ok
}

// markBackfilled persists the marker (setLoginExtraKey takes extraWriteMu and saves).
func (x *xmppClient) markBackfilled(ctx context.Context) error {
	raw, err := json.Marshal(backfillMarker{V: 1, At: time.Now().UTC().Format(time.RFC3339)})
	if err != nil {
		return err
	}
	return setLoginExtraKey(ctx, x.login, bookmarkBackfillKey, raw)
}

// backfillResult are the counters of one backfill run.
type backfillResult struct {
	Ran        bool // gate and fetch allowed a run
	Created    int
	AlreadyHad int // had a bookmark: left untouched
	Failed     int
}

// backfillNick is the nick the bridge currently uses in a room: the confirmed occupant nick when joined,
// else the nick a join would use now.
func (x *xmppClient) backfillNick(ctx context.Context, room string) string {
	if occ := x.getMUCOurOccupantJID(room); occ != "" {
		if i := strings.LastIndexByte(occ, '/'); i >= 0 && i+1 < len(occ) {
			return occ[i+1:]
		}
	}
	_, nick := x.mucNickChoice(ctx, room)
	return nick
}

// runBookmarkBackfill creates autojoin=true bookmarks for the given bridged rooms (lower-cased bare JIDs)
// that have none. Without force it only runs once per login (marker) and only when writes are supported.
// The marker is set only when nothing failed, so a failed run is retried next session. Never call it on
// the Serve goroutine.
func (x *xmppClient) runBookmarkBackfill(ctx context.Context, rooms []string, force bool) (res backfillResult) {
	if !force && x.backfillDone() {
		return res
	}
	s, ok := x.ensureBookmarkGate(ctx)
	if !ok || !s.Write || x.bm.writesDisabled() {
		return res
	}
	ctx, span := telemetry.StartChild(ctx, "xmpp.bookmarks.backfill")
	defer span.End()
	log := x.logFor(ctx)
	x.bm.writeMu.Lock()
	defer x.bm.writeMu.Unlock()
	fctx, cancel := context.WithTimeout(ctx, bookmarkWriteTimeout)
	bms, _, err := x.bookmarkFetch(fctx)
	cancel()
	if err != nil {
		log.Warn().Err(err).Msg("Bookmarks: fetch before backfill failed; backfill postponed")
		return res
	}
	x.bm.setCache(bms)
	res.Ran = true
	aborted := false
	for i, room := range rooms {
		if bookmarkStoredFor(bms, room) != nil {
			res.AlreadyHad++
			continue
		}
		if i > 0 && !waitCtx(ctx, bookmarkBackfillInterval) {
			aborted = true
			break
		}
		merged, _ := mergeBookmark(nil, room, true, x.backfillNick(ctx, room))
		pctx, pcancel := context.WithTimeout(ctx, bookmarkWriteTimeout)
		err := x.publishBookmarkIQ(pctx, merged)
		pcancel()
		if err != nil {
			res.Failed++
			if publishErrorDisablesWrites(err) {
				if x.bm.disableWrites() {
					log.Warn().Err(err).Msg("Bookmarks: the server refused a private bookmark write; bookmark writes are disabled for this session")
				}
				break
			}
			log.Warn().Err(err).Str("room", room).Msg("Bookmarks: backfill publish failed")
			continue
		}
		x.bm.cacheSet(room, merged)
		res.Created++
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("xmpp.bookmarks.created", res.Created), attribute.Int("xmpp.bookmarks.already", res.AlreadyHad), attribute.Int("xmpp.bookmarks.failed", res.Failed))
	}
	log.Info().Int("rooms", len(rooms)).Int("created", res.Created).Int("already_had", res.AlreadyHad).Int("failed", res.Failed).Bool("forced", force).
		Msg("Bookmarks: backfill finished")
	if res.Failed == 0 && !aborted && ctx.Err() == nil {
		if err := x.markBackfilled(ctx); err != nil {
			log.Warn().Err(err).Msg("Bookmarks: failed to persist the backfill marker; the backfill will run again next session")
		}
		if res.Created > 0 && !force {
			x.notifyManagement(ctx, fmt.Sprintf("Created bookmarks (auto-join on) for %d room(s) this bridge is already in, so your other clients see them too. Existing bookmarks were not changed.", res.Created))
		}
	}
	return res
}

// notifyManagement posts an m.notice to the user's management room only.
func (x *xmppClient) notifyManagement(ctx context.Context, text string) {
	if x.mucNoticeHook != nil {
		x.mucNoticeHook("", text)
		return
	}
	if x.conn == nil || x.conn.br == nil || x.conn.br.Bot == nil || x.login == nil || x.login.User == nil || x.login.User.ManagementRoom == "" {
		return
	}
	content := format.RenderMarkdown(text, true, false)
	content.MsgType = event.MsgNotice
	if _, err := x.conn.br.Bot.SendMessage(ctx, x.login.User.ManagementRoom, event.EventMessage, &event.Content{Parsed: &content}, nil); err != nil {
		x.logFor(ctx).Warn().Err(err).Msg("Failed to send bookmark notice")
	}
}
