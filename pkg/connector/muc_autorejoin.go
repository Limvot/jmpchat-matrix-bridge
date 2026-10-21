package connector

import (
	"context"
	"encoding/xml"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// MUC auto-rejoin: after every session start the bridge re-joins the MUCs it has a Matrix room for,
// instead of waiting for the user to send into (or view) each room. See runMUCAutoRejoin.

const (
	// mucRejoinMaxRooms bounds one batch (the rest is skipped with a warning).
	mucRejoinMaxRooms = 200
	// mucRejoinMaxAge bounds how far back the history request reaches for a quiet room.
	mucRejoinMaxAge = 7 * 24 * time.Hour
	// mucRejoinMaxStanzas bounds a since-based history request; mucRejoinFallbackStanzas is used
	// when the portal has no bridged message to anchor "since" on.
	mucRejoinMaxStanzas      = 100
	mucRejoinFallbackStanzas = 20
)

// Package variables so tests can run without real delays.
var (
	mucRejoinInitialDelay = 3 * time.Second
	mucRejoinInterval     = 500 * time.Millisecond
)

// mucHistory is the XEP-0045 history request of a join presence.
type mucHistory struct {
	Since      time.Time // zero = no since attribute
	MaxStanzas int       // 0 = no maxstanzas attribute
}

// attrs returns the attributes of the <history/> element.
func (h *mucHistory) attrs() []xml.Attr {
	var a []xml.Attr
	if !h.Since.IsZero() {
		a = append(a, xml.Attr{Name: xml.Name{Local: "since"}, Value: h.Since.UTC().Format(time.RFC3339)})
	}
	if h.MaxStanzas > 0 {
		a = append(a, xml.Attr{Name: xml.Name{Local: "maxstanzas"}, Value: strconv.Itoa(h.MaxStanzas)})
	}
	return a
}

// mucRejoinHistory computes the history request for an auto-rejoin from the timestamp of the newest
// bridged message of the room (zero if none): everything since then, at most 7 days and 100 stanzas
// back; without an anchor the last 20 stanzas. Messages already bridged are dropped as duplicates.
func mucRejoinHistory(newest, now time.Time) *mucHistory {
	if newest.IsZero() {
		return &mucHistory{MaxStanzas: mucRejoinFallbackStanzas}
	}
	if floor := now.Add(-mucRejoinMaxAge); newest.Before(floor) {
		newest = floor
	}
	return &mucHistory{Since: newest, MaxStanzas: mucRejoinMaxStanzas}
}

// selectMUCRejoinCandidates picks the portals to re-join: this login's portals with a Matrix room that
// are MUCs and were not left explicitly. Cheogram 1:1 and group MMS portals are never MUCs. A portal with
// an unset room type is classified with isMUC (disco/heuristic, may do I/O). skipped counts rejected
// portals of this login.
func selectMUCRejoinCandidates(ctx context.Context, portals []*database.Portal, login networkid.UserLoginID, isMUC func(ctx context.Context, jid string) bool) (rooms []*database.Portal, skipped int) {
	for _, p := range portals {
		if p == nil || p.Receiver != login || p.MXID == "" {
			continue
		}
		id := string(p.ID)
		if IsCheogramDMJID(id) {
			skipped++
			continue
		}
		if meta, _ := p.Metadata.(*PortalMetadata); meta != nil && meta.MUCLeft {
			skipped++
			continue
		}
		switch p.RoomType {
		case database.RoomTypeGroupDM:
		case database.RoomTypeDM:
			skipped++
			continue
		default:
			if !isMUC(ctx, id) {
				skipped++
				continue
			}
		}
		rooms = append(rooms, p)
	}
	return rooms, skipped
}

// listMUCRejoinPortals returns every portal with a Matrix room (all logins; filtered by the selector).
func (x *xmppClient) listMUCRejoinPortals(ctx context.Context) ([]*database.Portal, error) {
	if x.mucRejoinPortalsHook != nil {
		return x.mucRejoinPortalsHook(ctx)
	}
	if x.conn == nil || x.conn.br == nil {
		return nil, fmt.Errorf("no bridge")
	}
	return x.conn.br.DB.Portal.GetAllWithMXID(ctx)
}

// newestBridgedMessage returns the timestamp of the newest bridged message of the room, zero if none.
func (x *xmppClient) newestBridgedMessage(ctx context.Context, roomBare string) time.Time {
	if x.mucNewestMsgHook != nil {
		return x.mucNewestMsgHook(ctx, roomBare)
	}
	if x.conn == nil || x.conn.br == nil {
		return time.Time{}
	}
	msgs, err := x.conn.br.DB.Message.GetLastNInPortal(ctx, networkid.PortalKey{ID: networkid.PortalID(roomBare), Receiver: x.loginID}, 1)
	if err != nil || len(msgs) == 0 {
		return time.Time{}
	}
	return msgs[0].Timestamp
}

// runMUCAutoRejoin is the startup coordinator, run once per session in its own goroutine (never on the
// Serve goroutine) with its own root span. ctx is the session context: when the session ends it is
// cancelled and the batch stops before the next join. Steps:
//  1. the bookmark capability gate and one fetch (fills the nick/password cache; runs while the initial
//     delay elapses). When the fetch fails, everything below behaves as before bookmarks existed;
//  2. the portals to re-join, minus those whose bookmark says autojoin=false (D2: the bookmark wins; they
//     are marked left and the user gets a notice), only when the fetch succeeded;
//  3. the autojoin bookmarks that have no Matrix room yet (imports, per-session budget);
//  4. one staggered (mucRejoinInterval) join loop over the rooms, then the imports, through the normal
//     joinMUC path (nick choice incl. the bookmark nick, 409 handling) with a bounded history request;
//  5. the one-time backfill of bookmarks for the bridged rooms that have none (xep0402_backfill.go).
func (x *xmppClient) runMUCAutoRejoin(sessCtx context.Context) {
	ctx, span := telemetry.Start(sessCtx, "xmpp.muc.auto_rejoin", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	log := x.logFor(ctx)

	start := time.Now()
	bms, fetched := x.bookmarkStartup(ctx)
	if !waitCtx(ctx, mucRejoinInitialDelay-time.Since(start)) {
		return
	}
	portals, err := x.listMUCRejoinPortals(ctx)
	if err != nil {
		telemetry.Fail(span, err)
		log.Warn().Err(err).Msg("MUC auto-rejoin: failed to list portals")
		return
	}
	rooms, skipped := selectMUCRejoinCandidates(ctx, portals, x.loginID, x.isMUC)
	if fetched {
		var off []*database.Portal
		rooms, off = splitBookmarkAutojoinOff(rooms, bms)
		for _, p := range off {
			room := string(p.ID)
			if err := x.setMUCLeft(ctx, room, true); err != nil {
				log.Warn().Err(err).Str("room", room).Msg("Bookmarks: failed to mark a room left")
			}
			x.notifyMUC(ctx, room, bookmarkNotJoinedNotice(x.mucCmdPrefix(), room))
			log.Info().Str("room", room).Msg("Bookmarks: bookmark has autojoin off; not re-joining, marked left")
			skipped++
		}
	}
	if len(rooms) > mucRejoinMaxRooms {
		log.Warn().Int("rooms", len(rooms)).Int("limit", mucRejoinMaxRooms).Msg("MUC auto-rejoin: too many rooms, joining the first ones only")
		skipped += len(rooms) - mucRejoinMaxRooms
		rooms = rooms[:mucRejoinMaxRooms]
	}
	var imports []bookmarkConf
	if fetched {
		var st importStats
		imports, st = selectBookmarkImports(bms, portals, x.loginID, x.bm.importBudgetLeft())
		x.bm.takeImportBudget(len(imports))
		if st.Capped > 0 {
			log.Warn().Int("imported", len(imports)).Int("autojoin", st.Autojoin).Int("capped", st.Capped).Msg("Bookmarks: import budget reached, the rest is skipped")
			if x.bm.markBudgetNoticed() {
				x.notifyManagement(ctx, bookmarkBudgetNotice(x.mucCmdPrefix()))
			}
		}
	}
	joined, failed, imported := 0, 0, 0
	total := len(rooms) + len(imports)
	for i := 0; i < total; i++ {
		if i > 0 && !waitCtx(ctx, mucRejoinInterval) {
			skipped += total - i
			break
		}
		if ctx.Err() != nil {
			skipped += total - i
			break
		}
		if i < len(rooms) {
			room := string(rooms[i].ID)
			hist := mucRejoinHistory(x.newestBridgedMessage(ctx, room), time.Now())
			if err := x.joinMUC(ctx, room, hist); err != nil {
				failed++
				log.Warn().Err(err).Str("room", room).Msg("MUC auto-rejoin: join failed")
				continue
			}
			joined++
			log.Debug().Str("room", room).Msg("MUC auto-rejoin: joined")
			continue
		}
		room := bookmarkKey(imports[i-len(rooms)].ID)
		if err := x.importBookmarkedRoom(ctx, room); err != nil {
			failed++
			log.Warn().Err(err).Str("room", room).Msg("Bookmarks: import join failed")
			continue
		}
		imported++
		log.Info().Str("room", room).Msg("Bookmarks: imported room")
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("xmpp.muc.rejoin.joined", joined), attribute.Int("xmpp.muc.rejoin.skipped", skipped), attribute.Int("xmpp.muc.rejoin.failed", failed),
			attribute.Int("xmpp.muc.rejoin.imported", imported))
	}
	log.Info().Int("joined", joined).Int("imported", imported).Int("skipped", skipped).Int("failed", failed).Msg("MUC auto-rejoin finished")
	if fetched && ctx.Err() == nil && !x.backfillDone() {
		if !waitCtx(ctx, bookmarkBackfillSettle) {
			return
		}
		keys := make([]string, 0, len(rooms))
		for _, p := range rooms {
			keys = append(keys, bookmarkKey(string(p.ID)))
		}
		x.runBookmarkBackfill(ctx, keys, false)
	}
}

// setMUCLeft persists the "left" flag of a room in its portal metadata (nothing to do when the room has
// no portal or the flag already has the value).
func (x *xmppClient) setMUCLeft(ctx context.Context, roomBare string, left bool) error {
	if x.mucLeftHook != nil {
		return x.mucLeftHook(roomBare, left)
	}
	if x.conn == nil || x.conn.br == nil {
		return nil
	}
	portal, err := x.conn.br.GetExistingPortalByKey(ctx, networkid.PortalKey{ID: networkid.PortalID(roomBare), Receiver: x.loginID})
	if err != nil || portal == nil {
		return err
	}
	meta, _ := portal.Metadata.(*PortalMetadata)
	if meta == nil {
		if !left {
			return nil
		}
		meta = &PortalMetadata{}
	}
	if meta.MUCLeft == left {
		return nil
	}
	meta.MUCLeft = left
	portal.Metadata = meta
	return portal.Save(ctx)
}

// lookupMUCPortal returns the existing portal of a room for this login (nil when there is none).
func (x *xmppClient) lookupMUCPortal(ctx context.Context, roomBare string) (*database.Portal, error) {
	if x.mucRejoinPortalsHook != nil {
		ps, err := x.mucRejoinPortalsHook(ctx)
		if err != nil {
			return nil, err
		}
		for _, p := range ps {
			if p != nil && p.Receiver == x.loginID && bookmarkKey(string(p.ID)) == bookmarkKey(roomBare) {
				return p, nil
			}
		}
		return nil, nil
	}
	if x.conn == nil || x.conn.br == nil {
		return nil, nil
	}
	portal, err := x.conn.br.GetExistingPortalByKey(ctx, networkid.PortalKey{ID: networkid.PortalID(roomBare), Receiver: x.loginID})
	if err != nil || portal == nil {
		return nil, err
	}
	return portal.Portal, nil
}

// isMUCLeft reports whether the room's portal is marked as left (false when there is no portal).
func (x *xmppClient) isMUCLeft(ctx context.Context, roomBare string) bool {
	p, err := x.lookupMUCPortal(ctx, roomBare)
	if err != nil || p == nil {
		return false
	}
	meta, _ := p.Metadata.(*PortalMetadata)
	return meta != nil && meta.MUCLeft
}
