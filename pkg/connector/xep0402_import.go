package connector

import (
	"context"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
)

// Reading bookmarks: the connect-time import (part of the startup coordinator, runMUCAutoRejoin), the
// autojoin=false skip set and the bookmarks-sync command. Nothing here ever writes a bookmark (the
// one-time backfill of xep0402_backfill.go and the join/leave commands do).

// bookmarkImportHistory is the history request for a room that was never bridged: no flood.
func bookmarkImportHistory() *mucHistory { return &mucHistory{MaxStanzas: mucRejoinFallbackStanzas} }

// splitBookmarkAutojoinOff separates the rejoin candidates whose bookmark says autojoin=false (the user
// turned the room off on another client) from the rest. Rooms without a bookmark stay in keep.
func splitBookmarkAutojoinOff(rooms []*database.Portal, bms []bookmarkConf) (keep, off []*database.Portal) {
	autojoinOff := map[string]bool{}
	for _, b := range bms {
		if !b.Autojoin {
			autojoinOff[bookmarkKey(b.ID)] = true
		}
	}
	for _, p := range rooms {
		if autojoinOff[bookmarkKey(string(p.ID))] {
			off = append(off, p)
		} else {
			keep = append(keep, p)
		}
	}
	return keep, off
}

// bookmarkNotJoinedNotice words the connect-time notice for a room the bookmark turned off.
func bookmarkNotJoinedNotice(prefix, room string) string {
	return fmt.Sprintf("Not re-joining %s: its bookmark is set to not auto-join. Use `%s join %s` to return.", room, prefix, room)
}

// bookmarkLeftNotice words the notice of a live remote change (bookmark turned off or deleted).
func bookmarkLeftNotice(prefix, room string) string {
	return fmt.Sprintf("Left %s: its bookmark was set to not auto-join (or was deleted) on another client. Use `%s join %s` to return.", room, prefix, room)
}

// bookmarkLeftLateNotice words the notice for a re-joined room whose bookmark (read only after the
// re-join, see retryBookmarkStartup) says not to auto-join.
func bookmarkLeftLateNotice(prefix, room string) string {
	return fmt.Sprintf("Left %s again: its bookmark is set to not auto-join (bookmarks could only be read after the re-join). Use `%s join %s` to return.", room, prefix, room)
}

// bookmarkBudgetNotice is posted once per session when the import budget is used up.
func bookmarkBudgetNotice(prefix string) string {
	return fmt.Sprintf("Imported the maximum of %d bookmarked rooms this session; more bookmarks were not imported. Run `%s bookmarks-sync` or reconnect to continue.", bookmarkImportMaxRooms, prefix)
}

// getImportedMUCChatInfo is GetChatInfo for an imported room: we KNOW it is a MUC, so the isMUC
// disco/heuristic detection is skipped.
func (x *xmppClient) getImportedMUCChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	return x.getMUCChatInfo(ctx, portal, string(portal.ID))
}

// ensureUserInPortal makes the user's double puppet join a freshly created portal room (the join command
// does the same explicitly).
func (x *xmppClient) ensureUserInPortal(ctx context.Context, portal *bridgev2.Portal) {
	if portal == nil || portal.MXID == "" || x.login == nil || x.login.User == nil {
		return
	}
	if intent := x.login.User.DoublePuppet(ctx); intent != nil {
		if err := intent.EnsureJoined(ctx, portal.MXID); err != nil {
			x.logFor(ctx).Warn().Err(err).Msg("Failed to ensure user is joined to imported portal room")
		}
	}
}

// queueImportedPortal asks the framework to create the portal of an imported room (idempotent when it
// exists; the framework invites and double-puppets the user like for any created portal).
func (x *xmppClient) queueImportedPortal(ctx context.Context, room string) {
	x.queueRemoteEvent(ctx, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:           bridgev2.RemoteEventChatResync,
			PortalKey:      networkid.PortalKey{ID: networkid.PortalID(room), Receiver: x.loginID},
			CreatePortal:   true,
			PostHandleFunc: x.ensureUserInPortal,
		},
		GetChatInfoFunc: x.getImportedMUCChatInfo,
	})
}

// importBookmarkedRoom joins a bookmarked room that has no Matrix room yet and queues the portal creation.
func (x *xmppClient) importBookmarkedRoom(ctx context.Context, room string) error {
	if err := x.joinMUC(ctx, room, bookmarkImportHistory()); err != nil {
		return err
	}
	x.queueImportedPortal(ctx, room)
	return nil
}

// waitCtx waits d or until ctx is done; false when ctx ended.
func waitCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// BookmarkSyncResult is what the bookmarks-sync command reports (counts only).
type BookmarkSyncResult struct {
	ReadSupported  bool
	WriteSupported bool
	CompatSeen     bool
	Total          int // bookmarks on the server
	Autojoin       int // of those with autojoin=true
	Imported       int // new rooms joined and queued for portal creation
	Existing       int // autojoin bookmarks that already have a Matrix room
	Invalid        int // skipped: not a room JID, or a Cheogram number
	Capped         int // skipped: per-session import budget
	Failed         int // joins that failed
	Push           bool
	BackfillRan    bool
	Created        int // bookmarks created by the push
	AlreadyHad     int // rooms that already had a bookmark (never modified)
	PushFailed     int
}

// BookmarkSyncer is implemented by the real XMPP client (the bookmarks-sync command).
type BookmarkSyncer interface {
	SyncBookmarks(ctx context.Context, push bool) (BookmarkSyncResult, error)
}

var _ BookmarkSyncer = (*xmppClient)(nil)

// SyncBookmarks fetches the bookmarks and imports the autojoin=true rooms that have no Matrix room yet
// (same per-session budget as the connect import and live events). It never leaves a room and never
// writes, except with push, which re-runs the one-time backfill (bookmarks for bridged rooms that have
// none) regardless of the persisted marker.
func (x *xmppClient) SyncBookmarks(ctx context.Context, push bool) (BookmarkSyncResult, error) {
	res := BookmarkSyncResult{Push: push}
	s, ok := x.ensureBookmarkGate(ctx)
	if !ok {
		return res, fmt.Errorf("could not check the server's bookmark support (not connected?)")
	}
	res.ReadSupported, res.WriteSupported, res.CompatSeen = s.Read, s.Write && !x.bm.writesDisabled(), s.Compat
	if !s.Read {
		return res, nil
	}
	bms, _, err := x.bookmarkFetch(ctx)
	if err != nil {
		return res, fmt.Errorf("fetching bookmarks: %w", err)
	}
	x.bm.setCache(bms)
	portals, err := x.listMUCRejoinPortals(ctx)
	if err != nil {
		return res, fmt.Errorf("listing rooms: %w", err)
	}
	sel, st := selectBookmarkImports(bms, portals, x.loginID, x.bm.importBudgetLeft())
	x.bm.takeImportBudget(len(sel))
	res.Total, res.Autojoin, res.Existing, res.Invalid, res.Capped = st.Total, st.Autojoin, st.Existing, st.Invalid, st.Capped
	for i, b := range sel {
		if i > 0 && !waitCtx(ctx, mucRejoinInterval) {
			res.Failed += len(sel) - i
			break
		}
		if err := x.importBookmarkedRoom(ctx, bookmarkKey(b.ID)); err != nil {
			res.Failed++
			x.logFor(ctx).Warn().Err(err).Str("room", bookmarkKey(b.ID)).Msg("Bookmarks: import join failed")
			continue
		}
		res.Imported++
	}
	x.logFor(ctx).Info().Int("total", res.Total).Int("autojoin", res.Autojoin).Int("imported", res.Imported).
		Int("existing", res.Existing).Int("invalid", res.Invalid).Int("capped", res.Capped).Int("failed", res.Failed).
		Msg("Bookmarks: import finished")
	if push {
		rooms, _ := selectMUCRejoinCandidates(ctx, portals, x.loginID, x.isMUC)
		keys := make([]string, 0, len(rooms))
		for _, p := range rooms {
			keys = append(keys, bookmarkKey(string(p.ID)))
		}
		bf := x.runBookmarkBackfill(ctx, keys, true)
		res.BackfillRan, res.Created, res.AlreadyHad, res.PushFailed = bf.Ran, bf.Created, bf.AlreadyHad, bf.Failed
	}
	return res, nil
}
