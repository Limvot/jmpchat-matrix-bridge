package connector

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// mucMaxNickAlternatives is how many alternative nicks (base_1 .. base_N) are tried after the
// preferred one got a 409 nick conflict.
const mucMaxNickAlternatives = 3

// mucJoinTrack follows one join attempt chain for a room (guarded by xmppClient.mucPendingJoinMu).
type mucJoinTrack struct {
	base       string   // preferred nick for this room when the chain started
	tried      []string // nicks that were answered with 409 in this chain
	conflicted bool     // at least one 409 happened (so a confirmed join is announced)
}

// chooseMUCNick picks the base (preferred) nick and the nick to actually join with. explicit is the
// per-room muc-nick, def the muc-nick-default. An automatically chosen alternative (autoNick) is only
// reused while it was derived from the same base: changing muc-nick or muc-nick-default makes the
// bridge try the new preferred nick again.
func chooseMUCNick(explicit, def, loginID, autoNick, autoBase string) (base, nick string) {
	switch {
	case explicit != "":
		base = explicit
	case def != "":
		base = def
	default:
		base = loginID
		if at := strings.IndexByte(base, '@'); at > 0 {
			base = base[:at]
		}
		if base == "" {
			base = "bridge"
		}
	}
	if autoNick != "" && autoBase == base {
		return base, autoNick
	}
	return base, base
}

// setExplicitMUCNick records a user-chosen nick and forgets any automatic alternative.
func setExplicitMUCNick(meta *PortalMetadata, nick string) {
	meta.MUCPreferredNick = nick
	meta.MUCAutoNick = ""
	meta.MUCAutoNickBase = ""
}

// mucNickChoice returns the preferred and the effective join nick for a room.
func (x *xmppClient) mucNickChoice(ctx context.Context, roomJIDStr string) (base, nick string) {
	var explicit, autoNick, autoBase string
	if x.conn != nil && x.conn.br != nil {
		portalKey := networkid.PortalKey{ID: networkid.PortalID(roomJIDStr), Receiver: x.loginID}
		if portal, err := x.conn.br.GetPortalByKey(ctx, portalKey); err == nil && portal != nil {
			if meta, _ := portal.Metadata.(*PortalMetadata); meta != nil {
				explicit, autoNick, autoBase = meta.MUCPreferredNick, meta.MUCAutoNick, meta.MUCAutoNickBase
			}
		}
	}
	var def string
	if x.login != nil {
		if meta, _ := x.login.Metadata.(*JabberLoginMeta); meta != nil {
			def = meta.DefaultMUCNick
		}
	}
	return chooseMUCNick(explicit, def, string(x.loginID), autoNick, autoBase)
}

// startMUCJoinTrack registers a fresh join chain and marks nick as pending.
func (x *xmppClient) startMUCJoinTrack(roomBare, base, nick string) {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	if x.mucPendingJoin == nil {
		x.mucPendingJoin = make(map[string]string)
	}
	if x.mucJoinTracks == nil {
		x.mucJoinTracks = make(map[string]*mucJoinTrack)
	}
	x.mucPendingJoin[roomBare] = nick
	x.mucJoinTracks[roomBare] = &mucJoinTrack{base: base}
}

// dropMUCJoinTrack forgets the pending join and chain for a room.
func (x *xmppClient) dropMUCJoinTrack(roomBare string) {
	x.mucPendingJoinMu.Lock()
	delete(x.mucPendingJoin, roomBare)
	delete(x.mucJoinTracks, roomBare)
	x.mucPendingJoinMu.Unlock()
}

// onMUCJoinConflict advances the chain after a 409 for ourNick. handled is false when the error does
// not belong to the join in flight (stale or duplicate). Otherwise next is the alternative nick to try
// (now pending), or empty when all are exhausted, in which case tried lists every nick that failed.
// Pure state change: safe on the Serve goroutine.
func (x *xmppClient) onMUCJoinConflict(roomBare, ourNick string) (next string, tried []string, handled bool) {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	track := x.mucJoinTracks[roomBare]
	if track == nil || x.mucPendingJoin[roomBare] != ourNick {
		return "", nil, false
	}
	track.tried = append(track.tried, ourNick)
	track.conflicted = true
	delete(x.mucPendingJoin, roomBare)
	for i := 1; i <= mucMaxNickAlternatives; i++ {
		cand := fmt.Sprintf("%s_%d", track.base, i)
		taken := false
		for _, t := range track.tried {
			if t == cand {
				taken = true
				break
			}
		}
		if !taken {
			x.mucPendingJoin[roomBare] = cand
			return cand, append([]string(nil), track.tried...), true
		}
	}
	tried = append([]string(nil), track.tried...)
	delete(x.mucJoinTracks, roomBare)
	return "", tried, true
}

// takeMUCJoinTrackLocked removes and returns the chain of a confirmed join. Caller holds mucPendingJoinMu.
func (x *xmppClient) takeMUCJoinTrackLocked(roomBare string) *mucJoinTrack {
	t := x.mucJoinTracks[roomBare]
	delete(x.mucJoinTracks, roomBare)
	return t
}

// continueMUCJoinAfterConflict runs in its own goroutine (never on the Serve goroutine): it sends the
// join with the next alternative nick, or reports that every nick is taken.
func (x *xmppClient) continueMUCJoinAfterConflict(roomBare, next string, tried []string) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.join_retry", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	log := x.logFor(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomBare), attribute.Int("xmpp.muc.nick_attempts", len(tried)))
	}
	if next == "" {
		log.Warn().Str("room", roomBare).Strs("tried", tried).Msg("MUC join failed: all nicks taken")
		x.notifyMUC(ctx, roomBare, mucAllTakenNotice(x.mucCmdPrefix(), roomBare, tried))
		return
	}
	if err := x.sendMUCJoin(ctx, roomBare, next); err != nil {
		x.dropMUCJoinTrack(roomBare)
		telemetry.Fail(span, err)
		log.Warn().Err(err).Str("room", roomBare).Str("nick", next).Msg("MUC 409 retry: send failed")
		return
	}
	log.Info().Str("room", roomBare).Str("nick", next).Msg("MUC 409 retry: sent join with alternative nick")
}

// announceMUCNickChange runs in its own goroutine once a join with an alternative nick is confirmed:
// remember the nick for rejoins and tell the user.
func (x *xmppClient) announceMUCNickChange(roomBare, base, nick string) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.nick_notice", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	log := x.logFor(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomBare))
	}
	if err := x.rememberAutoMUCNick(ctx, roomBare, base, nick); err != nil {
		telemetry.Fail(span, err)
		log.Warn().Err(err).Str("room", roomBare).Msg("Failed to remember alternative MUC nick")
	}
	x.notifyMUC(ctx, roomBare, mucNickChangedNotice(x.mucCmdPrefix(), roomBare, base, nick))
}

// rememberAutoMUCNick stores a working alternative nick in the portal metadata (separate from the
// user-set MUCPreferredNick, see chooseMUCNick).
func (x *xmppClient) rememberAutoMUCNick(ctx context.Context, roomBare, base, nick string) error {
	if x.mucNickSaveHook != nil {
		return x.mucNickSaveHook(roomBare, base, nick)
	}
	if x.conn == nil || x.conn.br == nil {
		return fmt.Errorf("no bridge")
	}
	portal, err := x.conn.br.GetPortalByKey(ctx, networkid.PortalKey{ID: networkid.PortalID(roomBare), Receiver: x.loginID})
	if err != nil {
		return err
	}
	meta, _ := portal.Metadata.(*PortalMetadata)
	if meta == nil {
		meta = &PortalMetadata{}
	}
	meta.MUCAutoNick = nick
	meta.MUCAutoNickBase = base
	portal.Metadata = meta
	return portal.Save(ctx)
}

// notifyMUC posts an m.notice from the bridge bot to the room's portal (when it has a Matrix room)
// and to the user's management room.
func (x *xmppClient) notifyMUC(ctx context.Context, roomBare, text string) {
	if x.mucNoticeHook != nil {
		x.mucNoticeHook(roomBare, text)
		return
	}
	if x.conn == nil || x.conn.br == nil || x.conn.br.Bot == nil {
		return
	}
	var targets []id.RoomID
	if portal, err := x.conn.br.GetPortalByKey(ctx, networkid.PortalKey{ID: networkid.PortalID(roomBare), Receiver: x.loginID}); err == nil && portal != nil && portal.MXID != "" {
		targets = append(targets, portal.MXID)
	}
	if x.login != nil && x.login.User != nil && x.login.User.ManagementRoom != "" {
		targets = append(targets, x.login.User.ManagementRoom)
	}
	content := format.RenderMarkdown(text, true, false)
	content.MsgType = event.MsgNotice
	for _, rid := range targets {
		c := content
		if _, err := x.conn.br.Bot.SendMessage(ctx, rid, event.EventMessage, &event.Content{Parsed: &c}, nil); err != nil {
			x.logFor(ctx).Warn().Err(err).Msg("Failed to send MUC nick notice")
		}
	}
}

// mucCmdPrefix returns the configured bot command prefix.
func (x *xmppClient) mucCmdPrefix() string {
	if x.conn != nil && x.conn.br != nil && x.conn.br.Config != nil && x.conn.br.Config.CommandPrefix != "" {
		return x.conn.br.Config.CommandPrefix
	}
	return "!jabber"
}

func mucNickChangedNotice(prefix, room, base, nick string) string {
	return fmt.Sprintf("`%s` is taken in %s, joined as `%s`. Use `%s muc-nick %s <nick>` to pick another.", base, room, nick, prefix, room)
}

func mucAllTakenNotice(prefix, room string, tried []string) string {
	quoted := make([]string, len(tried))
	for i, t := range tried {
		quoted[i] = "`" + t + "`"
	}
	return fmt.Sprintf("Could not join %s: nicks %s are all taken. Use `%s muc-nick %s <nick>` and `%s join %s` to try again.",
		room, strings.Join(quoted, ", "), prefix, room, prefix, room)
}
