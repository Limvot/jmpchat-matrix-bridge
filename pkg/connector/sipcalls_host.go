//go:build sipcalls

package connector

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
	"github.com/williamkray/mrtc-sip-bridge/pkg/obs"
	"github.com/williamkray/mrtc-sip-bridge/pkg/phone"
)

// isCallPortal reports whether p is a Cheogram 1:1 portal of loginID (the only portals that carry
// calls: never group MMS, MUC or other DMs).
func isCallPortal(p *bridgev2.Portal, loginID networkid.UserLoginID) (string, bool) {
	if p == nil || p.Portal == nil || p.Receiver != loginID || p.RoomType == database.RoomTypeGroupDM {
		return "", false
	}
	return cheogramE164(string(p.ID))
}

// jabberCallHost is the embedded core.CallHost: a number's calls ring in its Cheogram portal, as its
// Cheogram ghost.
type jabberCallHost struct {
	x *callsExt
	// lookupPortal is a test seam (nil = br.GetPortalByMXID).
	lookupPortal func(ctx context.Context, roomID id.RoomID) (*bridgev2.Portal, error)
}

var _ core.CallHost = (*jabberCallHost)(nil)

func (x *callsExt) callHost() *jabberCallHost { return &jabberCallHost{x: x} }

func (h *jabberCallHost) login(ref core.LoginRef) (*bridgev2.UserLogin, error) {
	ul := h.x.br.GetCachedUserLoginByID(networkid.UserLoginID(ref))
	if ul == nil {
		return nil, core.ErrUnknownLogin
	}
	return ul, nil
}

// ResolveCallRoom returns the Cheogram portal and ghost of e164, creating the portal room (without the
// MAM backfill of the start-chat path) if needed. It does no XMPP I/O.
func (h *jabberCallHost) ResolveCallRoom(ctx context.Context, ref core.LoginRef, e164 string) (*core.CallRoom, error) {
	if !phone.IsE164(e164) {
		return nil, fmt.Errorf("%q is not an E.164 number", e164)
	}
	ul, err := h.login(ref)
	if err != nil {
		return nil, err
	}
	xc, ok := ul.Client.(*xmppClient)
	if !ok {
		return nil, errors.New("login is not connected")
	}
	jid := cheogramJID(e164)
	br := h.x.br
	portal, err := br.GetPortalByKey(ctx, networkid.PortalKey{ID: networkid.PortalID(jid), Receiver: ul.ID})
	if err != nil {
		return nil, fmt.Errorf("get portal: %w", err)
	}
	created := false
	if portal.MXID == "" {
		// Decorated with the call power levels by dmCreateChatResponse (decorateChatInfo).
		resp := xc.dmCreateChatResponse(jid)
		if err = portal.CreateMatrixRoom(ctx, ul, resp.PortalInfo); err != nil {
			return nil, fmt.Errorf("create room: %w", err)
		}
		created = true
	} else if _, err = h.ensurePL(ctx, portal.MXID); err != nil {
		h.x.logFor(ctx).Warn().Err(err).Stringer("room_id", portal.MXID).Msg("Failed to ensure call power levels")
	}
	room, err := h.roomFor(ctx, ul, portal, e164)
	if err != nil {
		return nil, err
	}
	room.Created = created
	if created {
		h.x.widgetEnqueue(room.RoomID, "Call "+e164)
	}
	return room, nil
}

func (h *jabberCallHost) ensurePL(ctx context.Context, roomID id.RoomID) (bool, error) {
	return h.x.ensureCallPL(ctx, roomID)
}

// roomFor builds the CallRoom of an existing portal room: the Cheogram ghost (joined), its intent and
// the encryption state. It also makes sure the room is in the owner's m.direct: Element X only
// offers voice calls and DM-style ringing for DM rooms, and functional members alone do not
// qualify a room (missing m.direct meant video-only call buttons and video-style rings).
func (h *jabberCallHost) roomFor(ctx context.Context, ul *bridgev2.UserLogin, portal *bridgev2.Portal, e164 string) (*core.CallRoom, error) {
	br := h.x.br
	ghost, err := br.GetGhostByID(ctx, networkid.UserID(cheogramJID(e164)))
	if err != nil {
		return nil, fmt.Errorf("get ghost: %w", err)
	}
	if err = ghost.Intent.EnsureJoined(ctx, portal.MXID); err != nil {
		h.x.logFor(ctx).Warn().Err(err).Stringer("room_id", portal.MXID).Msg("Failed to ensure the ghost is joined")
	}
	ensureDirectChat(ctx, h.x, ul, portal.MXID, ghost.Intent.GetMXID())
	h.x.pickupEnsure(ctx, ul.ID, e164, portal.MXID)
	gi, err := hostkit.NewGhostIntent(ghost)
	if err != nil {
		return nil, err
	}
	return &core.CallRoom{
		RoomID:     portal.MXID,
		RemoteE164: e164,
		Ghost:      gi,
		Encrypted:  hostkit.RoomEncrypted(ctx, br, portal.MXID),
	}, nil
}

// LookupCallRoom returns the call room of an existing Cheogram 1:1 portal of ref. It creates nothing
// and changes no power levels (it does the ghost join and the m.direct ensure of roomFor).
func (h *jabberCallHost) LookupCallRoom(ctx context.Context, ref core.LoginRef, roomID id.RoomID) (*core.CallRoom, error) {
	lookup := h.lookupPortal
	if lookup == nil {
		lookup = h.x.br.GetPortalByMXID
	}
	portal, err := lookup(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get portal: %w", err)
	}
	e164, ok := isCallPortal(portal, networkid.UserLoginID(ref))
	if !ok || portal.MXID == "" {
		return nil, core.ErrNotCallRoom
	}
	ul, err := h.login(ref)
	if err != nil {
		return nil, err
	}
	return h.roomFor(ctx, ul, portal, e164)
}

// roomLeaver is the LeaveRoom method of concrete Matrix intents (bridgev2.MatrixAPI does not
// expose it).
type roomLeaver interface {
	LeaveRoom(ctx context.Context, roomID id.RoomID, extra ...any) error
}

// pickupEnsure runs the pickup trim unless a test seam is installed.
func (x *callsExt) pickupEnsure(ctx context.Context, loginID networkid.UserLoginID, e164 string, roomID id.RoomID) {
	if x.pickupTrim != nil {
		x.pickupTrim(ctx, loginID, e164, roomID)
		return
	}
	x.ensurePickupRoom(ctx, loginID, e164, roomID)
}

// ensurePickupRoom trims a 1:1 number portal to [owner, ghost] for Element X's outbound-call
// pickup detection (ADR-010): Element Call picks the ring recipient as
// roomMemberUserIds.find(id => id != localUser) and does NOT respect io.element.functional_members
// (their own TODO), so any extra member (the bot, the login's own-account puppet) becomes the
// "recipient" and outbound calls ring client-side forever. The ghost is raised to PL 50 first so
// it manages the room after the bot leaves. Best-effort.
func (x *callsExt) ensurePickupRoom(ctx context.Context, loginID networkid.UserLoginID, e164 string, roomID id.RoomID) {
	if !x.cfg.BotLeavesPortalsEnabled() {
		return
	}
	ghost, err := x.br.GetGhostByID(ctx, networkid.UserID(cheogramJID(e164)))
	if err != nil || ghost == nil {
		return
	}
	ghostMXID := ghost.Intent.GetMXID()
	x.ensureGhostPL(ctx, roomID, ghost)
	// The login's own-account ghost (the user's XMPP self) must not sit in the portal.
	if self, err := x.br.GetGhostByID(ctx, networkid.UserID(string(loginID))); err == nil && self != nil && self.Intent.GetMXID() != ghostMXID {
		leaveCallRoom(ctx, self.Intent, roomID, x.logFor(ctx))
	}
	// The bot leaves last: the ghost owns the room afterwards (ADR-010 trade-offs apply).
	leaveCallRoom(ctx, x.br.Bot, roomID, x.logFor(ctx))
}

// ensureGhostPL raises the ghost to PL 50 as the bot, so the ghost can manage the room (redact
// dtmf commands, re-invite) once the bot has left.
func (x *callsExt) ensureGhostPL(ctx context.Context, roomID id.RoomID, ghost *bridgev2.Ghost) {
	pl, err := x.br.Matrix.GetPowerLevels(ctx, roomID)
	if err != nil {
		return
	}
	ghostMXID := ghost.Intent.GetMXID()
	if pl.Users[ghostMXID] >= 50 {
		return
	}
	if pl.Users == nil {
		pl.Users = make(map[id.UserID]int)
	}
	pl.Users[ghostMXID] = 50
	if _, err = x.br.Bot.SendState(ctx, roomID, event.StatePowerLevels, "", &event.Content{Parsed: pl}, time.Time{}); err != nil {
		x.logFor(ctx).Warn().Err(err).Stringer("room_id", roomID).Msg("Failed to raise the ghost power level")
	}
}

// leaveCallRoom is a best-effort room leave for the pickup trim.
func leaveCallRoom(ctx context.Context, intent bridgev2.MatrixAPI, roomID id.RoomID, log *zerolog.Logger) {
	l, ok := intent.(roomLeaver)
	if !ok {
		return
	}
	if err := l.LeaveRoom(ctx, roomID); err != nil {
		log.Warn().Err(err).Stringer("room_id", roomID).Msg("Failed to leave the call room (pickup trim)")
	}
}

// dmMarker is the part of the double-puppet intent that maintains m.direct
// (bridgev2/matrix.ASIntent.MarkAsDM).
type dmMarker interface {
	MarkAsDM(ctx context.Context, roomID id.RoomID, withUser id.UserID) error
}

// ensureDirectChat adds the room to the owner's m.direct via double puppeting (best-effort:
// failures only log; without double puppeting the user can still mark the room direct manually).
func ensureDirectChat(ctx context.Context, x *callsExt, ul *bridgev2.UserLogin, roomID id.RoomID, ghost id.UserID) {
	if ul == nil || ul.User == nil {
		return
	}
	dp := ul.User.DoublePuppet(ctx)
	if dp == nil {
		return
	}
	m, ok := dp.(dmMarker)
	if !ok {
		return
	}
	if err := m.MarkAsDM(ctx, roomID, ghost); err != nil {
		x.logFor(ctx).Warn().Err(err).Stringer("room_id", roomID).Msg("Failed to mark the call room as a direct chat")
	}
}

// SendCallNotice posts a notice in the room as the ghost.
func (h *jabberCallHost) SendCallNotice(ctx context.Context, room *core.CallRoom, text string) error {
	_, err := room.Ghost.SendMessage(ctx, room.RoomID, event.EventMessage, map[string]any{
		"msgtype": "m.notice",
		"body":    text,
	})
	return err
}

// OwnerMXID returns the Matrix user who owns the login, "" if unknown.
func (h *jabberCallHost) OwnerMXID(ref core.LoginRef) id.UserID {
	ul, err := h.login(ref)
	if err != nil {
		return ""
	}
	return ul.UserMXID
}

// MarkReadAsOwner sends a read receipt as the owner's double puppet.
func (h *jabberCallHost) MarkReadAsOwner(ctx context.Context, ref core.LoginRef, roomID id.RoomID, eventID id.EventID) error {
	ul, err := h.login(ref)
	if err != nil {
		return err
	}
	return hostkit.MarkReadAsOwner(ctx, ul, roomID, eventID)
}

// decorateChatInfo adds the call.member PL-0 overrides to the chat info of Cheogram 1:1 portals (new
// rooms and every resync), keeping other overrides.
func (x *callsExt) decorateChatInfo(portalID string, info *bridgev2.ChatInfo) {
	if !x.enabled || info == nil || info.Members == nil {
		return
	}
	if _, ok := cheogramE164(portalID); !ok {
		return
	}
	add := hostkit.CallPowerLevelOverrides()
	pl := info.Members.PowerLevels
	if pl == nil {
		info.Members.PowerLevels = add
		return
	}
	if pl.Events == nil {
		pl.Events = make(map[event.Type]int, len(add.Events))
	}
	for t, lvl := range add.Events {
		pl.Events[t] = lvl
	}
}

// suppressCallNotice drops XEP-0353 notices from Cheogram 1:1 numbers while the login's SIP account is
// registered (the SIP path rings the same PSTN call). Reads only the registered flag kept by onState:
// no account calls (they take locks held across UA start) and no I/O; runs on the XMPP Serve goroutine.
func (x *callsExt) suppressCallNotice(loginID networkid.UserLoginID, peerBareJID string) bool {
	if !x.enabled {
		return false
	}
	if _, ok := cheogramE164(peerBareJID); !ok {
		return false
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	lc := x.accounts[loginID]
	return lc != nil && lc.acct != nil && lc.registered
}

// ensureCallPL makes sure the room's owner may publish call.member (bridge bot sends the PL change).
func (x *callsExt) ensureCallPL(ctx context.Context, roomID id.RoomID) (bool, error) {
	if x.ensurePL != nil {
		return x.ensurePL(ctx, roomID)
	}
	return hostkit.EnsureCallPowerLevels(ctx, x.br, roomID)
}

// sweepCallPowerLevels adds the call power-level overrides to every existing Cheogram 1:1 portal of the
// login, in a goroutine under a bridge.calls.pl_sweep linked root (link from ctx's span).
func (x *callsExt) sweepCallPowerLevels(from context.Context, loginID networkid.UserLoginID) {
	carried := obs.CarrySpan(context.Background(), from)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		ctx, span := obs.StartLinkedRoot(ctx, "bridge.calls.pl_sweep", carried,
			trace.WithAttributes(attribute.String("xmpp.login.jid", string(loginID))))
		defer span.End()
		defer x.widgetSweep(ctx) // after the PL sweep (the widget needs the same PLs), still inside the span
		checked, updated, failed, err := x.runPLSweep(ctx, loginID)
		span.SetAttributes(
			attribute.Int("portals.checked", checked),
			attribute.Int("portals.updated", updated),
			attribute.Int("portals.failed", failed))
		if err != nil {
			obs.Fail(span, err)
			x.logFor(ctx).Warn().Err(err).Str("login", string(loginID)).Msg("Call power-level sweep failed")
			return
		}
		x.logFor(ctx).Info().Str("login", string(loginID)).Int("checked", checked).Int("updated", updated).
			Int("failed", failed).Msg("Call power-level sweep done")
	}()
}

// runPLSweep is the sequential body of the sweep (stops when ctx ends).
func (x *callsExt) runPLSweep(ctx context.Context, loginID networkid.UserLoginID) (checked, updated, failed int, err error) {
	list := x.listPortals
	if list == nil {
		list = x.br.GetAllPortalsWithMXID
	}
	portals, err := list(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	for _, p := range portals {
		if ctx.Err() != nil {
			return checked, updated, failed, ctx.Err()
		}
		e164, ok := isCallPortal(p, loginID)
		if !ok || p.MXID == "" {
			continue
		}
		checked++
		x.pickupEnsure(ctx, loginID, e164, p.MXID)
		changed, perr := x.ensureCallPL(ctx, p.MXID)
		switch {
		case perr != nil:
			failed++
			x.logFor(ctx).Debug().Err(perr).Stringer("room_id", p.MXID).Msg("Failed to update call power levels")
		case changed:
			updated++
		}
	}
	return checked, updated, failed, nil
}
