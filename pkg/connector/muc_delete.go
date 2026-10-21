package connector

import (
	"context"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
)

// Deleting a MUC portal on the Matrix side (the bridge's delete-portal / delete-all-portals commands, or
// Beeper's com.beeper.delete_chat) counts as leaving the room. bridgev2 invokes no connector hook for the
// two commands, so cmd/jmpchat-matrix-bridge wraps them and calls LeaveOnPortalDelete before delegating;
// HandleMatrixDeleteChat covers the Beeper path.
//
// Once the portal row is gone the MUCLeft flag cannot carry the decision, so the bookmark's autojoin=false
// is what keeps the room from coming back (the connect-time import skips autojoin=false). If that write
// cannot be made, the user is told in the management room.

// portalDeleteLeavesMUC reports whether deleting the portal is a MUC leave: group rooms only, never DMs
// or Cheogram group MMS.
func (x *xmppClient) portalDeleteLeavesMUC(ctx context.Context, portal *bridgev2.Portal) bool {
	if portal == nil || IsCheogramDMJID(string(portal.ID)) {
		return false
	}
	return x.portalIsMUC(ctx, portal)
}

// leaveOnPortalDelete sends the MUC leave presence (when joined), sets the bookmark's autojoin=false
// (fresh fetch + merge, never creates a bookmark) and warns the user when the room may come back. It
// never fails the deletion. Never call it on the Serve goroutine.
func (x *xmppClient) leaveOnPortalDelete(ctx context.Context, portal *bridgev2.Portal) {
	if !x.portalDeleteLeavesMUC(ctx, portal) {
		return
	}
	room := string(portal.ID)
	log := x.logFor(ctx)
	if x.getMUCOurOccupantJID(room) != "" {
		if err := x.leaveMUC(ctx, room); err != nil {
			log.Warn().Err(err).Str("room", room).Msg("Portal deleted: failed to send the MUC leave presence")
		}
	}
	switch out := x.publishBookmarkFor(ctx, room, false, "", false); out {
	case bmWritten, bmNoop:
		return
	default:
		if out == bmSkipped {
			// No readable bookmark storage means no import either: nothing can bring the room back.
			if s, ok := x.ensureBookmarkGate(ctx); ok && !s.Read {
				return
			}
		}
		log.Warn().Str("room", room).Int("outcome", int(out)).Msg("Portal deleted: could not turn off the room's bookmark auto-join; it may be re-imported")
		x.notifyManagement(ctx, fmt.Sprintf("You deleted the Matrix room for %s and the bridge left it, but its XMPP bookmark could not be set to not auto-join, so the room may come back at a later connect. Leave it in your other XMPP clients to be sure.", room))
	}
}

// HandleMatrixDeleteChat implements bridgev2.DeleteChatHandlingNetworkAPI (Beeper chat deletion). Only MUC
// portals are handled; everything else keeps the old "not supported" answer, so DM deletion is unchanged.
func (x *xmppClient) HandleMatrixDeleteChat(ctx context.Context, msg *bridgev2.MatrixDeleteChat) error {
	if msg == nil || !x.portalDeleteLeavesMUC(ctx, msg.Portal) {
		return bridgev2.ErrDeleteChatNotSupported
	}
	x.leaveOnPortalDelete(ctx, msg.Portal)
	return nil
}

// LeaveOnPortalDelete is called by the wrapped delete-portal and delete-all-portals commands before the
// portal is deleted. The portal's receiver login does the work; portals without a connected Jabber login
// are skipped.
func LeaveOnPortalDelete(ctx context.Context, br *bridgev2.Bridge, portals ...*bridgev2.Portal) {
	for _, p := range portals {
		if p == nil || p.Receiver == "" {
			continue
		}
		ul := br.GetCachedUserLoginByID(p.Receiver)
		if ul == nil {
			continue
		}
		if x, ok := ul.Client.(*xmppClient); ok {
			x.leaveOnPortalDelete(ctx, p)
		}
	}
}
