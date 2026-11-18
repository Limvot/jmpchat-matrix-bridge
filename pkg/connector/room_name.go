package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// maxRoomNameRunes bounds the name `setname` accepts. Matrix has no hard limit, but clients truncate
// long names and a room name is a label, not a document.
const maxRoomNameRunes = 255

// ensureBotInRoom heals a portal whose room the bridge bot is no longer in (it can leave one while the
// portal lives on): a bot outside the room cannot send its own state events or command replies, so the
// user's intent invites it and the bot joins. Best-effort; errors are only logged.
func (x *xmppClient) ensureBotInRoom(ctx context.Context, portal *bridgev2.Portal, sender bridgev2.MatrixAPI) {
	if sender == nil || x.conn == nil || x.conn.br == nil || x.conn.br.Bot == nil || portal == nil || portal.MXID == "" {
		return
	}
	botMXID := x.conn.br.Bot.GetMXID()
	if err := sender.EnsureInvited(ctx, portal.MXID, botMXID); err != nil {
		x.logFor(ctx).Debug().Err(err).Stringer("room_id", portal.MXID).Msg("Could not invite the bridge bot to the room")
		return
	}
	if err := x.conn.br.Bot.EnsureJoined(ctx, portal.MXID); err != nil {
		x.logFor(ctx).Debug().Err(err).Stringer("room_id", portal.MXID).Msg("Could not join the bridge bot to the room")
	}
}

// errRoomNameNotSent means the override was stored but the m.room.name event was refused (the
// bridge bot is not in the room and the user's double-puppet intent could not send it either).
var errRoomNameNotSent = errors.New("the bridge could not send the m.room.name event")

// savePortal persists the portal after a name change (a var for tests).
var savePortal = func(ctx context.Context, portal *bridgev2.Portal) error { return portal.Save(ctx) }

// roomNameOverride returns the Matrix room name the user pinned for the portal, or "" when the name
// follows the XMPP side (roster name for a DM, disco#info room name for a MUC).
func roomNameOverride(portal *bridgev2.Portal) string {
	if portal == nil || portal.Portal == nil {
		return ""
	}
	meta, _ := portal.Metadata.(*PortalMetadata)
	if meta == nil {
		return ""
	}
	return meta.CustomName
}

// cleanRoomName joins args into one room name: control characters removed, ends trimmed.
func cleanRoomName(args []string) string {
	name := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, strings.Join(args, " "))
	return strings.TrimSpace(name)
}

// applyRoomName sets the Matrix room name of portal to name and persists the override in the portal
// metadata so the bridge's own resyncs (roster sync, ChatResync, MUC rejoin) stop overwriting it.
// name == "" clears the override and reverts to the name the XMPP side gives the room.
// send=true sends the m.room.name state event through sender (nil = the bridge bot; bridgev2 retries
// with the bot when sender is refused); send=false records a name the user already set in Matrix (the
// bridgev2 RoomNameHandlingNetworkAPI path) without sending anything.
// It returns the name now in effect.
func (x *xmppClient) applyRoomName(ctx context.Context, portal *bridgev2.Portal, name string, sender bridgev2.MatrixAPI, send bool) (string, error) {
	if portal == nil || portal.Portal == nil {
		return "", errors.New("no portal")
	}
	meta, _ := portal.Metadata.(*PortalMetadata)
	if meta == nil {
		meta = &PortalMetadata{}
	}
	portal.Metadata = meta
	if name == "" {
		// Clear first: getChatInfo reads the override, so the default name is derived without it.
		prev := meta.CustomName
		meta.CustomName = ""
		info, err := x.getChatInfo(ctx, portal)
		if err != nil {
			meta.CustomName = prev
			return "", err
		}
		if info == nil || info.Name == nil || *info.Name == "" {
			meta.CustomName = prev
			return "", errors.New("could not derive the room's default name")
		}
		name = *info.Name
	} else {
		meta.CustomName = name
	}
	if send && portal.MXID != "" {
		x.ensureBotInRoom(ctx, portal, sender)
		// Try the caller's intent first (the user who ran the command is in the room; the bot may not be),
		// and fall back to the bot when that intent cannot send (bridgev2 does not retry in this direction).
		if !portal.Internal().UpdateName(ctx, name, sender, time.Time{}, false) && sender != nil {
			portal.Internal().UpdateName(ctx, name, nil, time.Time{}, false)
		}
	} else {
		portal.Name = name
		portal.NameSet = true
	}
	if err := savePortal(ctx, portal); err != nil {
		return name, err
	}
	if send && portal.MXID != "" {
		if !portal.NameSet {
			return name, errRoomNameNotSent
		}
		portal.UpdateBridgeInfo(ctx)
	}
	return name, nil
}

// HandleMatrixRoomName implements bridgev2.RoomNameHandlingNetworkAPI: a room name the user sets in
// Matrix is stored as the portal's override instead of being reverted or lost on the next resync.
// An empty name clears the override (back to the XMPP-side name).
func (x *xmppClient) HandleMatrixRoomName(ctx context.Context, msg *bridgev2.MatrixRoomName) (bool, error) {
	if msg == nil || msg.Portal == nil || msg.Content == nil {
		return false, nil
	}
	_, span := telemetry.Start(ctx, "matrix.handler.room_name")
	defer span.End()
	if _, err := x.applyRoomName(ctx, msg.Portal, cleanRoomName([]string{msg.Content.Name}), nil, false); err != nil {
		telemetry.Fail(span, err)
		return false, err
	}
	return true, nil
}

// SetRoomNameCommand is the handler of `setname [<login JID>] <new name>|reset`: inside a bridged room
// it pins the Matrix room name (the bridge never overwrites it again), or with `reset` drops the pin and
// follows the XMPP-side name again. The name lives in the portal metadata, so it survives restarts.
func SetRoomNameCommand(ce *commands.Event) {
	prefix := "!jabber"
	if ce.Bridge != nil && ce.Bridge.Config != nil && ce.Bridge.Config.CommandPrefix != "" {
		prefix = ce.Bridge.Config.CommandPrefix
	}
	// An explicit leading login ID selects the account (the name is then the rest).
	login, args, explicit := CommandLogin(ce, 1)
	ce.Reply("%s", setRoomNameReply(ce.Ctx, prefix, ce.Portal, args, func(p *bridgev2.Portal) *bridgev2.UserLogin {
		if explicit {
			return login
		}
		// The portal's login must belong to the sender (a portal room can have another user's login).
		ul := ce.Bridge.GetCachedUserLoginByID(p.Receiver)
		if ul == nil || ce.User == nil || ul.UserMXID != ce.User.MXID {
			return nil
		}
		return ul
	}, func(ctx context.Context) bridgev2.MatrixAPI {
		// Send as the user who ran the command: the bridge bot may not be in the room (it can leave one
		// without the portal noticing), while the user is. bridgev2 falls back to the bot on M_FORBIDDEN.
		if ce.User == nil {
			return nil
		}
		return ce.User.DoublePuppet(ctx)
	}))
}

// senderOf returns the intent doublePuppet builds (nil when there is none, i.e. the bot is used).
func senderOf(ctx context.Context, doublePuppet func(context.Context) bridgev2.MatrixAPI) bridgev2.MatrixAPI {
	if doublePuppet == nil {
		return nil
	}
	return doublePuppet(ctx)
}

// setRoomNameReply runs `setname` for portal and returns the reply text. loginFor resolves the
// portal's login (nil = none or not the sender's); doublePuppet returns the intent used to send the
// m.room.name event (nil = the bridge bot).
func setRoomNameReply(ctx context.Context, prefix string, portal *bridgev2.Portal, args []string, loginFor func(*bridgev2.Portal) *bridgev2.UserLogin, doublePuppet func(context.Context) bridgev2.MatrixAPI) string {
	if portal == nil || portal.Portal == nil || portal.Receiver == "" {
		return fmt.Sprintf("Run `%s setname <new name>` inside a bridged room.", prefix)
	}
	if portal.MXID == "" {
		return "This room has not been created in Matrix yet; try again once it exists."
	}
	name := cleanRoomName(args)
	if name == "" {
		return fmt.Sprintf("Usage: `%s setname <new name>` — or `%s setname reset` to go back to the name the XMPP side gives the room.", prefix, prefix)
	}
	if utf8.RuneCountInString(name) > maxRoomNameRunes {
		return fmt.Sprintf("That name is too long (max %d characters).", maxRoomNameRunes)
	}
	reset := strings.EqualFold(name, "reset")
	if reset {
		name = ""
	}
	login := loginFor(portal)
	if login == nil || string(login.ID) != string(portal.Receiver) {
		return fmt.Sprintf("You must be logged in. Use `%s login` first.", prefix)
	}
	x, ok := login.Client.(*xmppClient)
	if !ok {
		return "Log in to Jabber first."
	}
	if !x.IsLoggedIn() {
		return "Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again."
	}
	got, err := x.applyRoomName(ctx, portal, name, senderOf(ctx, doublePuppet), true)
	if errors.Is(err, errRoomNameNotSent) {
		return fmt.Sprintf("Saved the name **%s**, but Matrix refused the room name change (no permission, or the bridge is not in this room). It will be applied on the next room sync.", got)
	}
	if err != nil {
		return fmt.Sprintf("Failed to set the room name: %v", err)
	}
	if reset {
		return fmt.Sprintf("Room name reset to **%s**; it follows the XMPP side again.", got)
	}
	return fmt.Sprintf("Room name set to **%s**. The bridge will keep this name; `%s setname reset` goes back to the XMPP-side name.", got, prefix)
}
