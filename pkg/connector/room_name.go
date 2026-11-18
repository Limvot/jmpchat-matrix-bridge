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
// send=true sends the m.room.name state event as the bridge bot (command path); send=false records a
// name the user already set in Matrix (the bridgev2 RoomNameHandlingNetworkAPI path).
// It returns the name now in effect.
func (x *xmppClient) applyRoomName(ctx context.Context, portal *bridgev2.Portal, name string, send bool) (string, error) {
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
		portal.Internal().UpdateName(ctx, name, nil, time.Time{}, false)
	} else {
		portal.Name = name
		portal.NameSet = true
	}
	if err := savePortal(ctx, portal); err != nil {
		return "", err
	}
	if send && portal.MXID != "" {
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
	if _, err := x.applyRoomName(ctx, msg.Portal, cleanRoomName([]string{msg.Content.Name}), false); err != nil {
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
	}))
}

// setRoomNameReply runs `setname` for portal and returns the reply text. loginFor resolves the
// portal's login (nil = none or not the sender's).
func setRoomNameReply(ctx context.Context, prefix string, portal *bridgev2.Portal, args []string, loginFor func(*bridgev2.Portal) *bridgev2.UserLogin) string {
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
	got, err := x.applyRoomName(ctx, portal, name, true)
	if err != nil {
		return fmt.Sprintf("Failed to set the room name: %v", err)
	}
	if reset {
		return fmt.Sprintf("Room name reset to **%s**; it follows the XMPP side again.", got)
	}
	return fmt.Sprintf("Room name set to **%s**. The bridge will keep this name; `%s setname reset` goes back to the XMPP-side name.", got, prefix)
}
