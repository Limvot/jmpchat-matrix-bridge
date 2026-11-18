package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// maxRoomNameRunes bounds the name `setname` accepts. Matrix has no hard limit, but clients truncate
// long names and a room name is a label, not a document.
const maxRoomNameRunes = 255

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
// send=true sends the m.room.name state event: first through sender (nil = the bridge bot), then through
// the bot, which temporarily rejoins a room it was removed from to send it and leaves again afterwards
// (a bot-free DM portal room is a normal thing: two members make Matrix clients offer a direct call).
// send=false records a name the user already set in Matrix (the bridgev2 RoomNameHandlingNetworkAPI
// path) without sending anything.
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
		x.sendRoomName(ctx, portal, name, sender)
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
		if x.botInRoom(ctx, portal.MXID) {
			portal.UpdateBridgeInfo(ctx) // the bot must be in the room to refresh m.bridge
		}
	}
	return name, nil
}

// sendRoomName sends the m.room.name state event for portal. It tries the caller's intent first (a user
// with enough power in the room), then the bridge bot. A bot that is not in the room is invited and joined
// for the send and leaves again afterwards, so a portal whose bot was deliberately removed stays bot-free.
func (x *xmppClient) sendRoomName(ctx context.Context, portal *bridgev2.Portal, name string, sender bridgev2.MatrixAPI) {
	if portal.MXID == "" {
		return
	}
	if sender != nil {
		portal.Internal().UpdateName(ctx, name, sender, time.Time{}, false)
		if portal.NameSet {
			return
		}
	}
	botInRoom := x.botInRoom(ctx, portal.MXID)
	if !botInRoom {
		x.botJoinTemporarily(ctx, portal.MXID, sender)
	}
	portal.Internal().UpdateName(ctx, name, nil, time.Time{}, false)
	if !botInRoom {
		x.botLeave(ctx, portal.MXID)
	}
}

// botMXID returns the bridge bot's Matrix ID ("" when unavailable).
func (x *xmppClient) botMXID() id.UserID {
	if x.conn == nil || x.conn.br == nil || x.conn.br.Bot == nil {
		return ""
	}
	return x.conn.br.Bot.GetMXID()
}

// botInRoom reports whether the bridge bot is currently joined to roomID (false when it cannot be checked).
func (x *xmppClient) botInRoom(ctx context.Context, roomID id.RoomID) bool {
	bot := x.botMXID()
	mx, ok := x.conn.br.Matrix.(bridgev2.MatrixConnectorWithArbitraryRoomState)
	if !ok || bot == "" || roomID == "" {
		return false
	}
	evt, err := mx.GetStateEvent(ctx, roomID, event.StateMember, bot.String())
	if err != nil || evt == nil {
		return false
	}
	member := evt.Content.AsMember()
	return member != nil && member.Membership == event.MembershipJoin
}

// botJoinTemporarily invites the bridge bot into roomID through sender and joins it. Best-effort: the
// caller checks botInRoom before sending anything.
func (x *xmppClient) botJoinTemporarily(ctx context.Context, roomID id.RoomID, sender bridgev2.MatrixAPI) {
	bot := x.botMXID()
	if bot == "" || roomID == "" {
		return
	}
	if sender != nil {
		if err := sender.EnsureInvited(ctx, roomID, bot); err != nil {
			x.logFor(ctx).Debug().Err(err).Stringer("room_id", roomID).Msg("Could not invite the bridge bot")
		}
	}
	if err := x.conn.br.Bot.EnsureJoined(ctx, roomID); err != nil {
		x.logFor(ctx).Debug().Err(err).Stringer("room_id", roomID).Msg("Could not join the bridge bot")
	}
}

// botLeave makes the bridge bot leave roomID (a user may remove it from a DM portal to keep the room a
// two-member direct chat). A user always may leave a room, so no power level is needed. The appservice
// intent updates its membership cache, so the next temporary join is not skipped as "already in room".
func (x *xmppClient) botLeave(ctx context.Context, roomID id.RoomID) {
	if roomID == "" || x.conn == nil || x.conn.br == nil {
		return
	}
	if mc, ok := x.conn.br.Matrix.(*matrix.Connector); ok && mc.Bot != nil {
		if _, err := mc.Bot.LeaveRoom(ctx, roomID); err != nil {
			x.logFor(ctx).Debug().Err(err).Stringer("room_id", roomID).Msg("Could not make the bridge bot leave")
		}
		return
	}
	bot := x.botMXID()
	if bot == "" {
		return
	}
	_, err := x.conn.br.Bot.SendState(ctx, roomID, event.StateMember, bot.String(), &event.Content{
		Parsed: &event.MemberEventContent{Membership: event.MembershipLeave},
	}, time.Time{})
	if err != nil {
		x.logFor(ctx).Debug().Err(err).Stringer("room_id", roomID).Msg("Could not make the bridge bot leave")
	}
}

// LeaveBotCommand is the handler of `leave-bot`: the bridge bot leaves the current portal room. Use it in a
// DM portal whose room should stay a two-member direct chat (Matrix clients then offer a direct voice
// call). The portal keeps working; the bridge temporarily rejoins the bot when it has to send state.
func LeaveBotCommand(ce *commands.Event) {
	prefix := "!jabber"
	if ce.Bridge != nil && ce.Bridge.Config != nil && ce.Bridge.Config.CommandPrefix != "" {
		prefix = ce.Bridge.Config.CommandPrefix
	}
	if ce.Portal == nil || ce.Portal.Portal == nil || ce.Portal.MXID == "" || ce.Portal.Receiver == "" {
		ce.Reply("Run `%s leave-bot` inside a bridged room.", prefix)
		return
	}
	login := ce.Bridge.GetCachedUserLoginByID(ce.Portal.Receiver)
	if login == nil || ce.User == nil || login.UserMXID != ce.User.MXID {
		ce.Reply("You are not logged in to this room's account. Use `%s login` first.", prefix)
		return
	}
	x, _ := login.Client.(*xmppClient)
	if x == nil {
		ce.Reply("Log in to Jabber first.")
		return
	}
	if !x.botInRoom(ce.Ctx, ce.Portal.MXID) {
		ce.Reply("The bridge bot is already not in this room.")
		return
	}
	ce.Reply("The bridge bot is leaving this room. It comes back only to send a state event (e.g. `setname`); the portal keeps working without it.")
	x.botLeave(ce.Ctx, ce.Portal.MXID)
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
	reply := setRoomNameReply(ce.Ctx, prefix, ce.Portal, args, func(p *bridgev2.Portal) *bridgev2.UserLogin {
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
		// Send as the user who ran the command: the bridge bot may not be in the room (it can be removed
		// to keep a portal a two-member DM), while the user is. bridgev2 falls back to the bot otherwise.
		if ce.User == nil {
			return nil
		}
		return ce.User.DoublePuppet(ctx)
	})
	if ce.Reply("%s", reply) == "" {
		// The bot cannot post here either: reply as the user so the command is not silent.
		replyAsUser(ce, reply)
	}
}

// replyAsUser posts a command reply as the sender's double-puppeted account. It is used when the bridge
// bot is not in the room (a portal whose bot was deliberately removed to keep the room a two-member DM,
// where ce.Reply cannot work). The event carries the double-puppet source marker, so the bridge ignores
// it instead of sending the notice to XMPP as a message from the user.
func replyAsUser(ce *commands.Event, text string) {
	if ce == nil || ce.User == nil || ce.Bridge == nil {
		return
	}
	mc, ok := ce.Bridge.Matrix.(*matrix.Connector)
	if !ok || mc.AS == nil || mc.AS.DoublePuppetValue == "" {
		return
	}
	intent := ce.User.DoublePuppet(ce.Ctx)
	if intent == nil {
		return
	}
	content := format.RenderMarkdown(text, true, false)
	content.MsgType = event.MsgNotice
	_, err := intent.SendMessage(ce.Ctx, ce.OrigRoomID, event.EventMessage, &event.Content{
		Parsed: &content,
		Raw:    map[string]any{appservice.DoublePuppetKey: mc.AS.DoublePuppetValue},
	}, nil)
	if err != nil && ce.Log != nil {
		ce.Log.Err(err).Msg("Failed to reply to command as the user")
	}
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
