package connector

import (
	"context"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
)

// maxContactNameRunes bounds the name `save-contact` accepts.
const maxContactNameRunes = 64

// SaveContactCommand is the handler of `save-contact <name>`: inside a one-to-one chat it saves the
// chat's contact (the portal's JID, or Cheogram number) to the login's roster under <name>. The roster
// set refreshes the ghost and the portal name (SetRosterItem queues the resync). Works in every build.
func SaveContactCommand(ce *commands.Event) {
	prefix := "!jabber"
	if ce.Bridge != nil && ce.Bridge.Config != nil && ce.Bridge.Config.CommandPrefix != "" {
		prefix = ce.Bridge.Config.CommandPrefix
	}
	ce.Reply("%s", saveContactReply(ce.Ctx, prefix, ce.Portal, ce.Args, func(p *bridgev2.Portal) *bridgev2.UserLogin {
		// The portal's login must belong to the sender (a portal room can have another user's login).
		ul := ce.Bridge.GetCachedUserLoginByID(p.Receiver)
		if ul == nil || ce.User == nil || ul.UserMXID != ce.User.MXID {
			return nil
		}
		return ul
	}))
}

// cleanContactName joins args into one roster name: control characters removed, ends trimmed.
func cleanContactName(args []string) string {
	name := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, strings.Join(args, " "))
	return strings.TrimSpace(name)
}

// saveContactReply runs save-contact for portal and returns the reply text. loginFor resolves the
// portal's login (nil = none or not the sender's).
func saveContactReply(ctx context.Context, prefix string, portal *bridgev2.Portal, args []string, loginFor func(*bridgev2.Portal) *bridgev2.UserLogin) string {
	if portal == nil || portal.Portal == nil || portal.Receiver == "" {
		return fmt.Sprintf("Run `%s save-contact <name>` inside a chat with the person you want to save.", prefix)
	}
	jidStr := string(portal.ID)
	if _, isGroupMMS := ParseCheogramGroupMMSJID(jidStr); isGroupMMS || portal.RoomType == database.RoomTypeGroupDM || isMUCJID(jidStr) {
		return "`save-contact` only works in a one-to-one chat, not in a group chat or room."
	}
	name := cleanContactName(args)
	if name == "" {
		return fmt.Sprintf("Usage: `%s save-contact <name>` — run it inside the chat with the person you want to save, e.g. `%s save-contact Alice`.", prefix, prefix)
	}
	if utf8.RuneCountInString(name) > maxContactNameRunes {
		return fmt.Sprintf("The name is too long (at most %d characters).", maxContactNameRunes)
	}
	ul := loginFor(portal)
	if ul == nil {
		return fmt.Sprintf("You must be logged in. Use `%s login` first.", prefix)
	}
	rm, ok := ul.Client.(RosterManager)
	if !ok {
		return "Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again."
	}
	if err := rm.SetRosterItem(ctx, jidStr, name); err != nil {
		return fmt.Sprintf("Failed to save contact: %v", err)
	}
	return fmt.Sprintf("Saved **%s** (%s) to your roster. The room name will update shortly.", name, jidStr)
}
