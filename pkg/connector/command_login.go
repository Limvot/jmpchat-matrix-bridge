package connector

import (
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
)

// CommandLogin picks the Jabber login a command acts on, like bridgev2's getClientForStartingChat.
// If args[0] equals (case-insensitively) the ID of one of the sender's own logins it is consumed as
// an explicit selection, unless that would leave fewer than minArgs arguments (then it is the command's
// own first argument, e.g. a JID that happens to be one of the user's accounts). Otherwise the login is
// the portal's receiver when the room is a portal whose login belongs to the sender, else the sender's
// default login. Another user's login can never be selected. login is nil when the sender has none.
func CommandLogin(ce *commands.Event, minArgs int) (login *bridgev2.UserLogin, args []string, explicit bool) {
	return pickCommandLogin(ce.Args, minArgs, ce.User.GetUserLogins(), portalLoginOf(ce), ce.User.GetDefaultLogin)
}

// portalLoginOf returns the login of the portal the command runs in if it belongs to the sender.
func portalLoginOf(ce *commands.Event) *bridgev2.UserLogin {
	if ce.Portal == nil || ce.Portal.Receiver == "" {
		return nil
	}
	ul := ce.Bridge.GetCachedUserLoginByID(ce.Portal.Receiver)
	if ul == nil || ul.UserMXID != ce.User.MXID {
		return nil
	}
	return ul
}

// pickCommandLogin is the pure core of CommandLogin: own are the sender's logins, portalLogin the
// sender's login of the current portal (nil if none) and def yields the default login.
func pickCommandLogin(args []string, minArgs int, own []*bridgev2.UserLogin, portalLogin *bridgev2.UserLogin, def func() *bridgev2.UserLogin) (*bridgev2.UserLogin, []string, bool) {
	if len(args) > 0 && len(args)-1 >= minArgs {
		for _, ul := range own {
			if ul != nil && strings.EqualFold(string(ul.ID), args[0]) {
				return ul, args[1:], true
			}
		}
	}
	if portalLogin != nil {
		return portalLogin, args, false
	}
	return def(), args, false
}

// CommandLoginNotice is appended to a success reply when the sender has several logins and did not
// pick one while outside a portal (so the default was used silently); "" otherwise.
func CommandLoginNotice(ce *commands.Event, login *bridgev2.UserLogin, explicit bool) string {
	if explicit || login == nil || ce.Portal != nil || len(ce.User.GetUserLogins()) < 2 {
		return ""
	}
	prefix := "!jabber"
	if ce.Bridge != nil && ce.Bridge.Config != nil && ce.Bridge.Config.CommandPrefix != "" {
		prefix = ce.Bridge.Config.CommandPrefix
	}
	return fmt.Sprintf("\n\nUsed account `%s`. Use `%s %s <login-jid> ...` to pick another (`%s list-logins`).", login.ID, prefix, ce.Command, prefix)
}
