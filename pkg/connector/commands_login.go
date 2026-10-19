package connector

import (
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
)

// lookupCommandLogin picks the login by the sender and room (a var for tests).
var lookupCommandLogin = func(ce *commands.Event) *bridgev2.UserLogin {
	if ce.Portal != nil && ce.Portal.Receiver != "" {
		ul := ce.Bridge.GetCachedUserLoginByID(ce.Portal.Receiver)
		if ul == nil || ul.UserMXID != ce.User.MXID {
			return nil
		}
		return ul
	}
	return ce.User.GetDefaultLogin()
}

// checkJabberLogin refuses a nil login and one that is not a real jabber login ("" refusal and the login
// otherwise). prefix is the configured command prefix, used in the refusal text.
func checkJabberLogin(ul *bridgev2.UserLogin, prefix string) (*bridgev2.UserLogin, string) {
	if ul == nil {
		return nil, fmt.Sprintf("You are not logged in. Use `%s login` first.", prefix)
	}
	if _, ok := ul.Client.(*xmppClient); !ok {
		return nil, "Log in to Jabber first."
	}
	return ul, ""
}
