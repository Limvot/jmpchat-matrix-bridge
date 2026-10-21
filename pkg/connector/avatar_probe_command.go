package connector

import (
	"context"
	"strings"
	"sync"

	"maunium.net/go/mautrix/bridgev2/commands"
	"mellium.im/xmpp/jid"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// avatarProbeBusy holds the logins with a probe running (one at a time, so a typo loop cannot
// flood a real room with IQs).
var avatarProbeBusy sync.Map // networkid.UserLoginID -> struct{}

// CmdAvatarProbe is the avatar-probe bot command: `avatar-probe [<login-jid>] <jid>`. Bridgev2 runs
// command handlers off the XMPP Serve goroutine, but four probes can take minutes, so the run is
// detached and reports through a copy of the command event (as carddav-sync does).
func CmdAvatarProbe(ce *commands.Event) {
	prefix := "!jabber"
	if ce.Bridge != nil && ce.Bridge.Config != nil && ce.Bridge.Config.CommandPrefix != "" {
		prefix = ce.Bridge.Config.CommandPrefix
	}
	login, args, explicit := CommandLogin(ce, 1)
	if len(args) != 1 {
		ce.Reply("Usage: `%s avatar-probe [<login-jid>] <room@service/nick | jid>`. Sends only IQ get queries to that JID.", prefix)
		return
	}
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", prefix)
		return
	}
	target, err := jid.Parse(strings.TrimSpace(args[0]))
	if err != nil || target.Localpart() == "" {
		ce.Reply("Invalid JID. Use `room@service/nick` or `user@domain`.")
		return
	}
	x, ok := login.Client.(*xmppClient)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	if _, loaded := avatarProbeBusy.LoadOrStore(login.ID, struct{}{}); loaded {
		ce.Reply("An avatar probe is already running for this login.")
		return
	}
	ev := *ce
	from := telemetry.CarrySpan(context.Background(), ce.Ctx)
	base := context.WithoutCancel(ce.Ctx)
	clean := ce.Bridge.Log.With().Str("component", "avatar-probe").Logger()
	notice := CommandLoginNotice(ce, login, explicit)
	loginID := login.ID
	ce.Reply("Avatar probe started (up to 60 s per probe). I will report back when it finishes.")
	go func() {
		defer avatarProbeBusy.Delete(loginID)
		ctx, span := telemetry.StartLinkedRoot(base, "cmd.avatar_probe", from)
		defer span.End()
		log := telemetry.Logger(ctx, &clean)
		ev.Ctx, ev.Log = ctx, log
		lines := runAvatarProbe(ctx, sessionProbeSender(sess), target, avatarProbeTimeout, log)
		ev.Reply("%s%s", formatProbeReply(target.String(), lines), notice)
	}()
}
