//go:build sipcalls

package connector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
	"github.com/williamkray/mrtc-sip-bridge/pkg/obs"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipaccount"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipua"
)

func init() { registerCallsCommands = registerSIPCommands }

// sipRegisterTimeout is how long sip-login waits for the registrar's first answer (a var for tests).
var sipRegisterTimeout = 30 * time.Second

// saveUserLogin persists ul (a var for tests).
var saveUserLogin = func(ctx context.Context, ul *bridgev2.UserLogin) error { return ul.Save(ctx) }

// sipCommands are the bot commands of the calls extension.
type sipCommands struct{ x *callsExt }

func registerSIPCommands(x *callsExt, proc *commands.Processor) {
	h := &sipCommands{x: x}
	for _, fh := range []*commands.FullHandler{{
		Func:          hostkit.WithSpan("sip-login", h.cmdLogin),
		Name:          "sip-login",
		RequiresLogin: true,
		Help: commands.HelpMeta{
			Section:     commands.HelpSectionAuth,
			Description: "Set up (or replace) the SIP account used for phone calls. The bridge asks for each value in turn.",
		},
	}, {
		Func:          hostkit.WithSpan("sip-logout", h.cmdLogout),
		Name:          "sip-logout",
		RequiresLogin: true,
		Help: commands.HelpMeta{
			Section:     commands.HelpSectionAuth,
			Description: "Remove the SIP account of this login and stop its registration.",
		},
	}, {
		Func:          hostkit.WithSpan("sip-status", h.cmdStatus),
		Name:          "sip-status",
		RequiresLogin: true,
		Help: commands.HelpMeta{
			Section:     commands.HelpSectionAuth,
			Description: "Show the SIP account and call status of this login.",
		},
	}} {
		proc.AddHandler(fh)
	}
	if !x.enabled {
		return
	}
	hostkit.RegisterCallCommands(proc, x.callCommandDeps())
}

// callCommandDeps adapts the call, hangup and dtmf commands to this host.
func (x *callsExt) callCommandDeps() hostkit.CommandDeps {
	return hostkit.CommandDeps{
		Calls: x.callAPI, // a nil interface (never a typed nil) while the manager does not exist
		Login: x.commandLoginWithAccount,
		RoomNumber: func(p *bridgev2.Portal) (string, bool) {
			if p.RoomType == database.RoomTypeGroupDM {
				return "", false
			}
			return cheogramE164(string(p.ID))
		},
		RedactDTMF: x.cfg.RedactDTMFCommand(),
		Widget:     x.widgetCommandInstaller(),
		Dialpad:    x.dialpadCommandDeps(),
	}
}

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

// commandLogin returns the login a command acts on: the portal's receiver (which must belong to the
// sender) in a portal, else the sender's default login. "" refusal when it is a real jabber login.
func (x *callsExt) commandLogin(ce *commands.Event) (*bridgev2.UserLogin, string) {
	return x.checkLogin(lookupCommandLogin(ce))
}

// checkLogin refuses a nil login and one that is not a real jabber login.
func (x *callsExt) checkLogin(ul *bridgev2.UserLogin) (*bridgev2.UserLogin, string) {
	if ul == nil {
		return nil, fmt.Sprintf("You are not logged in. Use `%s login` first.", x.commandPrefix())
	}
	if _, ok := ul.Client.(*xmppClient); !ok {
		return nil, "Log in to Jabber first."
	}
	return ul, ""
}

// commandLoginWithAccount is commandLogin that also refuses logins without a SIP account.
func (x *callsExt) commandLoginWithAccount(ce *commands.Event) (*bridgev2.UserLogin, string) {
	return x.checkLoginWithAccount(lookupCommandLogin(ce))
}

// checkLoginWithAccount is checkLogin that also refuses logins without an attached SIP account.
func (x *callsExt) checkLoginWithAccount(ul *bridgev2.UserLogin) (*bridgev2.UserLogin, string) {
	ul, refusal := x.checkLogin(ul)
	if ul == nil {
		return nil, refusal
	}
	if x.hasAccount(ul.ID) {
		return ul, ""
	}
	if _, ok := sipCreds(loginMeta(ul)); ok {
		return nil, fmt.Sprintf("Your SIP account is not registered right now. Use `%s sip-status`, or `%s sip-login` to set it up again.",
			x.commandPrefix(), x.commandPrefix())
	}
	return nil, fmt.Sprintf("No SIP account is set up for this login. Use `%s sip-login`.", x.commandPrefix())
}

func (h *sipCommands) cmdLogin(ce *commands.Event) {
	x := h.x
	if len(ce.Args) > 0 {
		ce.Redact() // the arguments may be a password
		ce.Reply("%s", fmt.Sprintf("Run `%s sip-login` without arguments; the bridge asks for each value.", x.commandPrefix()))
		return
	}
	// Prompt answers are unprefixed: outside the management room bridgev2 would not treat them as commands
	// and the bridge would send them to the contact.
	if !ce.Sudo && ce.RoomID != ce.User.ManagementRoom {
		ce.Reply("%s", fmt.Sprintf("Run `%s sip-login` in your management room (your DM with the bridge bot): answers typed in a chat room would be sent to the contact.", x.commandPrefix()))
		return
	}
	if !x.enabled {
		ce.Reply("SIP calls are disabled on this bridge (network.calls.enabled).")
		return
	}
	ul, refusal := x.commandLogin(ce)
	if ul == nil {
		ce.Reply("%s", refusal)
		return
	}
	if loginMeta(ul) == nil {
		ce.Reply("Log in to Jabber first.")
		return
	}
	if x.runtime() == nil {
		ce.Reply("The bridge is still starting. Try again in a moment.")
		return
	}
	ce.Reply("Enter your SIP account details. Answer each prompt as its own message (passwords may contain spaces; " +
		"the password message is deleted). Registering takes over incoming calls for this account; if a desk phone or ATA " +
		"uses the same account, calls may stop reaching it. This registers the account from this bridge. Make sure no " +
		"other bridge is registered with the same SIP account.")
	hostkit.PromptFields(ce, "SIP login", hostkit.LoginFields(x.cfg.DefaultCountryCode),
		func(ce *commands.Event, input map[string]string) { h.submitLogin(ce, ul, input) }, nil)
}

// submitLogin registers the entered account and, only when the registrar accepted it, saves it.
func (h *sipCommands) submitLogin(ce *commands.Event, ul *bridgev2.UserLogin, input map[string]string) {
	x := h.x
	x.mu.Lock()
	busy := x.loginBusy[ul.ID]
	if !busy {
		x.loginBusy[ul.ID] = true
	}
	x.mu.Unlock()
	if busy {
		ce.Reply("A SIP login is already in progress for this login.")
		return
	}
	defer func() {
		x.mu.Lock()
		delete(x.loginBusy, ul.ID)
		x.mu.Unlock()
	}()
	ctx, span := obs.Start(ce.Ctx, "cmd.sip_login.submit", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	outcome := "error"
	var rerr error
	defer func() {
		span.SetAttributes(obs.AttrOutcome.String(outcome))
		obs.Fail(span, rerr)
		obs.M().Logins.Add(ctx, 1, metric.WithAttributes(obs.AttrOutcome.String(outcome)))
	}()
	creds, err := sipaccount.ParseInput(input, x.cfg.DefaultCountryCode)
	if err != nil {
		rerr = err
		ce.Reply("%s", "Invalid SIP account details: "+obs.Scrub(err.Error()))
		return
	}
	span.SetAttributes(sipSpanAttrs(ul.ID, *creds)...)
	rt := x.runtime()
	meta := loginMeta(ul)
	if rt == nil || meta == nil {
		ce.Reply("The bridge is still starting. Try again in a moment.")
		return
	}
	// The previous account is stopped first: it frees its SIP port and avoids two registrations.
	hadPrev := x.detachAccount(ctx, ul.ID)
	prevNote := ""
	if hadPrev {
		prevNote = " Your previous SIP registration was stopped."
	}
	acct := x.newLoginAccount(ul, *creds)
	ev, err := sipaccount.Register(ctx, acct, sipRegisterTimeout)
	outcome = sipaccount.Outcome(ev, err)
	rerr = err
	lg := x.logFor(ctx).With().Str("login", string(ul.ID)).Str("sip_user", sipua.MaskUsername(creds.Username)).Logger()
	lg.Info().Str("outcome", outcome).Msg("SIP login attempt finished")
	registrar := creds.Registrar
	switch outcome {
	case "ok":
	case "auth_failed":
		ce.Reply("%s", fmt.Sprintf("The registrar rejected the credentials (SIP %d %s).", ev.StatusCode, ev.Reason)+prevNote)
		return
	case "timeout":
		ce.Reply("%s", fmt.Sprintf("Timed out registering with %s.", registrar)+prevNote)
		return
	case "transient_error":
		ce.Reply("%s", fmt.Sprintf("Could not register with %s: %s.", registrar, ev.Reason)+prevNote)
		return
	default:
		msg := "unknown error"
		if err != nil {
			msg = obs.Scrub(err.Error())
		}
		ce.Reply("%s", fmt.Sprintf("Could not start the SIP client: %s.", msg)+prevNote)
		return
	}
	prev, hadCreds := sipCreds(meta)
	setSIPCreds(meta, creds)
	if err = saveUserLogin(ctx, ul); err != nil {
		rerr, outcome = err, "error"
		if hadCreds {
			setSIPCreds(meta, prev)
		} else {
			setSIPCreds(meta, nil)
		}
		acct.Stop(ctx)
		ce.Reply("%s", "Registered, but saving failed: "+obs.Scrub(err.Error())+"."+prevNote)
		return
	}
	if !x.attachAccount(ul, acct) {
		outcome = "error"
		acct.Stop(ctx)
		ce.Reply("%s", fmt.Sprintf("Registered, but the Jabber login was disconnected meanwhile; run `%s sip-login` again.", x.commandPrefix()))
		return
	}
	ce.Reply("%s", fmt.Sprintf("Registered %s via %s. Incoming calls to that number ring in its Cheogram room; start a call in a number's room or use `%s call <number>`.",
		creds.DisplayNumber, registrar, x.commandPrefix()))
}

func (h *sipCommands) cmdLogout(ce *commands.Event) {
	x := h.x
	ul, refusal := x.commandLogin(ce)
	if ul == nil {
		ce.Reply("%s", refusal)
		return
	}
	meta := loginMeta(ul)
	creds, ok := sipCreds(meta)
	acct := x.account(ul.ID)
	if !ok && acct == nil {
		ce.Reply("No SIP account is set up for this login.")
		return
	}
	number := ""
	switch {
	case ok:
		number = creds.DisplayNumber
	case acct != nil:
		number = acct.Creds().DisplayNumber
	}
	x.detachAccount(ce.Ctx, ul.ID)
	if ok {
		setSIPCreds(meta, nil)
		if err := saveUserLogin(ce.Ctx, ul); err != nil {
			ce.Reply("%s", "The SIP account was stopped, but removing it failed: "+obs.Scrub(err.Error())+".")
			return
		}
	}
	ce.Reply("%s", fmt.Sprintf("SIP account removed. Calls to %s no longer ring here.", number))
}

func (h *sipCommands) cmdStatus(ce *commands.Event) {
	x := h.x
	ul, refusal := x.commandLogin(ce)
	if ul == nil {
		ce.Reply("%s", refusal)
		return
	}
	yn := func(b bool) string {
		if b {
			return "yes"
		}
		return "no"
	}
	lines := []string{"Calls enabled: " + yn(x.enabled), "Cheogram: " + map[bool]string{true: "on", false: "off"}[CheogramEnabled(x.br)]}
	if x.enabled && encryptionDefault(x.br) {
		lines = append(lines, "Warning: encryption.default is on, calls only ring in unencrypted rooms.")
	}
	creds, ok := sipCreds(loginMeta(ul))
	acct := x.account(ul.ID)
	if acct != nil {
		c := acct.Creds()
		creds, ok = &c, true
	}
	if !ok {
		lines = append(lines, "SIP account: none")
	} else {
		transport := creds.Transport
		if transport == "" {
			transport = "udp"
		}
		lines = append(lines,
			fmt.Sprintf("SIP account: %s via %s (%s)", sipua.MaskUsername(creds.Username), creds.Registrar, transport),
			"Number: "+creds.DisplayNumber,
			"Registration: "+registrationState(acct))
	}
	ce.Reply("%s", strings.Join(lines, "\n"))
}

// registrationState describes acct's registration for sip-status (no secrets).
func registrationState(acct *sipaccount.Account) string {
	if acct == nil || !acct.Running() {
		return "stopped"
	}
	ev := acct.State()
	switch ev.State {
	case sipua.StateRegistered:
		return "registered"
	case sipua.StateAuthFailed:
		return fmt.Sprintf("auth_failed (SIP %d)", ev.StatusCode)
	case sipua.StateTransientError:
		return fmt.Sprintf("transient_error (retry in %ds)", int(ev.NextRetry.Round(time.Second)/time.Second))
	case sipua.StateStopped:
		return "stopped"
	default:
		return "registering"
	}
}
