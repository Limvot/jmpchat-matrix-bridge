//go:build sipcalls

package connector

import (
	"errors"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
)

func TestRosterOnlyCommand(t *testing.T) {
	h := newCmdHarness(t, true)
	xc := h.ul.Client.(*xmppClient)
	reply := func(args ...string) string {
		t.Helper()
		h.run(t, "roster-only-calls", args...)
		return h.bot.last()
	}

	// Roster not loaded yet.
	if got := reply(); got != "Roster-only calls: off (roster loaded: no)" {
		t.Fatalf("status: %q", got)
	}
	got := reply("on")
	if !strings.HasPrefix(got, "Incoming calls now ring only for numbers in your Jabber roster (0 phone contacts). Other callers are sent to voicemail. Turn off with `!jc roster-only-calls off`. Your roster has not loaded yet: calls ring normally until it has.") {
		t.Fatalf("on (not loaded): %q", got)
	}
	if !h.x.loginPrefs(h.ul).RosterOnly || h.saves != 1 {
		t.Fatalf("not enabled/saved: saves=%d", h.saves)
	}
	if !strings.Contains(got, "No SIP account is registered for this login yet (`!jc sip-login`)") {
		t.Fatalf("missing no-account note: %q", got)
	}

	// Loaded but empty: enabling works, with a prominent warning.
	xc.applyRosterItems(true, rosterEntry{jid: "bob@example.com", name: "Bob"})
	got = reply("ON")
	if !strings.Contains(got, "(0 phone contacts)") || !strings.Contains(got, "**Warning: your roster has no phone contacts, so every incoming call will be sent to voicemail.**") ||
		!strings.Contains(got, "`!jc save-contact`, `!jc add-to-roster`") {
		t.Fatalf("on (empty): %q", got)
	}

	// Phone contacts only are counted (named or not).
	xc.applyRosterItems(true,
		rosterEntry{jid: "bob@example.com", name: "Bob"},
		rosterEntry{jid: "+15550001111@cheogram.com"},
		rosterEntry{jid: "+15550002222@cheogram.com", name: "Al"},
		rosterEntry{jid: "+1555,+1666@cheogram.com"})
	got = reply("on")
	if !strings.Contains(got, "(2 phone contacts)") || strings.Contains(got, "Warning") {
		t.Fatalf("on: %q", got)
	}
	if got = reply("status"); got != "Roster-only calls: on (roster loaded: yes, 2 phone contacts)" {
		t.Fatalf("status: %q", got)
	}
	if got = reply("off"); got != "Incoming calls now ring for everyone." {
		t.Fatalf("off: %q", got)
	}
	if h.x.loginPrefs(h.ul).RosterOnly || callPrefsOf(loginMeta(h.ul)).RosterOnly {
		t.Fatal("not disabled")
	}
	if got = reply("status"); got != "Roster-only calls: off (roster loaded: yes, 2 phone contacts)" {
		t.Fatalf("status: %q", got)
	}

	// sip-status shows the setting.
	reply("on")
	h.run(t, "sip-status")
	if got = h.bot.last(); !strings.Contains(got, "Roster-only calls: on") {
		t.Fatalf("sip-status: %q", got)
	}

	// Bad usage.
	saves := h.saves
	for _, args := range [][]string{{"maybe"}, {"on", "off"}} {
		if got = reply(args...); got != "Usage: `!jc roster-only-calls on|off|status`" {
			t.Fatalf("usage %v: %q", args, got)
		}
	}
	if h.saves != saves {
		t.Fatal("saved on bad usage")
	}

	// Save failure.
	h.saveE = errors.New("db down")
	if got = reply("off"); got != "Could not save the setting: db down." {
		t.Fatalf("save failure: %q", got)
	}
	if !h.x.loginPrefs(h.ul).RosterOnly {
		t.Fatal("failed save changed the setting")
	}
	h.saveE = nil

	// No login / stub login / calls disabled.
	lookupCommandLogin = func(*commands.Event) *bridgev2.UserLogin { return nil }
	if got = reply("on"); got != "You are not logged in. Use `!jc login` first." {
		t.Fatalf("no login: %q", got)
	}
	lookupCommandLogin = func(*commands.Event) *bridgev2.UserLogin { return h.ul }
	h.x.enabled = false
	if got = reply("on"); got != "SIP calls are disabled on this bridge (network.calls.enabled)." {
		t.Fatalf("disabled: %q", got)
	}
}

func TestRosterOnlyCommandHelp(t *testing.T) {
	h := newCmdHarness(t, false)
	fh := h.handler(t, "roster-only-calls")
	if !fh.RequiresLogin || fh.Help.Section != commands.HelpSectionAuth {
		t.Fatalf("login=%v section=%v", fh.RequiresLogin, fh.Help.Section)
	}
}
