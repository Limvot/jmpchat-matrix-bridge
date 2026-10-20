package connector

import (
	"reflect"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func cmdTestLogin(id string) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID(id)}}
}

func TestPickCommandLogin(t *testing.T) {
	a, b := cmdTestLogin("a@x.org"), cmdTestLogin("b@x.org")
	own := []*bridgev2.UserLogin{a, b}
	def := func() *bridgev2.UserLogin { return a }

	// Explicit match consumes the argument; matching is case-insensitive.
	for _, first := range []string{"b@x.org", "B@X.org"} {
		ul, rest, explicit := pickCommandLogin([]string{first, "room@c.x.org"}, 1, own, nil, def)
		if ul != b || !explicit || !reflect.DeepEqual(rest, []string{"room@c.x.org"}) {
			t.Fatalf("explicit %q: got %v %v %v", first, ul, rest, explicit)
		}
	}
	// A login that is not the sender's (not in own) is never selected.
	ul, rest, explicit := pickCommandLogin([]string{"other@x.org", "room@c.x.org"}, 1, own, nil, def)
	if ul != a || explicit || len(rest) != 2 {
		t.Fatalf("foreign login: got %v %v %v", ul, rest, explicit)
	}
	// Portal receiver wins over the default, but not over an explicit pick.
	ul, _, explicit = pickCommandLogin([]string{"room@c.x.org"}, 1, own, b, def)
	if ul != b || explicit {
		t.Fatalf("portal: got %v %v", ul, explicit)
	}
	ul, _, explicit = pickCommandLogin([]string{"a@x.org", "room@c.x.org"}, 1, own, b, def)
	if ul != a || !explicit {
		t.Fatalf("explicit over portal: got %v %v", ul, explicit)
	}
	// Default fallback, also with no args at all.
	if ul, _, explicit = pickCommandLogin(nil, 0, own, nil, def); ul != a || explicit {
		t.Fatalf("default: got %v %v", ul, explicit)
	}
	// Not consumed when too few arguments would remain (e.g. `join b@x.org`, `add-to-roster b@x.org Name`).
	ul, rest, explicit = pickCommandLogin([]string{"b@x.org"}, 1, own, nil, def)
	if ul != a || explicit || !reflect.DeepEqual(rest, []string{"b@x.org"}) {
		t.Fatalf("join own jid: got %v %v %v", ul, rest, explicit)
	}
	ul, rest, explicit = pickCommandLogin([]string{"b@x.org", "Name"}, 2, own, nil, def)
	if ul != a || explicit || len(rest) != 2 {
		t.Fatalf("add-to-roster own jid: got %v %v %v", ul, rest, explicit)
	}
	// ...and the documented escape: write the login first.
	ul, rest, explicit = pickCommandLogin([]string{"a@x.org", "b@x.org", "Name"}, 2, own, nil, def)
	if ul != a || !explicit || !reflect.DeepEqual(rest, []string{"b@x.org", "Name"}) {
		t.Fatalf("escape: got %v %v %v", ul, rest, explicit)
	}
	// No logins at all.
	if ul, _, _ = pickCommandLogin([]string{"x"}, 0, nil, nil, func() *bridgev2.UserLogin { return nil }); ul != nil {
		t.Fatalf("no logins: got %v", ul)
	}
}

func TestMUCCommandArgs(t *testing.T) {
	a, b := cmdTestLogin("a@x.org"), cmdTestLogin("b@x.org")
	own := []*bridgev2.UserLogin{a, b}
	def := func() *bridgev2.UserLogin { return a }
	// join [login] <room>
	ul, rest, _ := pickCommandLogin(strings.Fields("b@x.org room@c.x.org"), 1, own, nil, def)
	if ul != b || len(rest) != 1 || rest[0] != "room@c.x.org" {
		t.Fatalf("join: %v %v", ul, rest)
	}
	// muc-nick [login] <room> <nick>; `muc-nick b@x.org nick` keeps b@x.org as the room.
	ul, rest, _ = pickCommandLogin(strings.Fields("b@x.org room@c.x.org Nick"), 2, own, nil, def)
	if ul != b || len(rest) != 2 {
		t.Fatalf("muc-nick: %v %v", ul, rest)
	}
	ul, rest, explicit := pickCommandLogin(strings.Fields("b@x.org Nick"), 2, own, nil, def)
	if ul != a || explicit || len(rest) != 2 {
		t.Fatalf("muc-nick short: %v %v", ul, rest)
	}
}
