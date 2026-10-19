package connector

import (
	"context"
	"errors"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// fakeRosterClient is a bridgev2.NetworkAPI-less stand-in: only the RosterManager part matters.
type fakeRosterClient struct {
	stubJabberClient
	jid, name string
	calls     int
	err       error
}

func (f *fakeRosterClient) SetRosterItem(_ context.Context, jidStr, name string) error {
	f.calls++
	f.jid, f.name = jidStr, name
	return f.err
}

func testDMPortal(id string, rt database.RoomType) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID(id), Receiver: "me@example.com"}, RoomType: rt}}
}

func TestSaveContactReply(t *testing.T) {
	fc := &fakeRosterClient{}
	ul := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "me@example.com"}, Client: fc}
	having := func(*bridgev2.Portal) *bridgev2.UserLogin { return ul }
	none := func(*bridgev2.Portal) *bridgev2.UserLogin { return nil }
	ctx := context.Background()

	outside := "Run `!jc save-contact <name>` inside a chat with the person you want to save."
	if got := saveContactReply(ctx, "!jc", nil, []string{"Al"}, having); got != outside {
		t.Errorf("no portal: %q", got)
	}
	noRecv := testDMPortal("a@example.com", database.RoomTypeDM)
	noRecv.Receiver = ""
	if got := saveContactReply(ctx, "!jc", noRecv, []string{"Al"}, having); got != outside {
		t.Errorf("no receiver: %q", got)
	}
	for _, p := range []*bridgev2.Portal{
		testDMPortal("room@conference.example.com", database.RoomTypeGroupDM),
		testDMPortal("room@muc.example.com", database.RoomTypeDM),
		testDMPortal("+15550001111,+15550002222@cheogram.com", database.RoomTypeDM),
	} {
		if got := saveContactReply(ctx, "!jc", p, []string{"Al"}, having); !strings.Contains(got, "one-to-one") {
			t.Errorf("%s: %q", p.ID, got)
		}
	}
	dm := testDMPortal("+15550001111@cheogram.com", database.RoomTypeDM)
	if got := saveContactReply(ctx, "!jc", dm, nil, having); !strings.HasPrefix(got, "Usage: `!jc save-contact <name>`") {
		t.Errorf("no name: %q", got)
	}
	if got := saveContactReply(ctx, "!jc", dm, []string{" \x00\n "}, having); !strings.HasPrefix(got, "Usage:") {
		t.Errorf("blank name: %q", got)
	}
	if got := saveContactReply(ctx, "!jc", dm, []string{strings.Repeat("é", 65)}, having); !strings.Contains(got, "too long") {
		t.Errorf("long name: %q", got)
	}
	if got := saveContactReply(ctx, "!jc", dm, []string{"Al"}, none); got != "You must be logged in. Use `!jc login` first." {
		t.Errorf("no login: %q", got)
	}
	ul.Client = &stubJabberClient{}
	if got := saveContactReply(ctx, "!jc", dm, []string{"Al"}, having); !strings.HasPrefix(got, "Not connected to Jabber") {
		t.Errorf("stub client: %q", got)
	}
	ul.Client = fc
	if fc.calls != 0 {
		t.Fatal("roster touched on a refused command")
	}

	fc.err = errors.New("boom")
	if got := saveContactReply(ctx, "!jc", dm, []string{"Al"}, having); got != "Failed to save contact: boom" {
		t.Errorf("error: %q", got)
	}
	fc.err = nil
	got := saveContactReply(ctx, "!jc", dm, []string{"  Alice", "Smith\t"}, having)
	if want := "Saved **Alice Smith** (+15550001111@cheogram.com) to your roster. The room name will update shortly."; got != want {
		t.Errorf("success: %q", got)
	}
	if fc.jid != "+15550001111@cheogram.com" || fc.name != "Alice Smith" {
		t.Errorf("roster set %q %q", fc.jid, fc.name)
	}
}

// fakeRemoverClient records RemoveRosterItem calls and reports a fixed roster membership.
type fakeRemoverClient struct {
	stubJabberClient
	jids       []string
	err        error
	in, loaded bool
}

func (f *fakeRemoverClient) RemoveRosterItem(_ context.Context, jidStr string) error {
	f.jids = append(f.jids, jidStr)
	return f.err
}

func (f *fakeRemoverClient) inRoster(string) (bool, bool) { return f.in, f.loaded }

func TestRemoveContactReply(t *testing.T) {
	fc := &fakeRemoverClient{in: true, loaded: true}
	ul := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "me@example.com"}, Client: fc}
	having := func(*bridgev2.Portal) *bridgev2.UserLogin { return ul }
	none := func(*bridgev2.Portal) *bridgev2.UserLogin { return nil }
	def := func() *bridgev2.UserLogin { return ul }
	ctx := context.Background()
	dm := testDMPortal("+15550001111@cheogram.com", database.RoomTypeDM)

	if got := removeContactReply(ctx, "!jc", nil, nil, having, def); !strings.HasPrefix(got, "Usage: `!jc remove-from-roster <jid-or-phone>`") {
		t.Errorf("no arg outside portal: %q", got)
	}
	for _, p := range []*bridgev2.Portal{
		testDMPortal("room@conference.example.com", database.RoomTypeGroupDM),
		testDMPortal("room@muc.example.com", database.RoomTypeDM),
		testDMPortal("+15550001111,+15550002222@cheogram.com", database.RoomTypeDM),
	} {
		if got := removeContactReply(ctx, "!jc", p, nil, having, def); !strings.Contains(got, "one-to-one") {
			t.Errorf("%s: %q", p.ID, got)
		}
	}
	if got := removeContactReply(ctx, "!jc", dm, nil, none, def); got != "You must be logged in. Use `!jc login` first." {
		t.Errorf("receiver not sender: %q", got)
	}
	if got := removeContactReply(ctx, "!jc", nil, []string{"not a jid!"}, having, def); !strings.HasPrefix(got, "Invalid JID") {
		t.Errorf("garbage arg: %q", got)
	}
	if got := removeContactReply(ctx, "!jc", nil, []string{"a@example.com"}, having, func() *bridgev2.UserLogin { return nil }); !strings.HasPrefix(got, "You must be logged in") {
		t.Errorf("no default login: %q", got)
	}
	if len(fc.jids) != 0 {
		t.Fatal("remover called on a refused command")
	}

	// Not in the roster: no IQ.
	fc.in = false
	if got := removeContactReply(ctx, "!jc", dm, nil, having, def); got != "**+15550001111@cheogram.com** is not in your roster." {
		t.Errorf("not in roster: %q", got)
	}
	if len(fc.jids) != 0 {
		t.Fatal("IQ sent for a contact not in the roster")
	}
	// Roster not loaded: let the server decide.
	fc.loaded = true
	fc.in, fc.loaded = false, false
	if got := removeContactReply(ctx, "!jc", nil, []string{"A@Example.com"}, having, def); !strings.HasPrefix(got, "Removed **a@example.com**") || len(fc.jids) != 1 {
		t.Errorf("unloaded roster: %q %v", got, fc.jids)
	}

	fc.jids = nil
	fc.in, fc.loaded = true, true
	if got := removeContactReply(ctx, "!jc", dm, nil, having, def); got != "Removed **+15550001111@cheogram.com** from your roster. Display and room names should update shortly." {
		t.Errorf("success: %q", got)
	}
	// Phone argument is normalized to a Cheogram JID (default +1), outside a portal via the default login.
	if got := removeContactReply(ctx, "!jc", nil, []string{"555-000-2222"}, none, def); !strings.Contains(got, "**+15550002222@cheogram.com**") {
		t.Errorf("phone: %q", got)
	}
	if len(fc.jids) != 2 || fc.jids[0] != "+15550001111@cheogram.com" || fc.jids[1] != "+15550002222@cheogram.com" {
		t.Errorf("removed %v", fc.jids)
	}

	fc.err = errors.New("boom")
	if got := removeContactReply(ctx, "!jc", dm, nil, having, def); got != "Failed to remove roster item: boom" {
		t.Errorf("error: %q", got)
	}
	ul.Client = &stubJabberClient{}
	if got := removeContactReply(ctx, "!jc", dm, nil, having, def); !strings.HasPrefix(got, "Not connected to Jabber") {
		t.Errorf("stub client: %q", got)
	}
}
