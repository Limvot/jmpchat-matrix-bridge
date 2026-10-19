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
