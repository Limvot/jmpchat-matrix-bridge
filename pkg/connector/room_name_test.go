package connector

import (
	"context"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
)

// noSavePortal replaces the portal save seam for a test (the fake portals have no Bridge/DB).
func noSavePortal(t *testing.T) {
	t.Helper()
	prev := savePortal
	savePortal = func(context.Context, *bridgev2.Portal) error { return nil }
	t.Cleanup(func() { savePortal = prev })
}

func testPortalWithName(id string, rt database.RoomType, name string) *bridgev2.Portal {
	p := testDMPortal(id, rt)
	p.Metadata = &PortalMetadata{CustomName: name}
	return p
}

func TestRoomNameOverrideInChatInfo(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com", conn: &JabberConnector{}}
	ctx := context.Background()
	cases := []struct {
		name, id, override string
		rt                 database.RoomType
		wantContains       string // checked when there is no override
	}{
		{"dm pinned", "+15550001111@cheogram.com", "Grandma", database.RoomTypeDM, ""},
		{"dm default", "+15550001111@cheogram.com", "", database.RoomTypeDM, "15550001111"},
		{"muc pinned", "room@conference.example.com", "Book club", database.RoomTypeGroupDM, ""},
		{"muc default", "room@conference.example.com", "", database.RoomTypeGroupDM, "room"},
	}
	for _, c := range cases {
		p := testPortalWithName(c.id, c.rt, c.override)
		info, err := x.getChatInfo(ctx, p)
		if err != nil || info == nil || info.Name == nil {
			t.Fatalf("%s: getChatInfo = %v, %v", c.name, info, err)
		}
		if c.override != "" {
			if *info.Name != c.override {
				t.Errorf("%s: name = %q, want the pinned %q", c.name, *info.Name, c.override)
			}
		} else if !strings.Contains(*info.Name, c.wantContains) {
			t.Errorf("%s: name = %q, want the XMPP-side name (containing %q)", c.name, *info.Name, c.wantContains)
		}
	}
}

// A room name the user sets in Matrix is recorded as the override, not reverted.
func TestHandleMatrixRoomNamePinsName(t *testing.T) {
	noSavePortal(t)
	x := &xmppClient{loginID: "me@example.com", conn: &JabberConnector{}}
	p := testPortalWithName("+15550001111@cheogram.com", database.RoomTypeDM, "")
	msg := &bridgev2.MatrixRoomName{MatrixEventBase: bridgev2.MatrixEventBase[*event.RoomNameEventContent]{
		Portal:  p,
		Content: &event.RoomNameEventContent{Name: "  Grandma\x00  "},
	}}
	changed, err := x.HandleMatrixRoomName(context.Background(), msg)
	if err != nil || !changed {
		t.Fatalf("HandleMatrixRoomName = %v, %v", changed, err)
	}
	if got := roomNameOverride(p); got != "Grandma" {
		t.Errorf("override = %q, want Grandma", got)
	}
	if p.Name != "Grandma" || !p.NameSet {
		t.Errorf("portal name = %q set=%v, want Grandma set", p.Name, p.NameSet)
	}
}

// An empty name clears the pin and reverts to the name derived from the XMPP side.
func TestApplyRoomNameReset(t *testing.T) {
	noSavePortal(t)
	x := &xmppClient{loginID: "me@example.com", conn: &JabberConnector{}}
	p := testPortalWithName("+15550001111@cheogram.com", database.RoomTypeDM, "Grandma")
	got, err := x.applyRoomName(context.Background(), p, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if want := "+15550001111@cheogram.com (me@example.com)"; got != want {
		t.Errorf("reset name = %q, want %q", got, want)
	}
	if roomNameOverride(p) != "" {
		t.Error("override not cleared")
	}
}

func TestCleanRoomName(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{nil, ""},
		{[]string{"  Al  "}, "Al"},
		{[]string{"Family\x00", "chat"}, "Family chat"},
		{[]string{"a\tb\nc"}, "abc"},
		{[]string{"My", "nice", "room", "name"}, "My nice room name"},
	} {
		if got := cleanRoomName(c.args); got != c.want {
			t.Errorf("cleanRoomName(%q) = %q, want %q", c.args, got, c.want)
		}
	}
}

func TestSetRoomNameReplyRefusals(t *testing.T) {
	ctx := context.Background()
	none := func(*bridgev2.Portal) *bridgev2.UserLogin { return nil }
	for _, c := range []struct {
		name   string
		portal *bridgev2.Portal
		args   []string
		login  func(*bridgev2.Portal) *bridgev2.UserLogin
		want   string
	}{
		{"no portal", nil, []string{"Al"}, none, "Run `!jc setname <new name>` inside a bridged room."},
		{"no name", testRoomPortal(), nil, none, "Usage:"},
		{"no login", testRoomPortal(), []string{"Al"}, none, "You must be logged in. Use `!jc login` first."},
		{"long name", testRoomPortal(), []string{strings.Repeat("é", maxRoomNameRunes+1)}, none, "That name is too long"},
	} {
		if got := setRoomNameReply(ctx, "!jc", c.portal, c.args, c.login, nil); !strings.HasPrefix(got, c.want) {
			t.Errorf("%s: %q, want prefix %q", c.name, got, c.want)
		}
	}
	// A login that does not own the portal must not be able to rename it.
	other := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "other@example.com"}, Client: &xmppClient{loginID: "other@example.com"}}
	if got := setRoomNameReply(ctx, "!jc", testRoomPortal(), []string{"Al"}, func(*bridgev2.Portal) *bridgev2.UserLogin { return other }, nil); !strings.HasPrefix(got, "You must be logged in") {
		t.Errorf("foreign login: %q", got)
	}
}

// testRoomPortal is a DM portal that exists in Matrix (rooms without an MXID are refused early).
func testRoomPortal() *bridgev2.Portal {
	p := testDMPortal("a@example.com", database.RoomTypeDM)
	p.MXID = "!room:example.com"
	return p
}

// A MUC with no disco#info room name takes the name from the user's XMPP bookmark (never a bare JID).
func TestMUCNameFallsBackToBookmark(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com", conn: &JabberConnector{}}
	x.bm.setCache([]bookmarkConf{{ID: "Book@Conference.Example.com", Name: "Book club"}})
	p := testPortalWithName("book@conference.example.com", database.RoomTypeGroupDM, "")
	info, err := x.getChatInfo(context.Background(), p)
	if err != nil || info == nil || info.Name == nil {
		t.Fatalf("getChatInfo = %v, %v", info, err)
	}
	if want := "Book club (book@conference.example.com) (me@example.com)"; *info.Name != want {
		t.Errorf("name = %q, want %q", *info.Name, want)
	}
}
