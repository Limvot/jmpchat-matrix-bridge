package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func TestIsMUCJIDHeuristic(t *testing.T) {
	for jidStr, want := range map[string]bool{
		"room@conference.example.com": true,
		"room@muc.example.com":        true,
		"room@groups.example.com":     true,
		"bob@example.com":             false,
		"+15551230001@cheogram.com":   false,
		"groups@example.com":          false,
	} {
		if got := isMUCJID(jidStr); got != want {
			t.Errorf("isMUCJID(%q) = %v, want %v", jidStr, got, want)
		}
	}
}

// With no stored room type and no session (disco unavailable), getChatInfo falls back to the
// heuristic; Cheogram 1:1 JIDs stay DMs and a stored GroupDM type (set by the join command) wins.
func TestGetChatInfoTypeSelection(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com", conn: &JabberConnector{}, rosterLoaded: true, rosterJIDs: map[string]struct{}{}}
	cases := []struct {
		id     string
		stored database.RoomType
		want   database.RoomType
	}{
		{"room@groups.example.com", "", database.RoomTypeGroupDM},
		{"room@conference.example.com", "", database.RoomTypeGroupDM},
		{"+15551230001@cheogram.com", "", database.RoomTypeDM},
		{"bob@example.com", "", database.RoomTypeDM},
		// join command: the type is forced even when detection would say DM.
		{"room@chat.example.com", database.RoomTypeGroupDM, database.RoomTypeGroupDM},
	}
	for _, c := range cases {
		p := &bridgev2.Portal{Portal: &database.Portal{
			PortalKey: networkid.PortalKey{ID: networkid.PortalID(c.id), Receiver: "me@example.com"},
			RoomType:  c.stored,
		}}
		info, err := x.getChatInfo(context.Background(), p)
		if err != nil || info == nil {
			t.Fatalf("getChatInfo(%q) = %v, %v", c.id, info, err)
		}
		if c.want == database.RoomTypeDM {
			// DM ChatInfo may leave Type nil (framework default) or set DM.
			if info.Type != nil && *info.Type != database.RoomTypeDM {
				t.Errorf("getChatInfo(%q) type = %v, want DM", c.id, *info.Type)
			}
			continue
		}
		if info.Type == nil || *info.Type != c.want {
			t.Errorf("getChatInfo(%q) type = %v, want %v", c.id, info.Type, c.want)
		}
	}
}
