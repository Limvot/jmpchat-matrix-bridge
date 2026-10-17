//go:build !sipcalls

package connector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func TestNoExtensionInDefaultBuild(t *testing.T) {
	if registerExtension != nil {
		t.Fatal("registerExtension must be nil in the default build")
	}
	c := &JabberConnector{}
	if c.extension() != nil {
		t.Fatal("extension must be nil")
	}
	var nilConn *JabberConnector
	info := &bridgev2.ChatInfo{}
	nilConn.decorateChatInfo("a@b", info) // must not panic
	c.decorateChatInfo("a@b", nil)
	if info.Members != nil || info.Type != nil || info.ExtraUpdates != nil {
		t.Fatal("decorateChatInfo must not touch info without an extension")
	}
	if c.ExtensionStartAttrs() != nil {
		t.Fatal("no start attrs without an extension")
	}
}

func TestDMCreateChatResponse(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com", conn: &JabberConnector{}}
	tests := []struct {
		name      string
		jid       string
		wantCount int
		wantOther networkid.UserID
		wantMembs []networkid.UserID
	}{
		{"plain jid", "bob@example.com", 2, "bob@example.com", []networkid.UserID{"me@example.com", "bob@example.com"}},
		{"cheogram 1:1", "+15551230001@cheogram.com", 2, "+15551230001@cheogram.com", []networkid.UserID{"me@example.com", "+15551230001@cheogram.com"}},
		{"group mms", "+15551230001,+15551230002@cheogram.com", 3, "+15551230001@cheogram.com", []networkid.UserID{"me@example.com", "+15551230001@cheogram.com", "+15551230002@cheogram.com"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := x.dmCreateChatResponse(tc.jid)
			if resp.PortalKey.ID != networkid.PortalID(tc.jid) || resp.PortalKey.Receiver != x.loginID {
				t.Errorf("portal key %+v", resp.PortalKey)
			}
			pi := resp.PortalInfo
			if pi == nil || pi.Name == nil || *pi.Name != tc.jid || pi.Type == nil || *pi.Type != database.RoomTypeDM {
				t.Fatalf("unexpected portal info %+v", pi)
			}
			m := pi.Members
			if !m.IsFull || m.TotalMemberCount != tc.wantCount || m.OtherUserID != tc.wantOther || len(m.MemberMap) != len(tc.wantMembs) {
				t.Errorf("unexpected members %+v", m)
			}
			for _, id := range tc.wantMembs {
				if _, ok := m.MemberMap[id]; !ok {
					t.Errorf("missing member %s", id)
				}
			}
			if pi.ExtraUpdates == nil {
				t.Error("ExtraUpdates must be set")
			}
		})
	}
}
