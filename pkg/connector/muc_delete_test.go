package connector

import (
	"context"
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"mellium.im/xmpp/stanza"
)

func deletePortal(r *bmRig, room string, rt database.RoomType) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: rejoinPortal(room, rt, "!"+room+":x", false, r.x.loginID)}
}

func TestPortalDeleteLeavesMUCAndTurnsOffAutojoin(t *testing.T) {
	r := newBMRig(t, true, bm(bmRoom, true, "alice"))
	r.joinedRoom(bmRoom, "alice")
	r.x.leaveOnPortalDelete(context.Background(), deletePortal(r, bmRoom, database.RoomTypeGroupDM))
	if len(r.leaves) != 1 || r.leaves[0] != bmRoom {
		t.Fatalf("leaves = %v", r.leaves)
	}
	if p := r.published(); len(p) != 1 || p[0].Autojoin || p[0].Nick != "alice" {
		t.Fatalf("publishes = %+v", p)
	}
	if n := r.noticeList(); len(n) != 0 {
		t.Fatalf("unexpected notices %v", n)
	}
	// The echo of our own autojoin=false never re-imports the (now portal-less) room.
	apply(r, pub(bmRoom, false, "alice"))
	if len(r.createdPortals()) != 0 || len(r.joins()) != 0 {
		t.Fatalf("echo re-imported: portals=%v joins=%v", r.createdPortals(), r.joins())
	}
}

func TestPortalDeleteNotJoinedSendsNoPresence(t *testing.T) {
	r := newBMRig(t, true, bm(bmRoom, true, "alice"))
	r.x.leaveOnPortalDelete(context.Background(), deletePortal(r, bmRoom, database.RoomTypeGroupDM))
	if len(r.leaves) != 0 || len(r.published()) != 1 {
		t.Fatalf("leaves=%v publishes=%d", r.leaves, len(r.published()))
	}
}

func TestPortalDeleteDMAndGroupMMSUntouched(t *testing.T) {
	r := newBMRig(t, true, bm(bmRoom, true, "alice"))
	for _, p := range []*bridgev2.Portal{
		deletePortal(r, "friend@example.com", database.RoomTypeDM),
		deletePortal(r, "+15551230001,+15551230002@cheogram.com", database.RoomTypeGroupDM),
		deletePortal(r, "+15551230001@cheogram.com", database.RoomTypeDM),
	} {
		r.x.leaveOnPortalDelete(context.Background(), p)
		if err := r.x.HandleMatrixDeleteChat(context.Background(), &bridgev2.MatrixDeleteChat{Portal: p}); !errors.Is(err, bridgev2.ErrDeleteChatNotSupported) {
			t.Fatalf("delete chat err = %v", err)
		}
	}
	if len(r.leaves) != 0 || len(r.published()) != 0 || r.fetchCount() != 0 || len(r.noticeList()) != 0 {
		t.Fatalf("touched: leaves=%v publishes=%d fetches=%d", r.leaves, len(r.published()), r.fetchCount())
	}
}

func TestPortalDeleteWriteFailureNotifies(t *testing.T) {
	r := newBMRig(t, true, bm(bmRoom, true, "alice"))
	r.publishErr = stanza.Error{Condition: stanza.Forbidden}
	r.x.leaveOnPortalDelete(context.Background(), deletePortal(r, bmRoom, database.RoomTypeGroupDM))
	if n := r.noticeList(); len(n) != 1 {
		t.Fatalf("notices = %v", n)
	}
	// Fetch failure notifies too; writes unsupported (read-only server) too.
	r = newBMRig(t, true, bm(bmRoom, true, "alice"))
	r.fetchErr = errors.New("boom")
	r.x.leaveOnPortalDelete(context.Background(), deletePortal(r, bmRoom, database.RoomTypeGroupDM))
	if len(r.noticeList()) != 1 || len(r.published()) != 0 {
		t.Fatalf("fetch failure: notices=%v", r.noticeList())
	}
	r = newBMRig(t, false, bm(bmRoom, true, "alice"))
	r.x.leaveOnPortalDelete(context.Background(), deletePortal(r, bmRoom, database.RoomTypeGroupDM))
	if len(r.noticeList()) != 1 || len(r.published()) != 0 {
		t.Fatalf("no write support: notices=%v", r.noticeList())
	}
}

func TestPortalDeleteNoBookmarkNeverCreates(t *testing.T) {
	r := newBMRig(t, true)
	r.joinedRoom(bmRoom, "alice")
	p := deletePortal(r, bmRoom, database.RoomTypeGroupDM)
	if err := r.x.HandleMatrixDeleteChat(context.Background(), &bridgev2.MatrixDeleteChat{Portal: p}); err != nil {
		t.Fatal(err)
	}
	if len(r.published()) != 0 || len(r.noticeList()) != 0 || len(r.leaves) != 1 {
		t.Fatalf("publishes=%d notices=%v leaves=%v", len(r.published()), r.noticeList(), r.leaves)
	}
}
