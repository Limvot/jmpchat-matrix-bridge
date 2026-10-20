package connector

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

func TestRefLookupPlan(t *testing.T) {
	for _, tc := range []struct {
		kind refKind
		want []refStep
	}{
		{refDM, []refStep{{direct: true}, {keys: []string{"stanza_id", "origin_id", "xmpp_message_id"}}}},
		// A moderation names a room stanza-id: never the occupant-chosen id attribute.
		{refMUCModeration, []refStep{{keys: []string{"stanza_id"}}}},
		{refMUCSelf, []refStep{{keys: []string{"stanza_id"}}, {direct: true}, {keys: []string{"origin_id", "xmpp_message_id"}}}},
	} {
		if got := refLookupPlan(tc.kind); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("plan(%d) = %+v, want %+v", tc.kind, got, tc.want)
		}
	}
}

// realSchemaDB opens an in-memory SQLite DB with bridgev2's real schema (all upgrades), so a
// mautrix bump that renames a column used by aliasMessageIDs fails here.
func realSchemaDB(t *testing.T) *database.Database {
	t.Helper()
	raw, err := dbutil.NewWithDialect(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()), "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	c := &JabberConnector{}
	db := database.New("test", c.GetDBMetaTypes(), raw)
	if err := db.Upgrade(context.Background()); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	return db
}

func TestFindMessagesByXMPPRefRealSchema(t *testing.T) {
	ctx := context.Background()
	db := realSchemaDB(t)
	const me networkid.UserLoginID = "me@example.com"
	portalA := networkid.PortalKey{ID: "room@muc.example.com", Receiver: me}
	portalB := networkid.PortalKey{ID: "bob@example.com", Receiver: me}
	for _, p := range []networkid.PortalKey{portalA, portalB} {
		if err := db.Portal.Insert(ctx, &database.Portal{PortalKey: p, MXID: id.RoomID("!" + string(p.ID) + ":hs")}); err != nil {
			t.Fatalf("insert portal: %v", err)
		}
	}
	for _, g := range []networkid.UserID{"room@muc.example.com/alice", "bob@example.com", "me@example.com"} {
		if err := db.Ghost.Insert(ctx, &database.Ghost{ID: g}); err != nil {
			t.Fatalf("insert ghost: %v", err)
		}
	}
	base := time.Unix(1_700_000_000, 0)
	n := 0
	insert := func(room networkid.PortalKey, msgID string, meta *MessageMetadata, age time.Duration) {
		t.Helper()
		n++
		m := &database.Message{
			ID: networkid.MessageID(msgID), PartID: "", MXID: id.EventID(fmt.Sprintf("$e%d", n)),
			Room: room, SenderID: "me@example.com", Timestamp: base.Add(-age), Metadata: meta,
		}
		if err := db.Message.Insert(ctx, m); err != nil {
			t.Fatalf("insert message %s: %v", msgID, err)
		}
	}
	insert(portalA, "occ-chosen-id", &MessageMetadata{XMPPMessageID: "occ-chosen-id", StanzaID: "S-ATTACKER"}, time.Minute)
	insert(portalA, "victim-id", &MessageMetadata{XMPPMessageID: "victim-id", StanzaID: "S-VICTIM", OriginID: "O-VICTIM", OccupantID: "OCC"}, 2*time.Minute)
	insert(portalA, "matrix:$abc", &MessageMetadata{XMPPMessageID: "gen-xmpp-id"}, 3*time.Minute)
	insert(portalA, "S-VICTIM", &MessageMetadata{XMPPMessageID: "S-VICTIM"}, 4*time.Minute) // attacker sets id attr = victim stanza-id
	insert(portalB, "other-portal", &MessageMetadata{XMPPMessageID: "other-portal", StanzaID: "S-B"}, time.Minute)
	insert(portalA, "old-no-meta", nil, 5*time.Minute) // pre-upgrade row: empty metadata

	ids := func(kind refKind, p networkid.PortalKey, ref string) []string {
		t.Helper()
		rows, err := findMessagesByXMPPRef(ctx, db, p, ref, kind)
		if err != nil {
			t.Fatalf("find(%q): %v", ref, err)
		}
		var out []string
		for _, r := range rows {
			if r.Room != p {
				t.Errorf("row of %v returned for %v", r.Room, p)
			}
			out = append(out, string(r.ID))
		}
		return out
	}
	eq := func(name string, got []string, want ...string) {
		t.Helper()
		if !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}

	// Each alias key matches.
	eq("stanza_id", ids(refDM, portalA, "S-ATTACKER"), "occ-chosen-id")
	eq("origin_id", ids(refDM, portalA, "O-VICTIM"), "victim-id")
	eq("xmpp_message_id alias of matrix row", ids(refDM, portalA, "gen-xmpp-id"), "matrix:$abc")
	eq("direct id", ids(refDM, portalA, "old-no-meta"), "old-no-meta")
	eq("unknown", ids(refDM, portalA, "nope"))
	eq("empty ref", ids(refDM, portalA, ""))
	// Room scoping: portal B's rows are invisible from A and vice versa, direct and alias.
	eq("cross-portal direct", ids(refDM, portalA, "other-portal"))
	eq("cross-portal alias", ids(refDM, portalA, "S-B"))
	eq("other portal sees own", ids(refDM, portalB, "S-B"), "other-portal")
	eq("other portal cannot see A", ids(refDM, portalB, "S-VICTIM"))
	// Moderation: the stanza_id alias wins over an occupant whose id attr equals the victim's stanza-id.
	eq("moderation victim", ids(refMUCModeration, portalA, "S-VICTIM"), "victim-id")
	eq("moderation never direct", ids(refMUCModeration, portalA, "occ-chosen-id"))
	// Self-retraction: stanza_id first, then direct/origin/xmpp id for rooms without stanza-ids.
	eq("self stanza first", ids(refMUCSelf, portalA, "S-VICTIM"), "victim-id")
	eq("self fallback direct", ids(refMUCSelf, portalA, "old-no-meta"), "old-no-meta")
	eq("self fallback origin", ids(refMUCSelf, portalA, "O-VICTIM"), "victim-id")
	// A DM direct hit wins before aliases.
	eq("dm direct before alias", ids(refDM, portalA, "S-VICTIM"), "S-VICTIM")

	// Ordering and LIMIT: newest first, at most maxRefAliasCandidates.
	for i := 0; i < 8; i++ {
		insert(portalA, fmt.Sprintf("dup-%d", i), &MessageMetadata{StanzaID: "S-DUP"}, time.Duration(10+i)*time.Second)
	}
	got := ids(refMUCModeration, portalA, "S-DUP")
	if len(got) != maxRefAliasCandidates || got[0] != "dup-0" || got[1] != "dup-1" {
		t.Errorf("limit/order: %v", got)
	}

	if _, err := aliasMessageIDs(ctx, db, portalA, []string{"x'; DROP TABLE message;--"}, "r"); err == nil {
		t.Error("non-whitelisted key must be rejected")
	}
}
