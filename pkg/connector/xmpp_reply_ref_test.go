package connector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

func replyTestDB(t *testing.T) (*database.Database, networkid.PortalKey, networkid.PortalKey, func(room networkid.PortalKey, msgID string, meta *MessageMetadata)) {
	t.Helper()
	ctx := context.Background()
	db := realSchemaDB(t)
	const me networkid.UserLoginID = "me@example.com"
	muc := networkid.PortalKey{ID: "room@muc.example.com", Receiver: me}
	dm := networkid.PortalKey{ID: "bob@example.com", Receiver: me}
	for _, p := range []networkid.PortalKey{muc, dm} {
		if err := db.Portal.Insert(ctx, &database.Portal{PortalKey: p, MXID: id.RoomID("!" + string(p.ID) + ":hs")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Ghost.Insert(ctx, &database.Ghost{ID: "bob@example.com"}); err != nil {
		t.Fatal(err)
	}
	n := 0
	insert := func(room networkid.PortalKey, msgID string, meta *MessageMetadata) {
		t.Helper()
		n++
		m := &database.Message{
			ID: networkid.MessageID(msgID), MXID: id.EventID(fmt.Sprintf("$e%d", n)), Room: room,
			SenderID: "bob@example.com", Timestamp: time.Unix(1_700_000_000+int64(n), 0), Metadata: meta,
		}
		if err := db.Message.Insert(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	return db, muc, dm, insert
}

func TestResolveMessageRef(t *testing.T) {
	ctx := context.Background()
	db, muc, dm, insert := replyTestDB(t)
	insert(muc, "client-id-1", &MessageMetadata{XMPPMessageID: "client-id-1", StanzaID: "ROOM-S1"})
	insert(muc, "matrix:$abc", &MessageMetadata{XMPPMessageID: "sent-id-2", StanzaID: "ROOM-S2"})
	insert(muc, "old-id-3", &MessageMetadata{XMPPMessageID: "old-id-3"}) // pre-v0.7.0: no stanza-id
	insert(dm, "dm-id-1", &MessageMetadata{XMPPMessageID: "dm-id-1", StanzaID: "ARCH-1", OriginID: "ORIG-1"})
	insert(dm, "matrix:$dm", &MessageMetadata{XMPPMessageID: "sent-dm-2"})

	for _, tc := range []struct {
		name   string
		portal networkid.PortalKey
		isMUC  bool
		ref    string
		want   string
	}{
		{"muc stanza-id", muc, true, "ROOM-S1", "client-id-1"},
		{"muc stanza-id of matrix-sent", muc, true, "ROOM-S2", "matrix:$abc"},
		{"muc direct id fallback", muc, true, "old-id-3", "old-id-3"},
		{"muc xmpp id alias", muc, true, "sent-id-2", "matrix:$abc"},
		{"dm direct id", dm, false, "dm-id-1", "dm-id-1"},
		{"dm origin id", dm, false, "ORIG-1", "dm-id-1"},
		{"dm matrix-sent xmpp id", dm, false, "sent-dm-2", "matrix:$dm"},
		{"dm archive stanza-id", dm, false, "ARCH-1", "dm-id-1"},
		{"muc ref in dm portal: isolation", dm, false, "ROOM-S1", ""},
		{"dm ref in muc portal: isolation", muc, true, "dm-id-1", ""},
		{"not found", muc, true, "nope", ""},
		{"empty", muc, true, "", ""},
	} {
		got, err := resolveMessageRef(ctx, db, tc.portal, tc.ref, tc.isMUC)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if tc.want == "" {
			if got != nil {
				t.Errorf("%s: want nil, got %s", tc.name, got.ID)
			}
			continue
		}
		if got == nil || string(got.ID) != tc.want {
			t.Errorf("%s: got %v, want %s", tc.name, got, tc.want)
		}
	}
}

// In a MUC a sender-chosen id equal to another message's room stanza-id must not win.
func TestResolveMessageRefMUCPrefersStanzaID(t *testing.T) {
	ctx := context.Background()
	db, muc, _, insert := replyTestDB(t)
	insert(muc, "S-VICTIM", &MessageMetadata{XMPPMessageID: "S-VICTIM", StanzaID: "ROOM-ATTACKER-CHOSE-ID"})
	insert(muc, "real", &MessageMetadata{XMPPMessageID: "real", StanzaID: "S-VICTIM"})
	got, err := resolveMessageRef(ctx, db, muc, "S-VICTIM", true)
	if err != nil || got == nil || got.ID != "real" {
		t.Fatalf("got %v err %v, want row real", got, err)
	}
}

func TestRefLookupPlanReply(t *testing.T) {
	mucPlan := refLookupPlan(refReplyMUC)
	if len(mucPlan) != 3 || mucPlan[0].direct || mucPlan[0].keys[0] != metaKeyStanzaID || !mucPlan[1].direct {
		t.Errorf("muc plan = %+v", mucPlan)
	}
	dmPlan := refLookupPlan(refReplyDM)
	if len(dmPlan) != 3 || !dmPlan[0].direct || dmPlan[2].keys[0] != metaKeyStanzaID {
		t.Errorf("dm plan = %+v", dmPlan)
	}
}

func TestStripReplyFallback(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       string
		start, end int
		want       string
	}{
		{"leading quote", "> bob: hi\nthe reply", 0, 10, "the reply"},
		{"emoji code points", "> 😀😀 x\nanswer", 0, 7, "answer"}, // 7 code points, 12 bytes
		{"end clamped", "> q\nok", 0, 999, "> q\nok"},           // clamped range covers everything: would empty, so unchanged
		{"start clamped past end", "abc", 50, 60, "abc"},
		{"trailing fallback", "reply\n> quote", 5, 13, "reply"},
		{"would empty", "> q", 0, 3, "> q"},
	} {
		got := stripReplyFallback(tc.body, replyFallback{Start: tc.start, End: tc.end, Valid: true})
		if got != tc.want {
			t.Errorf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
	if got := stripReplyFallback("> q\nr", replyFallback{}); got != "> q\nr" {
		t.Errorf("invalid range must not strip, got %q", got)
	}
}

func parseFallback(t *testing.T, stanza string) (string, replyFallback, string) {
	t.Helper()
	r, _ := startOfMessage(t, stanza)
	body, _, replyID, _, _, _, ids, err := parseMessageBodyOOBIDs(r)
	if err != nil {
		t.Fatal(err)
	}
	return body, ids.ReplyFallback, replyID
}

func TestParseReplyFallback(t *testing.T) {
	const fb = `<reply xmlns="urn:xmpp:reply:0" to="room@muc.example.com/bob" id="S1"/><fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:reply:0"><body start="0" end="9"/></fallback>`
	body, rf, rid := parseFallback(t, `<message type="groupchat"><body>&gt; bob: hi
answer</body>`+fb+`</message>`)
	if rid != "S1" || !rf.Valid || rf.Start != 0 || rf.End != 9 {
		t.Fatalf("rid=%q fb=%+v", rid, rf)
	}
	if got := stripReplyFallback(body, rf); got != "answer" {
		t.Errorf("stripped = %q", got)
	}

	// Leading whitespace in the raw body shifts offsets relative to the trimmed body.
	body, rf, _ = parseFallback(t, `<message type="groupchat"><body>  &gt; q
a</body><reply xmlns="urn:xmpp:reply:0" id="S1"/><fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:reply:0"><body start="2" end="5"/></fallback></message>`)
	if got := stripReplyFallback(body, rf); got != "a" {
		t.Errorf("trim-shifted strip = %q (fb %+v, body %q)", got, rf, body)
	}

	// Wrong `for`, wrong namespace, missing body child, missing/invalid offsets, nested fallback.
	for name, xmlStr := range map[string]string{
		"wrong for":   `<fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:other:0"><body start="0" end="3"/></fallback>`,
		"wrong ns":    `<fallback xmlns="urn:other" for="urn:xmpp:reply:0"><body start="0" end="3"/></fallback>`,
		"no body":     `<fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:reply:0"/>`,
		"no offsets":  `<fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:reply:0"><body/></fallback>`,
		"bad offsets": `<fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:reply:0"><body start="5" end="2"/></fallback>`,
		"nested":      `<x xmlns="urn:t"><fallback xmlns="urn:xmpp:fallback:0" for="urn:xmpp:reply:0"><body start="0" end="3"/></fallback></x>`,
	} {
		body, rf, _ := parseFallback(t, `<message type="groupchat"><body>&gt; q
answer</body>`+xmlStr+`</message>`)
		if rf.Valid {
			t.Errorf("%s: fallback must be ignored, got %+v", name, rf)
		}
		if body != "> q\nanswer" {
			t.Errorf("%s: body changed: %q", name, body)
		}
	}
}

func TestRefIsMUC(t *testing.T) {
	pk := networkid.PortalKey{ID: "bob@example.com"}
	if !(&jabberRemoteMessage{portalKey: pk, groupchat: true}).refIsMUC(nil) {
		t.Error("groupchat origin must be MUC")
	}
	room := networkid.PortalKey{ID: "r@conference.example.com"}
	if !(&jabberRemoteMessage{portalKey: room}).refIsMUC(nil) {
		t.Error("heuristic")
	}
	if (&jabberRemoteMessage{portalKey: room}).refIsMUC(&bridgev2.Portal{Portal: &database.Portal{RoomType: database.RoomTypeDM}}) {
		t.Error("stored DM room type wins over heuristic")
	}
	if !(&jabberRemoteMessage{portalKey: pk}).refIsMUC(&bridgev2.Portal{Portal: &database.Portal{RoomType: database.RoomTypeGroupDM}}) {
		t.Error("stored group room type")
	}
}

func TestReplyRefForMessage(t *testing.T) {
	muc := &database.Message{ID: "client-id", Metadata: &MessageMetadata{XMPPMessageID: "client-id", StanzaID: "ROOM-S"}}
	if ref, room, ok := replyRefForMessage(muc, true); !ok || !room || ref != "ROOM-S" {
		t.Errorf("muc = %q %v %v", ref, room, ok)
	}
	if ref, room, ok := replyRefForMessage(muc, false); !ok || room || ref != "client-id" {
		t.Errorf("dm keeps row id: %q %v %v", ref, room, ok)
	}
	old := &database.Message{ID: "old", Metadata: &MessageMetadata{XMPPMessageID: "old"}}
	if ref, room, ok := replyRefForMessage(old, true); !ok || room || ref != "old" {
		t.Errorf("muc fallback = %q %v %v", ref, room, ok)
	}
	mx := &database.Message{ID: "matrix:$e", Metadata: &MessageMetadata{XMPPMessageID: "sent-1"}}
	if ref, _, ok := replyRefForMessage(mx, false); !ok || ref != "sent-1" {
		t.Errorf("matrix-originated dm = %q %v", ref, ok)
	}
	if _, _, ok := replyRefForMessage(&database.Message{ID: "matrix:$e"}, true); ok {
		t.Error("matrix row without ids must not resolve")
	}
}
