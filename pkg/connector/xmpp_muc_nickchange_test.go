package connector

import (
	"testing"

	"github.com/rs/zerolog"
)

const renameRoom = "room@muc.example.com"

func renameBody(newNick string, codes ...string) string {
	b := `<x xmlns="http://jabber.org/protocol/muc#user"><item nick="` + newNick + `" role="participant"/>`
	for _, c := range codes {
		b += `<status code="` + c + `"/>`
	}
	return b + `</x>`
}

// XEP-0045 7.6: our own occupant renamed (by us or by another client sharing the nick): the bridge
// follows it, so its echoes are still recognized and a message from another client under the new nick is
// still bridged as the user's (issue #11 logic keeps working).
func TestOwnMUCNickChangeFollowedForEchoMatching(t *testing.T) {
	x, got := hookClient()
	x.conn = &JabberConnector{} // no backfill config: nothing is triggered
	x.log = zerolog.Nop()
	x.mucOurOccupantJID = map[string]string{renameRoom: renameRoom + "/me"}

	presenceStanza(t, x, "unavailable", renameRoom+"/me", renameBody("me2", "303", "110"))
	if ours := x.getMUCOurOccupantJID(renameRoom); ours != renameRoom+"/me2" {
		t.Fatalf("occupant JID = %q", ours)
	}
	// The follow-up available presence under the new nick changes nothing.
	presenceStanza(t, x, "", renameRoom+"/me2", selfBody)
	if ours := x.getMUCOurOccupantJID(renameRoom); ours != renameRoom+"/me2" {
		t.Fatalf("occupant JID after available = %q", ours)
	}

	// A registered echo under the new nick is dropped, an unregistered message is bridged from-me.
	x.mucEcho.register(renameRoom, "mine1")
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me2" id="mine1"><body>echo</body></message>`)
	if len(*got) != 0 {
		t.Fatalf("echo under the new nick was bridged: %d", len(*got))
	}
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me2" id="other1"><body>from gajim</body></message>`)
	if len(*got) != 1 {
		t.Fatalf("queued %d events", len(*got))
	}
	if m := (*got)[0].(*jabberRemoteMessage); !m.sender.IsFromMe {
		t.Fatalf("sender = %+v", m.sender)
	}
	// A message under the OLD nick is now another occupant (a different person who took the nick).
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="x1"><body>someone else</body></message>`)
	if m := (*got)[len(*got)-1].(*jabberRemoteMessage); m.sender.IsFromMe {
		t.Fatalf("old nick still treated as ours: %+v", m.sender)
	}
}

func TestMUCNickChangeOwnDetection(t *testing.T) {
	x, _ := hookClient()
	x.mucOurOccupantJID = map[string]string{renameRoom: renameRoom + "/me"}

	// Someone else renames: ours is untouched.
	presenceStanza(t, x, "unavailable", renameRoom+"/bob", renameBody("bobby", "303"))
	if ours := x.getMUCOurOccupantJID(renameRoom); ours != renameRoom+"/me" {
		t.Fatalf("other occupant's rename changed ours: %q", ours)
	}
	// Recorded occupant renamed, no 110 (a server that omits it): still ours.
	presenceStanza(t, x, "unavailable", renameRoom+"/me", renameBody("me3", "303"))
	if ours := x.getMUCOurOccupantJID(renameRoom); ours != renameRoom+"/me3" {
		t.Fatalf("recorded occupant rename not followed: %q", ours)
	}
	// 110 with an unrecorded old nick (state lost): follow too.
	x.mucOurOccupantJID = map[string]string{}
	presenceStanza(t, x, "unavailable", renameRoom+"/old", renameBody("fresh", "303", "110"))
	if ours := x.getMUCOurOccupantJID(renameRoom); ours != renameRoom+"/fresh" {
		t.Fatalf("110 rename not followed: %q", ours)
	}
	// A rename without a new nick leaves the record alone.
	presenceStanza(t, x, "unavailable", renameRoom+"/fresh", `<x xmlns="http://jabber.org/protocol/muc#user"><status code="303"/><status code="110"/></x>`)
	if ours := x.getMUCOurOccupantJID(renameRoom); ours != renameRoom+"/fresh" {
		t.Fatalf("record changed: %q", ours)
	}
	// A plain own leave (no 303) means we are out: the record is dropped so the next send re-joins.
	presenceStanza(t, x, "unavailable", renameRoom+"/fresh", `<x xmlns="http://jabber.org/protocol/muc#user"><status code="110"/></x>`)
	if ours := x.getMUCOurOccupantJID(renameRoom); ours != "" {
		t.Fatalf("own leave kept the record: %q", ours)
	}
	x.mucOurOccupantJID[renameRoom] = renameRoom + "/fresh"
	// An invalid new nick is ignored, not fatal.
	presenceStanza(t, x, "unavailable", renameRoom+"/fresh", renameBody("a b/c", "303", "110"))
}

// The avatar occupant table: our own rename keeps the other occupants' records, a rename of another
// occupant drops only the old one (the new nick's own presence creates its record).
func TestMUCAvatarTableNickChange(t *testing.T) {
	var s mucAvatarState
	other := renameRoom + "/bob"
	mine := renameRoom + "/me"
	s.notePresence(other, renameRoom, presenceInfo{isMUC: true}, false, false)
	s.notePresence(renameRoom+"/carol", renameRoom, presenceInfo{isMUC: true}, false, false)

	rename := presenceInfo{isMUC: true, newNick: "me2", statusCodes: []string{"303", "110"}}
	s.notePresence(mine, renameRoom, rename, true, true)
	if len(s.occ) != 2 {
		t.Fatalf("own rename cleared the room: %d records", len(s.occ))
	}
	// Our own leave still clears the room.
	s.notePresence(mine, renameRoom, presenceInfo{isMUC: true, statusCodes: []string{"110"}}, true, true)
	if len(s.occ) != 0 {
		t.Fatalf("own leave kept %d records", len(s.occ))
	}

	s.notePresence(other, renameRoom, presenceInfo{isMUC: true}, false, false)
	s.notePresence(renameRoom+"/carol", renameRoom, presenceInfo{isMUC: true}, false, false)
	s.notePresence(other, renameRoom, presenceInfo{isMUC: true, newNick: "bobby", statusCodes: []string{"303"}}, true, false)
	if _, ok := s.occ[other]; ok || len(s.occ) != 1 {
		t.Fatalf("other rename: records = %v", s.occ)
	}
	s.notePresence(renameRoom+"/bobby", renameRoom, presenceInfo{isMUC: true}, false, false)
	if _, ok := s.occ[renameRoom+"/bobby"]; !ok {
		t.Fatal("new nick has no record")
	}
}
