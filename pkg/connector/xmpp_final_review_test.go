package connector

import (
	"testing"
)

func TestMUCJoinConfirmedWithServerAssignedNick(t *testing.T) {
	r := newNickRig()
	r.x.startMUCJoinTrack(nickRoom, "alice", "alice", nil, false)
	body := `<x xmlns="http://jabber.org/protocol/muc#user"><status code="110"/><status code="210"/></x>`
	presenceStanza(t, r.x, "", nickRoom+"/alice_srv", body)
	r.settle()
	if got := r.x.getMUCOurOccupantJID(nickRoom); got != nickRoom+"/alice_srv" {
		t.Fatalf("occupant JID = %q", got)
	}
	r.x.mucPendingJoinMu.Lock()
	_, pending := r.x.mucPendingJoin[nickRoom]
	track := r.x.mucJoinTracks[nickRoom]
	r.x.mucPendingJoinMu.Unlock()
	if pending || track != nil {
		t.Errorf("join still pending (%v) or track kept (%v)", pending, track)
	}
	if len(r.notices) != 0 || len(r.saved) != 0 {
		t.Errorf("server-assigned nick must not be announced: %v %v", r.notices, r.saved)
	}
}

func TestMUCJoinNotConfirmedByOtherNickWithoutStatus110(t *testing.T) {
	r := newNickRig()
	r.x.startMUCJoinTrack(nickRoom, "alice", "alice", nil, false)
	presenceStanza(t, r.x, "", nickRoom+"/bob", `<x xmlns="http://jabber.org/protocol/muc#user"/>`)
	if got := r.x.getMUCOurOccupantJID(nickRoom); got != "" {
		t.Fatalf("occupant JID = %q, want none", got)
	}
}

func TestMUCSubjectAndMessagePortalKeyLowerCased(t *testing.T) {
	x, got := hookClient()
	x.conn = &JabberConnector{}
	markJoinedForTest(x, "Room@muc.example.com")
	runHandler(t, x, `<message type="groupchat" from="Room@muc.example.com/alice" id="s1"><subject>Hi</subject></message>`)
	evt := topicEvent(t, got)
	if evt.GetPortalKey().ID != "room@muc.example.com" {
		t.Errorf("subject portal = %q", evt.GetPortalKey().ID)
	}
	*got = nil
	runHandler(t, x, `<message type="groupchat" from="Room@muc.example.com/alice" id="m1"><body>hello</body></message>`)
	for _, e := range *got {
		if k := e.GetPortalKey().ID; k != "room@muc.example.com" {
			t.Errorf("message portal = %q", k)
		}
	}
}
