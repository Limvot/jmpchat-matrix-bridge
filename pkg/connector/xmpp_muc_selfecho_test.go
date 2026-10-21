package connector

import "testing"

const selfEchoRoom = "room@muc.example.com"

// Issue #11: a message from our own occupant JID that is not a registered echo (sent by another client
// of the user) is bridged as sent by the user; the registered echo is dropped.
func TestMUCOwnNickMessageBridgedUnlessRegisteredEcho(t *testing.T) {
	x, got := hookClient()
	x.conn = &JabberConnector{} // no backfill config: nothing is triggered
	x.mucOurOccupantJID = map[string]string{selfEchoRoom: selfEchoRoom + "/me"}

	// Not registered: bridged, is_from_me.
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="other1"><body>from gajim</body></message>`)
	if len(*got) != 1 {
		t.Fatalf("queued %d events, want 1", len(*got))
	}
	m, ok := (*got)[0].(*jabberRemoteMessage)
	if !ok {
		t.Fatalf("queued %T", (*got)[0])
	}
	if !m.sender.IsFromMe || m.sender.SenderLogin != x.loginID || string(m.sender.Sender) != string(x.loginID) {
		t.Errorf("sender = %+v, want the user (is_from_me)", m.sender)
	}
	if m.id != "other1" || m.body != "from gajim" || !m.groupchat {
		t.Errorf("message = %+v", m)
	}

	// Registered by id attribute: dropped.
	x.mucEcho.register(selfEchoRoom, "mine1")
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="mine1"><body>from bridge</body></message>`)
	// Registered, echoed by origin-id (id attr rewritten by the room): dropped.
	x.mucEcho.register(selfEchoRoom, "mine2")
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="srv9"><body>x</body><origin-id xmlns="urn:xmpp:sid:0" id="mine2"/></message>`)
	if len(*got) != 1 {
		t.Fatalf("registered echoes were bridged: %d events", len(*got))
	}

	// A reaction from our own nick that is not ours is bridged as the user's reaction.
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="re1"><reactions xmlns="urn:xmpp:reactions:0" id="S1"><reaction>👍</reaction></reactions></message>`)
	if len(*got) != 2 {
		t.Fatalf("reaction not bridged: %d events", len(*got))
	}
	if r := (*got)[1].(*jabberRemoteMessage); !r.sender.IsFromMe || r.reactionTarget != "S1" {
		t.Errorf("reaction = %+v", r)
	}
}

// Echoes of reactions the bridge sent are dropped too (registered before sending).
func TestMUCRegisteredReactionEchoDropped(t *testing.T) {
	x, got := hookClient()
	x.conn = &JabberConnector{} // no backfill config: nothing is triggered
	x.mucOurOccupantJID = map[string]string{selfEchoRoom: selfEchoRoom + "/me"}
	x.mucEcho.register(selfEchoRoom, "rx1")
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="rx1"><reactions xmlns="urn:xmpp:reactions:0" id="S1"><reaction>👍</reaction></reactions><origin-id xmlns="urn:xmpp:sid:0" id="rx1"/></message>`)
	if len(*got) != 0 {
		t.Fatalf("echo bridged: %d", len(*got))
	}
}

// Another occupant reusing a registered id is never treated as our echo.
func TestMUCSenderForOtherOccupantIsGhost(t *testing.T) {
	x := newOuterFromTestClient()
	x.mucOurOccupantJID = map[string]string{selfEchoRoom: selfEchoRoom + "/me"}
	if s := x.mucMessageSender(selfEchoRoom, selfEchoRoom+"/bob"); s.IsFromMe || string(s.Sender) != selfEchoRoom+"/bob" {
		t.Errorf("other occupant sender = %+v", s)
	}
	if s := x.mucMessageSender(selfEchoRoom, selfEchoRoom+"/me"); !s.IsFromMe {
		t.Errorf("own occupant sender = %+v", s)
	}
	// Unknown own occupant: never "from me".
	y := newOuterFromTestClient()
	if y.isOwnMUCOccupant(selfEchoRoom, selfEchoRoom+"/me") {
		t.Error("unknown occupant must not match")
	}
}

// Every bridge MUC send path registers its stanza id before the send. The id helpers accept a
// caller-chosen id (checked by the stanza the send helper builds is covered by the helpers' callers);
// here the registration/discard contract of the table is pinned.
func TestMUCEchoRegisterBeforeMatch(t *testing.T) {
	x := newOuterFromTestClient()
	x.mucOurOccupantJID = map[string]string{selfEchoRoom: selfEchoRoom + "/me"}
	if x.handleOwnMUCEcho(selfEchoRoom, selfEchoRoom+"/me", "late", messageIDs{}) {
		t.Fatal("unregistered id matched")
	}
	x.mucEcho.register(selfEchoRoom, "late")
	if !x.handleOwnMUCEcho(selfEchoRoom, selfEchoRoom+"/me", "late", messageIDs{}) {
		t.Fatal("registered id did not match")
	}
	x.mucEcho.register(selfEchoRoom, "failed")
	x.mucEcho.discard(selfEchoRoom, "failed")
	if x.handleOwnMUCEcho(selfEchoRoom, selfEchoRoom+"/me", "failed", messageIDs{}) {
		t.Fatal("discarded id matched")
	}
}
