package connector

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func parseErrStanza(t *testing.T, stanza string) messageError {
	t.Helper()
	r, start := startOfMessage(t, stanza)
	me, err := parseMessageError(r, &start)
	if err != nil {
		t.Fatal(err)
	}
	return me
}

func TestParseMessageError(t *testing.T) {
	const ns = `xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"`
	long := strings.Repeat("x", 500)
	for _, tc := range []struct {
		name   string
		stanza string
		want   messageError
	}{
		{"full", `<message type="error" id="m1" from="room@conference.example.com" to="me@example.com/r"><body>SECRET</body><error type="modify" by="room@conference.example.com"><policy-violation ` + ns + `/><text ` + ns + `>Too fast</text></error></message>`,
			messageError{ID: "m1", From: "room@conference.example.com", To: "me@example.com/r", ErrorType: "modify", By: "room@conference.example.com", Condition: "policy-violation", Text: "Too fast"}},
		{"no text no by", `<message type="error" id="m2" from="bob@example.com"><error type="cancel"><service-unavailable ` + ns + `/></error></message>`,
			messageError{ID: "m2", From: "bob@example.com", ErrorType: "cancel", Condition: "service-unavailable"}},
		{"no error child", `<message type="error" id="m3" from="bob@example.com"><body>SECRET</body></message>`,
			messageError{ID: "m3", From: "bob@example.com"}},
		{"app condition ignored", `<message type="error" id="m4" from="r@c.example.com"><error type="auth"><x-custom xmlns="urn:example:app"/><forbidden ` + ns + `/></error></message>`,
			messageError{ID: "m4", From: "r@c.example.com", ErrorType: "auth", Condition: "forbidden"}},
		{"text capped and cleaned", `<message type="error" id="m5" from="r@c.example.com"><error type="wait"><resource-constrained ` + ns + `/><text ` + ns + `>a` + "\n" + `b` + long + `</text></error></message>`,
			messageError{ID: "m5", From: "r@c.example.com", ErrorType: "wait", Condition: "resource-constrained", Text: "a b" + long[:197]}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseErrStanza(t, tc.stanza); got != tc.want {
				t.Errorf("got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestMessageErrorNoticeAndStatus(t *testing.T) {
	a := messageError{Condition: "forbidden", Text: "Not allowed", ErrorType: "auth"}
	if got, want := a.noticeText(), "Your message was not delivered: forbidden \u2013 Not allowed"; got != want {
		t.Errorf("notice %q, want %q", got, want)
	}
	if got := (messageError{}).noticeText(); got != "Your message was not delivered: unknown error" {
		t.Errorf("notice %q", got)
	}
	if a.status() != event.MessageStatusFail {
		t.Error("auth error must be permanent")
	}
	if (messageError{ErrorType: "wait"}).status() != event.MessageStatusRetriable {
		t.Error("wait error must be retriable")
	}
}

func TestFindAndReportRejectedMessage(t *testing.T) {
	ctx := context.Background()
	db := realSchemaDB(t)
	const me networkid.UserLoginID = "me@example.com"
	muc := networkid.PortalKey{ID: "room@conference.example.com", Receiver: me}
	dm := networkid.PortalKey{ID: "bob@example.com", Receiver: me}
	for _, p := range []networkid.PortalKey{muc, dm} {
		if err := db.Portal.Insert(ctx, &database.Portal{PortalKey: p, MXID: id.RoomID("!" + string(p.ID) + ":hs")}); err != nil {
			t.Fatal(err)
		}
	}
	for _, g := range []networkid.UserID{"me@example.com", "bob@example.com"} {
		if err := db.Ghost.Insert(ctx, &database.Ghost{ID: g}); err != nil {
			t.Fatal(err)
		}
	}
	ins := func(room networkid.PortalKey, mid, mxid string, sender networkid.UserID, meta *MessageMetadata) {
		t.Helper()
		if err := db.Message.Insert(ctx, &database.Message{ID: networkid.MessageID(mid), MXID: id.EventID(mxid), Room: room,
			SenderID: sender, Timestamp: time.Unix(1_700_000_000, 0), Metadata: meta}); err != nil {
			t.Fatal(err)
		}
	}
	ins(muc, "matrix:$m1", "$m1", "me@example.com", &MessageMetadata{XMPPMessageID: "x-muc"})
	ins(dm, "matrix:$d1", "$d1", "me@example.com", &MessageMetadata{XMPPMessageID: "x-dm", OriginID: "o-dm"})
	ins(dm, "inbound-1", "$in1", "bob@example.com", &MessageMetadata{XMPPMessageID: "x-in"})

	x := newOuterFromTestClient()
	x.login.UserMXID = "@user:hs"
	var notices []string
	var statuses []*bridgev2.MessageStatus
	var infos []*bridgev2.MessageStatusEventInfo
	x.noticeHook = func(p *bridgev2.Portal, text string) { notices = append(notices, string(p.MXID)+"|"+text) }
	x.statusHook = func(ms *bridgev2.MessageStatus, i *bridgev2.MessageStatusEventInfo) {
		statuses = append(statuses, ms)
		infos = append(infos, i)
	}
	reset := func() { notices, statuses, infos = nil, nil, nil }

	// MUC: from the room bare JID or an occupant.
	e := messageError{ID: "x-muc", From: "room@conference.example.com", Condition: "policy-violation", Text: "Too fast", ErrorType: "modify"}
	if !x.reportRejectedMessage(ctx, db, e) {
		t.Fatal("muc rejection not correlated")
	}
	if len(statuses) != 1 || statuses[0].Status != event.MessageStatusFail || !statuses[0].IsCertain ||
		infos[0].SourceEventID != "$m1" || infos[0].RoomID != "!room@conference.example.com:hs" || infos[0].Sender != "@user:hs" {
		t.Errorf("bad status %+v %+v", statuses, infos)
	}
	if len(notices) != 1 || notices[0] != "!room@conference.example.com:hs|Your message was not delivered: policy-violation \u2013 Too fast" {
		t.Errorf("notices %v", notices)
	}

	// DM by origin id, from a full JID.
	reset()
	e = messageError{ID: "o-dm", From: "bob@example.com/phone", Condition: "service-unavailable", ErrorType: "wait"}
	if !x.reportRejectedMessage(ctx, db, e) || statuses[0].Status != event.MessageStatusRetriable || infos[0].SourceEventID != "$d1" {
		t.Errorf("dm: %+v %+v", statuses, infos)
	}

	// Room scoped: the MUC id from the DM peer, an unknown id, and an inbound (not Matrix-originated) id.
	for _, e := range []messageError{
		{ID: "x-muc", From: "bob@example.com"},
		{ID: "x-dm", From: "room@conference.example.com"},
		{ID: "nope", From: "bob@example.com"},
		{ID: "x-in", From: "bob@example.com"},
	} {
		reset()
		if x.reportRejectedMessage(ctx, db, e) || len(statuses) != 0 || len(notices) != 0 {
			t.Errorf("%+v must not be correlated", e)
		}
	}
}

func TestMessageErrorLogAndEchoDiscard(t *testing.T) {
	const secret = "TOPSECRETBODY"
	x, _ := hookClient()
	buf := &bmLogBuf{}
	x.log = zerolog.New(buf).Level(zerolog.TraceLevel)
	x.mucEcho.register("room@conference.example.com", "m9")
	long := strings.Repeat("y", 400)
	serveStanza(t, x, `<message xmlns="jabber:client" type="error" id="m9" from="room@conference.example.com" to="me@example.com/r"><body>`+secret+`</body><error type="auth" by="room@conference.example.com"><forbidden xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/><text xmlns="urn:ietf:params:xml:ns:xmpp-stanzas">`+long+`</text></error></message>`)
	out := buf.String()
	for _, want := range []string{`"level":"warn"`, `"from":"room@conference.example.com"`, `"to":"me@example.com/r"`, `"id":"m9"`, `"error_type":"auth"`, `"condition":"forbidden"`, `"by":"room@conference.example.com"`, `"text":"` + long[:200] + `"`} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, long[:201]) {
		t.Error("text not capped")
	}
	if strings.Contains(out, secret) {
		t.Errorf("log leaks message body:\n%s", out)
	}
	if _, found, _ := x.mucEcho.wait(context.Background(), "room@conference.example.com", "m9", time.Millisecond); found {
		t.Error("pending echo entry not discarded")
	}
}
