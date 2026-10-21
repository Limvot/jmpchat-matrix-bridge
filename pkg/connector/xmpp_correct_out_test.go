package connector

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"mellium.im/xmlstream"
	"mellium.im/xmpp/stanza"
)

type outCapture struct {
	sent  []outMessage
	tried []outMessage // every attempt, failed ones included
	err   error
}

func (c *outCapture) hook(_ context.Context, m outMessage) (string, error) {
	c.tried = append(c.tried, m)
	if c.err != nil {
		return "", c.err
	}
	c.sent = append(c.sent, m)
	id := m.ID
	if id == "" {
		id = "gen-id"
	}
	return id, nil
}

func editClient(c *outCapture) *xmppClient {
	return &xmppClient{loginID: outLogin, log: zerolog.Nop(), sendOutHook: c.hook}
}

func editPortal(id string, rt database.RoomType) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID(id), Receiver: outLogin}, RoomType: rt}}
}

func editMsg(portal *bridgev2.Portal, target *database.Message, body string) *bridgev2.MatrixEdit {
	msg := &bridgev2.MatrixEdit{EditTarget: target}
	msg.Portal = portal
	msg.Content = &event.MessageEventContent{MsgType: event.MsgText, Body: body}
	return msg
}

func TestOutPayloadCorrectionStanza(t *testing.T) {
	toks, err := xmlstream.ReadAll(outPayload(outMessage{Replace: "REF"}, "new text", "NEW"))
	if err != nil {
		t.Fatal(err)
	}
	var starts []xml.StartElement
	var body string
	inBody := false
	for _, tok := range toks {
		switch v := tok.(type) {
		case xml.StartElement:
			starts = append(starts, v)
			inBody = v.Name.Local == "body"
		case xml.CharData:
			if inBody {
				body += string(v)
			}
		case xml.EndElement:
			inBody = false
		}
	}
	want := []struct{ space, local string }{
		{"", "body"}, {"urn:xmpp:sid:0", "origin-id"}, {"urn:xmpp:message-correct:0", "replace"}, {"urn:xmpp:hints", "store"},
	}
	if len(starts) != len(want) {
		t.Fatalf("elements = %v", starts)
	}
	for i, w := range want {
		if starts[i].Name.Space != w.space || starts[i].Name.Local != w.local {
			t.Errorf("element %d = %v, want %v", i, starts[i].Name, w)
		}
	}
	if attrValue(starts[1].Attr, "id") != "NEW" || attrValue(starts[2].Attr, "id") != "REF" || attrValue(starts[2].Attr, "xmlns") != "urn:xmpp:message-correct:0" {
		t.Errorf("attrs: origin-id=%+v replace=%+v", starts[1].Attr, starts[2].Attr)
	}
	if body != "new text" {
		t.Errorf("body = %q", body)
	}
}

// Plain messages keep their exact shape: no replace, no store hint.
func TestOutPayloadPlainMessageUnchanged(t *testing.T) {
	toks, _ := xmlstream.ReadAll(outPayload(outMessage{OOBURL: "https://example.com/f.png", ReplyID: "r1", ReplyTo: "bob@example.com"}, "cap", "ID"))
	var locals []string
	for _, tok := range toks {
		if se, ok := tok.(xml.StartElement); ok {
			locals = append(locals, se.Name.Local)
		}
	}
	if strings.Join(locals, ",") != "body,origin-id,x,url,reply" {
		t.Errorf("elements = %v", locals)
	}
}

func TestHandleMatrixEditStanza(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target *database.Message
		want   string
	}{
		{"matrix-originated row uses the id we sent", matrixRow("sent-1", ""), "sent-1"},
		{"carbon row with origin-id", &database.Message{ID: "abc", SenderID: "me@example.com", Metadata: &MessageMetadata{OriginID: "oid"}}, "oid"},
		{"carbon row without origin-id uses the row id", &database.Message{ID: "abc", SenderID: "me@example.com"}, "abc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &outCapture{}
			x := editClient(c)
			if err := x.handleMatrixEdit(context.Background(), editMsg(editPortal("bob@example.com", database.RoomTypeDM), tc.target, "corrected")); err != nil {
				t.Fatal(err)
			}
			if len(c.sent) != 1 {
				t.Fatalf("sent %d messages", len(c.sent))
			}
			m := c.sent[0]
			if m.Replace != tc.want || m.Body != "corrected" || m.Type != stanza.ChatMessage || m.To.String() != "bob@example.com" || m.ID != "" || m.OOBURL != "" || m.ReplyID != "" {
				t.Errorf("message = %+v", m)
			}
		})
	}
}

func TestHandleMatrixEditFormattedBodyFallback(t *testing.T) {
	c := &outCapture{}
	x := editClient(c)
	msg := editMsg(editPortal("bob@example.com", database.RoomTypeDM), matrixRow("sent-1", ""), "")
	msg.Content.FormattedBody = "<b>bold</b>"
	if err := x.handleMatrixEdit(context.Background(), msg); err != nil || c.sent[0].Body != "**bold**" {
		t.Fatalf("err=%v sent=%+v", err, c.sent)
	}
}

func TestHandleMatrixEditMUCUsesIDAndRegistersEcho(t *testing.T) {
	c := &outCapture{}
	x := editClient(c)
	x.mucOurOccupantJID = map[string]string{"room@muc.example.com": "room@muc.example.com/me"}
	target := matrixRow("sent-1", "ROOM-STANZA")
	if err := x.handleMatrixEdit(context.Background(), editMsg(editPortal("room@muc.example.com", database.RoomTypeGroupDM), target, "corrected")); err != nil {
		t.Fatal(err)
	}
	m := c.sent[0]
	if m.Type != stanza.MessageType("groupchat") || m.Replace != "sent-1" {
		t.Errorf("message = %+v: XEP-0308 names the id attribute, not the room stanza-id", m)
	}
	if m.ID == "" {
		t.Fatal("a MUC correction must pick its stanza id up front")
	}
	if _, found, _ := x.mucEcho.wait(context.Background(), "room@muc.example.com", m.ID, time.Millisecond); !found {
		t.Error("the stanza id must be registered in the echo table before sending")
	}
	// The registered id makes the room's echo of our correction drop out as our own message.
	if !x.handleOwnMUCEcho("room@muc.example.com", "room@muc.example.com/me", m.ID, messageIDs{}) {
		t.Error("echo of the correction not recognized")
	}
	// A failed send discards the registration.
	c.err = errors.New("boom")
	c.sent = nil
	err := x.handleMatrixEdit(context.Background(), editMsg(editPortal("room@muc.example.com", database.RoomTypeGroupDM), target, "again"))
	if err == nil {
		t.Fatal("send error must surface")
	}
	failed := c.tried[len(c.tried)-1]
	if failed.ID == "" {
		t.Fatal("the failed attempt must have carried a registered id")
	}
	if _, found, _ := x.mucEcho.wait(context.Background(), "room@muc.example.com", failed.ID, time.Millisecond); found {
		t.Error("a failed send must remove the id from the echo table")
	}
}

func TestHandleMatrixEditFormattedBodyOnly(t *testing.T) {
	c := &outCapture{}
	x := editClient(c)
	target := matrixRow("sent-1", "")
	msg := editMsg(editPortal("bob@example.com", database.RoomTypeDM), target, "")
	msg.Content.FormattedBody = "<b>bold</b> and <i>it</i>"
	msg.Content.Format = event.FormatHTML
	if err := x.handleMatrixEdit(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if got := c.sent[0].Body; strings.Contains(got, "<") || !strings.Contains(got, "bold") {
		t.Errorf("body = %q, want plain text without HTML", got)
	}
	empty := editMsg(editPortal("bob@example.com", database.RoomTypeDM), target, "")
	empty.Content.FormattedBody = "<p></p>"
	if err := x.handleMatrixEdit(context.Background(), empty); err == nil || len(c.sent) != 1 {
		t.Errorf("edit without derivable text must be refused: err=%v sent=%d", err, len(c.sent))
	}
}

func TestHandleMatrixEditRefusesNotOwn(t *testing.T) {
	c := &outCapture{}
	x := editClient(c)
	theirs := &database.Message{ID: "abc", SenderID: "bob@example.com", Metadata: &MessageMetadata{XMPPMessageID: "abc"}}
	if err := x.handleMatrixEdit(context.Background(), editMsg(editPortal("bob@example.com", database.RoomTypeDM), theirs, "x")); !errors.Is(err, errEditNotOwn) {
		t.Fatalf("err = %v", err)
	}
	if len(c.sent) != 0 {
		t.Error("nothing may be sent")
	}
}

func TestHandleMatrixEditRefusesRelay(t *testing.T) {
	c := &outCapture{}
	x := editClient(c)
	row := matrixRow("sent-1", "")
	row.SenderMXID = "@alice:example.org"
	msg := editMsg(editPortal("bob@example.com", database.RoomTypeDM), row, "x")
	msg.OrigSender = &bridgev2.OrigSender{}
	msg.Event = &event.Event{Sender: "@mallory:example.org"}
	if err := x.handleMatrixEdit(context.Background(), msg); !errors.Is(err, errEditNotOwn) {
		t.Fatalf("other relayed user: err = %v", err)
	}
	msg.Event = &event.Event{Sender: "@alice:example.org"}
	if err := x.handleMatrixEdit(context.Background(), msg); err != nil || len(c.sent) != 1 {
		t.Fatalf("own relayed message: err=%v sent=%d", err, len(c.sent))
	}
}

func TestHandleMatrixEditRefusesMedia(t *testing.T) {
	c := &outCapture{}
	x := editClient(c)
	mediaRow := matrixRow("sent-1", "")
	targetMeta(mediaRow).Media = true
	if err := x.handleMatrixEdit(context.Background(), editMsg(editPortal("bob@example.com", database.RoomTypeDM), mediaRow, "new caption")); !errors.Is(err, errEditMedia) {
		t.Fatalf("media row: err = %v", err)
	}
	img := editMsg(editPortal("bob@example.com", database.RoomTypeDM), matrixRow("sent-2", ""), "x")
	img.Content.MsgType = event.MsgImage
	if err := x.handleMatrixEdit(context.Background(), img); !errors.Is(err, bridgev2.ErrUnsupportedMessageType) {
		t.Fatalf("media content: err = %v", err)
	}
	// Still refused for SMS portals.
	if err := x.handleMatrixEdit(context.Background(), editMsg(editPortal("+15551234567@cheogram.com", database.RoomTypeDM), mediaRow, "x")); !errors.Is(err, errEditMedia) {
		t.Fatalf("cheogram media row: err = %v", err)
	}
	if len(c.sent) != 0 {
		t.Error("nothing may be sent")
	}
}

func TestHandleMatrixEditRefusesNoRef(t *testing.T) {
	c := &outCapture{}
	x := editClient(c)
	noRef := &database.Message{ID: "matrix:$e", SenderID: "me@example.com", Metadata: &MessageMetadata{}}
	if err := x.handleMatrixEdit(context.Background(), editMsg(editPortal("bob@example.com", database.RoomTypeDM), noRef, "x")); !errors.Is(err, errEditNoXMPPRef) {
		t.Fatalf("err = %v", err)
	}
}

func TestHandleMatrixEditNotConnected(t *testing.T) {
	x := &xmppClient{loginID: outLogin, log: zerolog.Nop()}
	if err := x.handleMatrixEdit(context.Background(), editMsg(editPortal("bob@example.com", database.RoomTypeDM), matrixRow("sent-1", ""), "x")); !errors.Is(err, errRetractNotConnected) {
		t.Fatalf("err = %v", err)
	}
}

// SMS cannot be corrected: the new text goes out as a fresh plain message "* text", no <replace>.
func TestCheogramEditSendsNewSMS(t *testing.T) {
	for _, portalID := range []string{"+15551234567@cheogram.com", "+15551234567,+15557654321@cheogram.com", "123;phone-context=ca-us.phone-context.example.com@cheogram.com"} {
		c := &outCapture{}
		x := editClient(c)
		// The original needs no XMPP id: nothing references it.
		target := &database.Message{ID: "matrix:$e", SenderID: networkid.UserID(outLogin), Metadata: &MessageMetadata{}}
		if err := x.handleMatrixEdit(context.Background(), editMsg(editPortal(portalID, database.RoomTypeDM), target, "fixed text")); err != nil {
			t.Fatalf("%s: %v", portalID, err)
		}
		if len(c.sent) != 1 {
			t.Fatalf("%s: sent %d messages, want exactly one", portalID, len(c.sent))
		}
		m := c.sent[0]
		if m.Body != "* fixed text" || m.Replace != "" || m.ID != "" || m.Type != stanza.ChatMessage || m.OOBURL != "" || m.ReplyID != "" {
			t.Errorf("%s: message = %+v, want a new plain message with the \"* \" prefix and no replace", portalID, m)
		}
		if m.To.String() != portalID {
			t.Errorf("%s: sent to %s", portalID, m.To)
		}
		// The payload carries no correction elements.
		toks, _ := xmlstream.ReadAll(outPayload(m, m.Body, "NEW"))
		for _, tok := range toks {
			if se, ok := tok.(xml.StartElement); ok && (se.Name.Local == "replace" || se.Name.Local == "store") {
				t.Errorf("%s: unexpected <%s> in an SMS edit", portalID, se.Name.Local)
			}
		}
	}
	// Someone else's message is still refused on SMS portals.
	c := &outCapture{}
	x := editClient(c)
	theirs := &database.Message{ID: "abc", SenderID: "+15551234567@cheogram.com"}
	if err := x.handleMatrixEdit(context.Background(), editMsg(editPortal("+15551234567@cheogram.com", database.RoomTypeDM), theirs, "x")); !errors.Is(err, errEditNotOwn) || len(c.sent) != 0 {
		t.Fatalf("err=%v sent=%d", err, len(c.sent))
	}
}

func TestEditContentNotLogged(t *testing.T) {
	const secret = "EDITSECRETBODY"
	c := &outCapture{}
	x := editClient(c)
	buf := &bytes.Buffer{}
	x.log = zerolog.New(buf).Level(zerolog.TraceLevel)
	ctx := x.log.WithContext(context.Background())
	_ = x.handleMatrixEdit(ctx, editMsg(editPortal("bob@example.com", database.RoomTypeDM), matrixRow("sent-1", ""), secret+" dm"))
	_ = x.handleMatrixEdit(ctx, editMsg(editPortal("+15551234567@cheogram.com", database.RoomTypeDM), matrixRow("sent-2", ""), secret+" sms"))
	if buf.Len() == 0 {
		t.Fatal("nothing was logged; the test proves nothing")
	}
	if strings.Contains(buf.String(), secret) {
		t.Errorf("logs leak the edited text:\n%s", buf.String())
	}
}

func TestEditCapabilities(t *testing.T) {
	if jabberCaps.Edit != event.CapLevelFullySupported || jabberCaps.EditMaxAge != nil || jabberCaps.EditMaxCount != 0 {
		t.Errorf("Edit=%v maxAge=%v maxCount=%d", jabberCaps.Edit, jabberCaps.EditMaxAge, jabberCaps.EditMaxCount)
	}
	// SMS portals get the same capabilities (their edits are sent as new texts): one set, one ID.
	x := &xmppClient{}
	if x.GetCapabilities(context.Background(), editPortal("+15551234567@cheogram.com", database.RoomTypeDM)) != jabberCaps {
		t.Error("cheogram portals must not get a separate capability set")
	}
	var _ bridgev2.EditHandlingNetworkAPI = x
}
