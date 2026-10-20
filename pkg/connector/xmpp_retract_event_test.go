package connector

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const retractTestLogin networkid.UserLoginID = "me@example.com"

func dmPortal() networkid.PortalKey {
	return networkid.PortalKey{ID: "bob@example.com", Receiver: retractTestLogin}
}
func mucPortal() networkid.PortalKey {
	return networkid.PortalKey{ID: "room@muc.example.com", Receiver: retractTestLogin}
}

func row(portal networkid.PortalKey, msgID, sender string, meta *MessageMetadata) *database.Message {
	m := &database.Message{ID: networkid.MessageID(msgID), Room: portal, SenderID: networkid.UserID(sender), MXID: id.EventID("$" + msgID)}
	if meta != nil {
		m.Metadata = meta
	}
	return m
}

// ---- validation matrix ----

func TestValidateRetraction(t *testing.T) {
	dm := portalFacts{key: dmPortal(), loginID: retractTestLogin}
	muc := portalFacts{key: mucPortal(), loginID: retractTestLogin, isMUC: true, ourOccupantFull: "room@muc.example.com/me"}
	mms := portalFacts{key: networkid.PortalKey{ID: "+15550001111,+15550002222@cheogram.com", Receiver: retractTestLogin}, loginID: retractTestLogin, isGroupMMS: true}
	bobMsg := row(dmPortal(), "m1", "bob@example.com", nil)
	ourMsg := row(dmPortal(), "m2", "me@example.com", nil)
	aliceMUC := row(mucPortal(), "s1", "room@muc.example.com/alice", &MessageMetadata{StanzaID: "S1"})
	aliceMUCOcc := row(mucPortal(), "s2", "room@muc.example.com/alice", &MessageMetadata{StanzaID: "S2", OccupantID: "OCC-A"})
	ourMUC := row(mucPortal(), "s3", "me@example.com", &MessageMetadata{StanzaID: "S3"})
	live := func(from string) retraction {
		return retraction{channel: retractChanLive, msgType: "chat", fromFull: from}
	}
	gc := func(from, occ string) retraction {
		return retraction{channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: from, occupantID: occ}
	}
	mod := func(from string, groupchat bool) retraction {
		r := gc(from, "")
		r.groupchat = groupchat
		r.info.Moderated = true
		return r
	}
	for _, tc := range []struct {
		name   string
		r      retraction
		p      portalFacts
		target *database.Message
		want   bool
	}{
		{"dm same bare jid", live("bob@example.com/phone"), dm, bobMsg, true},
		{"dm mixed case", live("BOB@Example.com/phone"), dm, bobMsg, true},
		{"dm different sender", live("mallory@example.com/p"), dm, bobMsg, false},
		{"dm contact retracts our message", live("bob@example.com/p"), dm, ourMsg, false},
		{"dm headline type", retraction{channel: retractChanLive, msgType: "headline", fromFull: "bob@example.com"}, dm, bobMsg, false},
		{"carbon sent own", retraction{channel: retractChanCarbonSent, msgType: "chat", fromFull: "me@example.com/laptop"}, dm, ourMsg, true},
		{"carbon sent from other", retraction{channel: retractChanCarbonSent, msgType: "chat", fromFull: "bob@example.com/p"}, dm, ourMsg, false},
		{"carbon sent of contact message", retraction{channel: retractChanCarbonSent, msgType: "chat", fromFull: "me@example.com/laptop"}, dm, bobMsg, false},
		{"carbon received", retraction{channel: retractChanCarbonReceived, msgType: "chat", fromFull: "bob@example.com/p"}, dm, bobMsg, true},
		{"mam own retraction of own message", retraction{channel: retractChanMAM, msgType: "chat", fromFull: "me@example.com/laptop"}, dm, ourMsg, true},
		{"group mms ignored", live("+15550001111@cheogram.com"), mms, row(mms.key, "g1", "+15550001111@cheogram.com", nil), false},
		{"target in another portal", live("bob@example.com/p"), dm, row(networkid.PortalKey{ID: "other@example.com", Receiver: retractTestLogin}, "x", "bob@example.com", nil), false},
		{"dm-style retraction into a muc portal", retraction{channel: retractChanLive, msgType: "chat", fromFull: "room@muc.example.com/alice"}, muc, row(mucPortal(), "pm", "room@muc.example.com", nil), false},
		{"moderation from room bare", mod("room@muc.example.com", true), muc, aliceMUC, true},
		{"moderation from room bare, other case", mod("ROOM@muc.example.com", true), muc, aliceMUC, true},
		{"moderation from occupant", mod("room@muc.example.com/mallory", true), muc, aliceMUC, false},
		{"moderation with type=chat", mod("room@muc.example.com", false), muc, aliceMUC, false},
		{"moderation from wrong room", mod("evil@muc.example.com", true), muc, aliceMUC, false},
		{"moderation in a non-muc portal", mod("bob@example.com", true), dm, bobMsg, false},
		{"bare-room retraction without <moderated> counts as moderation", gc("room@muc.example.com", ""), muc, aliceMUC, true},
		{"muc self nick match, no occupant-id", gc("room@muc.example.com/alice", ""), muc, aliceMUC, true},
		{"muc self other nick", gc("room@muc.example.com/mallory", ""), muc, aliceMUC, false},
		{"muc self equal occupant-id (nick changed)", gc("room@muc.example.com/alice2", "OCC-A"), muc, aliceMUCOcc, true},
		{"muc self different occupant-id, same nick", gc("room@muc.example.com/alice", "OCC-EVIL"), muc, aliceMUCOcc, false},
		{"muc self missing occupant-id while target has one, same nick", gc("room@muc.example.com/alice", ""), muc, aliceMUCOcc, false},
		{"muc self nick match with occupant-id, target has none (nick reuse)", gc("room@muc.example.com/alice", "OCC-M"), muc, aliceMUC, false},
		{"muc self our nick with occupant-id, our target has none", gc("room@muc.example.com/me", "OCC-M"), muc, ourMUC, false},
		{"muc self our other client with our nick", gc("room@muc.example.com/me", ""), muc, ourMUC, true},
		{"muc self someone else's nick on our message", gc("room@muc.example.com/alice", ""), muc, ourMUC, false},
		{"muc self in wrong room", gc("evil@muc.example.com/alice", ""), muc, aliceMUC, false},
		{"muc self in a dm portal", gc("bob@example.com/alice", ""), dm, bobMsg, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := validateRetraction(tc.r, tc.p, tc.target)
			if ok != tc.want {
				t.Fatalf("validate = %v (%s), want %v", ok, reason, tc.want)
			}
			if !ok && reason == "" {
				t.Error("rejection without a reason")
			}
		})
	}
	if ok, reason := validateRetraction(live("bob@example.com"), dm, nil); ok || reason != "not_found" {
		t.Errorf("nil target = %v %q", ok, reason)
	}
}

func TestRetractionKindAndRefKind(t *testing.T) {
	for _, tc := range []struct {
		name string
		r    retraction
		kind string
		ref  refKind
	}{
		{"dm", retraction{fromFull: "bob@example.com/p"}, retractKindRetraction, refDM},
		{"muc self", retraction{groupchat: true, fromFull: "room@muc/alice"}, retractKindRetraction, refMUCSelf},
		{"moderated", retraction{groupchat: true, fromFull: "room@muc", info: retractInfo{Moderated: true}}, retractKindModeration, refMUCModeration},
		{"bare room without moderated", retraction{groupchat: true, fromFull: "room@muc"}, retractKindModeration, refMUCModeration},
		{"moderated from occupant is still moderation (and gets rejected)", retraction{groupchat: true, fromFull: "room@muc/x", info: retractInfo{Moderated: true}}, retractKindModeration, refMUCModeration},
	} {
		if tc.r.kind() != tc.kind || tc.r.refKind() != tc.ref {
			t.Errorf("%s: kind=%s ref=%d", tc.name, tc.r.kind(), tc.r.refKind())
		}
	}
}

func TestPickRetractionTargetFirstValidWins(t *testing.T) {
	muc := portalFacts{key: mucPortal(), loginID: retractTestLogin, isMUC: true}
	r := retraction{groupchat: true, fromFull: "room@muc.example.com/bob", occupantID: "OCC-B"}
	bad := row(mucPortal(), "a", "room@muc.example.com/mallory", &MessageMetadata{OccupantID: "OCC-M"})
	good := row(mucPortal(), "b", "room@muc.example.com/bob", &MessageMetadata{OccupantID: "OCC-B"})
	got, reason := pickRetractionTarget(r, muc, []*database.Message{bad, good})
	if got != good || reason != "" {
		t.Errorf("got %v %q", got, reason)
	}
	got, reason = pickRetractionTarget(r, muc, []*database.Message{bad})
	if got != nil || reason != "occupant_id_mismatch" {
		t.Errorf("got %v %q", got, reason)
	}
	if got, reason = pickRetractionTarget(r, muc, nil); got != nil || reason != "not_found" {
		t.Errorf("none: %v %q", got, reason)
	}
}

// ---- event semantics with a fake environment ----

type fakeRetractEnv struct {
	cands     []*database.Message
	findErr   error
	gotRef    string
	gotKind   refKind
	pf        portalFacts
	removeRes bridgev2.EventHandlingResult
	removed   int
	reason    string
	moderated bool
	marked    [][]*database.Message
	gone      bool
}

func (f *fakeRetractEnv) findByRef(_ context.Context, _ networkid.PortalKey, ref string, kind refKind) ([]*database.Message, error) {
	f.gotRef, f.gotKind = ref, kind
	return f.cands, f.findErr
}
func (f *fakeRetractEnv) facts(context.Context, *bridgev2.Portal) portalFacts { return f.pf }
func (f *fakeRetractEnv) removeWithReason(_ context.Context, _ *bridgev2.Portal, _ []*database.Message, reason string, _ time.Time, moderation bool) bridgev2.EventHandlingResult {
	f.removed++
	f.reason, f.moderated = reason, moderation
	return f.removeRes
}
func (f *fakeRetractEnv) markRetracted(_ context.Context, _ networkid.PortalKey, parts []*database.Message) {
	f.marked = append(f.marked, parts)
}
func (f *fakeRetractEnv) rowsGone(context.Context, *bridgev2.Portal, networkid.MessageID) bool {
	return f.gone
}
func (f *fakeRetractEnv) log(context.Context) *zerolog.Logger { l := zerolog.Nop(); return &l }

func TestRetractionEventSemantics(t *testing.T) {
	victim := row(dmPortal(), "m1", "bob@example.com", &MessageMetadata{OriginID: "ORIG"})
	env := &fakeRetractEnv{cands: []*database.Message{victim}, pf: portalFacts{key: dmPortal(), loginID: retractTestLogin}}
	r := retraction{info: retractInfo{Target: "ORIG"}, channel: retractChanLive, msgType: "chat", fromFull: "bob@example.com/p"}
	evt := newRetractionEvent(env, dmPortal(), r, time.Unix(100, 0))

	if evt.GetType() != bridgev2.RemoteEventMessageRemove {
		t.Errorf("type = %v", evt.GetType())
	}
	if evt.ShouldCreatePortal() {
		t.Error("a retraction must never create a portal")
	}
	if evt.GetSender() != (bridgev2.EventSender{}) {
		t.Errorf("sender must stay zero, got %+v", evt.GetSender())
	}
	if evt.DontRenderPlaceholder() {
		t.Error("placeholder must be shown")
	}
	if evt.GetTargetMessage() != "ORIG" {
		t.Errorf("target before pre-handler = %q, want the raw ref", evt.GetTargetMessage())
	}
	var _ bridgev2.RemotePreHandlerWithResult = evt
	var _ bridgev2.RemoteMessageRemove = evt

	if res := evt.PreHandleWithResult(context.Background(), nil); res != nil {
		t.Fatalf("pre-handler must continue to the framework removal, got %+v", res)
	}
	if evt.GetTargetMessage() != "m1" {
		t.Errorf("target after pre-handler = %q, want the resolved id", evt.GetTargetMessage())
	}
	if env.gotRef != "ORIG" || env.gotKind != refDM {
		t.Errorf("lookup = %q kind %d", env.gotRef, env.gotKind)
	}
	if env.removed != 0 {
		t.Error("no reason: the framework path must be used")
	}
	// The tombstone is only written when the framework really removed the rows.
	env.gone = false
	evt.PostHandle(context.Background(), nil)
	if len(env.marked) != 0 {
		t.Error("tombstone written although the row is still there")
	}
}

func TestRetractionTombstoneAfterFrameworkRemoval(t *testing.T) {
	victim := row(dmPortal(), "m1", "bob@example.com", nil)
	env := &fakeRetractEnv{cands: []*database.Message{victim}, pf: portalFacts{key: dmPortal(), loginID: retractTestLogin}, gone: true}
	r := retraction{info: retractInfo{Target: "m1"}, channel: retractChanLive, msgType: "chat", fromFull: "bob@example.com/p"}
	evt := newRetractionEvent(env, dmPortal(), r, time.Time{})
	if res := evt.PreHandleWithResult(context.Background(), nil); res != nil {
		t.Fatalf("unexpected result %+v", res)
	}
	evt.PostHandle(context.Background(), nil)
	if len(env.marked) != 1 || env.marked[0][0] != victim {
		t.Errorf("marked = %v", env.marked)
	}
	evt.PostHandle(context.Background(), nil)
	if len(env.marked) != 1 {
		t.Error("tombstone written twice")
	}
}

func TestRetractionPreHandlerIgnoresUnknownAndInvalid(t *testing.T) {
	r := retraction{info: retractInfo{Target: "m1"}, channel: retractChanLive, msgType: "chat", fromFull: "mallory@example.com/p"}
	for name, cands := range map[string][]*database.Message{
		"not found": nil,
		"invalid":   {row(dmPortal(), "m1", "bob@example.com", nil)},
	} {
		env := &fakeRetractEnv{cands: cands, pf: portalFacts{key: dmPortal(), loginID: retractTestLogin}}
		evt := newRetractionEvent(env, dmPortal(), r, time.Time{})
		res := evt.PreHandleWithResult(context.Background(), nil)
		if res == nil || !res.Ignored || res.Error != nil {
			t.Errorf("%s: result = %+v, want Ignored", name, res)
		}
		if evt.GetTargetMessage() != "m1" || env.removed != 0 || len(env.marked) != 0 {
			t.Errorf("%s: event must be left untouched", name)
		}
	}
	env := &fakeRetractEnv{findErr: errors.New("db down")}
	res := newRetractionEvent(env, dmPortal(), r, time.Time{}).PreHandleWithResult(context.Background(), nil)
	if res == nil || res.Success || res.Error == nil {
		t.Errorf("lookup error result = %+v", res)
	}
}

// With a reason the connector redacts itself and returns success so the framework skips its own
// redaction; without one the framework path is used.
// A legacy :0 moderation whose reason sits inside <moderated> reaches the reason path.
func TestRetractionLegacyModerationReasonEndToEnd(t *testing.T) {
	_, _, ids := parseFull(t, `<message type="groupchat" from="room@muc.example.com"><apply-to xmlns="urn:xmpp:fasten:0" id="S1"><moderated xmlns="urn:xmpp:message-moderate:0" by="room@muc.example.com/mod"><retract xmlns="urn:xmpp:message-retract:0"/><reason>off topic</reason></moderated></apply-to></message>`)
	if ids.Retract == nil {
		t.Fatal("no retraction parsed")
	}
	room := mucPortal()
	victim := row(room, "m1", "room@muc.example.com/alice", &MessageMetadata{StanzaID: "S1"})
	env := &fakeRetractEnv{cands: []*database.Message{victim}, pf: portalFacts{key: room, loginID: retractTestLogin, isMUC: true}, removeRes: bridgev2.EventHandlingResultSuccess}
	r := retraction{info: *ids.Retract, channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: "room@muc.example.com"}
	res := newRetractionEvent(env, room, r, time.Time{}).PreHandleWithResult(context.Background(), nil)
	if res == nil || !res.Success || env.reason != "off topic" || !env.moderated {
		t.Errorf("res=%+v reason=%q moderated=%v", res, env.reason, env.moderated)
	}
}

func TestRetractionReasonPathSelection(t *testing.T) {
	room := mucPortal()
	victim := row(room, "m1", "room@muc.example.com/alice", &MessageMetadata{StanzaID: "S1"})
	pf := portalFacts{key: room, loginID: retractTestLogin, isMUC: true}
	moderation := retraction{info: retractInfo{Target: "S1", Moderated: true, HasReason: true, Reason: "spam"}, channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: "room@muc.example.com"}

	env := &fakeRetractEnv{cands: []*database.Message{victim}, pf: pf, removeRes: bridgev2.EventHandlingResultSuccess}
	evt := newRetractionEvent(env, room, moderation, time.Time{})
	res := evt.PreHandleWithResult(context.Background(), nil)
	if res == nil || !res.Success {
		t.Fatalf("reason path result = %+v, want Success (framework must skip its removal)", res)
	}
	if env.removed != 1 || env.reason != "spam" || !env.moderated || len(env.marked) != 1 {
		t.Errorf("removed=%d reason=%q moderation=%v marked=%d", env.removed, env.reason, env.moderated, len(env.marked))
	}
	if env.gotKind != refMUCModeration {
		t.Errorf("lookup kind = %d", env.gotKind)
	}
	// PostHandle is not called by the framework after a pre-handler result, and must not re-record.
	evt.PostHandle(context.Background(), nil)
	if len(env.marked) != 1 {
		t.Error("tombstone recorded by post-handler after the reason path")
	}

	// A failed redaction: failure result, no tombstone.
	env = &fakeRetractEnv{cands: []*database.Message{victim}, pf: pf, removeRes: bridgev2.EventHandlingResultFailed.WithError(errors.New("boom"))}
	res = newRetractionEvent(env, room, moderation, time.Time{}).PreHandleWithResult(context.Background(), nil)
	if res == nil || res.Success || res.Error == nil || len(env.marked) != 0 {
		t.Errorf("failed reason path: %+v marked=%d", res, len(env.marked))
	}

	// No reason: framework path (nil result), moderation flag irrelevant.
	moderation.info.Reason, moderation.info.HasReason = "", false
	env = &fakeRetractEnv{cands: []*database.Message{victim}, pf: pf}
	if res := newRetractionEvent(env, room, moderation, time.Time{}).PreHandleWithResult(context.Background(), nil); res != nil || env.removed != 0 {
		t.Errorf("no reason: result=%+v removed=%d", res, env.removed)
	}
}

// ---- redactions with a reason ----

type fakeIntent struct {
	bridgev2.MatrixAPI
	name string
	err  error
	sent *[]string
	last *event.RedactionEventContent
}

func (f *fakeIntent) SendMessage(_ context.Context, _ id.RoomID, typ event.Type, c *event.Content, _ *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	if typ != event.EventRedaction {
		return nil, errors.New("not a redaction")
	}
	if f.err != nil {
		return nil, f.err
	}
	rc := c.Parsed.(*event.RedactionEventContent)
	f.last = rc
	*f.sent = append(*f.sent, fmt.Sprintf("%s:%s:%s", f.name, rc.Redacts, rc.Reason))
	return &mautrix.RespSendEvent{}, nil
}

func TestRedactPartsWithReason(t *testing.T) {
	var sent []string
	bot := &fakeIntent{name: "bot", sent: &sent}
	ghost := &fakeIntent{name: "ghost", sent: &sent}
	p1 := &database.Message{ID: "m1", PartID: "", MXID: "$e1"}
	p2 := &database.Message{ID: "m1", PartID: "2", MXID: "$e2"}
	fake := &database.Message{ID: "m1", PartID: "3", MXID: "$fake"}
	fake.MXID = id.EventID("~fake:test") // HasFakeMXID
	order := func(first, second bridgev2.MatrixAPI) func(*database.Message) []bridgev2.MatrixAPI {
		return func(*database.Message) []bridgev2.MatrixAPI { return []bridgev2.MatrixAPI{first, second} }
	}
	if err := redactPartsWithReason(context.Background(), "!r:hs", []*database.Message{p1, fake, p2}, order(bot, ghost), "off topic", time.Time{}); err != nil {
		t.Fatal(err)
	}
	want := []string{"bot:$e1:off topic", "bot:$e2:off topic"}
	if fmt.Sprint(sent) != fmt.Sprint(want) {
		t.Errorf("sent = %v, want %v (fake mxid must be skipped)", sent, want)
	}
	// First intent may not redact: fall back to the next one.
	sent = nil
	failing := &fakeIntent{name: "bot", err: errors.New("M_FORBIDDEN"), sent: &sent}
	if err := redactPartsWithReason(context.Background(), "!r:hs", []*database.Message{p1}, order(failing, ghost), "r", time.Time{}); err != nil || len(sent) != 1 || sent[0] != "ghost:$e1:r" {
		t.Errorf("fallback: err=%v sent=%v", err, sent)
	}
	// Nothing can redact: error.
	if err := redactPartsWithReason(context.Background(), "!r:hs", []*database.Message{p1}, order(failing, failing), "r", time.Time{}); err == nil {
		t.Error("expected an error when no intent can redact")
	}
}

// ---- tombstone record ----

func TestRetractedKeyFormat(t *testing.T) {
	if got := retractedKey("me@example.com", "Room@MUC.example.com", "snd=room%2Fbob", "S1"); got != "jabber_retracted:me@example.com:room@muc.example.com:snd=room%2Fbob:S1" {
		t.Errorf("key = %q", got)
	}
	if got := senderScopes("Room@MUC.example.com/Bob:x", "OCC 1"); fmt.Sprint(got) != "[snd=room%40muc.example.com%2FBob%3Ax occ=OCC+1]" {
		t.Errorf("scopes = %v", got)
	}
	// MUC nicks are case-sensitive: "Bob" and "bob" are different occupants and must not share tombstones.
	if a, b := senderScopes("room@muc.example.com/Bob", ""), senderScopes("ROOM@muc.example.com/bob", ""); a[0] == b[0] {
		t.Errorf("nicks differing in case share scope %q", a[0])
	}
	if a, b := senderScopes("Alice@Example.com", ""), senderScopes("alice@example.com", ""); a[0] != b[0] {
		t.Errorf("bare JIDs differing in case: %q vs %q", a[0], b[0])
	}
}

func TestRetractedRefsOf(t *testing.T) {
	got := retractedRefsOf([]*database.Message{
		row(mucPortal(), "m1", "Room@muc.example.com/Bob", &MessageMetadata{XMPPMessageID: "m1", StanzaID: "S1", OriginID: "O1", OccupantID: "OCC"}),
		row(mucPortal(), "m1", "Room@muc.example.com/Bob", &MessageMetadata{StanzaID: "S1", OccupantID: "OCC"}), // second part, duplicates collapse
		row(mucPortal(), "matrix:$e", "x", &MessageMetadata{XMPPMessageID: "gen"}),
		row(mucPortal(), "old", "x", nil),
	})
	var keys []string
	for _, r := range got {
		keys = append(keys, r.Scope+"|"+r.Ref)
	}
	want := "[snd=room%40muc.example.com%2FBob|m1 occ=OCC|m1 snd=room%40muc.example.com%2FBob|S1 occ=OCC|S1 snd=room%40muc.example.com%2FBob|O1 occ=OCC|O1 stanza|S1 snd=x|gen snd=x|old]"
	if fmt.Sprint(keys) != want {
		t.Errorf("refs = %v\nwant %s (matrix: ids are not XMPP refs, aliases are)", keys, want)
	}
}

func newDBClient(t *testing.T) (*xmppClient, *database.Database) {
	t.Helper()
	db := realSchemaDB(t)
	br := &bridgev2.Bridge{DB: db}
	return &xmppClient{loginID: retractTestLogin, log: zerolog.Nop(), conn: &JabberConnector{br: br}}, db
}

func TestRetractedRecordRealKV(t *testing.T) {
	ctx := context.Background()
	x, db := newDBClient(t)
	bob := retractedSender{senderID: "room@muc.example.com/bob", occupantID: "OCC-BOB"}
	if x.isRetracted(ctx, "room@muc.example.com", bob, "S1", "") {
		t.Fatal("nothing recorded yet")
	}
	victim := row(mucPortal(), "m1", "room@muc.example.com/bob", &MessageMetadata{StanzaID: "S1", OccupantID: "OCC-BOB"})
	markRetractedRefs(ctx, db.KV, retractTestLogin, "room@muc.example.com", retractedRefsOf([]*database.Message{victim}))
	if !x.isRetracted(ctx, "ROOM@muc.example.com", bob, "nope", "m1") {
		t.Error("own id not found (portal id is case-insensitive)")
	}
	// Same occupant under another nick (occupant scope), same nick without occupant-id (sender scope).
	if !x.isRetracted(ctx, "room@muc.example.com", retractedSender{senderID: "room@muc.example.com/renamed", occupantID: "OCC-BOB"}, "m1") {
		t.Error("occupant-id scope not matched")
	}
	if !x.isRetracted(ctx, "room@muc.example.com", retractedSender{senderID: "Room@muc.example.com/bob"}, "m1") {
		t.Error("sender scope not matched (bare part is case-insensitive)")
	}
	if x.isRetracted(ctx, "room@muc.example.com", retractedSender{senderID: "room@muc.example.com/BOB"}, "m1") {
		t.Error("nick differing in case matched the sender scope")
	}
	// Another occupant using the same id attribute or victim's stanza-id as its id is not suppressed.
	mallory := retractedSender{senderID: "room@muc.example.com/mallory", occupantID: "OCC-M", stanzaID: "S-MAL"}
	if x.isRetracted(ctx, "room@muc.example.com", mallory, "m1", "S1") {
		t.Error("tombstone leaked to another sender")
	}
	// The room-assigned stanza-id is authoritative regardless of sender.
	if !x.isRetracted(ctx, "room@muc.example.com", retractedSender{senderID: "room@muc.example.com/other", stanzaID: "S1"}, "zzz") {
		t.Error("stanza-id tombstone not authoritative")
	}
	if x.isRetracted(ctx, "other@muc.example.com", bob, "m1", "S1") {
		t.Error("record leaked into another portal")
	}
	if (&xmppClient{}).isRetracted(ctx, "a", retractedSender{}, "b") {
		t.Error("client without a bridge must not report retractions")
	}
}

func TestConvertMessageIgnoresRetractedReplay(t *testing.T) {
	ctx := context.Background()
	x, db := newDBClient(t)
	const alice = "room@muc.example.com/alice"
	victim := row(mucPortal(), "id-retracted", alice, &MessageMetadata{StanzaID: "S-RETRACTED", OccupantID: "OCC-A"})
	markRetractedRefs(ctx, db.KV, retractTestLogin, "room@muc.example.com", retractedRefsOf([]*database.Message{victim}))
	mk := func(sender, occ, id, stanza string) *jabberRemoteMessage {
		return &jabberRemoteMessage{portalKey: mucPortal(), sender: bridgev2.EventSender{Sender: networkid.UserID(sender)}, occupantID: occ, id: networkid.MessageID(id), stanzaID: stanza, body: "again", x: x}
	}
	if _, err := mk(alice, "OCC-A", "fresh-id", "S-RETRACTED").ConvertMessage(ctx, nil, nil); !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Errorf("replay by stanza-id: err = %v", err)
	}
	if _, err := mk(alice, "OCC-A", "id-retracted", "").ConvertMessage(ctx, nil, nil); !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Errorf("replay by id attr: err = %v", err)
	}
	if cm, err := mk(alice, "OCC-A", "fresh-id", "S-NEW").ConvertMessage(ctx, nil, nil); err != nil || cm == nil || len(cm.Parts) != 1 {
		t.Errorf("unrelated message must convert: %v %v", cm, err)
	}
	// Spoof: another occupant picks the victim's stanza-id or id attribute as its own message id.
	for _, id := range []string{"S-RETRACTED", "id-retracted"} {
		if cm, err := mk("room@muc.example.com/mallory", "OCC-M", id, "S-MALLORY").ConvertMessage(ctx, nil, nil); err != nil || cm == nil {
			t.Errorf("message with spoofed id %s was suppressed: %v %v", id, cm, err)
		}
	}
}

// A MAM original hidden by an in-batch retraction is tombstoned so a later page cannot resurrect it.
func TestMAMHiddenOriginalIsTombstoned(t *testing.T) {
	ctx := context.Background()
	x, _ := newDBClient(t)
	orig := mamResultItem{From: "Bob@example.com/p", Type: "chat", StanzaID: "ORIG", ArchiveID: "A1", Body: "hi"}
	retr := mamResultItem{From: "bob@example.com/q", Type: "chat", StanzaID: "R1", Retract: &retractInfo{Target: "ORIG"}}
	b := collectBatchRetractions([]mamResultItem{orig, retr})
	if !x.mamItemHidden(ctx, dmPortal(), orig, b) {
		t.Fatal("original not hidden by the batch")
	}
	// A later page without the retraction.
	if !x.mamItemHidden(ctx, dmPortal(), orig, nil) {
		t.Error("original resurrected on a later page")
	}
	// Same id from somebody else is unaffected.
	other := mamResultItem{From: "carol@example.com/p", Type: "chat", StanzaID: "ORIG", Body: "hi"}
	if x.mamItemHidden(ctx, dmPortal(), other, nil) {
		t.Error("tombstone leaked to another sender")
	}
}

// ---- end to end with the real schema: security cases ----

func TestRetractionPreHandlerRealSchemaSpoofing(t *testing.T) {
	ctx := context.Background()
	x, db := newDBClient(t)
	roomKey, bobKey := mucPortal(), dmPortal()
	for _, p := range []networkid.PortalKey{roomKey, bobKey} {
		rt := database.RoomTypeDM
		if p == roomKey {
			rt = database.RoomTypeGroupDM
		}
		if err := db.Portal.Insert(ctx, &database.Portal{PortalKey: p, MXID: id.RoomID("!" + string(p.ID)), RoomType: rt}); err != nil {
			t.Fatal(err)
		}
	}
	for _, g := range []networkid.UserID{"room@muc.example.com/alice", "room@muc.example.com/mallory", "bob@example.com", "other@example.com"} {
		if err := db.Ghost.Insert(ctx, &database.Ghost{ID: g}); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	insert := func(p networkid.PortalKey, msgID, sender string, meta *MessageMetadata) {
		n++
		if err := db.Message.Insert(ctx, &database.Message{
			ID: networkid.MessageID(msgID), MXID: id.EventID(fmt.Sprintf("$e%d", n)), Room: p,
			SenderID: networkid.UserID(sender), Timestamp: time.Unix(1_700_000_000+int64(n), 0), Metadata: meta,
		}); err != nil {
			t.Fatal(err)
		}
	}
	insert(roomKey, "victim-id", "room@muc.example.com/alice", &MessageMetadata{XMPPMessageID: "victim-id", StanzaID: "S-VICTIM", OccupantID: "OCC-ALICE"})
	insert(roomKey, "S-VICTIM", "room@muc.example.com/mallory", &MessageMetadata{XMPPMessageID: "S-VICTIM", StanzaID: "S-MALLORY", OccupantID: "OCC-MALLORY"}) // id attr chosen = victim stanza-id
	insert(bobKey, "bobs-msg", "bob@example.com", nil)
	insert(networkid.PortalKey{ID: "other@example.com", Receiver: retractTestLogin}, "other-msg", "other@example.com", nil)

	portalOf := func(k networkid.PortalKey) *bridgev2.Portal {
		rt := database.RoomTypeDM
		if k == roomKey {
			rt = database.RoomTypeGroupDM
		}
		return &bridgev2.Portal{Portal: &database.Portal{PortalKey: k, RoomType: rt}}
	}
	run := func(k networkid.PortalKey, r retraction) (*simplevent.MessageRemove, *bridgev2.EventHandlingResult) {
		evt := newRetractionEvent(xmppRetractionEnv{x}, k, r, time.Time{})
		return evt, evt.PreHandleWithResult(ctx, portalOf(k))
	}
	roomMod := func(from, target string, groupchat bool) retraction {
		return retraction{info: retractInfo{Target: target, Moderated: true}, channel: retractChanLive, groupchat: groupchat, msgType: "groupchat", fromFull: from}
	}

	// Moderation from the room: the stanza-id alias wins over the occupant-chosen id attribute.
	evt, res := run(roomKey, roomMod("room@muc.example.com", "S-VICTIM", true))
	if res != nil || evt.GetTargetMessage() != "victim-id" {
		t.Errorf("moderation resolved to %q (res %+v), want victim-id", evt.GetTargetMessage(), res)
	}
	// Moderation by an occupant, or as type=chat: ignored.
	for name, r := range map[string]retraction{
		"occupant":     roomMod("room@muc.example.com/mallory", "S-VICTIM", true),
		"type=chat":    roomMod("room@muc.example.com", "S-VICTIM", false),
		"wrong room":   roomMod("evil@muc.example.com", "S-VICTIM", true),
		"no such room": roomMod("room@muc.example.com", "S-NOPE", true),
	} {
		if _, res := run(roomKey, r); res == nil || !res.Ignored {
			t.Errorf("moderation %s: result %+v, want Ignored", name, res)
		}
	}
	// Self-retraction with both occupant-ids present but different, same nick: rejected.
	self := func(from, occ, target string) retraction {
		return retraction{info: retractInfo{Target: target}, channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: from, occupantID: occ}
	}
	if _, res := run(roomKey, self("room@muc.example.com/alice", "OCC-EVIL", "S-VICTIM")); res == nil || !res.Ignored {
		t.Errorf("nick thief: %+v", res)
	}
	if evt, res := run(roomKey, self("room@muc.example.com/alice", "OCC-ALICE", "S-VICTIM")); res != nil || evt.GetTargetMessage() != "victim-id" {
		t.Errorf("legitimate self retraction: %q %+v", evt.GetTargetMessage(), res)
	}
	// Mallory cannot retract Alice's message by naming it with her own occupant-id.
	if _, res := run(roomKey, self("room@muc.example.com/mallory", "OCC-MALLORY", "S-VICTIM")); res == nil || res.Ignored == false {
		// S-VICTIM resolves (stanza_id first) to victim-id, whose occupant-id differs: rejected.
		t.Errorf("mallory retracting alice's message: %+v", res)
	}
	// type=chat retraction from room/nick into the MUC portal (private message path): rejected.
	pm := retraction{info: retractInfo{Target: "victim-id"}, channel: retractChanLive, msgType: "chat", fromFull: "room@muc.example.com/alice"}
	if _, res := run(roomKey, pm); res == nil || !res.Ignored {
		t.Errorf("dm-style retraction in muc portal: %+v", res)
	}
	// A DM contact naming another portal's message id: ignored (room scoping).
	cross := retraction{info: retractInfo{Target: "other-msg"}, channel: retractChanLive, msgType: "chat", fromFull: "other@example.com/p"}
	if _, res := run(bobKey, cross); res == nil || !res.Ignored {
		t.Errorf("cross-portal retraction: %+v", res)
	}
	// ... while a legitimate DM retraction resolves.
	ok := retraction{info: retractInfo{Target: "bobs-msg"}, channel: retractChanLive, msgType: "chat", fromFull: "bob@example.com/p"}
	if evt, res := run(bobKey, ok); res != nil || evt.GetTargetMessage() != "bobs-msg" {
		t.Errorf("legitimate dm retraction: %q %+v", evt.GetTargetMessage(), res)
	}
}
