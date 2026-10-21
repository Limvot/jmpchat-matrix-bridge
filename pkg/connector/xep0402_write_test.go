package connector

import (
	"context"
	"errors"
	"strings"
	"testing"

	"mellium.im/xmpp/disco"
	"mellium.im/xmpp/stanza"
)

const bmRoom = "room@groups.example.com"

func TestBookmarkPublishTokens(t *testing.T) {
	c := bookmarkConf{ID: "Room@Groups.Example.com", Autojoin: true, Name: "N", Nick: "alice", Password: "pw"}
	dec, err := decodeBookmarkConf(c.ID, tokenReaderOf(confWithExt))
	if err != nil {
		t.Fatal(err)
	}
	c.ext = dec.ext
	out := encodeTokens(t, &sliceTokenReader{toks: bookmarkPublishTokens(c)})
	for _, want := range []string{
		`<pubsub xmlns="http://jabber.org/protocol/pubsub">`,
		`<publish node="urn:xmpp:bookmarks:1"><item id="Room@Groups.Example.com">`, // stored id, original case
		`<conference xmlns="urn:xmpp:bookmarks:1" autojoin="true" name="N">`,
		`<publish-options><x xmlns="jabber:x:data" type="submit">`,
		`<field var="FORM_TYPE" type="hidden"><value>http://jabber.org/protocol/pubsub#publish-options</value></field>`,
		`<field var="pubsub#persist_items"><value>true</value></field>`,
		`<field var="pubsub#max_items"><value>max</value></field>`,
		`<field var="pubsub#send_last_published_item"><value>never</value></field>`,
		`<field var="pubsub#access_model"><value>whitelist</value></field>`,
		`urn:gajim:group`, // extensions survive
	} {
		if !strings.Contains(out, want) {
			t.Errorf("publish payload lacks %q:\n%s", want, out)
		}
	}
	// Well-formed with no duplicate attributes (the stream-killing bug of the upstream codec).
	if err := wellFormed(out); err != nil {
		t.Fatalf("not well-formed: %v\n%s", err, out)
	}
}

func TestPublishErrorClassification(t *testing.T) {
	for _, c := range []stanza.Condition{stanza.Conflict, stanza.FeatureNotImplemented, stanza.BadRequest, stanza.Forbidden, stanza.NotAllowed} {
		if !publishErrorDisablesWrites(stanza.Error{Condition: c}) {
			t.Errorf("%s must disable writes", c)
		}
	}
	for _, err := range []error{errors.New("timeout"), context.DeadlineExceeded, stanza.Error{Condition: stanza.ItemNotFound}, nil} {
		if publishErrorDisablesWrites(err) {
			t.Errorf("%v must not disable writes", err)
		}
	}
}

func TestPublishBookmarkMergesIntoServerItem(t *testing.T) {
	existing, _ := decodeBookmarkConf("Room@Groups.Example.com", tokenReaderOf(confWithExt))
	existing.Autojoin = false
	existing.Nick = "gajim-nick"
	r := newBMRig(t, true, existing)
	out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "bridge-nick", true)
	if out != bmWritten {
		t.Fatalf("outcome = %v", out)
	}
	p := r.published()
	if len(p) != 1 {
		t.Fatalf("publishes = %d", len(p))
	}
	got := p[0]
	if got.ID != "Room@Groups.Example.com" || !got.Autojoin || got.Nick != "gajim-nick" || got.Name != "Room One" || got.Password != "s3cret" || len(got.ext) != len(existing.ext) {
		t.Fatalf("merged = %+v", got)
	}
	// Second identical request: the cache says nothing would change, so no IQ at all.
	fetches := r.fetchCount()
	if out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "bridge-nick", true); out != bmNoop {
		t.Fatalf("repeat outcome = %v", out)
	}
	if r.fetchCount() != fetches || len(r.published()) != 1 {
		t.Fatal("no-op write did IQs")
	}
}

func TestPublishBookmarkCreatesWithLocalpartNameAndNick(t *testing.T) {
	r := newBMRig(t, true)
	if out := r.x.publishBookmarkFor(context.Background(), "ROOM@groups.example.com", true, "alice", true); out != bmWritten {
		t.Fatalf("outcome = %v", out)
	}
	c := r.published()[0]
	if c.ID != "room@groups.example.com" || c.Name != "room" || c.Nick != "alice" || !c.Autojoin {
		t.Fatalf("created = %+v", c)
	}
}

func TestPublishBookmarkGateAndFailures(t *testing.T) {
	// Read-only server (no publish-options): never writes.
	r := newBMRig(t, false, bm(bmRoom, false, "n"))
	if out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "", true); out != bmSkipped || len(r.published()) != 0 || r.fetchCount() != 0 {
		t.Fatalf("read-only: out=%v publishes=%d fetches=%d", out, len(r.published()), r.fetchCount())
	}
	// Fetch failure: no write (R1).
	r = newBMRig(t, true, bm(bmRoom, false, "n"))
	r.fetchErr = errors.New("boom")
	if out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "", true); out != bmFailed || len(r.published()) != 0 {
		t.Fatalf("fetch failure: out=%v", out)
	}
	// Non-private node (conflict): exactly one attempt, writes disabled, never a retry without options.
	r = newBMRig(t, true, bm(bmRoom, false, "n"))
	attempts := 0
	r.x.bm.publishHook = func(context.Context, bookmarkConf) error {
		attempts++
		return stanza.Error{Type: stanza.Cancel, Condition: stanza.Conflict}
	}
	if out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "", true); out != bmFailed || attempts != 1 {
		t.Fatalf("conflict: out=%v attempts=%d", out, attempts)
	}
	if out := r.x.publishBookmarkFor(context.Background(), "other@groups.example.com", true, "", true); out != bmSkipped || attempts != 1 {
		t.Fatalf("after disable: out=%v attempts=%d", out, attempts)
	}
	// A transient error does not disable writes.
	r = newBMRig(t, true, bm(bmRoom, false, "n"))
	r.publishErr = errors.New("timeout")
	r.x.publishBookmarkFor(context.Background(), bmRoom, true, "", true)
	r.publishErr = nil
	if out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "", true); out != bmWritten {
		t.Fatalf("after transient error: %v", out)
	}
	// Gate IQ error: unknown, nothing written, a later call retries.
	r = newBMRig(t, true, bm(bmRoom, false, "n"))
	calls := 0
	r.x.bm.discoHook = func(context.Context) (disco.Info, error) {
		calls++
		if calls == 1 {
			return disco.Info{}, errors.New("timeout")
		}
		return pepDisco(true), nil
	}
	if out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "", true); out != bmSkipped {
		t.Fatalf("gate error: %v", out)
	}
	if out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "", true); out != bmWritten {
		t.Fatalf("retry after gate error: %v", out)
	}
}

func TestLeavePublishesAutojoinFalseKeepsFieldsNeverCreates(t *testing.T) {
	existing, _ := decodeBookmarkConf(bmRoom, tokenReaderOf(confWithExt))
	r := newBMRig(t, true, existing)
	if err := r.x.LeaveMUC(context.Background(), bmRoom); err != nil {
		t.Fatal(err)
	}
	p := r.published()
	if len(p) != 1 || p[0].Autojoin || p[0].Nick != "alice" || p[0].Password != "s3cret" || p[0].Name != "Room One" || len(p[0].ext) == 0 {
		t.Fatalf("leave publish = %+v", p)
	}
	if !r.left[bmRoom] {
		t.Fatal("MUCLeft not set")
	}
	// No bookmark: nothing is created.
	r = newBMRig(t, true)
	if err := r.x.LeaveMUC(context.Background(), bmRoom); err != nil || len(r.published()) != 0 {
		t.Fatalf("err=%v publishes=%d", err, len(r.published()))
	}
	// A failing publish does not fail the leave and the flag is still set.
	r = newBMRig(t, true, bm(bmRoom, true, "n"))
	r.publishErr = stanza.Error{Condition: stanza.Forbidden}
	if err := r.x.LeaveMUC(context.Background(), bmRoom); err != nil || !r.left[bmRoom] {
		t.Fatalf("err=%v left=%v", err, r.left)
	}
}

func confirmJoin(t *testing.T, r *bmRig, room, nick string) {
	t.Helper()
	presenceStanza(t, r.x, "", room+"/"+nick, selfBody)
}

func TestOnlyJoinCommandWritesBookmark(t *testing.T) {
	// Command path: confirmed join writes the bookmark with the confirmed nick.
	r := newBMRig(t, true)
	if err := r.x.JoinMUC(context.Background(), bmRoom); err != nil {
		t.Fatal(err)
	}
	confirmJoin(t, r, bmRoom, "alice")
	bmWaitFor(t, "bookmark write", func() bool { return len(r.published()) == 1 })
	if c := r.published()[0]; !c.Autojoin || c.Nick != "alice" {
		t.Fatalf("published = %+v", c)
	}

	// Send/view/retract path: no write.
	r = newBMRig(t, true)
	r.addPortal(bmRoom, false)
	if err := r.x.joinMUCForPortal(context.Background(), bmRoom); err != nil {
		t.Fatal(err)
	}
	confirmJoin(t, r, bmRoom, "alice")
	// Auto-rejoin/import/event path: no write.
	if err := r.x.joinMUC(context.Background(), bmRoom, nil); err != nil {
		t.Fatal(err)
	}
	confirmJoin(t, r, bmRoom, "alice")
	r.x.mucPendingJoinMu.Lock() // let any stray goroutine run before asserting
	r.x.mucPendingJoinMu.Unlock()
	bmWaitFor(t, "joins", func() bool { return len(r.joins()) == 2 })
	if n := len(r.published()) + r.fetchCount(); n != 0 {
		t.Fatalf("non-command joins touched bookmarks: publishes+fetches=%d", n)
	}
}

func TestJoinWith409AlternativeDoesNotWriteNick(t *testing.T) {
	r := newBMRig(t, true)
	if err := r.x.JoinMUC(context.Background(), bmRoom); err != nil {
		t.Fatal(err)
	}
	presenceStanza(t, r.x, "error", bmRoom+"/alice", conflictBody)
	bmWaitFor(t, "retry join", func() bool { return len(r.joins()) == 2 })
	confirmJoin(t, r, bmRoom, "alice_1")
	bmWaitFor(t, "bookmark write", func() bool { return len(r.published()) == 1 })
	if c := r.published()[0]; !c.Autojoin || c.Nick != "" {
		t.Fatalf("alternative nick must stay bridge-local: %+v", c)
	}
}

func TestJoinForPortalSkipsLeftRoom(t *testing.T) {
	r := newBMRig(t, true)
	r.addPortal(bmRoom, true)
	if err := r.x.joinMUCForPortal(context.Background(), bmRoom); !errors.Is(err, errMUCLeft) {
		t.Fatalf("err = %v", err)
	}
	if len(r.joins()) != 0 {
		t.Fatal("joined a left room")
	}
	// The explicit command still brings it back and clears the flag.
	if err := r.x.JoinMUC(context.Background(), bmRoom); err != nil || r.left[bmRoom] {
		t.Fatalf("err=%v left=%v", err, r.left)
	}
}
