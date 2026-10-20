package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

func parseIDs(t *testing.T, stanza string) messageIDs {
	t.Helper()
	r, _ := startOfMessage(t, stanza)
	_, _, _, _, _, _, ids, err := parseMessageBodyOOBIDs(r)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return ids
}

func TestParseStableIDs(t *testing.T) {
	ids := parseIDs(t, `<message from="room@muc.example.com/bob" type="groupchat" id="c1"><body>hi</body>
<stanza-id xmlns="urn:xmpp:sid:0" by="room@muc.example.com" id="S-ROOM"/>
<stanza-id xmlns="urn:xmpp:sid:0" by="evil@example.com" id="S-EVIL"/>
<origin-id xmlns="urn:xmpp:sid:0" id="O1"/>
<occupant-id xmlns="urn:xmpp:occupant-id:0" id="OCC1"/>
<x xmlns="urn:test"><origin-id xmlns="urn:xmpp:sid:0" id="NESTED"/><occupant-id xmlns="urn:xmpp:occupant-id:0" id="NESTEDOCC"/></x>
<stanza-id xmlns="urn:other" by="room@muc.example.com" id="WRONGNS"/>
</message>`)
	if got := ids.stanzaIDBy("room@muc.example.com"); got != "S-ROOM" {
		t.Errorf("room stanza-id = %q", got)
	}
	if got := ids.stanzaIDBy("ROOM@muc.example.com/x"); got != "S-ROOM" {
		t.Errorf("bare/case-insensitive match = %q", got)
	}
	if got := ids.stanzaIDBy("me@example.com"); got != "" {
		t.Errorf("untrusted by must yield nothing, got %q", got)
	}
	if ids.OriginID != "O1" || ids.OccupantID != "OCC1" {
		t.Errorf("origin/occupant = %q/%q (nested or wrong-namespace elements must not count)", ids.OriginID, ids.OccupantID)
	}
}

// Elements that inherit their namespace (no literal xmlns on the child) are still matched.
func TestParseStableIDsInheritedNamespace(t *testing.T) {
	ids := parseIDs(t, `<message xmlns="urn:xmpp:sid:0" from="a@example.com"><stanza-id by="me@example.com" id="S1"/><origin-id id="O1"/></message>`)
	if ids.stanzaIDBy("me@example.com") != "S1" || ids.OriginID != "O1" {
		t.Errorf("inherited namespace not matched: %+v", ids)
	}
}

func TestParseStableIDsBounds(t *testing.T) {
	var b strings.Builder
	b.WriteString(`<message from="a@example.com">`)
	for i := 0; i < 20; i++ {
		b.WriteString(`<stanza-id xmlns="urn:xmpp:sid:0" by="x@example.com" id="s"/>`)
	}
	b.WriteString(`<origin-id xmlns="urn:xmpp:sid:0" id="` + strings.Repeat("a", maxStableIDLen+1) + `"/></message>`)
	ids := parseIDs(t, b.String())
	if len(ids.StanzaIDs) != maxStanzaIDRefs {
		t.Errorf("stanza-id refs = %d, want cap %d", len(ids.StanzaIDs), maxStanzaIDRefs)
	}
	if ids.OriginID != "" {
		t.Error("oversized origin-id must be ignored")
	}
}

func TestCarbonInnerStableIDs(t *testing.T) {
	r, _ := startOfMessage(t, `<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/p" to="bob@example.com" id="m1"><body>x</body><stanza-id xmlns="urn:xmpp:sid:0" by="me@example.com" id="S1"/><origin-id xmlns="urn:xmpp:sid:0" id="O1"/></message></forwarded></sent></message>`)
	// Step past <sent>, as handleMessageWithOOB does.
	if _, err := r.Token(); err != nil {
		t.Fatal(err)
	}
	_, _, _, id, body, _, ids, err := parseInnerMessageFromCarbon(r)
	if err != nil {
		t.Fatal(err)
	}
	if id != "m1" || body != "x" || ids.stanzaIDBy("me@example.com") != "S1" || ids.OriginID != "O1" {
		t.Errorf("id=%q body=%q ids=%+v", id, body, ids)
	}
}

func TestMAMResultStableIDs(t *testing.T) {
	x := newOuterFromTestClient()
	ch := x.mam.register("q1", networkid.PortalKey{ID: "bob@example.com", Receiver: x.loginID}, false)
	r, start := startOfMessage(t, `<message to="me@example.com"><result xmlns="urn:xmpp:mam:2" queryid="q1" id="ARCH1"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat" id="m1"><body>hello</body><origin-id xmlns="urn:xmpp:sid:0" id="O1"/></message></forwarded></result></message>`)
	if err := x.handleMessageWithOOB(r, &start); err != nil {
		t.Fatal(err)
	}
	item := <-ch
	stanzaID, originID := item.stableIDs()
	if stanzaID != "ARCH1" || originID != "O1" || item.StanzaID != "m1" {
		t.Errorf("item = %+v (stanza %q origin %q)", item, stanzaID, originID)
	}
	same := mamResultItem{StanzaID: "m1", OriginID: "m1", ArchiveID: "A"}
	if _, o := same.stableIDs(); o != "" {
		t.Errorf("origin-id equal to id must not be stored, got %q", o)
	}
}

func TestDBMetaAndConvertMessage(t *testing.T) {
	m := &jabberRemoteMessage{
		id: "m1", body: "hello", stanzaID: "S1", originID: "O1", occupantID: "OCC", backfillSource: "mam",
	}
	cm, err := m.ConvertMessage(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("parts = %d", len(cm.Parts))
	}
	meta, ok := cm.Parts[0].DBMetadata.(*MessageMetadata)
	if !ok || *meta != (MessageMetadata{XMPPMessageID: "m1", StanzaID: "S1", OriginID: "O1", OccupantID: "OCC", BackfillSource: "mam"}) {
		t.Errorf("DBMetadata = %#v", cm.Parts[0].DBMetadata)
	}
	// origin-id equal to the id attribute is redundant and dropped.
	m2 := &jabberRemoteMessage{id: "m1", originID: "m1"}
	if got := m2.dbMeta(); got.OriginID != "" || got.XMPPMessageID != "m1" {
		t.Errorf("dbMeta = %+v", got)
	}
}

func TestMessageMetadataJSON(t *testing.T) {
	var old MessageMetadata
	if err := json.Unmarshal([]byte(`{"xmpp_message_id":"abc","backfill_source":"mam"}`), &old); err != nil {
		t.Fatal(err)
	}
	if old != (MessageMetadata{XMPPMessageID: "abc", BackfillSource: "mam"}) {
		t.Errorf("old row = %+v", old)
	}
	b, _ := json.Marshal(MessageMetadata{XMPPMessageID: "a"})
	if string(b) != `{"xmpp_message_id":"a"}` {
		t.Errorf("empty fields must be omitted: %s", b)
	}
	b, _ = json.Marshal(MessageMetadata{StanzaID: "s", OriginID: "o", OccupantID: "c"})
	for _, k := range []string{`"stanza_id":"s"`, `"origin_id":"o"`, `"occupant_id":"c"`} {
		if !strings.Contains(string(b), k) {
			t.Errorf("missing %s in %s", k, b)
		}
	}
}

func TestOriginIDReader(t *testing.T) {
	var buf bytes.Buffer
	enc := xml.NewEncoder(&buf)
	r := originIDReader("abc123")
	for {
		tok, err := r.Token()
		if err != nil {
			break
		}
		if err := enc.EncodeToken(tok); err != nil {
			t.Fatal(err)
		}
	}
	_ = enc.Flush()
	got := buf.String()
	if !strings.Contains(got, `xmlns="urn:xmpp:sid:0"`) || !strings.Contains(got, `id="abc123"`) || !strings.Contains(got, "origin-id") {
		t.Errorf("origin-id encoding = %s", got)
	}
}
