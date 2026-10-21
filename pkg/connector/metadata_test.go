package connector

import (
	"encoding/json"
	"testing"

	"maunium.net/go/mautrix/bridgev2/database"
)

var _ database.MetaMerger = (*JabberLoginMeta)(nil)

func TestJabberLoginMetaCopyFrom(t *testing.T) {
	oldExtra := map[string]json.RawMessage{"sip": json.RawMessage(`{"u":1}`), "call_prefs": json.RawMessage(`{"roster_only":true}`)}
	m := &JabberLoginMeta{Password: "old", DefaultMUCNick: "nick", Server: "s", Port: 5222, Extra: oldExtra}

	m.CopyFrom(&JabberLoginMeta{Password: "new"})
	if m.Password != "new" {
		t.Fatalf("password %q", m.Password)
	}
	if m.DefaultMUCNick != "nick" || m.Server != "s" || m.Port != 5222 {
		t.Fatalf("existing fields lost: %+v", m)
	}
	if len(m.Extra) != 2 || string(m.Extra["call_prefs"]) != `{"roster_only":true}` {
		t.Fatalf("extra lost: %v", m.Extra)
	}

	// Incoming values override; Extra keys merge copy-on-write.
	m.CopyFrom(&JabberLoginMeta{Password: "p3", DefaultMUCNick: "n2", Extra: map[string]json.RawMessage{"x": json.RawMessage(`1`), "sip": json.RawMessage(`{"u":2}`)}})
	if m.DefaultMUCNick != "n2" || m.Port != 5222 || len(m.Extra) != 3 || string(m.Extra["sip"]) != `{"u":2}` {
		t.Fatalf("merge: %+v", m)
	}
	if string(oldExtra["sip"]) != `{"u":1}` || len(oldExtra) != 2 {
		t.Fatalf("old Extra map was mutated: %v", oldExtra)
	}

	// Nil Extra on both sides stays nil; wrong types and nil are ignored.
	n := &JabberLoginMeta{Password: "a"}
	n.CopyFrom(&JabberLoginMeta{Password: "b"})
	if n.Extra != nil || n.Password != "b" {
		t.Fatalf("%+v", n)
	}
	n.CopyFrom("junk")
	n.CopyFrom((*JabberLoginMeta)(nil))
	n.CopyFrom(n)
	if n.Password != "b" {
		t.Fatal("ignored input changed meta")
	}
}

func TestCopyFromServerExplicitReplacesAndClears(t *testing.T) {
	m := &JabberLoginMeta{Password: "a", Server: "old.example.com", Port: 5222}
	m.CopyFrom(&JabberLoginMeta{Password: "b", Server: "new.example.com", Port: 5223, DirectTLS: true, serverExplicit: true})
	if m.Server != "new.example.com" || m.Port != 5223 || !m.DirectTLS {
		t.Fatalf("override not replaced: %+v", m)
	}
	m.CopyFrom(&JabberLoginMeta{Password: "c", serverExplicit: true}) // "auto"
	if m.Server != "" || m.Port != 0 || m.DirectTLS {
		t.Fatalf("override not cleared: %+v", m)
	}
}

func TestCopyFromPlainFlowKeepsOverride(t *testing.T) {
	m := &JabberLoginMeta{Password: "a", Server: "s.example.com", Port: 5223, DirectTLS: true}
	m.CopyFrom(&JabberLoginMeta{Password: "b"})
	if m.Password != "b" || m.Server != "s.example.com" || m.Port != 5223 || !m.DirectTLS {
		t.Fatalf("override lost: %+v", m)
	}
}

func TestMetaDialOptions(t *testing.T) {
	m := &JabberLoginMeta{Server: "s.example.com", Port: 5223, DirectTLS: true}
	if got := m.dialOptions(); got != (dialOptions{Server: "s.example.com", Port: 5223, DirectTLS: true}) {
		t.Fatalf("%+v", got)
	}
	if (*JabberLoginMeta)(nil).dialOptions() != (dialOptions{}) {
		t.Fatal("nil meta")
	}
}
