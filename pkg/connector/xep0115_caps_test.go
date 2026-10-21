package connector

import (
	"crypto/sha1"
	"encoding/xml"
	"sort"
	"testing"

	"mellium.im/xmpp/disco"
	"mellium.im/xmpp/disco/info"
)

// XEP-0115 section 5.2 simple generation example.
func TestCapsHashXEP0115KnownVector(t *testing.T) {
	i := disco.Info{
		Identity: []info.Identity{{Category: "client", Type: "pc", Name: "Exodus 0.9.1"}},
		Features: []info.Feature{
			{Var: "http://jabber.org/protocol/disco#info"},
			{Var: "http://jabber.org/protocol/disco#items"},
			{Var: "http://jabber.org/protocol/muc"},
			{Var: "http://jabber.org/protocol/caps"},
		},
	}
	if got, want := i.Hash(sha1.New()), "QgayPKawpkPSDYmwT/WM94uAlu0="; got != want {
		t.Fatalf("ver = %q, want %q", got, want)
	}
}

func TestBridgeCapsFeatureList(t *testing.T) {
	want := []string{
		"http://jabber.org/protocol/disco#info",
		"jabber:x:oob",
		"urn:xmpp:bookmarks:1+notify",
		"urn:xmpp:fallback:0",
		"urn:xmpp:message-retract:0",
		"urn:xmpp:message-retract:1",
		"urn:xmpp:reactions:0",
		"urn:xmpp:reply:0",
		"urn:xmpp:sid:0",
	}
	got := append([]string(nil), bridgeFeatureVars...)
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("features = %v", got)
	}
	for i := range want {
		if got[i] != want[i] || bridgeFeatureVars[i] != want[i] {
			t.Fatalf("features = %v, want %v (sorted)", bridgeFeatureVars, want)
		}
	}
	i := newBridgeDiscoInfo("")
	if len(i.Identity) != 1 || i.Identity[0].Category != "client" || i.Identity[0].Type != "bot" || i.Identity[0].Name != "jmpchat-matrix-bridge" {
		t.Fatalf("identity = %+v", i.Identity)
	}
}

func TestBridgeCapsVerDeterministicAndSensitive(t *testing.T) {
	v1, v2 := bridgeCapsVer(), bridgeCapsVer()
	if v1 == "" || v1 != v2 {
		t.Fatalf("ver not deterministic: %q %q", v1, v2)
	}
	// Hashing must not disturb a later hash (Info.Hash sorts in place).
	if c := bridgeCaps(); c.Ver != v1 || c.Node != bridgeCapsNode {
		t.Fatalf("caps = %+v", c)
	}
	i := newBridgeDiscoInfo("")
	i.Features = append(i.Features, info.Feature{Var: "urn:xmpp:extra:0"})
	if i.Hash(sha1.New()) == v1 {
		t.Error("ver must change when features change")
	}
	j := newBridgeDiscoInfo("")
	j.Identity[0].Name = "other"
	if j.Hash(sha1.New()) == v1 {
		t.Error("ver must change when the identity changes")
	}
	t.Logf("bridge caps ver = %s", v1)
}

func TestBridgeCapsElement(t *testing.T) {
	var buf []xml.Token
	r := bridgeCaps().TokenReader()
	for {
		tok, err := r.Token()
		if tok != nil {
			buf = append(buf, xml.CopyToken(tok))
		}
		if err != nil {
			break
		}
	}
	if len(buf) != 2 {
		t.Fatalf("tokens = %v", buf)
	}
	se := buf[0].(xml.StartElement)
	attrs := map[string]string{}
	for _, a := range se.Attr {
		attrs[a.Name.Local] = a.Value
	}
	if se.Name.Space != "http://jabber.org/protocol/caps" || se.Name.Local != "c" ||
		attrs["hash"] != "sha-1" || attrs["node"] != bridgeCapsNode || attrs["ver"] != bridgeCapsVer() {
		t.Fatalf("c element = %+v", se)
	}
}
