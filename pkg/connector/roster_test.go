package connector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/roster"
)

func testRosterItem(t *testing.T, j, name, sub string) roster.Item {
	t.Helper()
	return roster.Item{JID: jid.MustParse(j), Name: name, Subscription: sub}
}

func TestRosterSetBookkeeping(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com"}
	if in, loaded := x.inRoster("a@example.com"); in || loaded {
		t.Fatalf("fresh client: in=%v loaded=%v", in, loaded)
	}

	// A push before the first fetch records the JID but does not mark the roster loaded.
	if err := x.handleRosterPush("", testRosterItem(t, "Pushed@Example.com", "", "both")); err != nil {
		t.Fatal(err)
	}
	if in, loaded := x.inRoster("pushed@example.com"); !in || loaded {
		t.Fatalf("after early push: in=%v loaded=%v", in, loaded)
	}

	// A complete fetch replaces the cache, keeps unnamed entries and drops subscription=remove.
	x.applyRosterItems(true,
		rosterEntry{jid: "named@example.com", name: "Named"},
		rosterEntry{jid: "unnamed@example.com"},
		rosterEntry{jid: "gone@example.com", remove: true},
	)
	for j, want := range map[string]bool{"named@example.com": true, "UNNAMED@example.com": true, "gone@example.com": false, "pushed@example.com": false} {
		if in, loaded := x.inRoster(j); in != want || !loaded {
			t.Errorf("inRoster(%q) = %v,%v want %v,true", j, in, loaded, want)
		}
	}
	if x.lookupRosterName("unnamed@example.com") != "" || x.lookupRosterName("named@example.com") != "Named" {
		t.Fatal("name map wrong")
	}

	// Pushes: rename, drop the name (stays in the roster), remove.
	push := func(j, name, sub string) {
		t.Helper()
		if err := x.handleRosterPush("", testRosterItem(t, j, name, sub)); err != nil {
			t.Fatal(err)
		}
	}
	push("named@example.com", "Renamed", "both")
	if x.lookupRosterName("named@example.com") != "Renamed" {
		t.Fatal("rename not applied")
	}
	push("named@example.com", "", "both")
	if in, _ := x.inRoster("named@example.com"); !in || x.lookupRosterName("named@example.com") != "" {
		t.Fatal("nameless push must drop the name only")
	}
	push("named@example.com", "", "remove")
	if in, _ := x.inRoster("named@example.com"); in {
		t.Fatal("remove push left the JID in the set")
	}

	// SetRosterItem's cache update path (named, then unnamed, keeps JID).
	x.applyRosterItems(false, rosterEntry{jid: "set@example.com", name: "Set"})
	x.applyRosterItems(false, rosterEntry{jid: "set@example.com"})
	if in, _ := x.inRoster("set@example.com"); !in || x.lookupRosterName("set@example.com") != "" {
		t.Fatal("set bookkeeping wrong")
	}
	// An empty replace (synced, empty roster) is loaded and empty.
	x.applyRosterItems(true)
	if n, loaded := x.countRoster(nil); n != 0 || !loaded {
		t.Fatalf("empty replace: %d %v", n, loaded)
	}
}

func TestCountRosterMatch(t *testing.T) {
	x := &xmppClient{}
	x.applyRosterItems(true,
		rosterEntry{jid: "+15550001111@cheogram.com"}, rosterEntry{jid: "bob@example.com", name: "Bob"}, rosterEntry{jid: "+15550002222@cheogram.com", name: "X"})
	n, loaded := x.countRoster(func(j string) bool { return strings.HasSuffix(j, "@cheogram.com") })
	if n != 2 || !loaded {
		t.Fatalf("n=%d loaded=%v", n, loaded)
	}
}

func TestRosterConcurrent(t *testing.T) {
	x := &xmppClient{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for k := 0; k < 200; k++ {
				j := fmt.Sprintf("u%d@example.com", k%7)
				switch i % 4 {
				case 0:
					x.applyRosterItems(true, rosterEntry{jid: j, name: "n"})
				case 1:
					x.applyRosterItems(false, rosterEntry{jid: j, remove: k%2 == 0})
				case 2:
					x.inRoster(j)
				default:
					x.countRoster(nil)
					x.lookupRosterName(j)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestDMDisplayName(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com", conn: &JabberConnector{}}
	x.applyRosterItems(true,
		rosterEntry{jid: "bob@example.com", name: "Bob"},
		rosterEntry{jid: "+15551230001@cheogram.com", name: "Alice"},
		rosterEntry{jid: "unnamed@example.com"},
	)
	for jidStr, want := range map[string]string{
		"bob@example.com":                        "Bob (me@example.com)",
		"unnamed@example.com":                    "unnamed@example.com (me@example.com)",
		"nobody@example.com":                     "nobody@example.com (me@example.com)",
		"+15551230001@cheogram.com":              "Alice (me@example.com)",
		"+15559990000@cheogram.com":              "+15559990000@cheogram.com (me@example.com)",
		"+15551230001,+15551230002@cheogram.com": "Alice, +15551230002@cheogram.com (me@example.com)",
	} {
		if got := x.dmDisplayName(jidStr); got != want {
			t.Errorf("dmDisplayName(%q) = %q, want %q", jidStr, got, want)
		}
		// New portals are named exactly like getDMChatInfo names them.
		resp := x.dmCreateChatResponse(jidStr)
		if resp.PortalInfo.Name == nil || *resp.PortalInfo.Name != want {
			t.Errorf("dmCreateChatResponse(%q) name = %v, want %q", jidStr, resp.PortalInfo.Name, want)
		}
		p := &bridgev2.Portal{Portal: &database.Portal{PortalKey: resp.PortalKey}}
		info, err := x.getDMChatInfo(context.Background(), p, jidStr)
		if err != nil || info.Name == nil || *info.Name != want {
			t.Errorf("getDMChatInfo(%q) = %v, %v; want name %q", jidStr, info, err, want)
		}
	}
}

// RemoveRosterItem's cache update (a removal entry) drops both maps and reverts the room name.
func TestRosterRemoveRevertsNames(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com", conn: &JabberConnector{}}
	x.applyRosterItems(true, rosterEntry{jid: "bob@example.com", name: "Bob"}, rosterEntry{jid: "keep@example.com", name: "Keep"})
	if got := x.dmDisplayName("bob@example.com"); got != "Bob (me@example.com)" {
		t.Fatalf("before: %q", got)
	}
	x.applyRosterItems(false, rosterEntry{jid: "bob@example.com", remove: true})
	if in, loaded := x.inRoster("bob@example.com"); in || !loaded {
		t.Fatalf("after remove: in=%v loaded=%v", in, loaded)
	}
	if x.lookupRosterName("bob@example.com") != "" {
		t.Fatal("name survived removal")
	}
	if got := x.dmDisplayName("bob@example.com"); got != "bob@example.com (me@example.com)" {
		t.Errorf("after: %q", got)
	}
	if in, _ := x.inRoster("keep@example.com"); !in || x.lookupRosterName("keep@example.com") != "Keep" {
		t.Fatal("other entries disturbed")
	}
}
