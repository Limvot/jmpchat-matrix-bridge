package connector

import (
	"reflect"
	"strings"
	"testing"
)

func contactStrs(es []contactEntry) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.name+"|"+e.e164)
	}
	return out
}

func TestPhoneContacts(t *testing.T) {
	roster := map[string]string{
		"+15550000001@cheogram.com":                                     "Alice",
		"+15550000002@cheogram.com":                                     "   ",   // empty after cleaning
		"+15550000003@CHEOGRAM.COM":                                     "Upper", // domain case-insensitive
		"+15550000004,+15550000005@cheogram.com":                        "Group", // group MMS
		"123;phone-context=ca-us.phone-context.soprani.ca@cheogram.com": "Short code",
		"friend@example.com":                                            "Plain XMPP",
		"+15550000006@example.com":                                      "Other domain",
	}
	clean := func(s string) string { return strings.TrimSpace(s) }
	got := contactStrs(phoneContacts(roster, clean))
	want := map[string]bool{"Alice|+15550000001": true, "Upper|+15550000003": true}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for _, g := range got {
		if !want[g] {
			t.Errorf("unexpected %q in %v", g, got)
		}
	}
}

func TestPageContacts(t *testing.T) {
	all := func() []contactEntry {
		return []contactEntry{
			{"bob", "+15550000020"},
			{"Alice", "+15550000010"},
			{"alice", "+15550000005"}, // same lowercase name: by number
			{"Zoë", "+15551234567"},
			{"Agent 555", "+15559990000"},
			{"Émile", "+15550001111"},
		}
	}
	cases := []struct {
		name, q       string
		offset, limit int
		want          []string
		total         int
	}{
		{"all sorted, case-insensitive, ties by number", "", 0, 50,
			[]string{"Agent 555|+15559990000", "alice|+15550000005", "Alice|+15550000010", "bob|+15550000020", "Zoë|+15551234567", "Émile|+15550001111"}, 6},
		{"name substring", "LIC", 0, 50, []string{"alice|+15550000005", "Alice|+15550000010"}, 2},
		{"accents untouched", "emile", 0, 50, nil, 0},
		{"accent exact", "émile", 0, 50, []string{"Émile|+15550001111"}, 1},
		{"digits in number", "123", 0, 50, []string{"Zoë|+15551234567"}, 1},
		{"digits also match a name containing digits", "555", 0, 50, nil, -1}, // checked below
		{"formatted digits", "+1 (555) 123", 0, 50, []string{"Zoë|+15551234567"}, 1},
		{"dashes and dots", "555.123-4567", 0, 50, []string{"Zoë|+15551234567"}, 1},
		{"one digit matches names only", "5", 0, 50, []string{"Agent 555|+15559990000"}, 1},
		{"letters disable number search", "a1", 0, 50, nil, 0},
		{"no match", "zzz", 0, 50, nil, 0},
		{"page 1", "", 0, 2, []string{"Agent 555|+15559990000", "alice|+15550000005"}, 6},
		{"page 2", "", 2, 2, []string{"Alice|+15550000010", "bob|+15550000020"}, 6},
		{"last partial page", "", 5, 2, []string{"Émile|+15550001111"}, 6},
		{"offset at total", "", 6, 2, nil, 6},
		{"offset past total", "", 100, 2, nil, 6},
		{"limit larger than rest", "", 4, 50, []string{"Zoë|+15551234567", "Émile|+15550001111"}, 6},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			items, total := pageContacts(all(), c.q, c.offset, c.limit)
			if c.total == -1 { // "555": every number has 555 in it, plus the name "Agent 555"
				if total != 6 {
					t.Errorf("total %d, want 6", total)
				}
				return
			}
			if total != c.total || !reflect.DeepEqual(contactStrs(items), c.want) {
				t.Errorf("got %v total %d, want %v total %d", contactStrs(items), total, c.want, c.total)
			}
		})
	}
	if items, total := pageContacts(nil, "", 0, 10); items != nil || total != 0 {
		t.Errorf("empty: %v %d", items, total)
	}
}

func TestRosterNamesSnapshot(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com"}
	if n, loaded := x.rosterNamesSnapshot(); len(n) != 0 || loaded {
		t.Fatalf("fresh: %v %v", n, loaded)
	}
	// A push before the first fetch is visible but not loaded.
	x.applyRosterItems(false, rosterEntry{jid: "+15550000001@cheogram.com", name: "Alice"})
	n, loaded := x.rosterNamesSnapshot()
	if loaded || n["+15550000001@cheogram.com"] != "Alice" {
		t.Fatalf("early push: %v %v", n, loaded)
	}
	x.applyRosterItems(true,
		rosterEntry{jid: "+15550000001@cheogram.com", name: "Alice"},
		rosterEntry{jid: "+15550000002@cheogram.com"}, // unnamed: not in the snapshot
		rosterEntry{jid: "+15550000003@cheogram.com", name: "Carol"},
	)
	n, loaded = x.rosterNamesSnapshot()
	if !loaded || len(n) != 2 || n["+15550000002@cheogram.com"] != "" {
		t.Fatalf("after fetch: %v %v", n, loaded)
	}
	// Push: rename, name dropped, removed.
	x.applyRosterItems(false, rosterEntry{jid: "+15550000001@cheogram.com", name: "Alicia"})
	x.applyRosterItems(false, rosterEntry{jid: "+15550000003@cheogram.com"})
	x.applyRosterItems(false, rosterEntry{jid: "+15550000004@cheogram.com", name: "Dave"})
	x.applyRosterItems(false, rosterEntry{jid: "+15550000004@cheogram.com", remove: true})
	n, _ = x.rosterNamesSnapshot()
	if len(n) != 1 || n["+15550000001@cheogram.com"] != "Alicia" {
		t.Errorf("after pushes: %v", n)
	}
	// The snapshot is a copy.
	n["+15550000009@cheogram.com"] = "X"
	if n2, _ := x.rosterNamesSnapshot(); len(n2) != 1 {
		t.Errorf("snapshot aliases the cache: %v", n2)
	}
}
