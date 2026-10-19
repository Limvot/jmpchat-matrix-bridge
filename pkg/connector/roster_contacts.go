package connector

import (
	"sort"
	"strings"
)

// contactEntry is one named phone contact of the roster, as listed by the Dialpad Contacts tab.
type contactEntry struct {
	name string // cleaned display name, never empty
	e164 string // valid E.164 (from a Cheogram 1:1 JID)
}

// rosterNamesSnapshot returns a copy of the roster's names (lowercase bare JID -> name; named entries
// only) and whether the first complete roster fetch has finished. The lock is held only for the copy,
// so a roster push on the XMPP handler path is never blocked by filtering or sorting.
func (x *xmppClient) rosterNamesSnapshot() (names map[string]string, loaded bool) {
	x.rosterMu.RLock()
	defer x.rosterMu.RUnlock()
	names = make(map[string]string, len(x.roster))
	for j, n := range x.roster {
		names[j] = n
	}
	return names, x.rosterLoaded
}

// phoneContacts keeps the named Cheogram phone contacts of names: JIDs that cheogramE164 accepts (1:1
// numbers; group MMS, short codes and other domains are rejected) whose clean(name) is not empty. clean
// is injected so this file does not depend on the call library.
func phoneContacts(names map[string]string, clean func(string) string) []contactEntry {
	out := make([]contactEntry, 0, len(names))
	for j, n := range names {
		e164, ok := cheogramE164(j)
		if !ok {
			continue
		}
		if n = clean(n); n == "" {
			continue
		}
		out = append(out, contactEntry{name: n, e164: e164})
	}
	return out
}

// digitQuery returns the digits of q when q is a number search: at least 2 digits and nothing but
// digits and + - ( ) . and space. ok=false otherwise (the query then matches names only).
func digitQuery(q string) (digits string, ok bool) {
	var b strings.Builder
	for _, r := range q {
		switch {
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '+' || r == '-' || r == '(' || r == ')' || r == '.' || r == ' ':
		default:
			return "", false
		}
	}
	if b.Len() < 2 {
		return "", false
	}
	return b.String(), true
}

// pageContacts filters entries by q, sorts the matches (lowercase name ascending, ties by number) and
// returns the page [offset, offset+limit) and the number of matches. It sorts entries in place. A match
// is a case-insensitive substring of the name, or, for a number search (digitQuery), a substring of the
// number's digits. There is no Unicode normalization or accent folding. An offset at or past the end
// gives no items and the right total.
func pageContacts(entries []contactEntry, q string, offset, limit int) (items []contactEntry, total int) {
	lq := strings.ToLower(q)
	digits, byNumber := digitQuery(q)
	matches := entries[:0:0]
	for _, e := range entries {
		switch {
		case q == "":
		case strings.Contains(strings.ToLower(e.name), lq):
		case byNumber && strings.Contains(strings.TrimPrefix(e.e164, "+"), digits):
		default:
			continue
		}
		matches = append(matches, e)
	}
	sort.Slice(matches, func(i, j int) bool {
		a, b := strings.ToLower(matches[i].name), strings.ToLower(matches[j].name)
		if a != b {
			return a < b
		}
		return matches[i].e164 < matches[j].e164
	})
	total = len(matches)
	if offset < 0 {
		offset = 0
	}
	if offset >= total || limit <= 0 {
		return nil, total
	}
	return matches[offset:min(offset+limit, total)], total
}
