package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

func TestMUCRoomRefusal(t *testing.T) {
	cancel := func(c stanza.Condition) stanza.Error { return stanza.Error{Type: stanza.Cancel, Condition: c} }
	withBy := func(se stanza.Error, by string) stanza.Error { se.By = jid.MustParse(by); return se }
	fromRoom := func(se stanza.Error) error { return &iqStanzaError{from: testRoom, err: se} }
	fromOcc := func(se stanza.Error) error { return &iqStanzaError{from: testOcc, err: se} }
	cases := []struct {
		name string
		err  error
		want bool
	}{
		// Answers about the occupant (forwarded from its server, or the occupant left): never count.
		{"item-not-found", fromOcc(cancel(stanza.ItemNotFound)), false},
		{"feature-not-implemented", fromOcc(cancel(stanza.FeatureNotImplemented)), false},
		{"service-unavailable unattributed", fromOcc(cancel(stanza.ServiceUnavailable)), false},
		{"forbidden unattributed", fromOcc(cancel(stanza.Forbidden)), false},
		{"auth forbidden unattributed", fromOcc(stanza.Error{Type: stanza.Auth, Condition: stanza.Forbidden}), false},
		{"remote-server-timeout", fromOcc(cancel(stanza.RemoteServerTimeout)), false},
		{"remote-server-not-found", fromOcc(cancel(stanza.RemoteServerNotFound)), false},
		{"recipient-unavailable", fromOcc(cancel(stanza.RecipientUnavailable)), false},
		{"item-not-found by room", fromOcc(withBy(cancel(stanza.ItemNotFound), testRoom)), false},
		{"timeout", fmt.Errorf("vcard fetch: %w", context.DeadlineExceeded), false},
		{"plain error", fmt.Errorf("boom"), false},
		{"wait", fromRoom(stanza.Error{Type: stanza.Wait, Condition: stanza.ResourceConstraint}), false},
		{"modify", fromRoom(stanza.Error{Type: stanza.Modify, Condition: stanza.BadRequest}), false},
		{"not-allowed by other domain", fromOcc(withBy(cancel(stanza.NotAllowed), "example.org")), false},
		// The room itself refuses.
		{"service-unavailable from room bare", fromRoom(cancel(stanza.ServiceUnavailable)), true},
		{"forbidden by room", fromOcc(withBy(cancel(stanza.Forbidden), testRoom)), true},
		{"feature-not-implemented by room occupant jid", fromOcc(withBy(cancel(stanza.FeatureNotImplemented), testOcc)), true},
		{"service-unavailable by muc service", fromOcc(withBy(cancel(stanza.ServiceUnavailable), "muc.example.com")), true},
		{"not-allowed unattributed", fromOcc(cancel(stanza.NotAllowed)), true},
		{"not-acceptable unattributed", fromOcc(cancel(stanza.NotAcceptable)), true},
		{"not-acceptable plain stanza error, wrapped", fmt.Errorf("vcard fetch x: %w", cancel(stanza.NotAcceptable)), true},
	}
	for _, c := range cases {
		if got := mucRoomRefusal(c.err, testRoom); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestIQStanzaErrorUnwrap(t *testing.T) {
	err := fmt.Errorf("vcard fetch %s: %w", testOcc, &iqStanzaError{from: testRoom, err: stanza.Error{Type: stanza.Cancel, Condition: stanza.ItemNotFound}})
	typ, cond := avatarErrInfo(err)
	if typ != "cancel" || cond != "item-not-found" || iqErrorFrom(err) != testRoom {
		t.Fatalf("got %q %q %q", typ, cond, iqErrorFrom(err))
	}
	if avatarFailClass(err) != "cancel/item-not-found" {
		t.Fatal(avatarFailClass(err))
	}
	if avatarFailClass(fmt.Errorf("x: %w", context.DeadlineExceeded)) != "timeout" || avatarFailClass(fmt.Errorf("x")) != "other" {
		t.Fatal("classes")
	}
}

// A big public room: many speakers whose servers have no vCard (or never answer) must not trip the
// breaker; each only gets its own failure window, and a later speaker is still fetched.
func TestWorkerOccupantErrorsDoNotDisableRoom(t *testing.T) {
	h := newHarness()
	errs := []error{
		&iqStanzaError{from: testRoom + "/a", err: stanza.Error{Type: stanza.Cancel, Condition: stanza.ItemNotFound}},
		&iqStanzaError{from: testRoom + "/b", err: stanza.Error{Type: stanza.Cancel, Condition: stanza.FeatureNotImplemented}},
		&iqStanzaError{from: testRoom + "/c", err: stanza.Error{Type: stanza.Cancel, Condition: stanza.ServiceUnavailable}},
		&iqStanzaError{from: testRoom + "/d", err: stanza.Error{Type: stanza.Cancel, Condition: stanza.RemoteServerTimeout}},
		fmt.Errorf("vcard fetch: %w", context.DeadlineExceeded),
		&iqStanzaError{from: testRoom + "/f", err: stanza.Error{Type: stanza.Auth, Condition: stanza.Forbidden}},
		&iqStanzaError{from: testRoom + "/g", err: stanza.Error{Type: stanza.Cancel, Condition: stanza.ItemNotFound}},
	}
	i := 0
	h.fetchFn = func(to string) ([]byte, string, error) {
		if strings.HasSuffix(to, "/late") {
			return pngBytes, hashN(99), nil
		}
		e := errs[i]
		i++
		return nil, "", e
	}
	for n := range errs {
		occ := fmt.Sprintf("%s/n%d", testRoom, n)
		h.ghosts[occ] = newFakeGhost("", true)
		queueOne(h, occ, hashPresence(hashN(n+1)))
		h.s.runOnce(context.Background())
		if r := h.rec(occ); r.failHash != hashN(n+1) {
			t.Fatalf("occupant %d: failure window not set", n)
		}
	}
	if h.s.roomDisabled[testRoom] || h.s.roomCancelErrs[testRoom] != 0 {
		t.Fatalf("room must stay enabled, refusals=%d", h.s.roomCancelErrs[testRoom])
	}
	occ := testRoom + "/late"
	h.ghosts[occ] = newFakeGhost("", true)
	queueOne(h, occ, hashPresence(hashN(99)))
	h.s.runOnce(context.Background())
	if len(h.ghosts[occ].updates) != 1 {
		t.Fatal("a later speaker must still be fetched")
	}
	if h.s.stats.failed != len(errs) || h.s.failBy["cancel/item-not-found"] != 2 || h.s.failBy["timeout"] != 1 || h.s.failBy["auth/forbidden"] != 1 {
		t.Fatalf("failure classes %v failed=%d", h.s.failBy, h.s.stats.failed)
	}
}

// The breaker only looks at the occupant answer: an error from the real-JID fallback is the
// member's own server and never counts, and a fallback success does not clear room refusals.
func TestWorkerRealJIDErrorDoesNotCount(t *testing.T) {
	h := newHarness()
	h.fetchFn = func(to string) ([]byte, string, error) {
		if strings.HasPrefix(to, testRoom) {
			return nil, "", stanza.Error{Type: stanza.Cancel, Condition: stanza.ItemNotFound}
		}
		return nil, "", stanza.Error{Type: stanza.Cancel, Condition: stanza.NotAllowed}
	}
	for n := 0; n < mucAvatarRoomErrMax+1; n++ {
		occ := fmt.Sprintf("%s/n%d", testRoom, n)
		h.ghosts[occ] = newFakeGhost("", true)
		p := hashPresence(hashN(n + 1))
		p.itemJID = fmt.Sprintf("u%d@example.org", n)
		queueOne(h, occ, p)
		h.s.runOnce(context.Background())
	}
	if h.s.roomDisabled[testRoom] || h.s.roomCancelErrs[testRoom] != 0 {
		t.Fatal("real JID errors must not count against the room")
	}

	h2 := newHarness()
	h2.fetchFn = func(to string) ([]byte, string, error) {
		if strings.HasPrefix(to, testRoom) {
			return nil, "", stanza.Error{Type: stanza.Cancel, Condition: stanza.NotAllowed}
		}
		return pngBytes, hashN(7), nil
	}
	for n := 0; n < mucAvatarRoomErrMax; n++ {
		occ := fmt.Sprintf("%s/n%d", testRoom, n)
		h2.ghosts[occ] = newFakeGhost("", true)
		p := hashPresence(hashN(n + 10)) // distinct advertised hashes: no cache hit, every job fetches
		p.itemJID = fmt.Sprintf("u%d@example.org", n)
		queueOne(h2, occ, p)
		h2.s.runOnce(context.Background())
	}
	if !h2.s.roomDisabled[testRoom] {
		t.Fatal("room refusals must accumulate even when the real-JID fallback succeeds")
	}
}

func captureLogs(h *harness) *bytes.Buffer {
	var buf bytes.Buffer
	l := zerolog.New(&buf).Level(zerolog.InfoLevel)
	h.s.logger = func(context.Context) *zerolog.Logger { return &l }
	return &buf
}

func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		m := map[string]any{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestWorkerRoomDisabledLogsInfo(t *testing.T) {
	h := newHarness()
	buf := captureLogs(h)
	h.fetchFn = func(string) ([]byte, string, error) {
		return nil, "", &iqStanzaError{from: testRoom, err: stanza.Error{Type: stanza.Cancel, Condition: stanza.ServiceUnavailable}}
	}
	for n := 0; n < mucAvatarRoomErrMax; n++ {
		occ := fmt.Sprintf("%s/n%d", testRoom, n)
		h.ghosts[occ] = newFakeGhost("", true)
		queueOne(h, occ, hashPresence(hashN(n+1)))
		h.s.runOnce(context.Background())
	}
	if !h.s.roomDisabled[testRoom] {
		t.Fatal("room refusing from its bare JID must be disabled")
	}
	lines := logLines(t, buf)
	if len(lines) != 1 {
		t.Fatalf("want exactly one info line, got %v", lines)
	}
	l := lines[0]
	if !strings.HasPrefix(l["message"].(string), "MUC avatar fetching disabled for room") || l["room"] != testRoom ||
		l["condition"] != "service-unavailable" || l["error_type"] != "cancel" || l["level"] != "info" {
		t.Fatalf("disable line %v", l)
	}
}

func TestMUCAvatarStatsInfoLine(t *testing.T) {
	h := newHarness()
	buf := captureLogs(h)
	h.fetchFn = func(to string) ([]byte, string, error) {
		if strings.HasSuffix(to, "/bad") {
			return nil, "", stanza.Error{Type: stanza.Cancel, Condition: stanza.ItemNotFound}
		}
		return pngBytes, hashN(1), nil
	}
	for i, nick := range []string{"good", "bad"} {
		occ := testRoom + "/" + nick
		h.ghosts[occ] = newFakeGhost("", true)
		queueOne(h, occ, hashPresence(hashN(i+1)))
		h.s.runOnce(context.Background())
	}
	h.s.flushStats(context.Background())
	lines := logLines(t, buf)
	if len(lines) != 1 || lines[0]["message"] != "MUC avatar stats" || lines[0]["level"] != "info" {
		t.Fatalf("stats line %v", lines)
	}
	l := lines[0]
	if l["fetched"].(float64) != 1 || l["applied"].(float64) != 1 || l["failed"].(float64) != 1 {
		t.Fatalf("counters %v", l)
	}
	if f, _ := l["failures"].(map[string]any); f["cancel/item-not-found"].(float64) != 1 {
		t.Fatalf("failures %v", l["failures"])
	}
	if strings.Contains(buf.String(), "PNG") {
		t.Fatal("stats must not contain bytes")
	}
	// Changed but within the period: no line; after the period: one line.
	buf.Reset()
	h.s.bump(func(st *mucAvatarStats) { st.removed++ })
	h.now = h.now.Add(mucAvatarStatsPeriod / 2)
	h.s.flushStats(context.Background())
	if buf.Len() != 0 {
		t.Fatal("stats must log at most once per period")
	}
	h.now = h.now.Add(mucAvatarStatsPeriod)
	h.s.flushStats(context.Background())
	if len(logLines(t, buf)) != 1 {
		t.Fatal("changed stats after the period must log")
	}
	// Unchanged: no new line, even long after the period.
	buf.Reset()
	h.now = h.now.Add(3 * mucAvatarStatsPeriod)
	h.s.flushStats(context.Background())
	if buf.Len() != 0 {
		t.Fatal("unchanged stats must not log")
	}
}

func TestWorkerPerJobDetailStaysDebug(t *testing.T) {
	h := newHarness()
	buf := captureLogs(h)
	h.fetchFn = func(string) ([]byte, string, error) {
		return nil, "", stanza.Error{Type: stanza.Cancel, Condition: stanza.ItemNotFound}
	}
	h.ghosts[testOcc] = newFakeGhost("", true)
	queueOne(h, testOcc, hashPresence(hashN(1)))
	h.s.runOnce(context.Background())
	if buf.Len() != 0 {
		t.Fatalf("per-job lines must stay at debug: %s", buf.String())
	}
}
