package connector

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"mellium.im/xmpp/disco"
	"mellium.im/xmpp/disco/info"
)

// bmRig is a client with a fake bookmark server and every MUC/portal seam hooked.
type bmRig struct {
	x  *xmppClient
	mu sync.Mutex

	server     []bookmarkConf // the "node"
	fetches    int
	publishes  []bookmarkConf
	fetchErr   error
	publishErr error

	sentJoins []string // "room|nick"
	leaves    []string
	notices   []string // "room|text"
	left      map[string]bool
	portals   []*database.Portal
	events    []bridgev2.RemoteEvent
}

func pepDisco(write bool) disco.Info {
	i := disco.Info{Identity: []info.Identity{{Category: "pubsub", Type: "pep"}}}
	if write {
		i.Features = []info.Feature{{Var: pubsubPublishOptionsFeature}}
	}
	return i
}

func newBMRig(t *testing.T, write bool, server ...bookmarkConf) *bmRig {
	t.Helper()
	r := &bmRig{server: server, left: map[string]bool{}}
	x := &xmppClient{loginID: "alice@example.com", log: zerolog.Nop()}
	r.x = x
	x.bm.reset()
	x.bm.discoHook = func(context.Context) (disco.Info, error) { return pepDisco(write), nil }
	x.bm.fetchHook = func(context.Context) ([]bookmarkConf, int, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.fetches++
		if r.fetchErr != nil {
			return nil, 0, r.fetchErr
		}
		return append([]bookmarkConf(nil), r.server...), 0, nil
	}
	x.bm.publishHook = func(_ context.Context, c bookmarkConf) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.publishErr != nil {
			return r.publishErr
		}
		r.publishes = append(r.publishes, c)
		replaced := false
		for i := range r.server {
			if r.server[i].ID == c.ID {
				r.server[i], replaced = c, true
			}
		}
		if !replaced {
			r.server = append(r.server, c)
		}
		return nil
	}
	x.mucSendJoinHook = func(room, nick string) error {
		r.mu.Lock()
		r.sentJoins = append(r.sentJoins, room+"|"+nick)
		r.mu.Unlock()
		return nil
	}
	x.mucLeaveHook = func(room string) error {
		r.mu.Lock()
		r.leaves = append(r.leaves, room)
		r.mu.Unlock()
		x.mucOurOccupantJIDMu.Lock()
		delete(x.mucOurOccupantJID, room)
		x.mucOurOccupantJIDMu.Unlock()
		return nil
	}
	x.mucNoticeHook = func(room, text string) {
		r.mu.Lock()
		r.notices = append(r.notices, room+"|"+text)
		r.mu.Unlock()
	}
	x.mucLeftHook = func(room string, left bool) error {
		r.mu.Lock()
		r.left[room] = left
		r.mu.Unlock()
		return nil
	}
	x.mucRejoinPortalsHook = func(context.Context) ([]*database.Portal, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		return append([]*database.Portal(nil), r.portals...), nil
	}
	x.mucNewestMsgHook = func(context.Context, string) time.Time { return time.Time{} }
	x.queueHook = func(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
		r.mu.Lock()
		r.events = append(r.events, evt)
		r.mu.Unlock()
		return bridgev2.EventHandlingResultSuccess
	}
	return r
}

func (r *bmRig) published() []bookmarkConf {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bookmarkConf(nil), r.publishes...)
}

func (r *bmRig) fetchCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fetches
}

func (r *bmRig) joins() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sentJoins...)
}

func (r *bmRig) noticeList() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.notices...)
}

// addPortal registers a bridged room with a Matrix room for the rig's login.
func (r *bmRig) addPortal(room string, left bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.portals = append(r.portals, rejoinPortal(room, database.RoomTypeGroupDM, "!"+room+":x", left, r.x.loginID))
}

func (r *bmRig) joinedRoom(room, nick string) {
	r.x.mucOurOccupantJIDMu.Lock()
	defer r.x.mucOurOccupantJIDMu.Unlock()
	if r.x.mucOurOccupantJID == nil {
		r.x.mucOurOccupantJID = map[string]string{}
	}
	r.x.mucOurOccupantJID[room] = room + "/" + nick
}

func bmWaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func bm(id string, autojoin bool, nick string) bookmarkConf {
	return bookmarkConf{ID: id, Autojoin: autojoin, Name: "Name of " + id, Nick: nick}
}

// wellFormed parses s with a raw std decoder and rejects duplicate attributes (the std decoder itself
// does not), the defect that makes a stream-level parser close the connection.
func wellFormed(s string) error {
	d := xml.NewDecoder(strings.NewReader(s))
	for {
		tok, err := d.RawToken()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if se, ok := tok.(xml.StartElement); ok {
			seen := map[string]bool{}
			for _, a := range se.Attr {
				k := a.Name.Space + ":" + a.Name.Local
				if seen[k] {
					return fmt.Errorf("duplicate attribute %s on <%s>", k, se.Name.Local)
				}
				seen[k] = true
			}
		}
	}
}

// bmLogBuf is a goroutine-safe log sink.
type bmLogBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *bmLogBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *bmLogBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
