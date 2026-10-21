package connector

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"mellium.im/xmpp"
	"mellium.im/xmpp/disco"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/roster"
	"mellium.im/xmpp/stanza"
	"mellium.im/xmpp/stream"
)

// pipeSession is a real mellium client session over net.Pipe, past negotiation, whose server end answers
// roster and disco#info gets. It reproduces the Serve semantics the roster fix depends on: an IQ result
// is handed to its waiter on the Serve goroutine, which then blocks until that response is closed.
func pipeSession(t *testing.T, rosterItems int) *xmpp.Session {
	t.Helper()
	cli, srv := net.Pipe()
	t.Cleanup(func() { cli.Close(); srv.Close() })

	// Server: write the stream header, then answer each IQ get by its payload namespace.
	go func() {
		fmt.Fprintf(srv, `<stream:stream xmlns="%s" xmlns:stream="%s" from="example.com" to="me@example.com" id="s1" version="1.0">`, stanza.NSClient, stream.NS)
		d := xml.NewDecoder(srv)
		for {
			tok, err := d.Token()
			if err != nil {
				return
			}
			se, ok := tok.(xml.StartElement)
			if !ok || se.Name.Local != "iq" {
				continue
			}
			var iq struct {
				ID    string `xml:"id,attr"`
				Query struct {
					XMLName xml.Name
				} `xml:",any"`
			}
			if d.DecodeElement(&iq, &se) != nil {
				return
			}
			switch iq.Query.XMLName.Space {
			case roster.NS:
				fmt.Fprintf(srv, `<iq type="result" id="%s"><query xmlns="%s">`, iq.ID, roster.NS)
				for i := 0; i < rosterItems; i++ {
					fmt.Fprintf(srv, `<item jid="c%d@example.com" name="Contact %d" subscription="both"/>`, i, i)
				}
				fmt.Fprint(srv, `</query></iq>`)
			case disco.NSInfo:
				fmt.Fprintf(srv, `<iq type="result" id="%s" from="me@example.com"><query xmlns="%s"><identity category="pubsub" type="pep"/></query></iq>`, iq.ID, disco.NSInfo)
			}
		}
	}()

	neg := func(ctx context.Context, in, out *stream.Info, s *xmpp.Session, _ interface{}) (xmpp.SessionState, io.ReadWriter, interface{}, error) {
		rc := s.TokenReader()
		defer rc.Close()
		_, err := rc.Token() // the server's stream header
		in.XMLNS, out.XMLNS = stanza.NSClient, stanza.NSClient
		return xmpp.Ready, nil, nil, err
	}
	sess, err := xmpp.NewSession(context.Background(), jid.MustParse("example.com"), jid.MustParse("me@example.com/r"), cli, 0, neg)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	go func() { _ = sess.Serve(nil) }()
	return sess
}

// Regression (prod: bookmark gate and occupant vCard gets timed out for exactly the time the roster
// refresh took): after fetchRoster returns, other IQs must complete while the caller is still busy with
// the result (the ghost/portal refresh). With the roster iterator still open, Serve would be stuck
// waiting for it to be closed and this disco#info would time out.
func TestFetchRosterReleasesServeBeforeReturning(t *testing.T) {
	sess := pipeSession(t, 50)
	x := &xmppClient{loginID: "me@example.com", log: zerolog.Nop()}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	names, entries, count, err := x.fetchRoster(ctx, sessionRosterFetcher{sess})
	if err != nil {
		t.Fatalf("fetchRoster: %v", err)
	}
	if count != 50 || len(entries) != 50 || names["c7@example.com"] != "Contact 7" {
		t.Fatalf("count=%d entries=%d name=%q", count, len(entries), names["c7@example.com"])
	}

	// Still "processing" the roster (nothing else was closed): another IQ must get its answer.
	dctx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dcancel()
	info, err := disco.GetInfo(dctx, "", jid.MustParse("me@example.com"), sess)
	if err != nil {
		t.Fatalf("disco#info after fetchRoster: %v (Serve still blocked on the roster response?)", err)
	}
	if len(info.Identity) != 1 || info.Identity[0].Type != "pep" {
		t.Fatalf("disco info = %+v", info)
	}
}

// fakeRosterIter records whether it was closed.
type fakeRosterIter struct {
	items  []roster.Item
	i      int
	err    error
	closed bool
}

func (f *fakeRosterIter) Next() bool {
	if f.i >= len(f.items) {
		return false
	}
	f.i++
	return true
}
func (f *fakeRosterIter) Item() roster.Item { return f.items[f.i-1] }
func (f *fakeRosterIter) Err() error        { return f.err }
func (f *fakeRosterIter) Close() error      { f.closed = true; return nil }

type fakeRosterFetcher struct{ it *fakeRosterIter }

func (f fakeRosterFetcher) fetchRoster(context.Context) rosterIter { return f.it }

func TestFetchRosterClosesOnSuccessAndError(t *testing.T) {
	x := &xmppClient{loginID: "me@example.com", log: zerolog.Nop()}
	ok := &fakeRosterIter{items: []roster.Item{
		{JID: jid.MustParse("Alice@Example.com"), Name: "Alice"},
		{JID: jid.MustParse("bob@example.com")},
		{JID: jid.MustParse("gone@example.com"), Subscription: "remove"},
	}}
	names, entries, count, err := x.fetchRoster(context.Background(), fakeRosterFetcher{ok})
	if err != nil || !ok.closed {
		t.Fatalf("err=%v closed=%v", err, ok.closed)
	}
	if count != 3 || len(entries) != 3 || len(names) != 1 || names["alice@example.com"] != "Alice" || !entries[2].remove {
		t.Fatalf("count=%d entries=%+v names=%v", count, entries, names)
	}

	bad := &fakeRosterIter{err: context.DeadlineExceeded}
	if _, _, _, err := x.fetchRoster(context.Background(), fakeRosterFetcher{bad}); err == nil || !bad.closed {
		t.Fatalf("err=%v closed=%v", err, bad.closed)
	}
}
