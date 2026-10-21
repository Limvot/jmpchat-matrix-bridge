package connector

import (
	"context"
	"encoding/xml"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/bookmarks"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/pubsub"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Wiring of the caps responder, the bookmark PEP event handler and the connect-time capability probe.
// Nothing here acts on bookmarks yet: the probe and the event handler only log (live data gathering).

const (
	bookmarkFetchTimeout = 15 * time.Second
	// bookmarkFetchMaxItems bounds how many items one fetch decodes.
	bookmarkFetchMaxItems = 1000
)

// capsDiscoHandler answers disco#info get queries from memory (XEP-0030/0115): the bridge's identity
// and feature list for the bare query and for node "<caps node>#<ver>"; item-not-found for any other
// node. It runs on the Serve goroutine: it only writes the reply (no I/O, no locks) and never returns
// an error for a bad query (a handler error would end the session).
type capsDiscoHandler struct{ x *xmppClient }

// capsDiscoReply builds the reply tokens for a disco#info get.
func capsDiscoReply(iq stanza.IQ, start xml.StartElement) xml.TokenReader {
	node := attrValueLocal(start.Attr, "node")
	if node != "" && node != bridgeCapsDiscoNode() {
		return iq.Error(stanza.Error{Type: stanza.Cancel, Condition: stanza.ItemNotFound})
	}
	return iq.Result(newBridgeDiscoInfo(node).TokenReader())
}

func (h capsDiscoHandler) HandleIQ(iq stanza.IQ, t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	if start == nil {
		return nil
	}
	if _, err := xmlstream.Copy(t, capsDiscoReply(iq, *start)); err != nil && h.x != nil {
		h.x.log.Debug().Err(err).Msg("Failed to send disco#info reply")
	}
	return nil
}

// handlePubSubEvent handles PEP notifications (headline messages) on the Serve goroutine: memory only,
// no IQ, no DB. For now it only parses bookmark events from our own account and logs counts. Always
// returns nil (a handler error ends the session).
func (x *xmppClient) handlePubSubEvent(msg stanza.Message, r xmlstream.TokenReadEncoder) error {
	own, err := jid.Parse(string(x.loginID))
	if err != nil || !bookmarkEventTrusted(msg, own) {
		x.log.Debug().Str("from", msg.From.String()).Msg("Dropped PEP event: not from our own account")
		return nil
	}
	changes, truncated, ok := parseBookmarkEvent(r)
	if !ok {
		x.log.Debug().Msg("PEP event is not a (well-formed) bookmarks event; ignored")
		return nil
	}
	var published, retracted, purged int
	for _, c := range changes {
		switch {
		case c.Purge:
			purged++
		case c.Retract:
			retracted++
		case c.Conf != nil:
			published++
		}
	}
	x.log.Info().Int("published", published).Int("retracted", retracted).Int("purged", purged).Bool("truncated", truncated).
		Msg("Bookmarks: PEP event received (not acted on)")
	return nil
}

// fetchBookmarks reads the whole bookmarks node with our own codec (15s cap, child span). A node that
// does not exist is an empty list, not an error. Items with a missing id or an undecodable payload are
// counted in skipped and never stop the loop. Never log or trace the returned content.
func (x *xmppClient) fetchBookmarks(ctx context.Context, sess *xmpp.Session) (bms []bookmarkConf, skipped int, err error) {
	ctx, span := telemetry.StartChild(ctx, "xmpp.iq.bookmarks_fetch", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, bookmarkFetchTimeout)
	defer cancel()
	it := pubsub.Fetch(ctx, sess, pubsub.Query{Node: bookmarks.NS})
	defer it.Close()
	for it.Next() {
		if len(bms)+skipped >= bookmarkFetchMaxItems {
			break
		}
		id, r := it.Item()
		if id == "" || r == nil {
			skipped++
			continue
		}
		c, derr := decodeBookmarkConf(id, r)
		if derr != nil {
			skipped++
			continue
		}
		bms = append(bms, c)
	}
	if ierr := it.Err(); ierr != nil {
		var se stanza.Error
		if errors.As(ierr, &se) && se.Condition == stanza.ItemNotFound {
			err = nil
		} else {
			err = ierr
			telemetry.Fail(span, ierr)
			return nil, skipped, ierr
		}
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("xmpp.bookmarks.count", len(bms)), attribute.Int("xmpp.bookmarks.skipped", skipped))
	}
	return bms, skipped, err
}

// logBookmarkGate logs the single info line of the capability decision.
func (x *xmppClient) logBookmarkGate(ctx context.Context, s bookmarkSupport) {
	x.logFor(ctx).Info().
		Bool("read_supported", s.Read).
		Bool("write_supported", s.Write).
		Bool("compat_seen", s.Compat).
		Msg("Bookmarks capability gate")
}

// runBookmarkProbe runs once per session in its own goroutine with its own root span (never on Serve):
// the capability gate and one fetch that fills the cache (bookmarkStartup). Superseded by the startup
// coordinator in P4.
func (x *xmppClient) runBookmarkProbe(sessCtx context.Context) {
	ctx, span := telemetry.Start(sessCtx, "xmpp.bookmarks.probe", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	x.bookmarkStartup(ctx)
}
