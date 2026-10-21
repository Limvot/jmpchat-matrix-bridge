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

// Wiring of the caps responder, the bookmark PEP event handler and the bookmark fetch.

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
// no IQ, no DB. It parses bookmark events from our own account and hands them to the session's event
// worker through a non-blocking channel send (a full buffer drops the event; the next connect or
// bookmarks-sync heals it). Always returns nil (a handler error ends the session).
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
		Msg("Bookmarks: PEP event received")
	if !x.bm.offer(bookmarkBatch{changes: changes, truncated: truncated}) {
		x.log.Warn().Msg("Bookmarks: PEP event queue full or no session; event dropped (reconnect or bookmarks-sync re-reads the bookmarks)")
	}
	return nil
}

// fetchBookmarks reads the whole bookmarks node with our own codec (15s cap, child span). A node that
// does not exist is an empty list, not an error. Items with a missing id or an undecodable payload are
// counted in skipped and never stop the loop. Never log or trace the returned content.
// bookmarkFetchInfo describes what a fetch could not return: items that were skipped (undecodable or
// without an id) and whether the item cap cut the list. A write must never create an item that may
// exist but is missing from the list (it would overwrite another client's bookmark): see mayHide.
type bookmarkFetchInfo struct {
	Skipped   int
	Opaque    map[string]bool // bookmarkKey of items that exist but could not be decoded
	Truncated bool            // bookmarkFetchMaxItems reached: anything may be missing
}

// mayHide reports whether the room's item could exist on the server although the fetch did not return it.
func (f bookmarkFetchInfo) mayHide(key string) bool { return f.Truncated || f.Opaque[key] }

func (x *xmppClient) fetchBookmarks(ctx context.Context, sess *xmpp.Session) (bms []bookmarkConf, info bookmarkFetchInfo, err error) {
	ctx, span := telemetry.StartChild(ctx, "xmpp.iq.bookmarks_fetch", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, bookmarkFetchTimeout)
	defer cancel()
	it := pubsub.Fetch(ctx, sess, pubsub.Query{Node: bookmarks.NS})
	defer it.Close()
	for it.Next() {
		if len(bms)+info.Skipped >= bookmarkFetchMaxItems {
			info.Truncated = true
			break
		}
		id, r := it.Item()
		if id == "" || r == nil {
			info.Skipped++
			continue
		}
		c, derr := decodeBookmarkConf(id, r)
		if derr != nil {
			info.Skipped++
			if info.Opaque == nil {
				info.Opaque = map[string]bool{}
			}
			info.Opaque[bookmarkKey(id)] = true
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
			return nil, info, ierr
		}
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("xmpp.bookmarks.count", len(bms)), attribute.Int("xmpp.bookmarks.skipped", info.Skipped), attribute.Bool("xmpp.bookmarks.truncated", info.Truncated))
	}
	return bms, info, err
}

// logBookmarkGate logs the single info line of the capability decision.
func (x *xmppClient) logBookmarkGate(ctx context.Context, s bookmarkSupport) {
	x.logFor(ctx).Info().
		Bool("read_supported", s.Read).
		Bool("write_supported", s.Write).
		Bool("compat_seen", s.Compat).
		Msg("Bookmarks capability gate")
}
