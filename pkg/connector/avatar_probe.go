package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Diagnostic avatar-probe: asks one JID (a MUC occupant JID or a contact) for its avatar through
// the four routes the bridge could use and reports what came back. STRICT: it only ever sends
// IQ stanzas of type get addressed to that JID (never a message or presence stanza). The only
// way the probe code reaches the wire is the probeSender seam, which takes a get IQ.

const (
	avatarProbeTimeout  = 60 * time.Second
	avatarProbeMaxBytes = 4 << 20 // cap on a reply payload kept in memory

	avatarMetadataNS = "urn:xmpp:avatar:metadata"
	avatarDataNS     = "urn:xmpp:avatar:data"
	pubsubNS         = "http://jabber.org/protocol/pubsub"
	discoInfoNS      = "http://jabber.org/protocol/disco#info"
)

// probeSender sends one IQ get and returns the reply's inner payload (the child elements of the
// result IQ, re-serialised and capped). An error reply is returned as *iqStanzaError.
type probeSender func(ctx context.Context, payload xml.TokenReader, iq stanza.IQ) ([]byte, error)

// sessionProbeSender is the real sender: the reply is read to the end and closed before returning.
func sessionProbeSender(sess *xmpp.Session) probeSender {
	return func(ctx context.Context, payload xml.TokenReader, iq stanza.IQ) (inner []byte, e error) {
		resp, err := sess.SendIQElement(ctx, payload, iq)
		if err != nil {
			return nil, err
		}
		defer func() {
			if cerr := resp.Close(); e == nil {
				e = cerr
			}
		}()
		tok, err := resp.Token()
		if err != nil {
			return nil, err
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			return nil, fmt.Errorf("expected IQ start token, got %T", tok)
		}
		if _, err := stanza.UnmarshalIQError(resp, start); err != nil {
			var se stanza.Error
			if errors.As(err, &se) {
				return nil, &iqStanzaError{from: attrValue(start.Attr, "from"), err: se}
			}
			return nil, err
		}
		var buf bytes.Buffer
		enc := xml.NewEncoder(&limitWriter{w: &buf, n: avatarProbeMaxBytes})
		if _, err := xmlstream.Copy(enc, xmlstream.Inner(resp)); err != nil {
			return nil, err
		}
		if err := enc.Flush(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
}

// limitWriter fails once more than n bytes were written.
type limitWriter struct {
	w io.Writer
	n int
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if len(p) > l.n {
		return 0, errors.New("reply too large")
	}
	l.n -= len(p)
	return l.w.Write(p)
}

// ---- payload builders ----

func vcardGetPayload() xml.TokenReader {
	return xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Space: vcardTempNS, Local: "vCard"}})
}

// pubsubItemsGetPayload builds <pubsub xmlns=pubsub><items node=..[ max_items=..]>[<item id=../>]</items></pubsub>.
// The item id goes in a child element (not mellium's Query.Item attribute, which servers reject).
func pubsubItemsGetPayload(node string, maxItems int, itemID string) xml.TokenReader {
	attrs := []xml.Attr{{Name: xml.Name{Local: "node"}, Value: node}}
	if maxItems > 0 {
		attrs = append(attrs, xml.Attr{Name: xml.Name{Local: "max_items"}, Value: strconv.Itoa(maxItems)})
	}
	var inner xml.TokenReader
	if itemID != "" {
		inner = xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Local: "item"}, Attr: []xml.Attr{{Name: xml.Name{Local: "id"}, Value: itemID}}})
	}
	return xmlstream.Wrap(
		xmlstream.Wrap(inner, xml.StartElement{Name: xml.Name{Local: "items"}, Attr: attrs}),
		xml.StartElement{Name: xml.Name{Space: pubsubNS, Local: "pubsub"}},
	)
}

func discoInfoPayload() xml.TokenReader {
	return xmlstream.Wrap(nil, xml.StartElement{Name: xml.Name{Space: discoInfoNS, Local: "query"}})
}

// ---- reply parsing and outcome formatting ----

// imageDesc describes decoded base64 image data as "N B <detected type>" (never the bytes).
func imageDesc(b64 string) (string, error) {
	dec, err := base64.StdEncoding.DecodeString(stripWhitespace(b64))
	if err != nil {
		return "", fmt.Errorf("bad base64: %w", err)
	}
	if len(dec) == 0 {
		return "", nil
	}
	return fmt.Sprintf("%d B %s", len(dec), http.DetectContentType(dec)), nil
}

// formatProbeErr renders an IQ failure: stanza error type/condition with the error's from, a
// timeout, or a short transport error.
func formatProbeErr(err error) string {
	var ie *iqStanzaError
	if errors.As(err, &ie) {
		s := fmt.Sprintf("error %s/%s", ie.err.Type, ie.err.Condition)
		if ie.from != "" {
			s += " from " + ie.from
		}
		return s
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "failed: " + err.Error()
}

// describeVCard formats a vcard-temp reply payload.
func describeVCard(inner []byte) string {
	if len(bytes.TrimSpace(inner)) == 0 {
		return "answered, no photo (empty vCard)"
	}
	var v vcardResult
	if err := xml.Unmarshal(inner, &v); err != nil {
		return "answered, unparseable vCard"
	}
	if strings.TrimSpace(v.Photo.BinVal) == "" {
		return "answered, no photo"
	}
	d, err := imageDesc(v.Photo.BinVal)
	if err != nil {
		return "answered, photo " + err.Error()
	}
	if d == "" {
		return "answered, no photo"
	}
	return "answered, photo " + d
}

type probeItems struct {
	Items []struct {
		ID       string `xml:"id,attr"`
		Metadata struct {
			Info []struct {
				ID    string `xml:"id,attr"`
				Bytes string `xml:"bytes,attr"`
				Type  string `xml:"type,attr"`
			} `xml:"info"`
		} `xml:"metadata"`
		Data string `xml:"data"`
	} `xml:"items>item"`
}

// parseMetadata returns the first item id of an XEP-0084 metadata reply and its description.
func parseMetadata(inner []byte) (itemID, desc string) {
	var p probeItems
	if err := xml.Unmarshal(inner, &p); err != nil {
		return "", "answered, unparseable pubsub reply"
	}
	if len(p.Items) == 0 {
		return "", "answered, no item"
	}
	it := p.Items[0]
	if it.ID == "" {
		return "", "answered, item without id"
	}
	if len(it.Metadata.Info) == 0 {
		return it.ID, fmt.Sprintf("answered, item %s, no <info> (avatar disabled or empty)", it.ID)
	}
	in := it.Metadata.Info[0]
	return it.ID, fmt.Sprintf("answered, item %s, %s B %s", it.ID, in.Bytes, in.Type)
}

// describeAvatarData formats an XEP-0084 data reply.
func describeAvatarData(inner []byte) string {
	var p probeItems
	if err := xml.Unmarshal(inner, &p); err != nil {
		return "answered, unparseable pubsub reply"
	}
	if len(p.Items) == 0 || strings.TrimSpace(p.Items[0].Data) == "" {
		return "answered, no data"
	}
	d, err := imageDesc(p.Items[0].Data)
	if err != nil {
		return "answered, data " + err.Error()
	}
	if d == "" {
		return "answered, no data"
	}
	return "answered, data " + d
}

// describeDisco formats a disco#info reply: whether vcard-temp and pubsub are advertised.
func describeDisco(inner []byte) string {
	var q struct {
		Features []struct {
			Var string `xml:"var,attr"`
		} `xml:"feature"`
	}
	if err := xml.Unmarshal(inner, &q); err != nil {
		return "answered, unparseable disco#info"
	}
	var vc, ps bool
	for _, f := range q.Features {
		switch {
		case f.Var == vcardTempNS:
			vc = true
		case f.Var == pubsubNS || strings.HasPrefix(f.Var, pubsubNS+"#"):
			ps = true
		}
	}
	return fmt.Sprintf("answered, %d features, vcard-temp: %s, pubsub: %s", len(q.Features), yesNo(vc), yesNo(ps))
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// ---- runner ----

// runAvatarProbe runs the four probes in order against to and returns one line per probe.
func runAvatarProbe(ctx context.Context, send probeSender, to jid.JID, timeout time.Duration, log *zerolog.Logger) []string {
	var lines []string
	do := func(name, ns string, payload xml.TokenReader, describe func([]byte) string) []byte {
		pctx, span := telemetry.StartChild(ctx, "xmpp.iq.avatar_probe", trace.WithSpanKind(trace.SpanKindClient))
		defer span.End()
		if span.IsRecording() {
			span.SetAttributes(attribute.String("xmpp.iq.namespace", ns), attribute.String("xmpp.peer.jid", to.String()))
		}
		pctx, cancel := context.WithTimeout(pctx, timeout)
		defer cancel()
		start := time.Now()
		inner, err := send(pctx, payload, stanza.IQ{Type: stanza.GetIQ, To: to, ID: newStanzaID()})
		ms := time.Since(start).Milliseconds()
		var outcome string
		if err != nil {
			telemetry.Fail(span, err)
			outcome = formatProbeErr(err)
		} else {
			outcome = describe(inner)
		}
		log.Debug().Str("jid", to.String()).Str("probe", name).Int64("elapsed_ms", ms).Str("outcome", outcome).Msg("Avatar probe")
		lines = append(lines, fmt.Sprintf("%s: %d ms, %s", name, ms, outcome))
		if err != nil {
			return nil
		}
		return inner
	}

	do("vcard-temp", vcardTempNS, vcardGetPayload(), describeVCard)

	var itemID string
	do("avatar metadata (XEP-0084)", avatarMetadataNS, pubsubItemsGetPayload(avatarMetadataNS, 1, ""), func(b []byte) string {
		var d string
		itemID, d = parseMetadata(b)
		return d
	})
	if itemID != "" {
		do("avatar data (XEP-0084)", avatarDataNS, pubsubItemsGetPayload(avatarDataNS, 0, itemID), describeAvatarData)
	}
	do("disco#info", discoInfoNS, discoInfoPayload(), describeDisco)
	return lines
}

// formatProbeReply builds the single markdown reply.
func formatProbeReply(target string, lines []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Avatar probe for `%s`\n\n", target)
	for _, l := range lines {
		b.WriteString("- " + l + "\n")
	}
	return strings.TrimRight(b.String(), "\n")
}
