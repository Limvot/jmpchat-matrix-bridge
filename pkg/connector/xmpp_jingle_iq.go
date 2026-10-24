package connector

// XEP-0166 <jingle/> IQ dispatch: forwards session IQ payloads to the connector extension
// (the call leg, when compiled in). Pure XMPP plumbing, no call logic here.

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"

	"mellium.im/xmlstream"
	"mellium.im/xmpp/stanza"
)

// jingleIQPayloadName is the payload of Jingle session IQs.
var jingleIQPayloadName = xml.Name{Space: "urn:xmpp:jingle:1", Local: "jingle"}

// jingleIQHandler consumes one <jingle/> IQ payload and hands it to the extension.
type jingleIQHandler struct{ x *xmppClient }

// HandleIQ implements mux.IQHandler: it consumes one <jingle/> payload and hands it to the
// extension.
func (h jingleIQHandler) HandleIQ(iq stanza.IQ, t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	from := iq.From.String()
	id := iq.ID
	payload, err := reencodeElement(start, t)
	if err != nil {
		return err
	}
	if h.x.conn != nil {
		if e := h.x.conn.extension(); e != nil && e.feedCallIQ(context.Background(), h.x, from, id, payload) {
			return nil
		}
	}
	return nil
}

// reencodeElement re-encodes the element whose start token was already consumed from r
// (and all of its children) into raw XML bytes.
func reencodeElement(start *xml.StartElement, r xml.TokenReader) ([]byte, error) {
	if start == nil {
		return nil, fmt.Errorf("connector: no start element")
	}
	var buf bytes.Buffer
	enc := xml.NewEncoder(&buf)
	if err := enc.EncodeToken(*start); err != nil {
		return nil, err
	}
	depth := 1
	for depth > 0 {
		tok, err := r.Token()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			err = enc.EncodeToken(t)
		case xml.EndElement:
			depth--
			err = enc.EncodeToken(t)
		default:
			err = enc.EncodeToken(tok)
		}
		if err != nil {
			return nil, err
		}
	}
	if err := enc.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
