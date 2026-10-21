// Package connector implements XEP-0363: HTTP File Upload for sending
// Matrix media to Jabber via the XMPP server's upload slot.
package connector

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const (
	xep0363NS          = "urn:xmpp:http:upload:0"
	slotRequestTimeout = 15 * time.Second
	uploadHTTPTimeout  = 60 * time.Second
)

// newStanzaID returns a unique stanza ID for IQ requests.
func newStanzaID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("upload-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// slotResult holds the result of an XEP-0363 slot request.
type slotResult struct {
	PutURL     string // URL for HTTP PUT
	PutHeaders map[string]string
	GetURL     string // Public URL to use in the message
}

// slotResponse is used to parse the IQ result payload.
type slotResponse struct {
	XMLName xml.Name `xml:"slot"`
	Put     struct {
		URL     string `xml:"url,attr"`
		Headers []struct {
			Name  string `xml:"name,attr"`
			Value string `xml:"value,attr"`
		} `xml:"header"`
	} `xml:"put"`
	Get struct {
		URL string `xml:"url,attr"`
	} `xml:"get"`
}

// pendingSlots holds channels for in-flight XEP-0363 slot requests.
type pendingSlots struct {
	mu sync.Mutex
	m  map[string]chan *slotResult
}

func (p *pendingSlots) register(id string) chan *slotResult {
	ch := make(chan *slotResult, 1)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.m == nil {
		p.m = make(map[string]chan *slotResult)
	}
	p.m[id] = ch
	return ch
}

func (p *pendingSlots) respond(id string, result *slotResult) bool {
	p.mu.Lock()
	ch, ok := p.m[id]
	if ok {
		delete(p.m, id)
	}
	p.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- result:
		return true
	default:
		return false
	}
}

func (p *pendingSlots) remove(id string) {
	p.mu.Lock()
	delete(p.m, id)
	p.mu.Unlock()
}

// requestReader returns an xml.TokenReader for <request xmlns='...' filename='...' size='...' content-type='...'/>.
func requestReader(filename string, size int64, contentType string) xml.TokenReader {
	attrs := []xml.Attr{
		{Name: xml.Name{Local: "xmlns"}, Value: xep0363NS},
		{Name: xml.Name{Local: "filename"}, Value: filename},
		{Name: xml.Name{Local: "size"}, Value: strconv.FormatInt(size, 10)},
		{Name: xml.Name{Local: "content-type"}, Value: contentType},
	}
	start := xml.StartElement{Name: xml.Name{Space: xep0363NS, Local: "request"}, Attr: attrs}
	end := xml.EndElement{Name: xml.Name{Space: xep0363NS, Local: "request"}}
	return xmlstream.MultiReader(
		xmlstream.Token(start),
		xmlstream.Token(end),
	)
}

// UploadFile requests an XEP-0363 slot from uploadJID, uploads data, and returns the public get URL.
// The session must have a slot result handler registered (handleSlotIQResult) for the same pendingSlots.
func UploadFile(ctx context.Context, sess *xmpp.Session, pending *pendingSlots, uploadJID jid.JID, data []byte, filename, contentType string) (getURL string, err error) {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	size := int64(len(data))

	iqID := newStanzaID()
	ch := pending.register(iqID)

	to := uploadJID
	iq := stanza.IQ{Type: stanza.GetIQ, To: to, ID: iqID}
	payload := requestReader(filename, size, contentType)
	if err := sess.Send(ctx, iq.Wrap(payload)); err != nil {
		return "", fmt.Errorf("send slot request: %w", err)
	}

	select {
	case res := <-ch:
		if res == nil {
			return "", fmt.Errorf("slot request failed or timed out")
		}
		putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, res.PutURL, bytes.NewReader(data))
		if err != nil {
			return "", fmt.Errorf("create PUT request: %w", err)
		}
		putReq.ContentLength = size
		putReq.Header.Set("Content-Type", contentType)
		for k, v := range res.PutHeaders {
			putReq.Header.Set(k, v)
		}
		client := &http.Client{Timeout: uploadHTTPTimeout}
		resp, err := client.Do(putReq)
		if err != nil {
			return "", fmt.Errorf("upload PUT: %w", redactURLError(err))
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", fmt.Errorf("upload PUT returned %d", resp.StatusCode)
		}
		return res.GetURL, nil
	case <-ctx.Done():
		pending.remove(iqID)
		return "", ctx.Err()
	case <-time.After(slotRequestTimeout):
		pending.remove(iqID)
		return "", fmt.Errorf("slot request timeout")
	}
}

// ParseSlotResult parses an IQ result payload into slotResult. Used by the IQ handler.
func ParseSlotResult(r io.Reader) (*slotResult, error) {
	var s slotResponse
	if err := xml.NewDecoder(r).Decode(&s); err != nil {
		return nil, err
	}
	res := &slotResult{
		PutURL:     s.Put.URL,
		PutHeaders: make(map[string]string),
		GetURL:     s.Get.URL,
	}
	for _, h := range s.Put.Headers {
		res.PutHeaders[h.Name] = h.Value
	}
	return res, nil
}

// ParseSlotFromStream parses a <slot> element from the token stream (start is the slot start element).
// Consumes the slot subtree and returns the result.
func ParseSlotFromStream(t xml.TokenReader, start *xml.StartElement) (*slotResult, error) {
	res := &slotResult{PutHeaders: make(map[string]string)}
	depth := 1
	inPut := false
	for depth > 0 {
		tok, err := t.Token()
		if err != nil {
			return nil, err
		}
		switch v := tok.(type) {
		case xml.StartElement:
			depth++
			switch v.Name.Local {
			case "put":
				inPut = true
				for _, a := range v.Attr {
					if a.Name.Local == "url" {
						res.PutURL = a.Value
						break
					}
				}
			case "get":
				for _, a := range v.Attr {
					if a.Name.Local == "url" {
						res.GetURL = a.Value
						break
					}
				}
			case "header":
				if inPut {
					var name, val string
					for _, a := range v.Attr {
						if a.Name.Local == "name" {
							name = a.Value
						} else if a.Name.Local == "value" {
							val = a.Value
						}
					}
					if name != "" {
						res.PutHeaders[name] = val
					}
				}
			}
		case xml.EndElement:
			depth--
			if v.Name.Local == "put" {
				inPut = false
			}
		}
	}
	return res, nil
}

// redactURLError drops the request URL from an HTTP client error (*url.Error prints it, and upload/OOB
// URLs are capability URLs that must not reach logs or spans); other errors are returned unchanged.
func redactURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) && ue.Err != nil {
		return fmt.Errorf("%s request: %w", ue.Op, ue.Err)
	}
	return err
}
