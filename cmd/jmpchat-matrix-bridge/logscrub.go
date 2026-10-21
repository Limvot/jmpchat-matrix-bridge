// Adapted from github.com/keplersj/mautrix-xmpp (MIT, see LICENSE-THIRD-PARTY).

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/rs/zerolog"
	"go.mau.fi/zeroconfig"
)

// mautrix-go logs the JSON body of Matrix API requests at debug level. For event sends,
// redactions and state events that body is bridged content (messages, retraction reasons, topics).
// scrubLogging wraps every configured log writer so those bodies are replaced (issue #12: never
// log content). There is deliberately no switch to turn this off.

var redactedBody = []byte(`"<content omitted>"`)

// scrubbedPathParts are the request path segments whose bodies carry bridged content.
var scrubbedPathParts = []string{"/send/", "/redact/", "/state/", "/createRoom", "/profile/"}

// scrubLogging rewrites the logging config in place so each writer goes through a scrubbing wrapper.
func scrubLogging(cfg *zeroconfig.Config) {
	for i, orig := range cfg.Writers {
		typ := zeroconfig.WriterType(fmt.Sprintf("jmpchat-scrubbed-%d", i))
		inner := orig
		zeroconfig.RegisterWriter(typ, func(*zeroconfig.WriterConfig) (io.Writer, error) {
			w, err := inner.Compile()
			if err != nil {
				return nil, err
			}
			return scrubWriter{inner: w}, nil
		})
		// The wrapper receives raw JSON; formatting and level filtering happen in the inner writer.
		cfg.Writers[i] = zeroconfig.WriterConfig{Type: typ, Format: zeroconfig.LogFormatJSON}
	}
}

type scrubWriter struct {
	inner io.Writer
}

func (w scrubWriter) Write(p []byte) (int, error) {
	_, err := w.inner.Write(scrubLine(p))
	return len(p), err
}

func (w scrubWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	line := scrubLine(p)
	var err error
	if lw, ok := w.inner.(zerolog.LevelWriter); ok {
		_, err = lw.WriteLevel(level, line)
	} else {
		_, err = w.inner.Write(line)
	}
	return len(p), err
}

// scrubLine replaces the req_body field of request log lines whose URL path is an event send,
// a redaction or a state event. Other lines, and malformed ones, are returned unchanged.
func scrubLine(p []byte) []byte {
	if !bytes.Contains(p, []byte(`"req_body"`)) {
		return p
	}
	// Locate the top-level req_body by token walking, so a context field whose text merely contains
	// `"req_body":` cannot misdirect the scrub.
	dec := json.NewDecoder(bytes.NewReader(p))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return p
	}
	var reqURL string
	start, end := -1, -1
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return p
		}
		k, _ := kt.(string)
		// Offset of the value: skip the colon and whitespace after the key in the raw input.
		off := int(dec.InputOffset())
		for off < len(p) && (p[off] == ':' || p[off] == ' ' || p[off] == '\t' || p[off] == '\n' || p[off] == '\r') {
			off++
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return p
		}
		switch k {
		case "url":
			_ = json.Unmarshal(raw, &reqURL)
		case "req_body":
			start, end = off, int(dec.InputOffset())
		}
	}
	if start < 0 {
		return p
	}
	path := reqURL
	if u, err := url.Parse(reqURL); err == nil {
		path = u.Path
	}
	scrub := false
	for _, part := range scrubbedPathParts {
		if strings.Contains(path, part) {
			scrub = true
			break
		}
	}
	if !scrub {
		return p
	}
	out := make([]byte, 0, len(p))
	out = append(out, p[:start]...)
	out = append(out, redactedBody...)
	return append(out, p[end:]...)
}
