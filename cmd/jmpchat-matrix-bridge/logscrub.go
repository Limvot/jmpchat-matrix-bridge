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
var scrubbedPathParts = []string{"/send/", "/redact/", "/state/"}

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
	const key = `"req_body":`
	idx := bytes.Index(p, []byte(key))
	if idx < 0 {
		return p
	}
	var head struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(p, &head); err != nil {
		return p
	}
	path := head.URL
	if u, err := url.Parse(head.URL); err == nil {
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
	start := idx + len(key)
	dec := json.NewDecoder(bytes.NewReader(p[start:]))
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return p
	}
	end := start + int(dec.InputOffset())
	out := make([]byte, 0, len(p))
	out = append(out, p[:start]...)
	out = append(out, redactedBody...)
	return append(out, p[end:]...)
}
