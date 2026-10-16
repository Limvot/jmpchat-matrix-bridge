package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/mautrix/jabber/pkg/telemetry"
)

// assertFields checks that out is one JSON line holding each trace key exactly once, with the
// trace id equal to wantTrace, the span id equal to wantSpan and different from notSpan.
func assertFields(t *testing.T, out string, wantTrace trace.TraceID, wantSpan trace.SpanID, notSpan trace.SpanID) {
	t.Helper()
	for _, k := range []string{"trace_id", "span_id", "dd.trace_id", "dd.span_id"} {
		if n := strings.Count(out, `"`+k+`"`); n != 1 {
			t.Errorf("key %s appears %d times in %s", k, n, out)
		}
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("bad json %q: %v", out, err)
	}
	if m["trace_id"] != wantTrace.String() {
		t.Errorf("trace_id = %v, want %s", m["trace_id"], wantTrace)
	}
	if m["span_id"] != wantSpan.String() {
		t.Errorf("span_id = %v, want %s", m["span_id"], wantSpan)
	}
	if m["span_id"] == notSpan.String() {
		t.Errorf("span_id must differ from %s", notSpan)
	}
}

// TestLogCorrelation covers disabled behaviour first, then enabled (telemetry.Init is once per
// process, so everything lives in one function).
func TestLogCorrelation(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3},
		SpanID:     trace.SpanID{4},
		TraceFlags: trace.FlagsSampled,
	})
	logWith := func(m *jabberRemoteMessage) string {
		var buf bytes.Buffer
		l := m.AddLogContext(zerolog.New(&buf).With()).Logger()
		l.Info().Msg("x")
		return buf.String()
	}

	// Disabled: logFor is x.log itself; MutateContext is identity.
	var dbuf bytes.Buffer
	x := &xmppClient{log: zerolog.New(&dbuf)}
	ctx := context.Background()
	if x.logFor(trace.ContextWithSpanContext(ctx, sc)) != &x.log {
		t.Error("logFor must return &x.log while tracing is disabled")
	}
	m := &jabberRemoteMessage{id: "abc", spanCtx: sc, x: x}
	if m.MutateContext(ctx) != ctx {
		t.Error("MutateContext must be identity while tracing is disabled")
	}
	m.PostHandle(ctx, nil)
	if out := logWith(m); !strings.Contains(out, `"message_id":"abc"`) || strings.Contains(out, "trace_id") {
		t.Errorf("AddLogContext must add only message_id: %s", out)
	}

	// Enabled.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	telemetry.Init(context.Background(), telemetry.Options{Log: zerolog.Nop()})
	if !telemetry.Enabled() {
		t.Skip("telemetry already initialised as disabled")
	}
	t.Cleanup(func() { telemetry.Shutdown(context.Background()) })

	if out := logWith(m); !strings.Contains(out, `"message_id":"abc"`) || strings.Contains(out, "trace_id") {
		t.Errorf("AddLogContext must add only message_id: %s", out)
	}

	// Framework-style ctx: fresh logger in ctx, then MutateContext.
	var buf bytes.Buffer
	lctx := zerolog.New(&buf).WithContext(ctx)
	mctx := m.MutateContext(lctx)
	got := trace.SpanContextFromContext(mctx)
	if !got.IsValid() || got.TraceID() != sc.TraceID() || got.SpanID() == sc.SpanID() {
		t.Errorf("unexpected span context %v", got)
	}
	zerolog.Ctx(mctx).Info().Msg("x")
	assertFields(t, buf.String(), sc.TraceID(), got.SpanID(), sc.SpanID())

	// ChatResync-style meta hooks.
	var meta simplevent.EventMeta
	attachTraceHooks(&meta, sc)
	var buf2 bytes.Buffer
	rctx := meta.MutateContextFunc(zerolog.New(&buf2).WithContext(ctx))
	rsc := trace.SpanContextFromContext(rctx)
	zerolog.Ctx(rctx).Info().Msg("x")
	assertFields(t, buf2.String(), sc.TraceID(), rsc.SpanID(), sc.SpanID())

	// x.logFor under a span.
	dbuf.Reset()
	x.logFor(mctx).Info().Msg("x")
	assertFields(t, dbuf.String(), sc.TraceID(), got.SpanID(), sc.SpanID())

	m.PostHandle(mctx, nil)
	zero := &jabberRemoteMessage{}
	if zero.MutateContext(ctx) != ctx {
		t.Error("zero spanCtx must be identity")
	}
}
