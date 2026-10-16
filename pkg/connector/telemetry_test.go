package connector

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"

	"github.com/mautrix/jabber/pkg/telemetry"
)

func TestRemoteMessageTraceHooks(t *testing.T) {
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

	// Disabled (or zero span context): identity and no trace fields.
	m := &jabberRemoteMessage{id: "abc", spanCtx: sc}
	ctx := context.Background()
	if m.MutateContext(ctx) != ctx {
		t.Error("MutateContext must be identity while tracing is disabled")
	}
	m.PostHandle(ctx, nil)
	if !strings.Contains(logWith(&jabberRemoteMessage{id: "abc"}), `"message_id":"abc"`) ||
		strings.Contains(logWith(&jabberRemoteMessage{id: "abc"}), "trace_id") {
		t.Error("zero spanCtx must add no trace fields")
	}

	// Enabled: span started in MutateContext is a child of spanCtx.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	telemetry.Init(context.Background(), telemetry.Options{Log: zerolog.Nop()})
	if !telemetry.Enabled() {
		t.Skip("telemetry already initialised as disabled")
	}
	t.Cleanup(func() { telemetry.Shutdown(context.Background()) })
	mctx := m.MutateContext(ctx)
	got := trace.SpanContextFromContext(mctx)
	if !got.IsValid() || got.TraceID() != sc.TraceID() || got.SpanID() == sc.SpanID() {
		t.Errorf("unexpected span context %v", got)
	}
	m.PostHandle(mctx, nil)
	out := logWith(m)
	if !strings.Contains(out, sc.TraceID().String()) || !strings.Contains(out, `"dd.trace_id"`) {
		t.Errorf("missing trace fields: %s", out)
	}
	zero := &jabberRemoteMessage{}
	if zero.MutateContext(ctx) != ctx {
		t.Error("zero spanCtx must be identity")
	}
}
