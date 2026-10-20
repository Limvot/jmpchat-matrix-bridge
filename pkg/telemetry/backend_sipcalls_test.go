//go:build sipcalls

package telemetry

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/williamkray/mrtc-sip-bridge/pkg/obs"
	mrtctelemetry "github.com/williamkray/mrtc-sip-bridge/pkg/telemetry"
)

const mrtcScope = "github.com/williamkray/mrtc-sip-bridge"

func resetEmbedded() {
	enabled.Store(false)
	tracer = noop.NewTracerProvider().Tracer(TracerName)
	obs.Use(obs.Providers{})
}

// useEmbeddedRecorder installs one recording provider for both jabber's tracer and obs.
func useEmbeddedRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tpv := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	tracer = tpv.Tracer(TracerName)
	enabled.Store(true)
	obs.Use(obs.Providers{Tracer: tpv})
	t.Cleanup(resetEmbedded)
	return rec
}

func TestEmbeddedParenting(t *testing.T) {
	rec := useEmbeddedRecorder(t)
	ctx, parent := Start(context.Background(), "jabber.parent")
	_, child := obs.Start(ctx, "mrtc.child")
	child.End()
	parent.End()

	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
	c, p := spans[0], spans[1]
	if c.Name() != "mrtc.child" || p.Name() != "jabber.parent" {
		t.Fatalf("unexpected span order %q, %q", c.Name(), p.Name())
	}
	if c.SpanContext().TraceID() != p.SpanContext().TraceID() {
		t.Error("child and parent are in different traces")
	}
	if c.Parent().SpanID() != p.SpanContext().SpanID() {
		t.Error("child parent ID does not match parent span ID")
	}
	if got := p.InstrumentationScope().Name; got != TracerName {
		t.Errorf("parent scope %q, want %q", got, TracerName)
	}
	if got := c.InstrumentationScope().Name; got != mrtcScope {
		t.Errorf("child scope %q, want %q", got, mrtcScope)
	}
}

func annotatedCtx(buf *bytes.Buffer) context.Context {
	l := zerolog.New(buf)
	return l.WithContext(context.Background())
}

func TestNoDoubleAnnotation(t *testing.T) {
	t.Run("jabber root over obs ctx", func(t *testing.T) {
		useEmbeddedRecorder(t)
		var buf bytes.Buffer
		octx, osp := obs.Start(annotatedCtx(&buf), "obs.root")
		defer osp.End()
		jctx, jsp := StartLinkedRoot(octx, "jabber.root", octx)
		defer jsp.End()
		zerolog.Ctx(jctx).Info().Msg("x")
		if n := strings.Count(buf.String(), `"trace_id"`); n != 1 {
			t.Fatalf("trace_id appears %d times in %q", n, buf.String())
		}
	})
	t.Run("obs linked root over jabber ctx", func(t *testing.T) {
		useEmbeddedRecorder(t)
		var buf bytes.Buffer
		jctx, jsp := Start(annotatedCtx(&buf), "jabber.root")
		defer jsp.End()
		octx, osp := obs.StartLinkedRoot(jctx, "obs.root", jctx)
		defer osp.End()
		zerolog.Ctx(octx).Info().Msg("x")
		if n := strings.Count(buf.String(), `"trace_id"`); n != 1 {
			t.Fatalf("trace_id appears %d times in %q", n, buf.String())
		}
	})
}

func TestInitServiceName(t *testing.T) {
	origInit := mrtcInit
	t.Cleanup(func() { mrtcInit = origInit; initOnce = sync.Once{}; resetEmbedded() })
	initOnce = sync.Once{}
	var got mrtctelemetry.Options
	mrtcInit = func(_ context.Context, o mrtctelemetry.Options) { got = o }
	Init(context.Background(), Options{ServiceVersion: "1.2.3", CommitSHA: "abc", RepositoryURL: "https://example.invalid/r", Log: zerolog.Nop()})
	if got.ServiceName != "jmpchat-matrix-bridge" || got.ServiceVersion != "1.2.3" || got.CommitSHA != "abc" || got.RepositoryURL != "https://example.invalid/r" {
		t.Fatalf("unexpected options %+v", got)
	}
	if Enabled() {
		t.Error("Init must not enable tracing when the delegate installed nothing")
	}
}
