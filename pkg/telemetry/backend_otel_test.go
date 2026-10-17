//go:build !sipcalls

package telemetry

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestResolveConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		enabled bool
		proto   string
		warn    string
	}{
		{"nothing", nil, false, "", ""},
		{"endpoint", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318"}, true, protoHTTP, ""},
		{"traces endpoint only", map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://c:4318/v1/traces"}, true, protoHTTP, ""},
		{"sdk disabled", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318", "OTEL_SDK_DISABLED": "true"}, false, "", ""},
		{"sdk disabled garbage", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318", "OTEL_SDK_DISABLED": "zzz"}, true, protoHTTP, "OTEL_SDK_DISABLED"},
		{"exporter none", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318", "OTEL_TRACES_EXPORTER": "none"}, false, "", ""},
		{"exporter console", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318", "OTEL_TRACES_EXPORTER": "console"}, false, "", "unsupported OTEL_TRACES_EXPORTER=console"},
		{"exporter otlp no endpoint", map[string]string{"OTEL_TRACES_EXPORTER": "otlp"}, false, "", ""},
		{"grpc", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4317", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, true, protoGRPC, ""},
		{"http/json", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318", "OTEL_EXPORTER_OTLP_PROTOCOL": "http/json"}, false, "", "unsupported OTLP protocol"},
		{"per-signal wins", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4317", "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "grpc"}, true, protoGRPC, ""},
		{"port hint", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4317"}, true, protoHTTP, "looks like grpc"},
		{"port hint grpc", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, true, protoGRPC, "looks like http"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := resolveConfig(env(tt.env))
			if c.enabled != tt.enabled || c.protocol != tt.proto {
				t.Fatalf("got enabled=%v proto=%q, want %v %q", c.enabled, c.protocol, tt.enabled, tt.proto)
			}
			joined := strings.Join(c.warnings, "|")
			if tt.warn == "" && joined != "" {
				t.Fatalf("unexpected warnings: %s", joined)
			}
			if tt.warn != "" && !strings.Contains(joined, tt.warn) {
				t.Fatalf("warnings %q lack %q", joined, tt.warn)
			}
		})
	}
}

func TestResourcePrecedence(t *testing.T) {
	get := func(m map[string]string) map[string]string {
		t.Setenv("OTEL_SERVICE_NAME", m["OTEL_SERVICE_NAME"])
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", m["OTEL_RESOURCE_ATTRIBUTES"])
		res, err := buildResource(context.Background(), Options{ServiceVersion: "1.0", CommitSHA: "abc123", RepositoryURL: "https://example.com/repo"}, env(m))
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for _, kv := range res.Attributes() {
			out[string(kv.Key)] = kv.Value.Emit()
		}
		return out
	}
	a := get(map[string]string{})
	if a["service.name"] != "mautrix-jabber" || a["service.version"] != "1.0" ||
		a["git.commit.sha"] != "abc123" || a["git.repository_url"] != "https://example.com/repo" {
		t.Errorf("defaults: %v", a)
	}
	a = get(map[string]string{"DD_GIT_COMMIT_SHA": "def456", "DD_GIT_REPOSITORY_URL": "https://example.com/fork"})
	if a["git.commit.sha"] != "def456" || a["git.repository_url"] != "https://example.com/fork" {
		t.Errorf("dd git: %v", a)
	}
	a = get(map[string]string{"DD_SERVICE": "dd", "DD_VERSION": "2", "DD_ENV": "prod"})
	if a["service.name"] != "dd" || a["service.version"] != "2" ||
		a["deployment.environment.name"] != "prod" || a["deployment.environment"] != "prod" {
		t.Errorf("dd: %v", a)
	}
	a = get(map[string]string{"DD_SERVICE": "dd", "OTEL_SERVICE_NAME": "otel", "OTEL_RESOURCE_ATTRIBUTES": "service.version=9"})
	if a["service.name"] != "otel" || a["service.version"] != "9" {
		t.Errorf("otel: %v", a)
	}
}

func TestLoggerDisabled(t *testing.T) {
	resetForTest()
	sc := sampledSC(testTraceID, testSpanID)
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	var buf bytes.Buffer
	base := zerolog.New(&buf)
	if Logger(ctx, &base) != &base || LoggerSC(sc, &base) != &base {
		t.Fatal("disabled must return base")
	}
	if n := testing.AllocsPerRun(100, func() {
		_ = Logger(ctx, &base)
		_ = LoggerSC(sc, &base)
	}); n != 0 {
		t.Fatalf("allocs = %v", n)
	}
	lctx := base.WithContext(context.Background())
	nctx, _ := Start(lctx, "x")
	if zerolog.Ctx(nctx) != zerolog.Ctx(lctx) {
		t.Fatal("Start must not touch ctx logger when disabled")
	}
	nctx = StartRemoteEvent(lctx, sc)
	if zerolog.Ctx(nctx) != zerolog.Ctx(lctx) {
		t.Fatal("StartRemoteEvent must not touch ctx logger when disabled")
	}
}

func TestLoggerFields(t *testing.T) {
	useRecorder(t)
	var buf bytes.Buffer
	base := zerolog.New(&buf)
	sc := sampledSC(testTraceID, testSpanID)
	LoggerSC(sc, &base).Info().Msg("x")
	keysOnce(t, buf.String())
	m := logKV(t, &buf)
	if m["trace_id"] != sc.TraceID().String() || m["span_id"] != sc.SpanID().String() ||
		m["dd.trace_id"] != "258" || m["dd.span_id"] != "256" {
		t.Fatalf("bad fields: %v", m)
	}
	buf.Reset()
	var ff trace.TraceID
	var fs trace.SpanID
	for i := range ff {
		ff[i] = 0xff
	}
	for i := range fs {
		fs[i] = 0xff
	}
	Logger(trace.ContextWithSpanContext(context.Background(), sampledSC(ff, fs)), &base).Info().Msg("x")
	m = logKV(t, &buf)
	if m["dd.trace_id"] != "18446744073709551615" || m["dd.span_id"] != "18446744073709551615" {
		t.Fatalf("high-bit: %v", m)
	}
	if base.GetLevel() == zerolog.Disabled {
		t.Fatal("base mutated")
	}
}

func TestLoggerUnsampledOrInvalid(t *testing.T) {
	useRecorder(t)
	var buf bytes.Buffer
	base := zerolog.New(&buf)
	unsampled := trace.NewSpanContext(trace.SpanContextConfig{TraceID: testTraceID, SpanID: testSpanID})
	if Logger(context.Background(), &base) != &base ||
		LoggerSC(trace.SpanContext{}, &base) != &base ||
		LoggerSC(unsampled, &base) != &base ||
		Logger(trace.ContextWithSpanContext(context.Background(), unsampled), &base) != &base {
		t.Fatal("expected base pointer")
	}
}

func TestStartAnnotatesRootsOnly(t *testing.T) {
	useRecorder(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())
	rctx, root := Start(ctx, "root")
	zerolog.Ctx(rctx).Info().Msg("hi")
	keysOnce(t, buf.String())
	m := logKV(t, &buf)
	if m["span_id"] != root.SpanContext().SpanID().String() || m["trace_id"] != root.SpanContext().TraceID().String() {
		t.Fatalf("bad ids: %v", m)
	}
	cctx, child := Start(rctx, "child")
	if zerolog.Ctx(cctx) != zerolog.Ctx(rctx) {
		t.Fatal("child must not re-annotate")
	}
	child.End()
	root.End()
}

func TestLinkedRootReannotates(t *testing.T) {
	useRecorder(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())
	rctx, root := Start(ctx, "root")
	lctx, l := StartLinkedRoot(rctx, "l", rctx)
	buf.Reset()
	zerolog.Ctx(lctx).Info().Msg("hi")
	keysOnce(t, buf.String())
	m := logKV(t, &buf)
	if m["trace_id"] != l.SpanContext().TraceID().String() || m["trace_id"] == root.SpanContext().TraceID().String() ||
		m["span_id"] != l.SpanContext().SpanID().String() {
		t.Fatalf("bad ids: %v", m)
	}
	l.End()
	root.End()
}

func TestStartSkipsDefaultAndDisabled(t *testing.T) {
	useRecorder(t)
	bg := context.Background()
	nctx, s := Start(bg, "r")
	if zerolog.Ctx(nctx) != zerolog.Ctx(bg) {
		t.Fatal("background root must not annotate")
	}
	s.End()
	var buf bytes.Buffer
	l := zerolog.New(&buf)
	old := zerolog.DefaultContextLogger
	zerolog.DefaultContextLogger = &l
	t.Cleanup(func() { zerolog.DefaultContextLogger = old })
	nctx, s = Start(bg, "r")
	if zerolog.Ctx(nctx) != &l {
		t.Fatal("default context logger must not be annotated")
	}
	s.End()
	zerolog.Ctx(nctx).Info().Msg("x")
	if strings.Contains(buf.String(), "trace_id") {
		t.Fatal("default logger polluted")
	}
}

func TestDerivedLoggerNotReannotated(t *testing.T) {
	useRecorder(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())
	rctx, root := Start(ctx, "root")
	dctx := zerolog.Ctx(rctx).With().Str("k", "v").Logger().WithContext(rctx)
	lctx, l := StartLinkedRoot(dctx, "l", dctx)
	buf.Reset()
	zerolog.Ctx(lctx).Info().Msg("hi")
	keysOnce(t, buf.String())
	l.End()
	root.End()
}

func TestLoggerOnAnnotatedCtxLogger(t *testing.T) {
	useRecorder(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())
	rctx, root := Start(ctx, "root")
	lg := Logger(rctx, zerolog.Ctx(rctx))
	if lg != zerolog.Ctx(rctx) {
		t.Fatal("expected same pointer")
	}
	lg.Info().Msg("x")
	keysOnce(t, buf.String())
	// a clean base gets enriched exactly once
	var b2 bytes.Buffer
	clean := zerolog.New(&b2)
	Logger(rctx, &clean).Info().Msg("x")
	keysOnce(t, b2.String())
	root.End()
}

func TestStartRemoteEventAnnotates(t *testing.T) {
	useRecorder(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())
	parent := sampledSC(testTraceID, testSpanID)
	nctx := StartRemoteEvent(ctx, parent)
	zerolog.Ctx(nctx).Info().Msg("x")
	keysOnce(t, buf.String())
	m := logKV(t, &buf)
	if m["trace_id"] != parent.TraceID().String() || m["span_id"] == parent.SpanID().String() ||
		m["span_id"] != trace.SpanContextFromContext(nctx).SpanID().String() {
		t.Fatalf("bad ids: %v", m)
	}
	EndRemoteEvent(nctx)
}

func TestInitUnreachableShutdown(t *testing.T) {
	resetForTest()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Cleanup(resetForTest)
	// Use initWith directly (Init's Once is process-global) and also exercise Init twice.
	initWith(context.Background(), Options{Log: zerolog.Nop()}, osGetenv)
	Init(context.Background(), Options{Log: zerolog.Nop()})
	Init(context.Background(), Options{Log: zerolog.Nop()})
	if !Enabled() {
		t.Fatal("expected enabled")
	}
	_, s := Start(context.Background(), "x")
	s.End()
	start := time.Now()
	Shutdown(context.Background())
	Shutdown(context.Background())
	if d := time.Since(start); d > 4*time.Second {
		t.Fatalf("shutdown took %s", d)
	}
}

func TestOTelLogSinkDropsValues(t *testing.T) {
	var buf strings.Builder
	l := logr.New(otelLogSink{log: zerolog.New(&buf)})
	l.Error(errors.New("missing '="), "parse headers", "input", "DD-API-KEY secretvalue")
	if strings.Contains(buf.String(), "secretvalue") || !strings.Contains(buf.String(), "parse headers") {
		t.Errorf("unexpected log: %s", buf.String())
	}
}
