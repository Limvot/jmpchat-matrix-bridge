package telemetry

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func resetForTest() {
	enabled.Store(false)
	tracer = noop.NewTracerProvider().Tracer(TracerName)
}

func useRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tpv := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	tracer = tpv.Tracer(TracerName)
	enabled.Store(true)
	t.Cleanup(resetForTest)
	return rec
}

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

type ctxKey struct{}

func TestDisabledIdentity(t *testing.T) {
	resetForTest()
	before := runtime.NumGoroutine()
	Init(context.Background(), Options{Log: zerolog.Nop()})
	if Enabled() {
		t.Fatal("must be disabled without env")
	}
	if n := runtime.NumGoroutine(); n != before {
		t.Fatalf("goroutines changed %d -> %d", before, n)
	}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled})
	checks := map[string]func() (context.Context, trace.Span){
		"Start":           func() (context.Context, trace.Span) { return Start(ctx, "x") },
		"StartChild":      func() (context.Context, trace.Span) { return StartChild(ctx, "x") },
		"StartLinkedRoot": func() (context.Context, trace.Span) { return StartLinkedRoot(ctx, "x", ctx) },
	}
	for name, f := range checks {
		c, s := f()
		if c != ctx || s.IsRecording() {
			t.Errorf("%s not identity/no-op", name)
		}
	}
	if CarrySpan(ctx, ctx) != ctx || WithParent(ctx, sc) != ctx || StartRemoteEvent(ctx, sc) != ctx {
		t.Error("ctx helpers not identity")
	}
	EndRemoteEvent(ctx) // must not panic
	Shutdown(context.Background())
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	})
	w := WrapMatrixTransport(base)
	req, _ := http.NewRequest("GET", "http://x/", nil)
	if _, err := w.RoundTrip(req); err != nil || req.Header.Get("traceparent") != "" {
		t.Error("transport should pass through")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestResourcePrecedence(t *testing.T) {
	get := func(m map[string]string) map[string]string {
		t.Setenv("OTEL_SERVICE_NAME", m["OTEL_SERVICE_NAME"])
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", m["OTEL_RESOURCE_ATTRIBUTES"])
		res, err := buildResource(context.Background(), "1.0", env(m))
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
	if a["service.name"] != "mautrix-jabber" || a["service.version"] != "1.0" {
		t.Errorf("defaults: %v", a)
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

func TestStartChild(t *testing.T) {
	rec := useRecorder(t)
	_, s := StartChild(context.Background(), "orphan")
	s.End()
	if len(rec.Ended()) != 0 {
		t.Fatal("orphan child must not record")
	}
	ctx, root := Start(context.Background(), "root")
	_, ch := StartChild(ctx, "child")
	ch.End()
	root.End()
	ended := rec.Ended()
	if len(ended) != 2 || ended[0].SpanContext().TraceID() != ended[1].SpanContext().TraceID() ||
		ended[0].Parent().SpanID() != root.SpanContext().SpanID() {
		t.Fatalf("bad child: %v", ended)
	}
}

func TestLinkedRootAndCarry(t *testing.T) {
	rec := useRecorder(t)
	origin, o := Start(context.WithValue(context.Background(), ctxKey{}, 1), "origin")
	base := context.Background()
	lctx, l := StartLinkedRoot(base, "linked", origin)
	l.End()
	if l.SpanContext().TraceID() == o.SpanContext().TraceID() {
		t.Error("linked root must be new trace")
	}
	c := CarrySpan(base, origin)
	if c.Value(ctxKey{}) != nil {
		t.Error("CarrySpan leaked ctx values")
	}
	_, cs := StartChild(c, "carried")
	cs.End()
	o.End()
	_ = lctx
	for _, s := range rec.Ended() {
		switch s.Name() {
		case "linked":
			if len(s.Links()) != 1 || s.Links()[0].SpanContext.SpanID() != o.SpanContext().SpanID() {
				t.Error("missing link")
			}
		case "carried":
			if s.SpanContext().TraceID() != o.SpanContext().TraceID() {
				t.Error("carried span in wrong trace")
			}
		}
	}
	// no valid origin: no link
	_, l2 := StartLinkedRoot(base, "nolink", base)
	l2.End()
	for _, s := range rec.Ended() {
		if s.Name() == "nolink" && len(s.Links()) != 0 {
			t.Error("unexpected link")
		}
	}
}

func TestRemoteEvent(t *testing.T) {
	rec := useRecorder(t)
	pctx, p := Start(context.Background(), "queue")
	sc := p.SpanContext()
	p.End()
	_ = pctx
	ctx := StartRemoteEvent(context.Background(), sc)
	if len(rec.Ended()) != 1 {
		t.Fatal("handle span ended too early")
	}
	EndRemoteEvent(ctx)
	EndRemoteEvent(ctx)
	ended := rec.Ended()
	if len(ended) != 2 || ended[1].Name() != "bridge.handle_remote_event" ||
		ended[1].Parent().SpanID() != sc.SpanID() || ended[1].SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("bad remote span: %v", ended)
	}
	// invalid parent: unchanged
	bg := context.Background()
	if StartRemoteEvent(bg, trace.SpanContext{}) != bg {
		t.Error("invalid parent should be identity")
	}
	// EndRemoteEvent must not end an unrelated span
	uctx, u := Start(bg, "unrelated")
	EndRemoteEvent(uctx)
	if u.(sdktrace.ReadWriteSpan).EndTime().After(time.Time{}) {
		t.Error("unrelated span ended")
	}
	u.End()
}

func TestLogFields(t *testing.T) {
	useRecorder(t)
	var buf bytes.Buffer
	lg := zerolog.New(&buf)
	ctx := lg.WithContext(context.Background())
	ctx, root := Start(ctx, "root")
	ctx2, child := Start(ctx, "child")
	zerolog.Ctx(ctx2).Info().Msg("hi")
	child.End()
	root.End()
	out := buf.String()
	if strings.Count(out, `"trace_id"`) != 1 || strings.Count(out, `"dd.trace_id"`) != 1 {
		t.Fatalf("expected exactly one of each: %s", out)
	}
	id := trace.TraceID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: id, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled})
	buf.Reset()
	l := zerolog.New(&buf)
	l1 := AddLogFields(l.With(), sc).Logger()
	l1.Info().Msg("x")
	if !strings.Contains(buf.String(), `"dd.trace_id":"258"`) || !strings.Contains(buf.String(), id.String()) {
		t.Fatalf("bad fields: %s", buf.String())
	}
	buf.Reset()
	l2 := AddLogFields(l.With(), trace.SpanContext{}).Logger()
	l2.Info().Msg("x")
	if strings.Contains(buf.String(), "trace_id") {
		t.Error("invalid sc must add nothing")
	}
}

func TestFail(t *testing.T) {
	rec := useRecorder(t)
	_, s := Start(context.Background(), "f")
	Fail(s, &urlErr{})
	Fail(s, nil)
	s.End()
	got := rec.Ended()[0]
	if got.Status().Code != 1 && got.Status().Code.String() != "Error" {
		t.Errorf("status: %v", got.Status())
	}
	if strings.Contains(got.Status().Description, "/secret") || !strings.Contains(got.Status().Description, "https://host.example") {
		t.Errorf("url not scrubbed: %q", got.Status().Description)
	}
	var typ string
	for _, kv := range got.Attributes() {
		if kv.Key == attribute.Key("error.type") {
			typ = kv.Value.AsString()
		}
	}
	if typ == "" {
		t.Error("error.type missing")
	}
	if ev := got.Events(); len(ev) != 1 || strings.Contains(ev[0].Attributes[0].Value.Emit(), "/secret") {
		t.Errorf("event not scrubbed: %v", ev)
	}
}

type urlErr struct{}

func (*urlErr) Error() string {
	return "PUT https://host.example/upload/secret?x=1 failed for user@example.com/res"
}

func TestURLTemplate(t *testing.T) {
	for in, want := range map[string]string{
		"/_matrix/client/v3/rooms/!abc:hs/send/m.room.message/mautrix-go_123": "/_matrix/client/v3/rooms/*/send/*/*",
		"/_matrix/client/v3/profile/@jabber_x=40y:hs/displayname":             "/_matrix/client/v3/profile/*/displayname",
		"/_matrix/client/v1/media/download/hs.example/AbCd":                   "/_matrix/client/v1/media/download/*/*",
	} {
		if got := urlTemplate(in); got != want {
			t.Errorf("%s: got %s want %s", in, got, want)
		}
	}
}

func TestTransport(t *testing.T) {
	rec := useRecorder(t)
	var gotTP string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTP = r.Header.Get("traceparent")
		w.WriteHeader(404)
		_, _ = w.Write([]byte("body"))
	}))
	defer srv.Close()
	var baseBody io.ReadCloser
	rt := WrapMatrixTransport(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(r)
		if resp != nil {
			baseBody = resp.Body
		}
		return resp, err
	}))
	// no parent
	req, _ := http.NewRequest("GET", srv.URL+"/_matrix/client/v3/rooms/!a:b/send/m.x/t1?q=secret", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if gotTP != "" || len(rec.Ended()) != 0 {
		t.Fatal("untraced request must be untouched")
	}
	// with parent
	ctx, root := Start(context.Background(), "root")
	req, _ = http.NewRequestWithContext(ctx, "GET", srv.URL+"/_matrix/client/v3/rooms/!a:b/send/m.x/t1?q=secret", nil)
	req.Header.Set("X-Orig", "1")
	resp, err = rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	root.End()
	if resp.Body != baseBody {
		t.Error("response body was wrapped")
	}
	if req.Header.Get("traceparent") != "" || req.Header.Get("X-Orig") != "1" {
		t.Error("caller request mutated")
	}
	if !strings.Contains(gotTP, root.SpanContext().TraceID().String()) {
		t.Errorf("traceparent %q", gotTP)
	}
	var found bool
	for _, s := range rec.Ended() {
		if s.Name() == "GET /_matrix/client/v3/rooms/*/send/*/*" {
			found = true
			for _, kv := range s.Attributes() {
				if strings.Contains(kv.Value.Emit(), "secret") {
					t.Error("query leaked")
				}
			}
		}
	}
	if !found {
		t.Errorf("transport span missing: %v", rec.Ended())
	}
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
