package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
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

var (
	testTraceID = trace.TraceID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2}
	testSpanID  = trace.SpanID{0, 0, 0, 0, 0, 0, 1, 0}
)

func sampledSC(tid trace.TraceID, sid trace.SpanID) trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
}

func logKV(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	m := map[string]any{}
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("bad json %q: %v", buf.String(), err)
	}
	return m
}

func keysOnce(t *testing.T, out string) {
	t.Helper()
	for _, k := range []string{"trace_id", "span_id", "dd.trace_id", "dd.span_id"} {
		if n := strings.Count(out, `"`+k+`"`); n != 1 {
			t.Errorf("key %s appears %d times: %s", k, n, out)
		}
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

func TestStripURLsMXC(t *testing.T) {
	got := stripURLs("download mxc://hs.example/AbCdSecret failed")
	if got != "download mxc://hs.example failed" {
		t.Errorf("got %q", got)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
