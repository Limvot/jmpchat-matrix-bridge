package telemetry

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type remoteSpanKey struct{}

// Start always starts a span (root if ctx has no span). When disabled it
// returns ctx unchanged and a non-recording span.
func Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !enabled.Load() {
		return ctx, noopSpan
	}
	parent := trace.SpanContextFromContext(ctx)
	nctx, span := tracer.Start(ctx, name, opts...)
	if sc := span.SpanContext(); !parent.IsValid() || sc.TraceID() != parent.TraceID() {
		nctx = withLogFields(nctx, sc)
	}
	return nctx, span
}

// StartChild starts a span only if ctx already carries a valid span context.
func StartChild(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !enabled.Load() || !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, noopSpan
	}
	return tracer.Start(ctx, name, opts...)
}

// StartLinkedRoot starts a new root span on base, linked to the span in from
// (the link is omitted if from carries no valid span context).
func StartLinkedRoot(base context.Context, name string, from context.Context, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !enabled.Load() {
		return base, noopSpan
	}
	all := make([]trace.SpanStartOption, 0, len(opts)+2)
	all = append(all, trace.WithNewRoot())
	if sc := trace.SpanContextFromContext(from); sc.IsValid() {
		all = append(all, trace.WithLinks(trace.Link{SpanContext: sc}))
	}
	all = append(all, opts...)
	return Start(base, name, all...)
}

// CarrySpan returns base with only the span context of from attached.
func CarrySpan(base, from context.Context) context.Context {
	if !enabled.Load() {
		return base
	}
	return WithParent(base, trace.SpanContextFromContext(from))
}

// WithParent returns base with sc as the parent span context.
func WithParent(base context.Context, sc trace.SpanContext) context.Context {
	if !enabled.Load() || !sc.IsValid() {
		return base
	}
	return trace.ContextWithSpanContext(base, sc)
}

// StartRemoteEvent starts the "bridge.handle_remote_event" span as a child of
// parent and stores it in the returned ctx for EndRemoteEvent.
func StartRemoteEvent(ctx context.Context, parent trace.SpanContext) context.Context {
	if !enabled.Load() || !parent.IsValid() {
		return ctx
	}
	pctx := trace.ContextWithSpanContext(ctx, parent)
	nctx, span := tracer.Start(pctx, "bridge.handle_remote_event", trace.WithSpanKind(trace.SpanKindConsumer))
	nctx = withLogFields(nctx, span.SpanContext())
	return context.WithValue(nctx, remoteSpanKey{}, span)
}

// EndRemoteEvent ends the span stored by StartRemoteEvent; otherwise no-op.
func EndRemoteEvent(ctx context.Context) {
	if span, ok := ctx.Value(remoteSpanKey{}).(trace.Span); ok && span != nil {
		span.End()
	}
}

var urlRe = regexp.MustCompile(`(?:https?|mxc)://\S+`)

// stripURLs replaces every http(s) or mxc URL in s with scheme://host.
func stripURLs(s string) string {
	return urlRe.ReplaceAllStringFunc(s, func(m string) string {
		u, err := url.Parse(m)
		if err != nil || u.Host == "" {
			return "<url>"
		}
		return u.Scheme + "://" + u.Host
	})
}

// Fail records err on span with URLs reduced to scheme://host.
func Fail(span trace.Span, err error) {
	if err == nil || span == nil || !span.IsRecording() {
		return
	}
	msg := stripURLs(err.Error())
	span.RecordError(errors.New(msg))
	span.SetStatus(codes.Error, msg)
	inner := err
	var ue *url.Error
	if errors.As(err, &ue) && ue.Err != nil {
		inner = ue.Err
	}
	span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", inner)))
}

func ddTraceID(id trace.TraceID) string {
	return strconv.FormatUint(binary.BigEndian.Uint64(id[8:16]), 10)
}

type logAnnotKey struct{}

// logAnnot records a ctx-logger annotation so it can be rebuilt (new root) or recognised
// (already enriched) without ever writing a key twice.
type logAnnot struct {
	clean     *zerolog.Logger // logger the annotation was built from (no trace fields)
	annotated *zerolog.Logger // logger installed in ctx
}

// withLogFields installs in ctx a logger carrying sc's four fields. It builds from the
// clean logger of a previous annotation when ctx's logger is that annotation (new root over
// an annotated ctx), and does nothing when ctx's logger is disabled, the zerolog default
// context logger, or a logger derived from an annotated one (it may already hold the keys).
func withLogFields(ctx context.Context, sc trace.SpanContext) context.Context {
	if !sc.IsValid() || !sc.IsSampled() {
		return ctx
	}
	cur := zerolog.Ctx(ctx)
	base := cur
	if a, ok := ctx.Value(logAnnotKey{}).(logAnnot); ok {
		if cur != a.annotated {
			return ctx
		}
		base = a.clean
	}
	if base.GetLevel() == zerolog.Disabled || base == zerolog.DefaultContextLogger {
		return ctx
	}
	l := addLogFields(base.With(), sc).Logger()
	// Logger.WithContext has a value receiver and stores a pointer to its own copy, so the
	// annotated pointer must be read back from the new ctx (not &l) for the identity checks.
	nctx := l.WithContext(ctx)
	return context.WithValue(nctx, logAnnotKey{}, logAnnot{clean: base, annotated: zerolog.Ctx(nctx)})
}

// Logger returns base enriched with the trace fields of the span in ctx. It returns base
// itself (same pointer, no allocation) when tracing is disabled, ctx has no valid sampled
// span, or base is ctx's already-annotated logger. base must be a clean logger (x.log, the
// bridge logger, ce.Log); never pass a logger derived from zerolog.Ctx(ctx).With(): that may
// already hold the keys and would duplicate them.
func Logger(ctx context.Context, base *zerolog.Logger) *zerolog.Logger {
	if !enabled.Load() {
		return base
	}
	if a, ok := ctx.Value(logAnnotKey{}).(logAnnot); ok && a.annotated == base {
		return base
	}
	return LoggerSC(trace.SpanContextFromContext(ctx), base)
}

// LoggerSC is Logger for callers that hold a SpanContext but no ctx.
func LoggerSC(sc trace.SpanContext, base *zerolog.Logger) *zerolog.Logger {
	if !enabled.Load() || !sc.IsValid() || !sc.IsSampled() {
		return base
	}
	l := addLogFields(base.With(), sc).Logger()
	return &l
}

func ddSpanID(id trace.SpanID) string {
	return strconv.FormatUint(binary.BigEndian.Uint64(id[:]), 10)
}

// addLogFields adds trace_id, span_id, dd.trace_id and dd.span_id (caller checked sc).
func addLogFields(c zerolog.Context, sc trace.SpanContext) zerolog.Context {
	return c.Str("trace_id", sc.TraceID().String()).
		Str("span_id", sc.SpanID().String()).
		Str("dd.trace_id", ddTraceID(sc.TraceID())).
		Str("dd.span_id", ddSpanID(sc.SpanID()))
}

// AddLogFields is a temporary shim for jabberRemoteMessage.AddLogContext; removed once
// that caller is gone.
func AddLogFields(c zerolog.Context, sc trace.SpanContext) zerolog.Context {
	if !sc.IsValid() || !sc.IsSampled() {
		return c
	}
	return addLogFields(c, sc)
}
