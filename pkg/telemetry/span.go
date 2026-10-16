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
	hadParent := trace.SpanContextFromContext(ctx).IsValid()
	nctx, span := tracer.Start(ctx, name, opts...)
	if !hadParent && span.IsRecording() {
		sc := span.SpanContext()
		nctx = zerolog.Ctx(nctx).With().
			Str("trace_id", sc.TraceID().String()).
			Str("dd.trace_id", ddTraceID(sc.TraceID())).
			Logger().WithContext(nctx)
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
	return context.WithValue(nctx, remoteSpanKey{}, span)
}

// EndRemoteEvent ends the span stored by StartRemoteEvent; otherwise no-op.
func EndRemoteEvent(ctx context.Context) {
	if span, ok := ctx.Value(remoteSpanKey{}).(trace.Span); ok && span != nil {
		span.End()
	}
}

var urlRe = regexp.MustCompile(`https?://\S+`)

// stripURLs replaces every http(s) URL in s with scheme://host.
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

// AddLogFields adds trace_id and dd.trace_id when sc is valid and sampled.
func AddLogFields(c zerolog.Context, sc trace.SpanContext) zerolog.Context {
	if !sc.IsValid() || !sc.IsSampled() {
		return c
	}
	return c.Str("trace_id", sc.TraceID().String()).Str("dd.trace_id", ddTraceID(sc.TraceID()))
}
