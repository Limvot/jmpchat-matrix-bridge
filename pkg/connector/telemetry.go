package connector

import (
	"context"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Stop implements bridgev2.StoppableNetwork. It is called after all network
// clients have disconnected and is the flush point for tracing.
func (c *JabberConnector) Stop() {
	if e := c.extension(); e != nil {
		e.stop()
	}
	telemetry.Shutdown(context.Background())
}

var _ bridgev2.StoppableNetwork = (*JabberConnector)(nil)

// queueRemoteEvent replaces x.login.QueueRemoteEvent. When ctx carries a span
// it records a queueing span and attaches the span context to the event so the
// framework's handling of it joins the same trace. The span context must be set
// before QueueRemoteEvent: with portal_event_buffer 0 the event is handled
// synchronously inside the call.
func (x *xmppClient) queueRemoteEvent(ctx context.Context, evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
	_, span := telemetry.StartChild(ctx, "bridge.queue_remote_event", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if sc := span.SpanContext(); sc.IsValid() {
		switch e := evt.(type) {
		case *jabberRemoteMessage:
			e.spanCtx = sc
		case *simplevent.ChatResync:
			attachTraceHooks(&e.EventMeta, sc)
		case *simplevent.PreConvertedMessage:
			attachTraceHooks(&e.EventMeta, sc)
		case *simplevent.MessageRemove:
			attachTraceHooks(&e.EventMeta, sc)
		}
		span.SetAttributes(attribute.String("xmpp.peer.jid", string(evt.GetPortalKey().ID)))
	}
	var res bridgev2.EventHandlingResult
	if x.queueHook != nil {
		res = x.queueHook(evt)
	} else {
		res = x.login.QueueRemoteEvent(evt)
	}
	if res.Error != nil {
		telemetry.Fail(span, res.Error)
	}
	return res
}

// attachTraceHooks makes the framework start and end a handling span for m.
func attachTraceHooks(m *simplevent.EventMeta, sc trace.SpanContext) {
	if m.MutateContextFunc == nil {
		m.MutateContextFunc = func(ctx context.Context) context.Context {
			return telemetry.StartRemoteEvent(ctx, sc)
		}
	}
	if m.PostHandleFunc == nil {
		m.PostHandleFunc = func(ctx context.Context, _ *bridgev2.Portal) {
			telemetry.EndRemoteEvent(ctx)
		}
	}
}

// logFor returns x.log enriched with the trace fields of ctx's span (x.log itself when none).
func (x *xmppClient) logFor(ctx context.Context) *zerolog.Logger {
	return telemetry.Logger(ctx, &x.log)
}
