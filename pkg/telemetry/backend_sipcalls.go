//go:build sipcalls

package telemetry

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/williamkray/mrtc-sip-bridge/pkg/obs"
	mrtctelemetry "github.com/williamkray/mrtc-sip-bridge/pkg/telemetry"
)

// serviceName is the resource service.name default of this binary.
const serviceName = "mautrix-jabber"

// mrtcInit is a test seam.
var mrtcInit = mrtctelemetry.Init

var initOnce sync.Once

// Init configures tracing and metrics through mrtc-sip-bridge's host telemetry with jabber's identity.
// It never fails the bridge. Only the first call has an effect.
func Init(ctx context.Context, o Options) {
	initOnce.Do(func() {
		mrtcInit(ctx, mrtctelemetry.Options{
			ServiceName:    serviceName,
			ServiceVersion: o.ServiceVersion,
			CommitSHA:      o.CommitSHA,
			RepositoryURL:  o.RepositoryURL,
			Log:            o.Log,
		})
		if obs.Enabled() {
			tracer = otel.GetTracerProvider().Tracer(TracerName)
			enabled.Store(true)
		}
	})
}

// Shutdown flushes and stops the providers installed by Init.
func Shutdown(ctx context.Context) { mrtctelemetry.Shutdown(ctx) }

// withLogFields annotates ctx's logger through the shared obs annotation.
func withLogFields(ctx context.Context, sc trace.SpanContext) context.Context {
	return obs.AnnotateLogger(ctx, sc)
}

// Logger returns base enriched with the trace fields of ctx's span (see obs.Logger).
func Logger(ctx context.Context, base *zerolog.Logger) *zerolog.Logger {
	return obs.Logger(ctx, base)
}

// LoggerSC is Logger for callers that hold a SpanContext but no ctx.
func LoggerSC(sc trace.SpanContext, base *zerolog.Logger) *zerolog.Logger {
	return obs.LoggerSC(sc, base)
}
