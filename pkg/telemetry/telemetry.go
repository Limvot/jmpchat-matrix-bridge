// Package telemetry provides optional OpenTelemetry tracing for the bridge.
//
// Tracing is off unless an OTLP endpoint is configured through the standard
// OTEL_EXPORTER_OTLP_* environment variables. When off, nothing is installed
// (no provider, goroutines, sockets or transport wrapping) and every helper
// returns its input context unchanged.
package telemetry

import (
	"context"
	"sync/atomic"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracerName is the instrumentation scope name used for all spans.
const TracerName = "github.com/mautrix/jabber"

// Options configures Init.
type Options struct {
	ServiceVersion string         // br.Version
	CommitSHA      string         // git commit the binary was built from (empty if unknown)
	RepositoryURL  string         // source repository URL (empty if unknown)
	Log            zerolog.Logger // br.Log, used for the enable/warn lines and the OTel error handler
}

var (
	enabled atomic.Bool
	tracer  trace.Tracer = noop.NewTracerProvider().Tracer(TracerName)

	// noopSpan is a non-recording span returned by helpers when tracing is off.
	noopSpan = trace.SpanFromContext(context.Background())
)

// Enabled reports whether tracing was successfully initialised.
func Enabled() bool { return enabled.Load() }
