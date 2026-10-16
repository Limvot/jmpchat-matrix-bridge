// Package telemetry provides optional OpenTelemetry tracing for the bridge.
//
// Tracing is off unless an OTLP endpoint is configured through the standard
// OTEL_EXPORTER_OTLP_* environment variables. When off, nothing is installed
// (no provider, goroutines, sockets or transport wrapping) and every helper
// returns its input context unchanged.
package telemetry

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracerName is the instrumentation scope name used for all spans.
const TracerName = "github.com/mautrix/jabber"

// Options configures Init.
type Options struct {
	ServiceVersion string         // br.Version
	Log            zerolog.Logger // br.Log, used for the enable/warn lines and the OTel error handler
}

const (
	protoHTTP = "http/protobuf"
	protoGRPC = "grpc"

	shutdownTimeout = 3 * time.Second
	errLogInterval  = 5 * time.Minute
)

var (
	enabled atomic.Bool
	tracer  trace.Tracer = noop.NewTracerProvider().Tracer(TracerName)
	tp      *sdktrace.TracerProvider

	initOnce     sync.Once
	shutdownOnce sync.Once

	// noopSpan is a non-recording span returned by helpers when tracing is off.
	noopSpan = trace.SpanFromContext(context.Background())
)

// Enabled reports whether tracing was successfully initialised.
func Enabled() bool { return enabled.Load() }

// config is the result of resolving the environment.
type config struct {
	enabled  bool
	protocol string
	endpoint string // effective endpoint as configured (may be empty when disabled)
	warnings []string
}

// resolveConfig decides, from the environment only, whether tracing is
// enabled and which protocol to use. It does no I/O.
func resolveConfig(getenv func(string) string) config {
	var c config
	get := func(k string) string { return strings.TrimSpace(getenv(k)) }

	if v := get("OTEL_SDK_DISABLED"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			c.warnings = append(c.warnings, "unparseable OTEL_SDK_DISABLED, treating as false")
		} else if b {
			return c
		}
	}
	if v := strings.ToLower(get("OTEL_TRACES_EXPORTER")); v != "" && v != "otlp" {
		if v != "none" {
			c.warnings = append(c.warnings, "unsupported OTEL_TRACES_EXPORTER="+v+", tracing disabled")
		}
		return c
	}
	ep := get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if ep == "" {
		ep = get("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if ep == "" {
		return c
	}
	c.endpoint = ep

	proto := get("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
	if proto == "" {
		proto = get("OTEL_EXPORTER_OTLP_PROTOCOL")
	}
	if proto == "" {
		proto = protoHTTP
	}
	switch proto {
	case protoHTTP, protoGRPC:
		c.protocol = proto
	default:
		c.warnings = append(c.warnings, "unsupported OTLP protocol "+proto+", tracing disabled")
		return c
	}
	c.enabled = true

	if u, err := url.Parse(ep); err == nil {
		switch {
		case c.protocol == protoHTTP && u.Port() == "4317":
			c.warnings = append(c.warnings, "endpoint port looks like grpc; set OTEL_EXPORTER_OTLP_PROTOCOL")
		case c.protocol == protoGRPC && u.Port() == "4318":
			c.warnings = append(c.warnings, "endpoint port looks like http/protobuf; set OTEL_EXPORTER_OTLP_PROTOCOL")
		}
	}
	return c
}

// safeEndpoint reduces an endpoint to scheme://host:port.
func safeEndpoint(ep string) string {
	u, err := url.Parse(ep)
	if err != nil || u.Host == "" {
		return "unknown"
	}
	return u.Scheme + "://" + u.Host
}

// Init configures tracing from the environment. It never fails the bridge:
// any problem results in one warning and tracing staying disabled.
func Init(ctx context.Context, o Options) {
	initOnce.Do(func() { initWith(ctx, o, osGetenv) })
}

func initWith(ctx context.Context, o Options, getenv func(string) string) {
	log := o.Log
	cfg := resolveConfig(getenv)
	for _, w := range cfg.warnings {
		log.Warn().Msg(w)
	}
	if !cfg.enabled {
		return
	}

	var exp *otlptrace.Exporter
	var err error
	switch cfg.protocol {
	case protoGRPC:
		exp, err = otlptracegrpc.New(ctx)
	default:
		exp, err = otlptracehttp.New(ctx)
	}
	if err != nil {
		log.Warn().Err(err).Msg("Failed to create OTLP trace exporter, tracing disabled")
		return
	}

	res, err := buildResource(ctx, o.ServiceVersion, getenv)
	if err != nil {
		log.Warn().Err(err).Msg("Problem building OpenTelemetry resource")
	}
	if res == nil {
		res = resource.Default()
	}

	provider := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(exp))
	otel.SetErrorHandler(newErrorHandler(log))
	otel.SetTracerProvider(provider)
	tp = provider
	tracer = provider.Tracer(TracerName)
	enabled.Store(true)

	sampler := strings.TrimSpace(getenv("OTEL_TRACES_SAMPLER"))
	if sampler == "" {
		sampler = "default"
	}
	log.Info().
		Str("protocol", cfg.protocol).
		Str("endpoint", safeEndpoint(cfg.endpoint)).
		Str("sampler", sampler).
		Msg("OpenTelemetry tracing enabled")
}

// buildResource builds the resource: defaults < DD_* < OTEL_* env.
func buildResource(ctx context.Context, version string, getenv func(string) string) (*resource.Resource, error) {
	name := "mautrix-jabber"
	if v := strings.TrimSpace(getenv("DD_SERVICE")); v != "" {
		name = v
	}
	if v := strings.TrimSpace(getenv("DD_VERSION")); v != "" {
		version = v
	}
	attrs := []attribute.KeyValue{attribute.String("service.name", name)}
	if version != "" {
		attrs = append(attrs, attribute.String("service.version", version))
	}
	if v := strings.TrimSpace(getenv("DD_ENV")); v != "" {
		attrs = append(attrs,
			attribute.String("deployment.environment.name", v),
			attribute.String("deployment.environment", v))
	}
	return resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithTelemetrySDK(),
		resource.WithFromEnv(),
	)
}

// Shutdown flushes and stops the tracer provider. It is a no-op when tracing
// is disabled and is bounded to a few seconds.
func Shutdown(ctx context.Context) {
	shutdownOnce.Do(func() {
		if !enabled.Load() || tp == nil {
			return
		}
		sctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
		defer cancel()
		if err := tp.Shutdown(sctx); err != nil {
			zerolog.Ctx(ctx).Warn().Str("error", stripURLs(err.Error())).Msg("OpenTelemetry shutdown error")
		}
	})
}

// newErrorHandler returns an OTel error handler that logs the first error and
// then at most one line per errLogInterval with a suppressed count.
func newErrorHandler(log zerolog.Logger) otel.ErrorHandler {
	var (
		mu         sync.Mutex
		last       time.Time
		suppressed int
	)
	return otel.ErrorHandlerFunc(func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		now := time.Now()
		if !last.IsZero() && now.Sub(last) < errLogInterval {
			suppressed++
			mu.Unlock()
			return
		}
		n := suppressed
		suppressed = 0
		last = now
		mu.Unlock()
		ev := log.Warn()
		if n > 0 {
			ev = ev.Int("suppressed", n)
		}
		ev.Str("error", stripURLs(fmt.Sprint(err))).Msg("OpenTelemetry export error")
	})
}
