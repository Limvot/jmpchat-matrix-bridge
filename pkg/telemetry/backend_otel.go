//go:build !sipcalls

package telemetry

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	protoHTTP = "http/protobuf"
	protoGRPC = "grpc"

	shutdownTimeout = 3 * time.Second
	errLogInterval  = 5 * time.Minute
)

var (
	tp *sdktrace.TracerProvider

	initOnce     sync.Once
	shutdownOnce sync.Once
)

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
	// Replace the SDK's default stderr logger before the exporters parse their env: its key/value
	// pairs can contain raw env values (e.g. a malformed OTEL_EXPORTER_OTLP_HEADERS with an API key).
	otel.SetLogger(logr.New(otelLogSink{log: log}))

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

	res, err := buildResource(ctx, o, getenv)
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

// buildResource builds the resource: defaults < DD_* < OTEL_* env. git.commit.sha and
// git.repository_url are what Datadog's source code integration reads from OTel resources.
func buildResource(ctx context.Context, o Options, getenv func(string) string) (*resource.Resource, error) {
	name := "mautrix-jabber"
	version := o.ServiceVersion
	commit, repo := o.CommitSHA, o.RepositoryURL
	if v := strings.TrimSpace(getenv("DD_GIT_COMMIT_SHA")); v != "" {
		commit = v
	}
	if v := strings.TrimSpace(getenv("DD_GIT_REPOSITORY_URL")); v != "" {
		repo = v
	}
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
	if commit != "" {
		attrs = append(attrs, attribute.String("git.commit.sha", commit))
	}
	if repo != "" {
		attrs = append(attrs, attribute.String("git.repository_url", repo))
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

// otelLogSink routes the OTel SDK's internal error log to zerolog. Key/value pairs are dropped
// (they may carry header values or URLs); only the message and the URL-stripped error are kept.
type otelLogSink struct{ log zerolog.Logger }

func (otelLogSink) Init(logr.RuntimeInfo)            {}
func (otelLogSink) Enabled(int) bool                 { return false }
func (otelLogSink) Info(int, string, ...any)         {}
func (s otelLogSink) WithValues(...any) logr.LogSink { return s }
func (s otelLogSink) WithName(string) logr.LogSink   { return s }
func (s otelLogSink) Error(err error, msg string, _ ...any) {
	ev := s.log.Warn()
	if err != nil {
		ev = ev.Str("error", stripURLs(err.Error()))
	}
	ev.Msg("OpenTelemetry: " + msg)
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
