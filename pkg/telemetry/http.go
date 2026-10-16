package telemetry

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var segmentRe = regexp.MustCompile(`^(_matrix|v[0-9]{1,2}|r0|[a-z_]{1,32})$`)

// urlTemplate reduces a Matrix API path to a bounded template.
func urlTemplate(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p != "" && !segmentRe.MatchString(p) {
			parts[i] = "*"
		}
	}
	return strings.Join(parts, "/")
}

func serverPort(req *http.Request) int {
	if p := req.URL.Port(); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			return n
		}
	}
	if req.URL.Scheme == "https" {
		return 443
	}
	return 80
}

// HTTPAttrs returns server.address, server.port and http.request.method for
// req. The URL is never recorded.
func HTTPAttrs(req *http.Request) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("server.address", req.URL.Hostname()),
		attribute.Int("server.port", serverPort(req)),
		attribute.String("http.request.method", req.Method),
	}
}

type matrixTransport struct{ base http.RoundTripper }

// WrapMatrixTransport wraps base so requests made with a traced context get a
// client span and a traceparent header. A nil base means http.DefaultTransport.
func WrapMatrixTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &matrixTransport{base: base}
}

func (t *matrixTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !enabled.Load() || !trace.SpanContextFromContext(req.Context()).IsValid() {
		return t.base.RoundTrip(req)
	}
	tmpl := urlTemplate(req.URL.Path)
	ctx, span := tracer.Start(req.Context(), req.Method+" "+tmpl, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(HTTPAttrs(req)...)
		span.SetAttributes(attribute.String("url.template", tmpl))
	}
	req = req.Clone(ctx)
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		Fail(span, err)
		return resp, err
	}
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode >= 400 {
		span.SetAttributes(attribute.String("error.type", strconv.Itoa(resp.StatusCode)))
	}
	if resp.StatusCode >= 500 {
		span.SetStatus(codes.Error, "")
	}
	return resp, nil
}
