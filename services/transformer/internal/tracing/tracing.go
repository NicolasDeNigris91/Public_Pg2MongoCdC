// Package tracing wires the transformer into an OpenTelemetry tracer.
// Mirrors services/sink/internal/tracing - the two services intentionally
// duplicate this small package rather than share a Go workspace, so each
// service stays a self-contained module. If a 4th service is added,
// migrate to go.work + a /pkg/tracing/ shared module.
package tracing

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TracerName is the instrumentation library name carried by every span
// emitted through Tracer(). Stable across releases so dashboards do not
// lose their grouping.
const TracerName = "pg2mongo-cdc"

// Init bootstraps the global OTel tracer + propagator. No-op if
// OTEL_EXPORTER_OTLP_ENDPOINT is unset.
func Init(ctx context.Context, serviceName, serviceVersion string) (func(context.Context) error, error) {
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		otel.SetTextMapPropagator(propagation.TraceContext{})
		return func(context.Context) error { return nil }, nil
	}
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	exp, err := otlptrace.New(ctx, otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	))
	if err != nil {
		return nil, fmt.Errorf("tracing.Init: otlp exporter: %w", err)
	}

	// NewSchemaless avoids the schema-URL conflict that resource.Merge
	// raises when our pinned semconv version drifts from the SDK's
	// internal default.
	res := resource.NewSchemaless(
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(serviceVersion),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(2*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return func(ctx context.Context) error {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return tp.Shutdown(shutdownCtx)
	}, nil
}

// Tracer returns the package's named tracer (or a noop if Init was not called).
func Tracer() trace.Tracer {
	if otel.GetTracerProvider() == nil {
		return noop.NewTracerProvider().Tracer(TracerName)
	}
	return otel.Tracer(TracerName)
}

// KafkaHeaderCarrier adapts kgo record headers to OTel TextMapCarrier.
// Pointer-backed so Set() mutations are visible to the produced record.
type KafkaHeaderCarrier struct{ headers *[]kgo.RecordHeader }

// NewKafkaHeaderCarrier returns a carrier backed by the given header slice.
// Pointer semantics matter — Set() appends/replaces in *headers.
func NewKafkaHeaderCarrier(h *[]kgo.RecordHeader) KafkaHeaderCarrier {
	return KafkaHeaderCarrier{headers: h}
}

// Get returns the first value for key, or "" if absent.
func (c KafkaHeaderCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces the header value for key, or appends if not present.
func (c KafkaHeaderCarrier) Set(key, value string) {
	if c.headers == nil {
		return
	}
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

// Keys returns the header keys in the order they appear.
func (c KafkaHeaderCarrier) Keys() []string {
	if c.headers == nil {
		return nil
	}
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}
