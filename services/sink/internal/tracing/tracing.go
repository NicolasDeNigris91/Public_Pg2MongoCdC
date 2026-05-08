// Package tracing wires the sink (and transformer; this file is shared
// in spirit) into an OpenTelemetry tracer. The single Init call returns
// a Shutdown function the caller MUST defer - flushing buffered spans
// at shutdown is what stops "the trace cuts off mid-batch on every
// SIGTERM" failure mode.
//
// Why OTel tracing in a CDC pipeline at all? Two on-call questions
// that nothing else answers:
//
//  1. "Doc <table>:<pk> looks wrong - which event chain produced it?"
//     Spans tagged with table + pk + lsn make this a Jaeger search.
//
//  2. "Lag p99 just spiked. Where is time being spent - decode, mongo,
//     or kafka commit?" Per-stage span timings break the histogram
//     down without sprinkling timer code through every function.
//
// Endpoint discovery: Init reads OTEL_EXPORTER_OTLP_ENDPOINT (the same
// var every OTel SDK uses). If unset, it returns a no-op shutdown and
// Tracer() returns a noop tracer so prod paths keep working untraced.
// This makes tracing strictly opt-in per environment.
package tracing

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

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

// Tracer name — every span emitted through Tracer() carries this as its
// instrumentation library name. Keep stable across releases so
// dashboards do not lose their grouping.
const TracerName = "pg2mongo-cdc"

// Init bootstraps the global OTel tracer + propagator and returns a
// shutdown func. If OTEL_EXPORTER_OTLP_ENDPOINT is unset, tracing is
// silently disabled (noop) so production deployments without an OTLP
// collector keep working.
//
// serviceName goes into resource.service.name — pick the binary name
// (sink, transformer). serviceVersion is best-effort; "dev" if unknown.
func Init(ctx context.Context, serviceName, serviceVersion string) (func(context.Context) error, error) {
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		// Cross-cut propagator still installs so an upstream-injected
		// trace context survives even when we don't sample - lets a
		// future upstream (e.g. the loadgen sidecar, instrumented later)
		// link traces if they come online before this service does.
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
	// internal default. We lose nothing structurally - service.name +
	// service.version are what Jaeger groups by, the rest is decoration.
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
		// Sample everything in dev/CI; flip to ParentBased+TraceIDRatio
		// in prod via env if cardinality becomes a concern.
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

// Tracer returns the package's named tracer. Safe to call before Init -
// returns a noop tracer in that case.
func Tracer() trace.Tracer {
	if otel.GetTracerProvider() == nil {
		return noop.NewTracerProvider().Tracer(TracerName)
	}
	return otel.Tracer(TracerName)
}
