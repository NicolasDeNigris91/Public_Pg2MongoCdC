// Package consumer runs the Kafka -> Mongo consume loop. Offsets commit
// only after the downstream write succeeds.
package consumer

import (
	"context"
	"errors"
	"fmt"

	"zdt/sink/internal/decoder"
	"zdt/sink/internal/tracing"
	"zdt/sink/internal/writer"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Record is the minimum view of a Kafka record the loop needs. Raw is an
// opaque handle for the underlying client (e.g. *kgo.Record) so MarkCommit
// can pass it back without the loop knowing the concrete type. Headers
// carry trace-context propagation (`traceparent`, `tracestate`) injected
// by the upstream producer.
type Record struct {
	Key, Value []byte
	Offset     int64
	Partition  int32
	Topic      string
	Headers    []HeaderKV
	Raw        any
}

// HeaderKV is a copy of one Kafka record header (key + value).
// Mirrors kgo.RecordHeader minimally so the consumer interface stays
// franz-go-free.
type HeaderKV struct {
	Key   string
	Value []byte
}

// KafkaConsumer is the subset of the broker client the loop drives:
// poll a batch, mark records for commit, then commit marked offsets.
type KafkaConsumer interface {
	Poll(ctx context.Context) ([]Record, error)
	MarkCommit(r Record)
	CommitMarked(ctx context.Context) error
}

// Writer applies a decoded CDC batch to the downstream sink. The
// implementation is responsible for idempotency by LSN.
type Writer interface {
	ApplyBatch(ctx context.Context, evs []writer.CDCEvent) error
}

// Loop holds the consume/apply/commit dependencies for one sink
// instance. RunOnce drains one batch per call.
type Loop struct {
	Cons         KafkaConsumer
	W            Writer
	SchemaVer    int
	SkipDecodeEr bool
}

// RunOnce drains one Poll batch, applies it as a single ApplyBatch, and
// commits every offset iff the batch succeeded. Tombstones skip dispatch
// but still mark so the pipeline does not stall on them. Any decode or
// apply error returns without committing; Kafka redelivers on next poll.
//
// Tracing: extracts trace context from the first record's headers (the
// upstream producer is expected to have injected `traceparent`),
// emits one "sink.consume_batch" span per RunOnce with batch.size +
// topic + partition attributes. ApplyBatch runs under a child
// "sink.apply_batch" span. A no-op call (zero records) emits no span,
// keeping the trace volume proportional to actual work.
func (l *Loop) RunOnce(ctx context.Context) error {
	records, err := l.Cons.Poll(ctx)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}

	// Continue the trace started by the upstream transformer if the
	// producer injected `traceparent`. Falls through to a brand-new
	// trace if not (single-service traces are still useful for
	// per-stage timing).
	if len(records[0].Headers) > 0 {
		ctx = otel.GetTextMapPropagator().Extract(ctx, recordHeaderCarrier(records[0].Headers))
	}

	ctx, span := tracing.Tracer().Start(ctx, "sink.consume_batch",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.Int("batch.size", len(records)),
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", records[0].Topic),
			attribute.Int64("messaging.kafka.partition", int64(records[0].Partition)),
		),
	)
	defer span.End()

	events := make([]writer.CDCEvent, 0, len(records))
	for _, r := range records {
		ev, derr := decoder.Decode(r.Key, r.Value)
		if errors.Is(derr, decoder.ErrTombstone) {
			continue
		}
		if derr != nil {
			err := fmt.Errorf("decode offset=%d: %w", r.Offset, derr)
			span.RecordError(err)
			return err
		}
		events = append(events, ev)
	}
	span.SetAttributes(attribute.Int("batch.events_after_tombstone_filter", len(events)))

	if len(events) > 0 {
		applyCtx, applySpan := tracing.Tracer().Start(ctx, "sink.apply_batch",
			trace.WithAttributes(attribute.Int("batch.size", len(events))),
		)
		werr := l.W.ApplyBatch(applyCtx, events)
		if werr != nil {
			applySpan.RecordError(werr)
			applySpan.End()
			err := fmt.Errorf("apply batch (size=%d): %w", len(events), werr)
			span.RecordError(err)
			return err
		}
		applySpan.End()
	}

	for _, r := range records {
		l.Cons.MarkCommit(r)
	}
	if cerr := l.Cons.CommitMarked(ctx); cerr != nil {
		span.RecordError(cerr)
		return cerr
	}
	return nil
}

// recordHeaderCarrier adapts the consumer-package's HeaderKV slice to
// OTel's TextMapCarrier. We define it locally rather than depend on
// the franz-go-typed carrier in the tracing package - the consumer
// interface stays Kafka-client-free.
type recordHeaderCarrier []HeaderKV

func (c recordHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set is unused on the consume path (we extract, never inject).
func (c recordHeaderCarrier) Set(string, string) {}

func (c recordHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, h.Key)
	}
	return keys
}
