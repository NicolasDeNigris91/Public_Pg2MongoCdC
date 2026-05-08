// Command transformer reads CDC events from `cdc.*`, rewrites field names
// per schema/transforms/<table>.yml, and publishes to `transformed.*`.
// Offsets commit only after produce is ack'd; redelivery on failure is a
// no-op downstream because the sink's LSN gate absorbs duplicates.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"transformer/internal/mapper"
	"transformer/internal/tracing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:29092"), ",")
	groupID := env("KAFKA_GROUP_ID", "zdt-transformer")
	topicRegex := env("KAFKA_TOPIC_REGEX", `^cdc\..*`)
	rulesDir := env("RULES_DIR", "/etc/transformer/rules")
	metricsAddr := env("METRICS_ADDR", ":8080")

	log.Printf("transformer starting: brokers=%v source-topic=%s rules=%s", brokers, topicRegex, rulesDir)

	m, err := mapper.Load(rulesDir)
	if err != nil {
		log.Fatalf("mapper: %v", err)
	}
	log.Printf("loaded %d rule(s)", len(m.Rules()))

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tracingShutdown, err := tracing.Init(rootCtx, "transformer", "dev")
	if err != nil {
		log.Fatalf("tracing init: %v", err) //nolint:gocritic
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if terr := tracingShutdown(ctx); terr != nil {
			log.Printf("tracing shutdown: %v", terr)
		}
	}()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics(topicRegex),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(45*time.Second),
		kgo.HeartbeatInterval(3*time.Second),
		kgo.MetadataMaxAge(10*time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(16*1024*1024),
		// On KRaft brokers (cp-kafka 7.6.1+), producing to a not-yet-existent
		// topic hangs ProduceSync forever unless the request itself flags
		// auto-creation, even with broker-side auto.create.topics.enable=true.
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Fatalf("kgo.NewClient: %v", err) //nolint:gocritic
	}
	defer client.Close()

	// /healthz + /metrics-placeholder so compose healthchecks work.
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
		srv := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("health server: %v", err)
		}
	}()

	for rootCtx.Err() == nil {
		if err := runOnce(rootCtx, client, m); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			log.Printf("loop error, backoff 1s: %v", err)
			select {
			case <-rootCtx.Done():
			case <-time.After(time.Second):
			}
		}
	}
	log.Printf("transformer shutting down")
}

// runOnce drains one poll batch, transforms each record, produces to
// transformed.<suffix>, and commits offsets of every record that produced
// successfully. Returns the first apply/produce error so the caller can
// decide whether to back off.
//
// Tracing: each record's span starts as a root (Debezium does not
// inject `traceparent` today) and propagates downstream via the
// outgoing record's headers - injected by the OTel TextMapPropagator
// so the sink resumes the same trace ID. End-to-end view in Jaeger
// shows transformer.process_record -> sink.consume_batch ->
// sink.apply_batch -> sink.mongo_bulk_write for every event.
func runOnce(ctx context.Context, client *kgo.Client, m *mapper.Mapper) error {
	fetches := client.PollFetches(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		return errs[0].Err
	}

	var firstErr error
	var committable []*kgo.Record

	fetches.EachRecord(func(r *kgo.Record) {
		if firstErr != nil {
			return
		}
		recordCtx, span := tracing.Tracer().Start(ctx, "transformer.process_record",
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination.name", r.Topic),
				attribute.Int64("messaging.kafka.partition", int64(r.Partition)),
				attribute.Int64("messaging.kafka.offset", r.Offset),
			),
		)

		out := &kgo.Record{Topic: targetTopic(r.Topic), Key: r.Key}
		if r.Value == nil {
			// Tombstone: forward unchanged (sink recognises nil value).
			out.Value = nil
		} else {
			transformed, err := m.ApplyJSON(r.Topic, r.Value)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "apply rules failed")
				span.End()
				firstErr = err
				return
			}
			out.Value = transformed
		}

		// Inject trace context into the outgoing record's headers so
		// the sink can extract and continue the same trace.
		otel.GetTextMapPropagator().Inject(recordCtx, tracing.NewKafkaHeaderCarrier(&out.Headers))

		if err := client.ProduceSync(recordCtx, out).FirstErr(); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "produce failed")
			span.End()
			firstErr = err
			return
		}
		span.End()
		committable = append(committable, r)
	})

	if len(committable) > 0 {
		client.MarkCommitRecords(committable...)
		if err := client.CommitMarkedOffsets(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// targetTopic maps "cdc.users" → "transformed.users". Any unknown prefix
// gets "transformed." prepended so pass-through still works for future
// topic naming schemes.
func targetTopic(source string) string {
	if rest, ok := strings.CutPrefix(source, "cdc."); ok {
		return "transformed." + rest
	}
	return "transformed." + source
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
