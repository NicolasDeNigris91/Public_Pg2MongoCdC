// Command sink is the Week-2 Go sink service. It replaces the off-the-shelf
// MongoDB Kafka Connector (which lost 1 row in the chaos 01 run documented
// in docs/chaos-findings.md) with a consume loop that structurally enforces
// commit-after-side-effect + LSN-gated idempotent upserts.
//
// Config via env (all have sensible defaults for the docker-compose wiring):
//
//	KAFKA_BROKERS        comma-separated, default "kafka:29092"
//	KAFKA_GROUP_ID       default "zdt-sink"
//	KAFKA_TOPIC_REGEX    default "^cdc\\..*"
//	MONGO_URI            default "mongodb://mongo:27017/?replicaSet=rs0"
//	MONGO_DB             default "migration"
//	SCHEMA_VERSION       default 1
//	METRICS_ADDR         default ":8080"
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"zdt/sink/internal/checkpoint"
	"zdt/sink/internal/consumer"
	"zdt/sink/internal/kafka"
	"zdt/sink/internal/lag"
	"zdt/sink/internal/metrics"
	"zdt/sink/internal/tracing"
	"zdt/sink/internal/writer"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:29092"), ",")
	groupID := env("KAFKA_GROUP_ID", "zdt-sink")
	topicRegex := env("KAFKA_TOPIC_REGEX", `^cdc\..*`)
	mongoURI := env("MONGO_URI", "mongodb://mongo:27017/?replicaSet=rs0")
	mongoDB := env("MONGO_DB", "migration")
	metricsAddr := env("METRICS_ADDR", ":8080")
	schemaVer := envInt("SCHEMA_VERSION", 1)

	log.Printf("sink starting: brokers=%v topic=%s group=%s mongo=%s/%s metrics=%s schemaVer=%d",
		brokers, topicRegex, groupID, mongoURI, mongoDB, metricsAddr, schemaVer)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Tracing: opt-in via OTEL_EXPORTER_OTLP_ENDPOINT. No-op shutdown if
	// unset (production deployments without an OTLP collector keep
	// running with zero overhead beyond the propagator install).
	tracingShutdown, err := tracing.Init(rootCtx, "sink", "dev")
	if err != nil {
		log.Fatalf("tracing init: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if terr := tracingShutdown(ctx); terr != nil {
			log.Printf("tracing shutdown: %v", terr)
		}
	}()

	// Mongo
	mClient, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		// Boot-time fatal. Process exit lets the OS reclaim the signal
		// notification channel and anything else deferred; there is no
		// useful cleanup to run at this point in startup.
		log.Fatalf("mongo connect: %v", err) //nolint:gocritic
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mClient.Disconnect(ctx)
	}()
	// Metrics + health server (constructed first so the writer's onSkip
	// callback can target the registered counter directly).
	m := metrics.New()

	mongoWriter := writer.NewMongoWriter(mClient, mongoDB, schemaVer, func(table string, n int) {
		m.IdempotentSkip.WithLabelValues(table).Add(float64(n))
	})

	srv := &http.Server{
		Addr:              metricsAddr,
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           buildHTTPMux(m),
	}
	go func() {
		if serr := srv.ListenAndServe(); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			log.Printf("metrics server: %v", serr)
		}
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	// Kafka
	kc, err := kafka.New(brokers, groupID, topicRegex)
	if err != nil {
		log.Fatalf("kafka new: %v", err)
	}
	defer kc.Close()

	// Checkpointer: persists per-group progress to Mongo and exposes the
	// migration_checkpoint_staleness_seconds gauge feeding the
	// CheckpointStaleness alert. It registers its own collector on m.Reg.
	cp := checkpoint.New(mClient, mongoDB, m.Reg, checkpoint.Config{
		GroupID:      groupID,
		Interval:     10 * time.Second,
		FlushTimeout: 5 * time.Second,
		Logf:         log.Printf,
	})
	go cp.Run(rootCtx)

	// Lag probe: consults Kafka admin for the consumer group's per-partition
	// lag and exposes migration_consumer_group_lag. Co-located with the
	// process so the ConsumerLagHigh alert no longer depends on a separate
	// kafka-exporter being deployed.
	lp := lag.New(kc.Client(), m.Reg, lag.Config{
		GroupID:      groupID,
		Interval:     30 * time.Second,
		ProbeTimeout: 5 * time.Second,
		Logf:         log.Printf,
	})
	go lp.Run(rootCtx)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if ferr := cp.Flush(ctx); ferr != nil {
			log.Printf("final checkpoint flush: %v", ferr)
		}
	}()

	// Instrumented writer wraps MongoWriter to record metrics + progress.
	instrumented := newInstrumentedWriter(mongoWriter, m, cp)
	loop := &consumer.Loop{Cons: kc, W: instrumented, SchemaVer: schemaVer}

	runLoop(rootCtx, loop, m)
	log.Printf("sink stopped")
}

// Backoff schedule on consecutive ApplyBatch / Poll failures. Capped at
// maxBackoff which MUST stay well below Kafka's max.poll.interval.ms
// (default 5m): if we sleep longer than that the broker kicks us out of
// the consumer group and we lose the assignment.
const (
	baseBackoff = 500 * time.Millisecond
	maxBackoff  = 30 * time.Second
)

func runLoop(ctx context.Context, loop *consumer.Loop, m *metrics.Metrics) {
	consecutive := 0
	for {
		if ctx.Err() != nil {
			return
		}
		if err := loop.RunOnce(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			consecutive++
			m.ConsecutiveFailures.Set(float64(consecutive))
			delay := backoffDuration(consecutive)
			log.Printf("loop error (consecutive=%d, backoff=%s): %v", consecutive, delay, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}
		if consecutive != 0 {
			log.Printf("loop recovered after %d consecutive failures", consecutive)
			consecutive = 0
			m.ConsecutiveFailures.Set(0)
		}
	}
}

// backoffDuration returns the sleep before retry n (1-based). Doubles
// every step (binary exponential), clamped at maxBackoff. No jitter -
// our retries are bounded and the broker side has no thundering-herd
// concern (single-instance retry per partition).
func backoffDuration(n int) time.Duration {
	if n <= 0 {
		return baseBackoff
	}
	d := baseBackoff << (n - 1) // 500ms, 1s, 2s, 4s, ...
	if d <= 0 || d > maxBackoff {
		return maxBackoff
	}
	return d
}

func buildHTTPMux(m *metrics.Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.HTTPHandler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

// instrumentedWriter decorates a Writer with Prometheus counters and the
// checkpoint progress hook. Kept in main.go because it is composition of
// three packages - none of them owns the others.
type instrumentedWriter struct {
	inner consumer.Writer
	m     *metrics.Metrics
	cp    *checkpoint.Checkpointer
}

func newInstrumentedWriter(inner consumer.Writer, m *metrics.Metrics, cp *checkpoint.Checkpointer) *instrumentedWriter {
	return &instrumentedWriter{inner: inner, m: m, cp: cp}
}

func (i *instrumentedWriter) ApplyBatch(ctx context.Context, evs []writer.CDCEvent) error {
	if err := i.inner.ApplyBatch(ctx, evs); err != nil {
		i.m.WriteErrors.WithLabelValues("mongo", classify(err)).Inc()
		return err
	}
	now := time.Now()
	var maxLSN int64
	for _, ev := range evs {
		i.m.EventsProcessed.WithLabelValues("sink", ev.Table, string(ev.Op)).Inc()
		if ev.LSN > maxLSN {
			maxLSN = ev.LSN
		}
		// SourceTsMs == 0 is "unknown" (snapshot reads, malformed
		// Debezium envelopes). Skip rather than emit lag = epoch.
		if ev.SourceTsMs > 0 {
			lag := now.Sub(time.UnixMilli(ev.SourceTsMs)).Seconds()
			if lag < 0 {
				// Source clock ahead of sink clock; clamp so the
				// histogram does not grow a useless negative bucket.
				lag = 0
			}
			i.m.ReplicationLag.WithLabelValues(ev.Table).Observe(lag)
		}
	}
	if i.cp != nil {
		i.cp.MarkProgress(maxLSN, len(evs))
	}
	return nil
}

// classify maps a Mongo write error onto a small, bounded set of reason labels
// for migration_write_errors_total. We prefer typed inspection over substring
// matching: connection-level failures wear net.Error / mongo.CommandError, and
// context.Canceled / DeadlineExceeded carry their own sentinels. Unknown
// errors collapse to "other" so cardinality stays bounded even if Mongo grows
// new error shapes.
func classify(err error) string {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "context"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "timeout"
		}
		return "connection"
	}
	var serverErr mongo.ServerError
	if errors.As(err, &serverErr) {
		switch {
		case serverErr.HasErrorCode(11000):
			return "duplicate_key"
		case serverErr.HasErrorLabel("NetworkError"):
			return "connection"
		case serverErr.HasErrorLabel("RetryableWriteError"):
			return "retryable"
		}
		return "server"
	}
	return "other"
}

// --- env helpers ---

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("env %s: not an int %q, using default %d", key, v, def)
	}
	return def
}
