// Package checkpoint persists the sink's logical progress (consumer-group
// id + max LSN seen) into a Mongo collection and exposes a Prometheus
// gauge measuring how long ago the last successful checkpoint write
// completed.
//
// Why both a doc and a gauge? The doc is the disaster-recovery anchor
// described in ADR-003: if the Kafka __consumer_offsets topic is lost we
// can rebuild the consumer-group position from `_migration_checkpoints`
// and rely on ADR-002's LSN gate to absorb the duplicates. The gauge is
// the liveness signal feeding the CheckpointStaleness alert: if the
// flush goroutine dies but the metrics server keeps serving (process
// alive, goroutine deadlocked, BulkWrite blocked) the staleness gauge
// keeps climbing and pages oncall.
//
// The flush loop has two modes. While `dirty` (MarkProgress fired since
// last flush) it flushes every Interval. While idle, it heartbeats every
// HeartbeatEvery intervals so the gauge stays bounded on a healthy but
// idle pipeline - otherwise the alert (`> 60s for 2m`) would fire
// continuously on any pipeline with no traffic, even though Mongo is
// fine. Stalled-consume-loop detection is left to ConsumerLagHigh
// (which keys off Kafka's consumer-group lag), the right tool for that
// failure mode.
package checkpoint

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Collection is the Mongo collection name where checkpoint docs land.
// Matches the name referenced in docs/architecture.md and ADR-003.
const Collection = "_migration_checkpoints"

// Checkpointer accumulates per-batch progress in memory and flushes a
// single document per consumer group on a fixed interval. Concurrent
// MarkProgress calls from the consumer loop are safe (atomic writes);
// only Run touches Mongo.
type Checkpointer struct {
	coll           *mongo.Collection
	groupID        string
	interval       time.Duration
	flushTO        time.Duration
	heartbeatEvery int
	logf           func(format string, args ...any)
	now            func() time.Time
	createdAt      time.Time

	maxLSN     atomic.Int64 // greatest LSN ever seen
	eventCount atomic.Int64 // running count of events committed
	dirty      atomic.Bool  // set on MarkProgress, cleared after a successful flush

	lastWriteAtUnixNano atomic.Int64 // updated only after a successful flush
}

// Config bundles the optional knobs. Zero values mean "use a sensible default".
type Config struct {
	GroupID      string
	Interval     time.Duration
	FlushTimeout time.Duration
	// HeartbeatEvery: when the pipeline is idle (no MarkProgress calls
	// since the last flush), force a flush every Nth interval so the
	// staleness gauge does not climb past the alert threshold. Default 6
	// (60s if Interval=10s) - just under the CheckpointStaleness alert's
	// 60s threshold so an idle but healthy sink never trips it.
	HeartbeatEvery int
	Logf           func(format string, args ...any)
	Now            func() time.Time
}

// New constructs a Checkpointer and registers its staleness gauge against
// reg. The gauge value is computed on every scrape: it is the number of
// seconds since the last successful flush to Mongo (or, before the first
// flush, since process start). Pass a nil registerer to skip gauge
// registration (useful in tests that want to assert flush behaviour
// without exposing metrics).
func New(client *mongo.Client, db string, reg prometheus.Registerer, cfg Config) *Checkpointer {
	if cfg.GroupID == "" {
		cfg.GroupID = "zdt-sink"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = 5 * time.Second
	}
	if cfg.HeartbeatEvery <= 0 {
		cfg.HeartbeatEvery = 6
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	cp := &Checkpointer{
		coll:           client.Database(db).Collection(Collection),
		groupID:        cfg.GroupID,
		interval:       cfg.Interval,
		flushTO:        cfg.FlushTimeout,
		heartbeatEvery: cfg.HeartbeatEvery,
		logf:           cfg.Logf,
		now:            cfg.Now,
		createdAt:      cfg.Now(),
	}
	cp.lastWriteAtUnixNano.Store(cp.createdAt.UnixNano())

	if reg != nil {
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "migration_checkpoint_staleness_seconds",
				Help: "Seconds since the sink last persisted a checkpoint doc. Climbs when the consume loop stalls.",
			},
			cp.stalenessSeconds,
		))
	}
	return cp
}

// MarkProgress records that the loop committed a batch whose maximum LSN
// is `lsn`. Cheap (one atomic load + two CAS) so it can run on every
// successful batch.
func (c *Checkpointer) MarkProgress(lsn int64, batchSize int) {
	if lsn > 0 {
		for {
			cur := c.maxLSN.Load()
			if lsn <= cur {
				break
			}
			if c.maxLSN.CompareAndSwap(cur, lsn) {
				break
			}
		}
	}
	if batchSize > 0 {
		c.eventCount.Add(int64(batchSize))
	}
	c.dirty.Store(true)
}

// Run drives the periodic flush. Returns when ctx is canceled. Errors
// from Mongo are logged but do not stop the loop - the staleness gauge
// is what will page oncall if flushes keep failing.
//
// Flush schedule: every interval if dirty, otherwise every
// HeartbeatEvery intervals. The heartbeat keeps the staleness gauge
// bounded on a healthy idle pipeline so the alert does not false-fire.
func (c *Checkpointer) Run(ctx context.Context) {
	t := time.NewTicker(c.interval)
	defer t.Stop()
	idleTicks := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			dirty := c.dirty.Load()
			if !dirty {
				idleTicks++
				if idleTicks < c.heartbeatEvery {
					continue
				}
			}
			if err := c.flush(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				c.logf("checkpoint flush: %v", err)
				continue // do NOT reset idleTicks - flush failed, keep trying every tick
			}
			idleTicks = 0
		}
	}
}

// Flush writes the current snapshot synchronously. Used at shutdown to
// avoid losing the in-flight progress and exposed to tests.
func (c *Checkpointer) Flush(ctx context.Context) error {
	return c.flush(ctx)
}

func (c *Checkpointer) flush(ctx context.Context) error {
	flushCtx, cancel := context.WithTimeout(ctx, c.flushTO)
	defer cancel()

	now := c.now()
	doc := bson.M{
		"$set": bson.M{
			"groupId":    c.groupID,
			"lastLSN":    c.maxLSN.Load(),
			"lastEvents": c.eventCount.Load(),
			"updatedAt":  now,
		},
	}
	opts := options.UpdateOne().SetUpsert(true)
	if _, err := c.coll.UpdateOne(flushCtx, bson.M{"_id": c.groupID}, doc, opts); err != nil {
		return err
	}
	c.lastWriteAtUnixNano.Store(now.UnixNano())
	c.dirty.Store(false)
	return nil
}

func (c *Checkpointer) stalenessSeconds() float64 {
	last := time.Unix(0, c.lastWriteAtUnixNano.Load())
	d := c.now().Sub(last).Seconds()
	if d < 0 {
		return 0
	}
	return d
}
