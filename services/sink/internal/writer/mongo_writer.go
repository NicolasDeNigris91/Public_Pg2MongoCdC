package writer

import (
	"context"
	"errors"
	"fmt"

	"zdt/sink/internal/tracing"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// MongoWriter dispatches CDC events to Mongo via BulkWrite, one BulkWrite
// per collection per call.
type MongoWriter struct {
	client        *mongo.Client
	db            string
	schemaVersion int
	onSkip        func(table string, n int)
}

// NewMongoWriter constructs a writer. onSkip, if non-nil, is invoked with the
// per-table count of events that the LSN gate rejected (either because the
// stored sourceLsn was already >= the incoming LSN, or because an upsert
// collided on _id - the E11000-only bulk failure path). It is the hook used
// to feed the migration_idempotent_skip_total counter; production code wires
// it, tests pass nil.
func NewMongoWriter(client *mongo.Client, db string, schemaVersion int, onSkip func(table string, n int)) *MongoWriter {
	return &MongoWriter{client: client, db: db, schemaVersion: schemaVersion, onSkip: onSkip}
}

// Apply is a single-event convenience wrapper around ApplyBatch.
func (m *MongoWriter) Apply(ctx context.Context, ev CDCEvent) error {
	return m.ApplyBatch(ctx, []CDCEvent{ev})
}

// ApplyBatch writes N events in one BulkWrite per collection. LSN gating in
// the per-op filter makes replays no-ops. A bulk failure that is entirely
// E11000 is treated as the stale-replay path and ignored; any other error
// returns and the caller retries the whole batch.
func (m *MongoWriter) ApplyBatch(ctx context.Context, evs []CDCEvent) error {
	if len(evs) == 0 {
		return nil
	}
	// Group by collection so we issue one BulkWrite per collection.
	byColl := make(map[string][]mongo.WriteModel, 4)
	for _, ev := range evs {
		op, err := BuildWriteOp(ev, m.schemaVersion)
		if err != nil {
			return fmt.Errorf("MongoWriter.ApplyBatch: build op for %s:%s: %w", ev.Table, ev.PK, err)
		}
		byColl[ev.Table] = append(byColl[ev.Table], ToMongoModel(op))
	}

	for table, models := range byColl {
		if err := m.bulkWriteTable(ctx, table, models); err != nil {
			return err
		}
	}
	return nil
}

// bulkWriteTable issues one BulkWrite per table and emits one
// "sink.mongo_bulk_write" span. Span attributes record the table,
// model count, and per-result counts so a slow span shows immediately
// whether work was useful (matched/upserted) or all idempotent skips.
func (m *MongoWriter) bulkWriteTable(ctx context.Context, table string, models []mongo.WriteModel) error {
	ctx, span := tracing.Tracer().Start(ctx, "sink.mongo_bulk_write",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "mongodb"),
			attribute.String("db.mongodb.collection", table),
			attribute.Int("db.mongodb.bulk_write.models", len(models)),
		),
	)
	defer span.End()

	coll := m.client.Database(m.db).Collection(table)
	// ordered=false so one E11000 doesn't abort the remaining inserts.
	opts := options.BulkWrite().SetOrdered(false)
	res, err := coll.BulkWrite(ctx, models, opts)
	if err != nil {
		if allDuplicateKey(err) {
			span.SetAttributes(attribute.Int("db.mongodb.bulk_write.idempotent_skips", len(models)))
			m.recordSkip(table, len(models))
			return nil // entire "failure" is expected idempotent-skip
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "bulk write failed")
		return fmt.Errorf("MongoWriter.ApplyBatch: bulkwrite %s n=%d: %w", table, len(models), err)
	}
	if res != nil {
		span.SetAttributes(
			attribute.Int64("db.mongodb.bulk_write.matched", res.MatchedCount),
			attribute.Int64("db.mongodb.bulk_write.upserted", res.UpsertedCount),
			attribute.Int64("db.mongodb.bulk_write.deleted", res.DeletedCount),
		)
		acked := res.MatchedCount + res.UpsertedCount + res.DeletedCount
		if skipped := int64(len(models)) - acked; skipped > 0 {
			span.SetAttributes(attribute.Int64("db.mongodb.bulk_write.idempotent_skips", skipped))
			m.recordSkip(table, int(skipped))
		}
	}
	return nil
}

func (m *MongoWriter) recordSkip(table string, n int) {
	if m.onSkip != nil && n > 0 {
		m.onSkip(table, n)
	}
}

// allDuplicateKey reports whether every per-record error in a bulk failure
// is E11000.
func allDuplicateKey(err error) bool {
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) {
		return false
	}
	if len(bwe.WriteErrors) == 0 {
		return false
	}
	for _, we := range bwe.WriteErrors {
		if we.Code != 11000 {
			return false
		}
	}
	return true
}
