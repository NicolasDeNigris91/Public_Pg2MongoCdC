package writer

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoWriter dispatches CDC events to Mongo via BulkWrite, one BulkWrite
// per collection per call.
type MongoWriter struct {
	client        *mongo.Client
	db            string
	schemaVersion int
}

// NewMongoWriter binds a Mongo client to a database name and the
// pipeline's schema version (used by BuildWriteOp to gate replays).
func NewMongoWriter(client *mongo.Client, db string, schemaVersion int) *MongoWriter {
	return &MongoWriter{client: client, db: db, schemaVersion: schemaVersion}
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
		coll := m.client.Database(m.db).Collection(table)
		// ordered=false so one E11000 doesn't abort the remaining inserts.
		opts := options.BulkWrite().SetOrdered(false)
		if _, err := coll.BulkWrite(ctx, models, opts); err != nil {
			if allDuplicateKey(err) {
				continue // entire "failure" is expected idempotent-skip
			}
			return fmt.Errorf("MongoWriter.ApplyBatch: bulkwrite %s n=%d: %w", table, len(models), err)
		}
	}
	return nil
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
