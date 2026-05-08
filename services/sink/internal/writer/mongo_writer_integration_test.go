//go:build integration

package writer_test

import (
	"context"
	"os"
	"testing"
	"time"

	"zdt/sink/internal/writer"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Integration test against a live Mongo. Default URI hits localhost:27017
// (override with MONGO_URI). Run with `-tags integration`.

func mongoURI() string {
	if u := os.Getenv("MONGO_URI"); u != "" {
		return u
	}
	// directConnection=true so the driver doesn't follow the replica-set
	// advertised host (the docker-internal "mongo:27017").
	return "mongodb://localhost:27017/?directConnection=true"
}

func newTestClient(t *testing.T) *mongo.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI()))
	if err != nil {
		t.Skipf("mongo unreachable at %s: %v (is the stack up?)", mongoURI(), err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Skipf("mongo ping failed at %s: %v", mongoURI(), err)
	}
	return client
}

func TestMongoWriter_LSNGateOrdering(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	defer func() { _ = client.Disconnect(ctx) }()

	testDB := "migration_test_" + time.Now().Format("150405")
	t.Cleanup(func() { _ = client.Database(testDB).Drop(ctx) })

	skips := map[string]int{}
	w := writer.NewMongoWriter(client, testDB, 1, func(table string, n int) {
		skips[table] += n
	})
	coll := client.Database(testDB).Collection("users")

	// 1. INSERT at LSN=100.
	ev := writer.CDCEvent{
		Table: "users", PK: "1", LSN: 100, Op: writer.OpInsert,
		After: map[string]any{"id": int64(1), "email": "alice@a.b", "name": "Alice"},
	}
	if err := w.Apply(ctx, ev); err != nil {
		t.Fatalf("initial insert: %v", err)
	}
	got := coll.FindOne(ctx, bson.M{"_id": "users:1"})
	var doc bson.M
	if err := got.Decode(&doc); err != nil {
		t.Fatalf("expected doc after insert: %v", err)
	}
	if doc["sourceLsn"] != int64(100) || doc["email"] != "alice@a.b" {
		t.Fatalf("doc after insert wrong: %v", doc)
	}

	// 2. Same-LSN replay: no-op.
	if err := w.Apply(ctx, ev); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// Still one doc, still same content.
	n, _ := coll.CountDocuments(ctx, bson.M{})
	if n != 1 {
		t.Errorf("after replay, want 1 doc, got %d", n)
	}

	// 3. UPDATE at LSN=200: overwrites.
	ev.LSN = 200
	ev.Op = writer.OpUpdate
	ev.After["email"] = "alice@updated.com"
	if err := w.Apply(ctx, ev); err != nil {
		t.Fatalf("update: %v", err)
	}
	_ = coll.FindOne(ctx, bson.M{"_id": "users:1"}).Decode(&doc)
	if doc["email"] != "alice@updated.com" || doc["sourceLsn"] != int64(200) {
		t.Fatalf("doc after update wrong: %v", doc)
	}

	// 4. Stale UPDATE at LSN=150 (less than current 200): no-op.
	ev.LSN = 150
	ev.After["email"] = "STALE@wrong.com"
	if err := w.Apply(ctx, ev); err != nil {
		t.Fatalf("stale replay: %v", err)
	}
	_ = coll.FindOne(ctx, bson.M{"_id": "users:1"}).Decode(&doc)
	if doc["email"] == "STALE@wrong.com" {
		t.Errorf("LSN gate failed: stale event overwrote newer state. doc=%v", doc)
	}
	if doc["sourceLsn"] != int64(200) {
		t.Errorf("LSN gate failed: stored sourceLsn should stay 200, got %v", doc["sourceLsn"])
	}

	// 5. DELETE at LSN=300: removes the row.
	ev.LSN = 300
	ev.Op = writer.OpDelete
	ev.Before = map[string]any{"id": int64(1)}
	ev.After = nil
	if err := w.Apply(ctx, ev); err != nil {
		t.Fatalf("delete: %v", err)
	}
	n, _ = coll.CountDocuments(ctx, bson.M{"_id": "users:1"})
	if n != 0 {
		t.Errorf("after delete, want 0 docs, got %d", n)
	}

	// 6. Re-insert at LSN=400, then a stale DELETE (LSN=300) should be a no-op.
	ev = writer.CDCEvent{
		Table: "users", PK: "1", LSN: 400, Op: writer.OpInsert,
		After: map[string]any{"id": int64(1), "email": "alice-v2@a.b"},
	}
	if err := w.Apply(ctx, ev); err != nil {
		t.Fatalf("reinsert: %v", err)
	}
	staleDelete := writer.CDCEvent{
		Table: "users", PK: "1", LSN: 300, Op: writer.OpDelete,
		Before: map[string]any{"id": int64(1)},
	}
	if err := w.Apply(ctx, staleDelete); err != nil {
		t.Fatalf("stale delete: %v", err)
	}
	n, _ = coll.CountDocuments(ctx, bson.M{"_id": "users:1"})
	if n != 1 {
		t.Errorf("stale delete removed re-inserted doc (should have been a no-op). count=%d", n)
	}

	// LSN gate skips that the onSkip callback must have seen:
	//   - same-LSN replay of the initial insert (E11000 on upsert),
	//   - stale UPDATE at LSN=150 (E11000 on upsert vs the LSN=200 doc),
	//   - stale DELETE at LSN=300 vs the re-inserted LSN=400 doc.
	// Three events total, all on table "users".
	if got := skips["users"]; got < 3 {
		t.Errorf("want >=3 idempotent-skips reported on users, got %d (full map=%v)", got, skips)
	}
}
