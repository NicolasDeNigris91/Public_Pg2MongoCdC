package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"zdt/sink/internal/consumer"
	"zdt/sink/internal/writer"
)

// --- test doubles ---

type fakeConsumer struct {
	records        []consumer.Record
	markedOffsets  []int64
	commitCalls    int
	committedAfter []int64 // snapshot of markedOffsets at each CommitMarked
}

func (f *fakeConsumer) Poll(_ context.Context) ([]consumer.Record, error) {
	out := f.records
	f.records = nil
	return out, nil
}

func (f *fakeConsumer) MarkCommit(r consumer.Record) {
	f.markedOffsets = append(f.markedOffsets, r.Offset)
}

func (f *fakeConsumer) CommitMarked(_ context.Context) error {
	f.commitCalls++
	f.committedAfter = append(f.committedAfter, slices.Clone(f.markedOffsets)...)
	return nil
}

type fakeWriter struct {
	batches   [][]writer.CDCEvent
	failOnLSN int64 // if >0, ApplyBatch fails when any event.LSN equals this
}

func (f *fakeWriter) ApplyBatch(_ context.Context, evs []writer.CDCEvent) error {
	if f.failOnLSN > 0 {
		for _, e := range evs {
			if e.LSN == f.failOnLSN {
				return errors.New("synthetic apply failure")
			}
		}
	}
	f.batches = append(f.batches, evs)
	return nil
}

func makeInsert(pk, lsn int64) (key, value []byte) {
	key = []byte(fmt.Sprintf(`{"payload":{"id":%d}}`, pk))
	value = []byte(fmt.Sprintf(`{"payload":{"before":null,"after":{"id":%d},"source":{"lsn":%d,"table":"users"},"op":"c"}}`, pk, lsn))
	return
}

func TestLoop_BatchSuccess(t *testing.T) {
	lsns := []int64{100, 101, 102}
	recs := make([]consumer.Record, 0, len(lsns))
	for i, lsn := range lsns {
		k, v := makeInsert(int64(i+1), lsn)
		recs = append(recs, consumer.Record{Key: k, Value: v, Offset: lsn, Topic: "cdc.users"})
	}
	fc := &fakeConsumer{records: recs}
	fw := &fakeWriter{}
	loop := &consumer.Loop{Cons: fc, W: fw, SchemaVer: 1}

	if err := loop.RunOnce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []int64{100, 101, 102}; !slices.Equal(fc.markedOffsets, want) {
		t.Errorf("want marked=%v, got %v", want, fc.markedOffsets)
	}
	if fc.commitCalls != 1 {
		t.Errorf("want CommitMarked called once, got %d", fc.commitCalls)
	}
	if len(fw.batches) != 1 || len(fw.batches[0]) != 3 {
		t.Errorf("want one batch of 3 events, got %d batches / %v", len(fw.batches), fw.batches)
	}
}

// On batch failure the loop must not commit anything; the whole poll batch
// is redelivered.
func TestLoop_BatchFailureCommitsNothing(t *testing.T) {
	lsns := []int64{100, 101, 102}
	recs := make([]consumer.Record, 0, len(lsns))
	for i, lsn := range lsns {
		k, v := makeInsert(int64(i+1), lsn)
		recs = append(recs, consumer.Record{Key: k, Value: v, Offset: lsn, Topic: "cdc.users"})
	}
	fc := &fakeConsumer{records: recs}
	fw := &fakeWriter{failOnLSN: 101} // any record in the batch triggers batch failure
	loop := &consumer.Loop{Cons: fc, W: fw, SchemaVer: 1}

	err := loop.RunOnce(context.Background())
	if err == nil {
		t.Fatal("want error, got nil")
	}

	if len(fc.markedOffsets) != 0 {
		t.Errorf("no offsets should be marked on batch failure; got %v", fc.markedOffsets)
	}
	if fc.commitCalls != 0 {
		t.Errorf("CommitMarked should NOT run when the batch failed; got %d calls", fc.commitCalls)
	}
}
