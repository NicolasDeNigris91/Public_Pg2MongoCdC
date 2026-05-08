// Package consumer runs the Kafka -> Mongo consume loop. Offsets commit
// only after the downstream write succeeds.
package consumer

import (
	"context"
	"errors"
	"fmt"

	"zdt/sink/internal/decoder"
	"zdt/sink/internal/writer"
)

// Record is the minimum view of a Kafka record the loop needs. Raw is an
// opaque handle for the underlying client (e.g. *kgo.Record) so MarkCommit
// can pass it back without the loop knowing the concrete type.
type Record struct {
	Key, Value []byte
	Offset     int64
	Partition  int32
	Topic      string
	Raw        any
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
func (l *Loop) RunOnce(ctx context.Context) error {
	records, err := l.Cons.Poll(ctx)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}

	events := make([]writer.CDCEvent, 0, len(records))
	for _, r := range records {
		ev, derr := decoder.Decode(r.Key, r.Value)
		if errors.Is(derr, decoder.ErrTombstone) {
			continue
		}
		if derr != nil {
			return fmt.Errorf("decode offset=%d: %w", r.Offset, derr)
		}
		events = append(events, ev)
	}

	if len(events) > 0 {
		if werr := l.W.ApplyBatch(ctx, events); werr != nil {
			return fmt.Errorf("apply batch (size=%d): %w", len(events), werr)
		}
	}

	for _, r := range records {
		l.Cons.MarkCommit(r)
	}
	if cerr := l.Cons.CommitMarked(ctx); cerr != nil {
		return cerr
	}
	return nil
}
