// Package kafka adapts franz-go to the consumer.KafkaConsumer interface.
package kafka

import (
	"context"
	"fmt"
	"time"

	"zdt/sink/internal/consumer"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FranzConsumer is the franz-go-backed implementation of
// consumer.KafkaConsumer.
type FranzConsumer struct {
	client *kgo.Client
}

// New connects to the supplied brokers as a member of groupID and
// subscribes by regex. Auto-commit is disabled — the loop commits
// only after the downstream write succeeds.
func New(brokers []string, groupID, topicRegex string) (*FranzConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics(topicRegex),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(45*time.Second),
		kgo.HeartbeatInterval(3*time.Second),
		// A pattern-subscribing consumer only picks up new topics on the
		// next metadata refresh. The 5m default leaves freshly-created
		// cdc.* topics unsubscribed for too long on cold start.
		kgo.MetadataMaxAge(10*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return &FranzConsumer{client: client}, nil
}

// Close releases the underlying client connection.
func (f *FranzConsumer) Close() {
	f.client.Close()
}

// Client exposes the underlying kgo client so admin-style queries (lag
// probing, end-offset listing) can share its connection pool. Returned
// pointer must NOT be Close()d by the caller - lifetime belongs to
// FranzConsumer.
func (f *FranzConsumer) Client() *kgo.Client {
	return f.client
}

// Poll drains one batch of records from the broker. A non-empty
// fetch error short-circuits — Kafka redelivers from the last
// committed offset on the next poll.
func (f *FranzConsumer) Poll(ctx context.Context) ([]consumer.Record, error) {
	fetches := f.client.PollFetches(ctx)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, fmt.Errorf("kafka.Poll: %w", errs[0].Err)
	}
	var out []consumer.Record
	fetches.EachRecord(func(r *kgo.Record) {
		var hdrs []consumer.HeaderKV
		if len(r.Headers) > 0 {
			hdrs = make([]consumer.HeaderKV, 0, len(r.Headers))
			for _, h := range r.Headers {
				hdrs = append(hdrs, consumer.HeaderKV{Key: h.Key, Value: h.Value})
			}
		}
		out = append(out, consumer.Record{
			Key:       r.Key,
			Value:     r.Value,
			Offset:    r.Offset,
			Partition: r.Partition,
			Topic:     r.Topic,
			Headers:   hdrs,
			Raw:       r,
		})
	})
	return out, nil
}

// MarkCommit uses MarkCommitRecords rather than MarkCommitOffsets - the
// latter (with Epoch:-1) silently no-ops against an active group session.
//
//nolint:gocritic // consumer.Record passes by value to match the interface
func (f *FranzConsumer) MarkCommit(r consumer.Record) {
	if raw, ok := r.Raw.(*kgo.Record); ok && raw != nil {
		f.client.MarkCommitRecords(raw)
	}
}

// CommitMarked flushes the marked offsets to the group. Called once
// per RunOnce after the batch apply succeeds.
func (f *FranzConsumer) CommitMarked(ctx context.Context) error {
	return f.client.CommitMarkedOffsets(ctx)
}
