// Command dlqtool inspects and (optionally) replays Kafka Connect
// dead-letter queue topics produced by Debezium / the Mongo sink.
//
// Default mode is dry-run: read every record currently in the DLQ topic
// up to the high-water-mark at start, group by `__dlq_error_reason`, and
// print a triage summary. With -replay the tool re-publishes each record
// to the topic recorded in `__dlq_source_topic`, preserving key + value
// bytes verbatim. The original record's reason / source-offset headers
// are NOT propagated on replay - if the same poison shape repeats it
// goes back to the DLQ with fresh provenance.
//
// Usage:
//
//	dlqtool -topic dlq.source                   # dry-run summary
//	dlqtool -topic dlq.sink -replay             # actually re-publish
//	dlqtool -topic dlq.source -max 100          # cap how many to read
//
// Env (with defaults that match docker-compose):
//
//	KAFKA_BROKERS  default "kafka:29092"
//
// Exit codes: 0 on success (including "topic empty"), 1 on connection /
// fatal errors, 2 if the topic does not exist.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	headerSourceTopic  = "__dlq_source_topic"
	headerSourceOffset = "__dlq_source_offset"
	headerErrorReason  = "__dlq_error_reason"
)

func main() {
	var (
		topic   = flag.String("topic", "", "DLQ topic to read (e.g. dlq.source, dlq.sink)")
		replay  = flag.Bool("replay", false, "Replay records back to their __dlq_source_topic (mutating)")
		maxRecs = flag.Int("max", 0, "Max records to process (0 = drain to high-water-mark at start)")
		timeout = flag.Duration("timeout", 30*time.Second, "Hard cap on the whole run")
		brokers = flag.String("brokers", env("KAFKA_BROKERS", "kafka:29092"), "Comma-separated Kafka bootstrap brokers")
	)
	flag.Parse()
	if *topic == "" {
		fmt.Fprintln(os.Stderr, "dlqtool: -topic is required (e.g. dlq.source or dlq.sink)")
		flag.Usage()
		os.Exit(64)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	brokerList := strings.Split(*brokers, ",")

	// Step 1: snapshot the high-water-mark so we know when to stop.
	endOffsets, exists, err := topicEndOffsets(ctx, brokerList, *topic)
	if err != nil {
		log.Printf("dlqtool: list offsets for %s: %v", *topic, err)
		os.Exit(1)
	}
	if !exists {
		log.Printf("dlqtool: topic %q does not exist (no DLQ activity yet?)", *topic)
		os.Exit(2)
	}
	if totalLag(endOffsets) == 0 {
		fmt.Printf("dlqtool: %s is empty (high-water-mark sums to 0). Nothing to do.\n", *topic)
		return
	}

	// Step 2: open a NoGroup consumer at the earliest offset.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokerList...),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Printf("dlqtool: kafka client: %v", err)
		os.Exit(1)
	}
	defer cl.Close()

	// Optional producer for replay mode. Idempotent so we don't double-publish
	// on a transient error mid-batch.
	var producer *kgo.Client
	if *replay {
		producer, err = kgo.NewClient(
			kgo.SeedBrokers(brokerList...),
			kgo.ProducerLinger(0),
			kgo.RequiredAcks(kgo.AllISRAcks()),
		)
		if err != nil {
			log.Printf("dlqtool: kafka producer: %v", err)
			os.Exit(1)
		}
		defer producer.Close()
	}

	mode := "DRY-RUN"
	if *replay {
		mode = "REPLAY"
	}
	target := totalLag(endOffsets)
	fmt.Printf("dlqtool [%s]: scanning %s, end offsets=%v (target ~%d records)\n", mode, *topic, endOffsets, target)

	reasons := map[string]int{}
	bySourceTopic := map[string]int{}
	var processed, replayed, replayFailed int64
	emptyPolls := 0

scan:
	for {
		// Per-poll deadline so an idle topic terminates quickly without
		// waiting for the outer -timeout cap.
		pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()

		if ctx.Err() != nil {
			break scan
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
					continue
				}
				log.Printf("dlqtool: fetch error on %s/%d: %v", fe.Topic, fe.Partition, fe.Err)
			}
		}

		got := 0
		fetches.EachRecord(func(r *kgo.Record) {
			got++
			processed++
			reason := headerString(r, headerErrorReason)
			srcTopic := headerString(r, headerSourceTopic)
			srcOffset := headerString(r, headerSourceOffset)

			reasons[truncate(reason, 80)]++
			if srcTopic != "" {
				bySourceTopic[srcTopic]++
			}

			if *replay {
				if srcTopic == "" {
					log.Printf("dlqtool: skip replay (no %s header): partition=%d offset=%d", headerSourceTopic, r.Partition, r.Offset)
					replayFailed++
					return
				}
				out := &kgo.Record{Topic: srcTopic, Key: r.Key, Value: r.Value}
				if perr := producer.ProduceSync(ctx, out).FirstErr(); perr != nil {
					log.Printf("dlqtool: replay to %s failed (src offset=%s): %v", srcTopic, srcOffset, perr)
					replayFailed++
					return
				}
				replayed++
			}
		})

		switch {
		case *maxRecs > 0 && processed >= int64(*maxRecs):
			break scan
		case processed >= target:
			break scan
		case got == 0:
			emptyPolls++
			if emptyPolls >= 2 {
				// Two empty polls in a row = nothing more is going to
				// arrive in the snapshot window we asked for.
				break scan
			}
		default:
			emptyPolls = 0
		}
	}

	fmt.Println()
	fmt.Printf("dlqtool: processed %d records\n", processed)
	if *replay {
		fmt.Printf("dlqtool: replayed=%d failed=%d\n", replayed, replayFailed)
	}
	if processed == 0 {
		return
	}
	fmt.Println()
	fmt.Println("By __dlq_source_topic:")
	for _, k := range sortedKeys(bySourceTopic) {
		fmt.Printf("  %6d  %s\n", bySourceTopic[k], k)
	}
	fmt.Println()
	fmt.Println("By __dlq_error_reason (truncated to 80 chars):")
	for _, k := range sortedKeys(reasons) {
		fmt.Printf("  %6d  %s\n", reasons[k], k)
	}
	if *replay && replayFailed > 0 {
		os.Exit(1)
	}
}

func topicEndOffsets(ctx context.Context, brokers []string, topic string) (map[int32]int64, bool, error) {
	adm, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, false, err
	}
	defer adm.Close()
	a := kadm.NewClient(adm)

	listed, err := a.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, false, err
	}
	out := map[int32]int64{}
	exists := false
	listed.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			if errors.Is(o.Err, kerr.UnknownTopicOrPartition) {
				return
			}
			return
		}
		exists = true
		out[o.Partition] = o.Offset
	})
	return out, exists, nil
}

func totalLag(end map[int32]int64) int64 {
	var n int64
	for _, v := range end {
		n += v
	}
	return n
}

func headerString(r *kgo.Record, name string) string {
	for _, h := range r.Headers {
		if h.Key == name {
			return string(h.Value)
		}
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return m[keys[i]] > m[keys[j]] })
	return keys
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
