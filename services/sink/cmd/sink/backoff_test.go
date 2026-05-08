package main

import (
	"testing"
	"time"
)

func TestBackoffDuration(t *testing.T) {
	cases := []struct {
		n    int
		want time.Duration
	}{
		{0, baseBackoff},          // bootstrap path
		{1, 500 * time.Millisecond}, // first failure
		{2, 1 * time.Second},
		{3, 2 * time.Second},
		{4, 4 * time.Second},
		{5, 8 * time.Second},
		{6, 16 * time.Second},
		{7, maxBackoff},           // 32s clamps to 30s
		{20, maxBackoff},          // way past saturation
		{63, maxBackoff},          // overflow guard - must not wrap to a tiny duration
		{64, maxBackoff},
	}
	for _, c := range cases {
		if got := backoffDuration(c.n); got != c.want {
			t.Errorf("backoffDuration(%d) = %v, want %v", c.n, got, c.want)
		}
	}
}

// Sanity: under no realistic n does the schedule emit a duration above the
// Kafka max.poll.interval.ms default (5 min). If maxBackoff is ever raised
// past that without also bumping the kgo SessionTimeout / max poll, this
// test breaks loud.
func TestBackoffStaysUnderKafkaPollInterval(t *testing.T) {
	const kafkaMaxPoll = 5 * time.Minute
	for n := 0; n < 100; n++ {
		if d := backoffDuration(n); d >= kafkaMaxPoll {
			t.Fatalf("backoffDuration(%d) = %v >= max.poll.interval.ms (%v) - broker would kick the consumer", n, d, kafkaMaxPoll)
		}
	}
}
