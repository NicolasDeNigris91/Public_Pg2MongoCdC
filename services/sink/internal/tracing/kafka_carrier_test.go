package tracing

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaHeaderCarrier_GetSetReplace(t *testing.T) {
	headers := []kgo.RecordHeader{}
	c := NewKafkaHeaderCarrier(&headers)

	if got := c.Get("missing"); got != "" {
		t.Errorf("Get on empty: want empty, got %q", got)
	}

	c.Set("traceparent", "00-aaaa-bbbb-01")
	if got := c.Get("traceparent"); got != "00-aaaa-bbbb-01" {
		t.Errorf("after Set: got %q", got)
	}
	if len(headers) != 1 {
		t.Errorf("after one Set, want 1 header, got %d", len(headers))
	}

	// Set should replace, not append.
	c.Set("traceparent", "00-cccc-dddd-01")
	if got := c.Get("traceparent"); got != "00-cccc-dddd-01" {
		t.Errorf("after replace: got %q", got)
	}
	if len(headers) != 1 {
		t.Errorf("after replace, want 1 header, got %d (full=%v)", len(headers), headers)
	}

	c.Set("tracestate", "k1=v1")
	if got, want := len(headers), 2; got != want {
		t.Errorf("after second key, want %d headers, got %d", want, got)
	}
	keys := c.Keys()
	if len(keys) != 2 {
		t.Errorf("Keys() len: got %v", keys)
	}
}

func TestKafkaHeaderCarrier_NilSafe(t *testing.T) {
	c := KafkaHeaderCarrier{headers: nil}
	if got := c.Get("anything"); got != "" {
		t.Errorf("nil Get: got %q", got)
	}
	c.Set("any", "thing") // must not panic
	if got := c.Keys(); got != nil {
		t.Errorf("nil Keys: got %v", got)
	}
}
