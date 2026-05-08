package tracing

import "github.com/twmb/franz-go/pkg/kgo"

// KafkaHeaderCarrier adapts a slice of franz-go record headers to the
// OTel TextMapCarrier interface so trace context (`traceparent`,
// `tracestate`) can be injected on produce and extracted on consume.
//
// Wrap the records' Headers slice by reference so Set() mutations are
// visible to the produced/consumed record:
//
//	carrier := tracing.KafkaHeaderCarrier(&record.Headers)
//	otel.GetTextMapPropagator().Inject(ctx, carrier)
type KafkaHeaderCarrier struct {
	headers *[]kgo.RecordHeader
}

// NewKafkaHeaderCarrier returns a carrier backed by the given header slice.
// Pointer semantics matter - Set() appends/replaces in *headers.
func NewKafkaHeaderCarrier(h *[]kgo.RecordHeader) KafkaHeaderCarrier {
	return KafkaHeaderCarrier{headers: h}
}

// Get returns the first value for key, or "" if absent.
func (c KafkaHeaderCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces the header value for key, or appends if not present.
func (c KafkaHeaderCarrier) Set(key, value string) {
	if c.headers == nil {
		return
	}
	// Replace if present; otherwise append. Kafka allows duplicate
	// header keys but OTel propagators are last-write-wins so we
	// prefer the cleaner replace.
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

// Keys returns the header keys in the order they appear.
func (c KafkaHeaderCarrier) Keys() []string {
	if c.headers == nil {
		return nil
	}
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}
