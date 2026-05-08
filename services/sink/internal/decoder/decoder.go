// Package decoder parses Debezium JSON-envelope CDC events into the
// normalized CDCEvent used by the writer package.
//
// The Debezium JsonConverter with schemas.enable=true emits records as:
//
//	{ "schema": {...}, "payload": { before|after|source|op|ts_ms|... } }
//
// and keys of the same shape with payload.<PK-field>. We keep the
// decoder strict: unknown/malformed envelopes return errors, never
// silent defaults. Tombstone messages (value==null) are signaled via
// ErrTombstone so the consumer can skip them without logging noise.
package decoder

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"zdt/sink/internal/writer"
)

// ErrTombstone is returned when Decode sees a Kafka tombstone (value=nil).
// Callers should skip tombstones - the preceding "d" event is the real op.
var ErrTombstone = errors.New("decoder: tombstone event, skip")

type keyEnvelope struct {
	Payload struct {
		// json.Number preserves the exact numeric token so PKs > 2^53
		// do not lose precision through float64.
		ID json.Number `json:"id"`
	} `json:"payload"`
}

type valueEnvelope struct {
	Payload struct {
		Before map[string]any `json:"before"`
		After  map[string]any `json:"after"`
		Source struct {
			LSN   json.Number `json:"lsn"`
			Table string      `json:"table"`
			TsMs  json.Number `json:"ts_ms"`
		} `json:"source"`
		Op string `json:"op"`
	} `json:"payload"`
}

// Decode parses one Debezium JSON envelope record into a normalized
// CDCEvent. A nil-value record is treated as a Kafka tombstone and
// returns ErrTombstone so the caller can skip it without error noise.
// Any malformed envelope is a hard error - the decoder never silently
// substitutes defaults for missing fields.
func Decode(key, value []byte) (writer.CDCEvent, error) {
	if len(value) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return writer.CDCEvent{}, ErrTombstone
	}

	var v valueEnvelope
	if err := json.Unmarshal(value, &v); err != nil {
		return writer.CDCEvent{}, fmt.Errorf("decoder.Decode: parse value: %w", err)
	}
	var k keyEnvelope
	if err := json.Unmarshal(key, &k); err != nil {
		return writer.CDCEvent{}, fmt.Errorf("decoder.Decode: parse key: %w", err)
	}

	lsn, err := v.Payload.Source.LSN.Int64()
	if err != nil {
		return writer.CDCEvent{}, fmt.Errorf("decoder.Decode: lsn: %w", err)
	}

	// source.ts_ms is best-effort. Snapshot reads ("r") sometimes carry 0
	// or omit the field; we propagate whatever we got and let downstream
	// treat 0 as "unknown" rather than reject the event for a missing
	// observability-only field.
	var sourceTsMs int64
	if s := v.Payload.Source.TsMs.String(); s != "" {
		if n, perr := v.Payload.Source.TsMs.Int64(); perr == nil {
			sourceTsMs = n
		}
	}

	pk := string(k.Payload.ID)
	if pk == "" {
		return writer.CDCEvent{}, errors.New("decoder.Decode: empty PK in key payload")
	}

	return writer.CDCEvent{
		Table:      v.Payload.Source.Table,
		PK:         pk,
		LSN:        lsn,
		SourceTsMs: sourceTsMs,
		Op:         writer.CDCOp(v.Payload.Op),
		After:      v.Payload.After,
		Before:     v.Payload.Before,
	}, nil
}
