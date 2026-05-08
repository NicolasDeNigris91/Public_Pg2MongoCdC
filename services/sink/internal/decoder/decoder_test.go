package decoder_test

import (
	"errors"
	"testing"

	"zdt/sink/internal/decoder"
	"zdt/sink/internal/writer"
)

func TestDecode_Insert(t *testing.T) {
	// Trimmed Debezium envelope from cdc.users (kafka-console-consumer).
	key := []byte(`{"schema":{},"payload":{"id":42}}`)
	value := []byte(`{
		"schema":{},
		"payload":{
			"before":null,
			"after":{"id":42,"email":"alice@example.com","full_name":"Alice"},
			"source":{"version":"2.6","lsn":1000,"table":"users","ts_ms":123,"db":"app"},
			"op":"c",
			"ts_ms":456
		}
	}`)

	ev, err := decoder.Decode(key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if ev.Table != "users" {
		t.Errorf("want Table=users, got %q", ev.Table)
	}
	if ev.PK != "42" {
		t.Errorf("want PK=42, got %q", ev.PK)
	}
	if ev.LSN != 1000 {
		t.Errorf("want LSN=1000, got %d", ev.LSN)
	}
	if ev.Op != writer.OpInsert {
		t.Errorf("want Op=c, got %q", ev.Op)
	}
	if ev.Before != nil {
		t.Errorf("want Before=nil, got %v", ev.Before)
	}
	if ev.After == nil || ev.After["email"] != "alice@example.com" {
		t.Errorf("want After.email=alice@example.com, got %v", ev.After)
	}
	if ev.SourceTsMs != 123 {
		t.Errorf("want SourceTsMs=123 (from source.ts_ms), got %d", ev.SourceTsMs)
	}
}

func TestDecode_SourceTsMissing(t *testing.T) {
	// Snapshot reads sometimes omit source.ts_ms. The decoder must not fail
	// and must surface 0 as "unknown" so callers can skip emitting lag.
	key := []byte(`{"schema":{},"payload":{"id":7}}`)
	value := []byte(`{
		"schema":{},
		"payload":{
			"before":null,
			"after":{"id":7},
			"source":{"version":"2.6","lsn":3000,"table":"users"},
			"op":"r"
		}
	}`)

	ev, err := decoder.Decode(key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.SourceTsMs != 0 {
		t.Errorf("want SourceTsMs=0 when source.ts_ms missing, got %d", ev.SourceTsMs)
	}
}

func TestDecode_Tombstone(t *testing.T) {
	key := []byte(`{"schema":{},"payload":{"id":42}}`)
	value := []byte(nil) // tombstone

	_, err := decoder.Decode(key, value)
	if !errors.Is(err, decoder.ErrTombstone) {
		t.Errorf("want ErrTombstone, got %v", err)
	}
}

func TestDecode_Delete(t *testing.T) {
	key := []byte(`{"schema":{},"payload":{"id":42}}`)
	value := []byte(`{
		"schema":{},
		"payload":{
			"before":{"id":42,"email":"alice@example.com","full_name":"Alice"},
			"after":null,
			"source":{"version":"2.6","lsn":2000,"table":"users"},
			"op":"d"
		}
	}`)

	ev, err := decoder.Decode(key, value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Op != writer.OpDelete {
		t.Errorf("want Op=d, got %q", ev.Op)
	}
	if ev.LSN != 2000 {
		t.Errorf("want LSN=2000, got %d", ev.LSN)
	}
	if ev.After != nil {
		t.Errorf("want After=nil, got %v", ev.After)
	}
	if ev.Before == nil || ev.Before["email"] != "alice@example.com" {
		t.Errorf("want Before.email=alice@example.com, got %v", ev.Before)
	}
}

func TestDecode_MalformedJSONIsError(t *testing.T) {
	key := []byte(`{"payload":{"id":42}}`)
	value := []byte(`{not json`)
	_, err := decoder.Decode(key, value)
	if err == nil {
		t.Fatalf("want error on malformed JSON, got nil")
	}
}
