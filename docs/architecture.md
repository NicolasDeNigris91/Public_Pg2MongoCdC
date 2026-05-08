# Architecture

Companion to the top-level [README](../README.md).

## Data flow (detailed)

```
┌─────────────────────┐      WAL (pgoutput)     ┌─────────────────────────┐
│   PostgreSQL 16     │◀────── logical ─────────│ Debezium PG Connector   │
│ publication:        │      replication slot   │ (Kafka Connect worker)  │
│   zdt_publication   │      zdt_slot           │ snapshot.mode=initial   │
│ REPLICA IDENTITY=   │                         │ heartbeat.interval=1s   │
│   FULL (all tables) │                         │ publication.autocreate  │
└─────────────────────┘                         │   =disabled             │
                                                └──────────┬──────────────┘
                                                           │ Avro-encoded,
                                                           │ keyed by PK,
                                                           │ via Schema Registry
                                                           ▼
              ┌───────────────────────────────────────────────────────────┐
              │  Kafka (KRaft)                                            │
              │  topics: cdc.users | cdc.orders | cdc.order_items         │
              │          dlq.source (Debezium failures)                   │
              │          dlq.sink   (Mongo sink failures)                 │
              │          transformed.<table>                              │
              │          _migration_checkpoints (sink-owned)              │
              └────────┬───────────────────────────────────────────────────┘
                       │
                       ▼
              ┌────────────────────┐
              │  transformer-svc   │
              │  Go, franz-go      │
              │  stateless         │
              └────────┬───────────┘
                       │
                       ▼
              ┌─────────────────────┐
              │  sink-svc           │
              │  Go, mongo-driver   │
              │  LSN-gated upserts  │
              └─────────┬───────────┘
                        │
                        ▼
              ┌─────────────────────┐
              │     MongoDB 7       │
              │  migration DB:      │
              │    users            │
              │    orders           │
              │    order_items      │
              │    _migration_*     │
              └─────────────────────┘

Observability:
   Prometheus -> [kafka-exporter, connect /metrics, transformer /metrics,
                  sink /metrics, mongo-exporter, postgres-exporter]
              -> Grafana + Alertmanager
   Toxiproxy injects faults between kafka <-> transformer and transformer <-> mongo.
   k6 drives writes via the loadgen HTTP API.
```

## Topic-level invariants

| Topic | Partitions | RF (dev) | RF (prod) | Retention | Notes |
|---|---|---|---|---|---|
| `cdc.users` | 6 | 1 | 3 | 7d | Partitioned by `id` (PK) |
| `cdc.orders` | 6 | 1 | 3 | 7d | Partitioned by `id` (PK) |
| `cdc.order_items` | 6 | 1 | 3 | 7d | Partitioned by `id` (PK) |
| `transformed.*` | 6 | 1 | 3 | 3d | Written by transformer |
| `dlq.source` | 3 | 1 | 3 | ∞ | Debezium failures |
| `dlq.sink` | 3 | 1 | 3 | ∞ | Sink failures |

**Why PK partitioning, not random.** Same-row events must stay in order. A later UPDATE cannot overtake an earlier INSERT for the same row, or the sink writes stale data. Co-partitioning by PK guarantees a single consumer processes all events for a given row in order. See [invariant #1 in docs/invariants.md](./invariants.md).

## Replication slot / WAL safety

- Postgres: `wal_level=logical`, `max_replication_slots=10`, `max_wal_senders=10`.
- Debezium uses the `pgoutput` plugin (built into Postgres, no extension required).
- Slot name `zdt_slot` persists across Debezium restarts → restart is crash-safe.
- Heartbeat interval 1s prevents WAL from being recycled when the target tables are idle but the broader DB is busy (the #1 cause of silent data loss in real deployments).
- `REPLICA IDENTITY FULL` on all source tables so DELETE events carry the full before-image - crucial for correct tombstone handling downstream.

## Services

- **`transformer-svc`** (Go, franz-go). Consume + produce loop. Loads
  `schema/transforms/*.yml` at boot, renames `payload.after` keys per rule,
  publishes to `transformed.<table>`. Tombstones forwarded as-is. Offsets
  commit only after produce is ack'd. See `services/transformer/`.

- **`sink-svc`** (Go, franz-go + mongo-driver). Decodes the Debezium envelope,
  builds LSN-gated upserts, dispatches one `BulkWrite` per collection per poll
  batch. See `services/sink/` and `docs/decisions/002-lsn-gated-upserts.md`.

- **`loadgen`** (Go, pgx). Translates k6 HTTP into Postgres writes. Used by
  `load/k6/write-mix.js`.

The transformer / sink split keeps a CPU-bound transform pipeline scaling on
partition count separate from the I/O-bound sink; both share the
commit-after-side-effect invariant at different granularities (per-produce vs
per-batch).

## Sink internals

The sink is composed of four small packages; the seam between them is the
property each one is responsible for.

| Package | Owns | Property it guarantees |
|---|---|---|
| `internal/decoder` | Debezium-envelope -> `CDCEvent` | Strict parsing; tombstones surfaced as `ErrTombstone`; `source.ts_ms` propagated as `SourceTsMs` (0 = unknown). |
| `internal/writer` | `BuildWriteOp` + `MongoWriter` | LSN-gated upserts (ADR-002); per-table BulkWrite; idempotent-skip detection via `BulkWriteResult` deltas + the all-E11000 path. |
| `internal/consumer` | Poll-decode-apply-commit loop | Commit-after-side-effect (ADR-003); whole batch redelivered on any apply error. |
| `internal/checkpoint` | `_migration_checkpoints` doc + `migration_checkpoint_staleness_seconds` gauge | Disaster-recovery anchor (ADR-003) plus a liveness gauge that climbs even if the metrics server is healthy but the consume loop is wedged. |

`cmd/sink` composes them via an `instrumentedWriter` that wraps `MongoWriter`;
the wrapper observes `migration_replication_lag_seconds` (per event,
`now() - SourceTsMs`), increments `migration_events_processed_total`,
classifies write errors with `errors.As` against `mongo.ServerError` /
`net.Error` (no substring matching), and reports per-batch progress to
the `Checkpointer` so the staleness gauge resets on every successful
commit.

### Idempotent-skip accounting

Two distinct on-the-wire signals collapse into the same metric:

- **All-E11000 bulk failure.** A same-LSN replay or a stale upsert hits
  the `_id` uniqueness constraint after the LSN gate filters it out. The
  driver returns a `BulkWriteException` whose `WriteErrors` are *all*
  code 11000; we count `len(models)` as skipped and continue.
- **`MatchedCount + UpsertedCount + DeletedCount < len(models)`.** Most
  commonly a stale DELETE landing on an already-deleted row, or an
  out-of-order upsert that didn't collide on `_id` because the row had
  already been deleted. The bulk succeeds with no error; the result
  shows the gate silently dropped them.

Both paths feed `migration_idempotent_skip_total{table}`. A non-zero
counter under steady-state is normal under replay; a sustained spike
when no replay is active is the anomaly signal.

### Checkpointer

```
consumer.Loop ── batch ──▶ instrumentedWriter ──▶ MongoWriter ──▶ Mongo
                                  │
                                  └─ MarkProgress(maxLSN, n)
                                          │
                                          ▼
                                  Checkpointer
                                  ├─ atomic in-memory state
                                  ├─ ticker (10s) ──▶ flush() ──▶ _migration_checkpoints upsert
                                  └─ NewGaugeFunc ──▶ migration_checkpoint_staleness_seconds
```

The gauge is computed on every Prometheus scrape as
`time.Since(lastSuccessfulFlush)`. If the flush goroutine dies but
`/metrics` keeps serving, the gauge keeps climbing — exactly the
liveness signal the `CheckpointStaleness` alert keys off. This is
verified end-to-end by `chaos/scenarios/06-checkpoint-recovery.sh`.

## Distributed tracing

OpenTelemetry spans propagate through the pipeline so a single CDC
event is followable from transformer ingest down to the Mongo
`BulkWrite`. The transformer injects W3C `traceparent` into the
outgoing record's Kafka headers; the sink extracts on consume and
continues the same trace ID.

```
Jaeger UI shows per record:

  transformer.process_record           [SpanKind=Consumer]
    └── (Kafka produce; trace context in headers)
         └── sink.consume_batch        [SpanKind=Consumer]
              ├── decode loop          (no span; no I/O)
              └── sink.apply_batch     [batch.size=N]
                   └── sink.mongo_bulk_write   [SpanKind=Client]
                        attrs: db.system=mongodb, collection,
                               matched, upserted, deleted,
                               idempotent_skips
```

Activation is opt-in per environment: services read
`OTEL_EXPORTER_OTLP_ENDPOINT`. When unset (the base `make demo`
topology), spans are silently no-op'd and there is zero overhead
beyond the propagator install. The chaos overlay
(`docker-compose.chaos.yml`) sets the env to `jaeger:4317` and ships
Jaeger all-in-one on `http://localhost:16686`.

Operationally this answers two questions nothing else does:

1. **"Doc `users:42` looks wrong — which event chain produced it?"**
   Search Jaeger for spans tagged `db.mongodb.collection=users` and
   the recent timeframe; find the trace that wrote that PK. Walk
   back through the transformer span on the same trace ID.
2. **"Lag p99 just spiked. Where is the time?"** Span timing breaks
   the histogram down without sprinkling timer code into every
   function — decode vs apply vs Mongo BulkWrite are each their own
   span.

## Supply chain

Released images (any tag matching `v*`) are:

- **Multi-arch** (`linux/amd64`, `linux/arm64`) via `buildx`.
- **Signed with cosign keyless.** No long-lived signing keys; GitHub
  OIDC issues a short-lived Sigstore Fulcio cert per workflow run and
  the signature is recorded in the public Rekor transparency log.
- **Attested with SPDX-JSON SBOM.** `syft` generates the SBOM at
  build time; `cosign attest --type spdxjson` binds it to the image
  digest. Consumers verify provenance with `cosign verify` +
  `cosign verify-attestation`.
- **Built with `provenance=mode=max` and `sbom=true`** in buildx so
  the SLSA provenance and BuildKit's own SBOM are also attached at
  the image-manifest level.

CI (every push, not just releases) runs `syft` against each service
module and uploads the SBOM as a build artifact for review.
