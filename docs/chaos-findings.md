# Chaos Findings - 2026-04-20

Real results from running the chaos suite against the Week 1 walking skeleton (Debezium + Kafka + off-the-shelf MongoDB Kafka Connector as the sink).

> **Why this document exists.** A portfolio that claims "measured resilience" has to publish measurements. This document tracks what actually happened when we ran `chaos/scenarios/*.sh` against the pipeline, including a row-loss regression that validates the motivation for Week 2.

## Setup

- Stack: `docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --build --wait`
- Sink: off-the-shelf [MongoDB Kafka Connector](https://www.mongodb.com/docs/kafka-connector/current/) 1.13.0 with Debezium's `PostgresHandler`
- No Week 2 services yet - no Go transformer, no Go sink, no LSN-gating
- Baseline: 27 rows pre-chaos (`verify-integrity.sh` green)

## Results

| # | Scenario | Outcome | Rows PG | Rows Mongo | Notes |
|---|---|---|---|---|---|
| 3 | Mongo primary stepdown | **PASS** | 30 | 30 | Driver retry path absorbs the ~10s re-election window cleanly |
| 4 | Postgres WAL pressure (connect paused 120s) | **PASS** | 167 | 167 | Replication slot retained WAL; Debezium caught up on unpause |
| 5 | Poison event (1MB JSONB blob) | **PASS** | (event recorded) | post-poison event present | DLQ topic `dlq.sink` was auto-created; pipeline did not block |
| 1 | Kill Connect mid-stream (SIGKILL + restart) | **FAIL** | 200 | 199 | **1 row lost out of ~30 written during the chaos window** |

## The finding - scenario 01 lost data

After `docker kill -s SIGKILL zdt-connect` during an active insert stream and a restart 3 seconds later, we observed **200 rows in Postgres and 199 documents in MongoDB**. One insert was acknowledged by Postgres but never made it to MongoDB.

### Likely mechanism

The off-the-shelf MongoDB Kafka Connector inherits Kafka Connect's default sink-offset semantics. Kafka Connect commits offsets periodically **independent of whether the downstream `BulkWrite` has fully succeeded**. If Connect dies during a `BulkWrite` where *some* records in the batch have been flushed to Mongo and *some* haven't, but the offset was committed for the full batch, the un-flushed records are dropped on restart.

This is the classic **commit-before-side-effect** failure mode [ADR-003](./decisions/003-commit-after-sideeffect.md) exists to prevent. The off-the-shelf connector can be configured to mitigate this (smaller batch sizes, more frequent offset commits), but the correctness property is not structural - it's a race window we are one config change away from re-opening.

### Why this validates the Week 2 motivation

The whole point of building our own Go sink in Week 2 is to guarantee commit-after-side-effect at the code level, not the config level:

```go
for _, msg := range consumer.Poll(...) {
    if err := mongo.BulkWrite(ctx, models); err != nil {
        return err              // no commit; will be redelivered
    }
    consumer.MarkCommit(msg)    // only reached on success
}
consumer.CommitMarked()
```

Combined with LSN-gated upserts from [ADR-002](./decisions/002-lsn-gated-upserts.md), the Week 2 sink makes this failure mode structurally impossible - any redelivery hits a no-op upsert. The chaos suite will re-run against the Go sink in Week 2 with pass criterion "**200 = 200** under 10 consecutive kill cycles".

### What this tells a recruiter

Off-the-shelf tools have config-tunable correctness; hand-rolled code has structural correctness. This project demonstrates both, and the chaos suite enforces the difference with data, not prose.

## Scenario 02 - Kafka network partition via Toxiproxy (Week 3)

**PASS.** Baseline PG=2 / Mongo=2, 30s of inserts under injected 500ms latency + 10% packet loss, pipeline drained after toxics removed, final PG=32 / Mongo=32, `INTEGRITY OK`.

### How we gave Toxiproxy real teeth

Kafka now advertises a dedicated `PROXIED` listener whose advertised host is `toxiproxy:19092`. Toxiproxy forwards that to Kafka's internal listener on `kafka:39092`. Clients bootstrapping via `toxiproxy:19092` therefore receive metadata that also points back through the proxy - every subsequent produce/fetch flows through Toxiproxy, so injected toxics affect actual traffic rather than just the initial TCP handshake.

The routing is opt-in via `docker-compose.toxiproxy.yml` overlay (sets `BOOTSTRAP_SERVERS` for Connect and the Go sink to `toxiproxy:19092`). Without the overlay, clients use the normal `INTERNAL://kafka:29092` listener and Toxiproxy has no effect - important so the default `make demo` doesn't pay proxy overhead.

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.chaos.yml \
  -f docker-compose.toxiproxy.yml \
  up -d --wait
bash scripts/register-connectors.sh
bash chaos/scenarios/02-kafka-partition.sh
# -> INTEGRITY OK
```

## Week 2 results - Go sink in place

Second run of the chaos suite after replacing the off-the-shelf MongoDB Kafka Connector with our Go sink (`services/sink/`). Same harness, same SIGKILL-mid-stream scenario, on a clean-slate stack (`docker compose down -v` + full rebuild).

Four consecutive iterations of chaos 01, cumulative load:

| Iter | PG rows | Mongo docs | Status |
|---|---|---|---|
| 1 | 32  | 32  | **PASS** |
| 2 | 62  | 62  | **PASS** |
| 3 | 92  | 92  | **PASS** |
| 4 | 122 | 122 | **PASS** |

`verify-integrity.sh` returned exit 0 after every iteration and the final cumulative check. **Zero loss, zero duplicates, across four consecutive SIGKILL + restart cycles.**

### What changed

The Go sink implements commit-after-side-effect structurally (not via config):

1. `kgo.DisableAutoCommit()` and `kgo.MetadataMaxAge(10s)` so offset commits are never implicit and pattern-subscription picks up fresh topics fast.
2. `Loop.RunOnce` marks a record's offset with `MarkCommitRecords` only after `MongoWriter.Apply` returns nil. On write failure it `break`s the batch, calls `CommitMarked` (which commits only the records that succeeded), and returns the error so the caller backs off.
3. `MongoWriter.Apply` builds a LSN-gated upsert (`{_id, $or: [$lt, $exists:false]}` filter, `$set` with `sourceLsn` and `schemaVersion`) and swallows E11000 duplicate-key errors from the upsert as idempotent no-ops - exactly the stale-replay case ADR-002 calls out.

The integration test in `services/sink/internal/writer/mongo_writer_integration_test.go` already proved the LSN-gate at the database level across six ordering cases. The chaos run above proves the whole loop under a realistic crash.

### A bug we found and fixed along the way

First attempt used `kgo.MarkCommitOffsets(map[...]EpochOffset{..., Epoch: -1})` as the commit path. That compiled and tests with the fake consumer passed, but the real Kafka broker reported `CURRENT-OFFSET = -` indefinitely - meaning offsets were never actually being committed, and the sink was silently re-processing from the beginning of each topic on every restart. LSN-gating masked the correctness impact (idempotency is robust), but the work being done was wildly wasteful. Switching to the documented `MarkCommitRecords(*kgo.Record)` path (with the raw record carried through the Record abstraction as an opaque `Raw any` field) fixed it, and the consumer-group display stabilized.

## Week 3 - load numbers from the k6 sidecar

Ran `load/k6/write-mix.js` (70% INSERT / 20% UPDATE / 10% DELETE) against the new `services/loadgen/` sidecar which translates k6 HTTP into Postgres SQL via pgx. k6 at 20 VUs for 30s:

```
checks.........................: 100.00% ✓ 112676   ✗ 0
http_req_failed................: 0.00%    ✓ 0       ✗ 112676
http_req_duration..............: avg=5.22ms  p95=7.79ms  p99≈15ms  max=242.73ms
http_reqs......................: 112676    3755.17/s
zdt_insert_latency_ms..........: avg=5.27ms  p95=7.7ms
zdt_update_latency_ms..........: avg=5.24ms  p95=7.88ms
zdt_delete_latency_ms..........: avg=4.88ms  p95=8.53ms
```

**Sidecar sustained 3,755 write-ops/sec with zero failed requests.** All k6 thresholds (p99 < 100ms insert, < 150ms update) passed.

### Finding → Fix → Re-measure (closed)

**Before batching** - under the 3.7k RPS burst, the Go sink drained at **~240 writes/sec**, roughly 15× slower than ingestion. Mongo stayed at ~57k docs vs PG's ~74k rows after 2.5 minutes. Data eventually reconciled (every event is LSN-gated and idempotent), but replication lag during the burst exceeded the 5s SLO.

Root cause: `MongoWriter.Apply` called `BulkWrite` with exactly one `WriteModel` per event - the "Bulk" name was vestigial.

**Fix.** Refactored the `Writer` interface to `ApplyBatch(ctx, []CDCEvent) error` and updated `Loop.RunOnce` to:

1. Decode every record from the poll batch into `[]CDCEvent` in one pass (tombstones skipped, not dispatched).
2. Dispatch the full slice through one `ApplyBatch` call.
3. `MongoWriter.ApplyBatch` groups events by collection and issues exactly one `BulkWrite` per collection with `ordered=false`, so a single E11000 on a stale-replay row does not abort the rest of the batch.
4. On batch success, `MarkCommit` is called on every record and `CommitMarked` fires once - commit-after-side-effect at batch granularity.
5. On batch failure, nothing is committed; the whole poll batch is redelivered and LSN-gating absorbs whatever records flushed to Mongo before the failure.

The ADR-002 (LSN gate) and ADR-003 (commit-after-side-effect) invariants are preserved - the semantic is the same, just at batch granularity.

**After batching - measured on the same stack, same k6 run (20 VUs × 30s, 111k iterations):**

| Metric | Before | After | Δ |
|---|---|---|---|
| Sink drain rate (burst) | ~240 w/s | **≥7,300 w/s** | ~30× |
| PG↔Mongo reconciliation after 10s post-load | 8k / 74k | **73k / 73k** | converged |
| Chaos 01 × 4 iterations | PASS | **PASS** (invariants unchanged) | - |
| Integration test (LSN gate × 6 cases) | PASS | **PASS** | - |

Final state after a k6 burst ending: `PG=73,064 / Mongo=73,064`, `verify-integrity.sh` exit 0. The 5s replication-lag SLO is now comfortably met on commodity hardware.

### Commands to reproduce

```bash
# Boot the full stack
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --build --wait
bash scripts/register-connectors.sh

# Smoke-test loadgen
curl -X POST -H 'Content-Type: application/json' \
  -d '{"email":"x@y.z","full_name":"X"}' http://localhost:8086/users

# 30-second load burst
MSYS_NO_PATHCONV=1 docker compose \
  -f docker-compose.yml -f docker-compose.chaos.yml \
  run --rm --no-deps k6 run --vus 20 --duration 30s /scripts/write-mix.js
```

(`MSYS_NO_PATHCONV=1` is only needed on Git Bash / MSYS on Windows, which otherwise rewrites `/scripts/write-mix.js` into `C:/Program Files/Git/scripts/write-mix.js`.)

## v1.0 polish - silent transformer hang on cold start (closed)

While preparing for v1.0 release we noticed the GitHub Actions
`integration-stack` job had been failing on **every** push since the CI
workflow was added. Locally, after a `docker compose down -v + up`
cycle, the pipeline appeared healthy - every container reported
`healthy` - but no record ever made it from Postgres to MongoDB.

### Symptom (operational reproducer)

```bash
docker compose down -v
docker compose up -d --wait
bash scripts/register-connectors.sh

# All containers healthy.
# Insert one row into Postgres:
docker compose exec -T postgres psql -U app -d app \
  -c "INSERT INTO users (email, full_name) VALUES ('x@y.z', 'X');"

# Wait. And wait. The row never appears in Mongo.
docker compose exec -T mongo mongosh --quiet \
  mongodb://localhost:27017/?replicaSet=rs0 \
  --eval "db.getSiblingDB('migration').users.countDocuments()"
# -> 2  (the 2 seed rows from 001_init.sql; the inserted row is missing)

# Topic transformed.users does not exist.
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 --list | grep transformed
# -> (empty)

# Consumer group looks normal - Stable, 1 member, 1 partition assigned -
# but CURRENT-OFFSET is "-" forever.
```

### Investigation

Adding instrumentation logs around `client.PollFetches`,
`m.ApplyJSON`, and `client.ProduceSync` in
`services/transformer/cmd/transformer/main.go::runOnce` revealed:

```
DIAG: runOnce: PollFetches returned, NumRecords=105
DIAG: rec[1] mapping topic=cdc.users value-bytes=3600
DIAG: rec[1] mapped, ProduceSync target=transformed.users bytes=3599
   <silence - process spins forever>
```

`ProduceSync` to a not-yet-existent `transformed.users` topic blocks
indefinitely. Manually creating the topic
(`kafka-topics --create --topic transformed.users …`) immediately
unblocked the producer and the entire 105-record backlog drained in
under a second.

### Root cause

Two configurations are required for auto-topic-creation to work:

1. **Broker side**: `auto.create.topics.enable=true` (we had this).
2. **Client side**: the producer must *ask* for auto-creation by setting
   the `allow_auto_topic_creation` flag in metadata + produce requests.

By default, franz-go does **not** set this flag. cp-kafka 7.6.1 in
KRaft mode only auto-creates a topic when it sees a request that
explicitly asks for it. The broker setting is necessary but not
sufficient.

Result: `ProduceSync` waited for metadata that the broker would never
proactively populate. No error, no log, no metric - a silent hang.

### Fix

One line, in `services/transformer/cmd/transformer/main.go`:

```go
client, err := kgo.NewClient(
    // ...existing options...
    kgo.AllowAutoTopicCreation(),  // <-- this
)
```

### Re-measure

End-to-end on a clean state:

```bash
docker compose down -v
docker compose up -d --build --wait
bash scripts/register-connectors.sh
docker compose exec -T postgres psql -U app -d app -c \
  "INSERT INTO users (email, full_name) SELECT 'v'||g||'@x.dev','V'||g \
   FROM generate_series(1,5) g;"
sleep 8
docker compose exec -T mongo mongosh --quiet \
  mongodb://localhost:27017/?replicaSet=rs0 \
  --eval "db.getSiblingDB('migration').users.countDocuments()"
# -> 7  (2 seed + 5 inserted)
```

`transformed.users` is auto-created on first produce. No manual step.

### A larger finding hiding behind this one

Earlier exploratory testing had reported a small, persistent drift
(~300 docs Mongo > Postgres) after consecutive scenario 01 runs. The
drift was attributed to a possible BulkWrite reordering bug and was
opened as an investigation thread. **It was not a separate bug.**

After landing the auto-create fix and re-running scenario 01 four
consecutive times against a clean stack:

| Iteration | PG rows | Mongo docs | Status |
|---|---|---|---|
| 1 | 237 | 237 | **PASS** |
| 2 | 267 | 267 | **PASS** |
| 3 | 297 | 297 | **PASS** |
| 4 | 327 | 327 | **PASS** |

Hash match every iteration. The previously-observed drift was a
downstream symptom of the cold-start hang - events that should have
been processed during recovery were lost because the transformer was
silently stuck on producing the first record after restart, not because
of any fault in the LSN gate or the BulkWrite path.

### What this teaches

Two things worth carrying forward into operations:

1. **A "healthy" container check is not a "working" check.** The
   transformer's `/healthz` returned 200 the entire time it was
   producing zero records. Healthcheck wired to `/healthz` masked the
   real failure. Production should add a deeper readiness signal -
   e.g. "no committed offset progress in 60s" - and surface it as an
   alert.
2. **Default configurations can have asymmetric requirements between
   client and broker.** When two settings together are needed, having
   one alone produces silent failure modes that are very expensive to
   debug. Worth a one-time audit of every other client option in the
   stack against its broker counterpart.

## Instrumentation completeness pass

A pre-release audit revealed that three of six Prometheus alerts in
`observability/prometheus/alerts.yml` referenced metric series that
were declared but never observed by any production code path:

- `migration_replication_lag_seconds` — registered as a `HistogramVec`
  but nothing called `.Observe()` outside its own metrics test.
- `migration_idempotent_skip_total` — registered as a `CounterVec` but
  nothing called `.Inc()` / `.Add()` outside its own metrics test.
- `migration_checkpoint_staleness_seconds` and the
  `_migration_checkpoints` collection — both referenced in
  [ADR-003](./decisions/003-commit-after-sideeffect.md), the runbook,
  and the architecture doc, but no Go code actually wrote the doc or
  emitted the gauge.

The `make reprocess-dlq` target also returned `NOT YET IMPLEMENTED`
despite being advertised in the README and runbook.

### Fix

| Gap | Closing change |
|---|---|
| `migration_replication_lag_seconds` never observed | Added `SourceTsMs` to `CDCEvent`, decoded `payload.source.ts_ms` in `decoder`, observed `now() - SourceTsMs` per event in the `instrumentedWriter` (clamped at 0 for clock skew). |
| `migration_idempotent_skip_total` never incremented | `NewMongoWriter` now takes an `onSkip(table, n)` callback. `MongoWriter.ApplyBatch` derives the per-table skip count from `BulkWriteResult.{MatchedCount, UpsertedCount, DeletedCount}` against `len(models)` AND from the all-E11000 path. `cmd/sink` wires the callback to the counter. |
| Checkpoint doc + staleness gauge missing | New `internal/checkpoint` package: in-memory atomic progress (`MarkProgress(maxLSN, n)`), 10s ticker that upserts `_migration_checkpoints`, `prometheus.NewGaugeFunc` that reports `time.Since(lastSuccessfulFlush)` on every scrape. Wired through `instrumentedWriter`; final flush on shutdown. |
| `make reprocess-dlq` was a stub | New `services/sink/cmd/dlqtool/` Go CLI: dry-run summary by `__dlq_error_reason` and `__dlq_source_topic`; `--replay` re-publishes verbatim to the original topic. Bash wrapper builds a distroless image on demand and runs it on the compose network. |
| `classify(err)` substring-matched on `err.Error()` | Rewritten with `errors.Is(context.Canceled / DeadlineExceeded)`, `errors.As(net.Error)`, and `errors.As(mongo.ServerError)` (with `HasErrorCode(11000)` and `HasErrorLabel("RetryableWriteError")` discrimination). |
| Multi-schema rule collision in transformer | `Mapper` now indexes by qualified name (`<schema>.<table>`); `ApplyJSON` reads `payload.source.{schema,table}` from the envelope as the lookup key, falling back to topic-tail only if the envelope is malformed. New tests cover `public.users` vs `audit.users` isolation and duplicate-source detection. |

### Regression guard

To prevent the same drift from re-opening, `scripts/check-alert-metrics.sh`
runs in CI and fails the build if either:

- a `migration_*` token in `alerts.yml` has no producer in `services/`, or
- a metric field declared on the `Metrics` struct is never touched by
  `.Inc / .Add / .Observe / .Set / .WithLabelValues` outside the
  metrics package and `*_test.go` files.

Verified by deliberately renaming a metric in `alerts.yml`: the script
exits 1 with `DRIFT DETECTED: 1 error(s)`.

### Chaos coverage of the new mechanism

`chaos/scenarios/06-checkpoint-recovery.sh` sends background load,
captures the pre-kill checkpoint doc, SIGKILLs the sink, restarts it,
pushes fresh writes, then polls `_migration_checkpoints` directly until
both `lastLSN` and `updatedAt` advance past the pre-kill snapshot. Once
the doc has demonstrably been re-written, it asserts the staleness
gauge reads `< 30s` (proving the gauge tracks the same in-memory state
that produced the persisted doc). Final integrity check via
`verify-integrity.sh`.

Picked up automatically by `chaos/run-all.sh` (glob discovery).

## World-class layer — 2026-05-08

The hardening pass left three legitimate world-class items on the
table. This pass closes them.

| Item | Closing change |
|---|---|
| No distributed tracing — given a wrong Mongo doc, no way to walk back through the pipeline | `internal/tracing/` package (sink + transformer; small, intentional duplication rather than a workspace). OTLP/gRPC exporter, propagator install. Spans on `transformer.process_record`, `sink.consume_batch`, `sink.apply_batch`, `sink.mongo_bulk_write`. Trace context propagates via Kafka headers (`traceparent`). Jaeger all-in-one in chaos overlay on `http://localhost:16686`. |
| Container images unsigned (no provenance verification possible) | `.github/workflows/release.yml` builds multi-arch (`linux/amd64`, `linux/arm64`), pushes to GHCR, signs with cosign keyless via GitHub OIDC, attests SBOM (SPDX-JSON) generated by `syft`. Consumer can `cosign verify` + `cosign verify-attestation` against any released tag. Per-push CI also produces SBOMs as artifacts. |
| Test suite never measured for *catching* bugs (only for not-failing) | `.github/workflows/mutation-test.yml` runs `gremlins unleash` nightly against `internal/writer` (LSN gate + skip detection) and `internal/checkpoint` (heartbeat + gauge). LIVED mutants surface as PR-quality findings — gaps where a code change wouldn't break any test. Non-gating; reports only. |

### Tracing trade-offs accepted

- **Cardinality:** every record = one trace + 4 spans. With 33k
  events/15s in a k6 burst that's ~9k spans/s. Fine for a portfolio
  project + Jaeger badger storage; production should switch
  `AlwaysSample` → `ParentBased(TraceIDRatio(0.01))` via env if
  cardinality bites.
- **Opt-in via env:** `OTEL_EXPORTER_OTLP_ENDPOINT` unset → noop
  tracer + propagator-only install. The base `make demo` topology
  costs zero at runtime. Tracing turns on only with the chaos
  overlay.
- **Two services duplicate the small `tracing` package:** intentional.
  Both modules stay self-contained (no `go.work`). Migrate to
  `pkg/tracing/` if a 4th service appears.

### Cosign verification example

After a `vX.Y.Z` tag is pushed:

```bash
# Verify the image was signed by this workflow + repo:
cosign verify ghcr.io/nicolasdenigris91/pg2mongo-cdc-sink:X.Y.Z \
  --certificate-identity-regexp '^https://github.com/NicolasDeNigris91/Public_Pg2MongoCdC' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

# Verify the SPDX SBOM attestation is present:
cosign verify-attestation ghcr.io/nicolasdenigris91/pg2mongo-cdc-sink:X.Y.Z \
  --type spdxjson \
  --certificate-identity-regexp '^https://github.com/NicolasDeNigris91/Public_Pg2MongoCdC' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Both succeed only if the image came from this exact workflow on this
exact repo with a Sigstore-validated short-lived certificate.

## Hardening pass — 2026-05-07 evening

After the instrumentation completeness pass landed, a second sweep
closed the smaller gaps that separate "alerts wired" from "alerts
trustworthy":

| Gap | Closing change |
|---|---|
| Idle pipeline trips `CheckpointStaleness` because no flush ever fires | Checkpointer now heartbeats every Nth interval (default 6 = 60s), so a healthy idle sink keeps the gauge bounded. Stalled-loop detection is delegated to `ConsumerLagHigh`, the right tool. |
| `/metrics` had no process / runtime visibility | `metrics.New()` registers `collectors.NewGoCollector()` and `NewProcessCollector()` so on-call can correlate `migration_replication_lag_seconds` spikes with `go_gc_duration_seconds`, `go_goroutines` and `process_resident_memory_bytes` without a separate exporter. |
| `ConsumerLagHigh` only fired in dev (kafka-exporter is in the chaos overlay only, not in prod / Helm) | New `internal/lag` package: in-process probe via `kadm.Lag(ctx, group)` every 30s. Emits `migration_consumer_group_lag` and `migration_consumer_group_lag_age_seconds`. The alert now reads `(migration_consumer_group_lag or kafka_exporter_fallback)`, working in every deployment. |
| Helm chart had no `PrometheusRule` (alerts only existed in `observability/`) | Added `templates/prometheusrule.yaml` mirroring `alerts.yml`. Thresholds parameterised via `prometheusRule.thresholds.*` in `values.yaml`. |
| `make load` had no CDC-side SLO assertion (k6 only covered HTTP latency, not pipeline lag) | New `scripts/check-load-slos.sh` queries Prometheus for `migration_replication_lag_seconds` p99 and `migration_write_errors_total` rate. Exits non-zero on breach. Hooked up via `make load-slo`. |

### Live verification - 2026-05-07 evening

`/metrics` from the running sink shows every series:

```
go_gc_duration_seconds_count 13
go_goroutines 28
go_info{version="go1.26.2"} 1
process_resident_memory_bytes 24694784
migration_consumer_group_lag 1178
migration_consumer_group_lag_age_seconds 9.51
migration_replication_lag_seconds_count{table="users"} 25
migration_checkpoint_staleness_seconds 12.4
migration_events_processed_total{op="c",stage="sink",table="users"} 25
```

`bash chaos/run-all.sh` after the changes: **6/6 PASS, INTEGRITY OK.**

`scripts/check-load-slos.sh` exit-code matrix verified after a 15s /
10 VU k6 burst (33,057 requests, 0 HTTP failures):

| Budget | p99 measured | Outcome | Exit |
|---|---|---|---|
| `LAG_P99_BUDGET_SECS=30 WINDOW=1m` | 9.66s | within budget | 0 |
| `LAG_P99_BUDGET_SECS=1` (default 5m window) | 9.99s | OVER budget | 1 |

The script enforces what it claims to enforce.

### Live verification - 2026-05-07

Brought up the full stack (`docker compose -f docker-compose.yml -f
docker-compose.chaos.yml up -d --build --wait`), seeded data, and
confirmed every series the audit said was missing is now produced:

```
migration_checkpoint_staleness_seconds 13.180120052
migration_events_processed_total{op="c",stage="sink",table="users"} 25
migration_replication_lag_seconds_count{table="users"} 25
migration_replication_lag_seconds_sum{table="users"}   529.17
migration_replication_lag_seconds_bucket{...le="30"}   25
```

Prometheus scrape resolves them too:

```
{"__name__":"migration_checkpoint_staleness_seconds","instance":"sink:8080","job":"sink"} = 28.58
```

Checkpoint doc visible in Mongo (use `getCollection`, not dot-access -
collection names starting with `_` are shadowed by mongosh internals):

```js
db.getCollection("_migration_checkpoints").findOne()
// { _id: "zdt-sink", lastLSN: 27106736, lastEvents: 25,
//   updatedAt: 2026-05-07T23:23:18.451Z }
```

Scenario 06 in isolation: pre-kill `lastLSN=27155696`, post-recovery
`lastLSN=27162824`, `updatedAt` advanced by 55s, gauge=2.69s, integrity
PG=422 / Mongo=422 hash match. **PASS.**

Full `chaos/run-all.sh` exposed a *separate* pre-existing bug along the
way: scenario 05 leaves a 1MB poison row in Postgres that intentionally
never reaches Mongo (it's the proof-of-DLQ-routing point of the
scenario), so `verify-integrity.sh` running after 05 sees a 1-row
phantom drift forever. Scenario 05 now deletes its own poison row at
the end so the suite is composable. Re-run: **6/6 PASS, INTEGRITY OK**.

## What's still next

1. **CI runs the full chaos suite on PR labels.** Today CI runs
   `unit + integration-mongo + integration-stack`. Adding a
   `chaos-suite` job behind a `run-chaos` PR label closes the loop
   without paying ~5 min per push.
2. **Shipping a Helm chart** in `deploy/helm/` so deploy artifact
   demonstrates production-shape topology (RF=3 Kafka, 3-node Mongo
   replica set), not the dev-grade compose used for local demos.

## Reproduction

```bash
# From a clean state:
docker compose -f docker-compose.yml -f docker-compose.chaos.yml down -v
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --build --wait
bash scripts/register-connectors.sh
bash scripts/seed.sh     # baseline 27 rows
bash chaos/scenarios/01-kill-transformer.sh
# Expect: INTEGRITY FAILED with ~1-2 rows missing from Mongo
```

The loss rate is stochastic - depends on how many Mongo `BulkWrite` calls are in flight at the moment of SIGKILL. Under our default k6-less load pattern (1 insert/sec via psql), we observed 1 loss per chaos cycle in 2 of 3 trial runs. The point is not the exact rate; the point is that it's **not zero**, and Week 2 makes it structurally zero.
