# pg2mongo-cdc

A change-data-capture pipeline that streams a live PostgreSQL table into MongoDB
without stopping writes on the source. Postgres logical replication feeds
Debezium into Kafka; two Go services (`transformer-svc`, `sink-svc`) consume
Kafka, apply YAML-driven mapping rules, and write LSN-gated upserts into Mongo.

[![CI](https://github.com/NicolasDeNigris91/Public_Pg2MongoCdC/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/NicolasDeNigris91/Public_Pg2MongoCdC/actions/workflows/ci.yml)
[![CodeQL](https://github.com/NicolasDeNigris91/Public_Pg2MongoCdC/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/NicolasDeNigris91/Public_Pg2MongoCdC/actions/workflows/codeql.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/NicolasDeNigris91/Public_Pg2MongoCdC)](https://goreportcard.com/report/github.com/NicolasDeNigris91/Public_Pg2MongoCdC)
[![Go version](https://img.shields.io/badge/go-1.26-00ADD8?logo=go)](./services/sink/go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](./CONTRIBUTING.md)

```
PG -> WAL -> Debezium -> Kafka -> transformer (Go) -> Kafka -> sink (Go) -> MongoDB
```

## Run it locally

```bash
cp .env.example .env
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --build --wait
bash scripts/register-connectors.sh
bash scripts/seed.sh
```

Grafana on :3000, Prometheus on :9090, Jaeger on :16686, Toxiproxy on :8474.

A chaos scenario (kill a service mid-stream and assert PG/Mongo converge):

```bash
bash chaos/scenarios/01-kill-transformer.sh
```

## How it works

- **Postgres -> Kafka:** Debezium, partitioned by primary key so events for the
  same row stay ordered.
- **Transformer:** stateless Kafka consumer. Reads YAML rules from
  `schema/transforms/<table>.yml`, rewrites the Debezium envelope into the
  target document shape, republishes to `transformed.<table>`.
- **Sink:** consumes `transformed.*`, batches into MongoDB BulkWrite. Every
  upsert is LSN-gated so a redelivered or stale event is a no-op rather than a
  stale overwrite. See [`docs/decisions/002-lsn-gated-upserts.md`](./docs/decisions/002-lsn-gated-upserts.md)
  for the filter shape and why E11000 on upsert is the expected idempotent path.
- **Offsets:** committed only after the downstream write succeeds. See
  [`docs/decisions/003-commit-after-sideeffect.md`](./docs/decisions/003-commit-after-sideeffect.md).

## Layout

| Path | What's there |
|---|---|
| `services/transformer/` | Go service: Kafka -> YAML mapper -> Kafka |
| `services/sink/` | Go service: Kafka -> Mongo BulkWrite |
| `services/loadgen/` | HTTP service used by k6 to generate write load |
| `schema/transforms/` | YAML mapping rules, one file per table |
| `chaos/scenarios/` | Bash chaos scripts; each carries a PASS check |
| `connectors/` | Debezium / Connect JSON configs |
| `deploy/helm/pg2mongo-cdc/` | Helm chart for the three workers |
| `docker-compose.prod.yml` | Reference data-plane topology (RF=3, 3-node Mongo) |
| `docs/` | Architecture, ops, runbook, SLOs, chaos findings |

## Tests

```bash
make test            # unit
make test-mongo      # sink writer against a live Mongo
make test-stack      # transformer + sink end-to-end on docker compose
```

## Limitations

- Single-region. No MirrorMaker 2 / cross-region tombstone handling.
- Local `docker-compose.yml` runs Mongo as a single-node replica set and Kafka
  with `RF=1`. `docker-compose.prod.yml` and the Helm chart restore RF=3 +
  `min.insync.replicas=2` and a 3-node Mongo set.
- Wire format is JSON, not Avro. Confluent Schema Registry is in the stack
  but unused. Switching is a Connect-config change.
- Secrets are loaded from `.env` for local dev. Production deployments should
  use Vault / AWS Secrets Manager / External Secrets; see [SECURITY.md](./SECURITY.md).
- No DLQ web UI. Triage and replay is `make reprocess-dlq` (dry-run by
  default; `make reprocess-dlq ARGS="--replay"` actually re-publishes each
  record back to its original topic from the `__dlq_source_topic` header).

## Production deployment

The Helm chart in `deploy/helm/pg2mongo-cdc/` deploys Connect, the transformer,
and the sink. The data plane (Postgres, Kafka, Mongo, Schema Registry) is
expected to come from managed services or separate operators.

```bash
helm install pg2mongo-cdc deploy/helm/pg2mongo-cdc \
  --namespace pg2mongo-cdc --create-namespace \
  -f values.production.yaml
```

See [`docs/deployment.md`](./docs/deployment.md) for the full procedure.

## Docs

- [`docs/architecture.md`](./docs/architecture.md) - components and data flow
- [`docs/operations.md`](./docs/operations.md) - day-to-day ops
- [`docs/runbook.md`](./docs/runbook.md) - per-alert response
- [`docs/slo.md`](./docs/slo.md) - SLIs and error budgets
- [`docs/security.md`](./docs/security.md) - threat model and secrets
- [`docs/chaos-findings.md`](./docs/chaos-findings.md) - chaos runs
- [`docs/invariants.md`](./docs/invariants.md) - dev/prod compose differences
- [`docs/decisions/`](./docs/decisions/) - architecture decision records
  - [ADR-002](./docs/decisions/002-lsn-gated-upserts.md) - LSN-gated idempotent upserts
  - [ADR-003](./docs/decisions/003-commit-after-sideeffect.md) - Commit-after-side-effect
  - [ADR-004](./docs/decisions/004-schema-evolution.md) - Schema evolution via dual-write windows

## License

[MIT](./LICENSE).
