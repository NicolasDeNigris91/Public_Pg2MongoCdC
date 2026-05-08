# Runbook - what to do when each alert fires

Every alert from `observability/prometheus/alerts.yml` has a section here. Keep the sections short, declarative, and in the same order as the alerts file.

---

## ReplicationLagHigh

**Symptom.** `histogram_quantile(0.99, ... replication_lag_seconds ...) > 10s` for 5 minutes.

**Likely causes (in order of probability):**

1. Sink-svc CPU saturated → scale out (`docker compose up --scale sink=3`).
2. Mongo write latency up → check `db.serverStatus().wiredTiger.concurrentTransactions.write.available`.
3. Kafka broker under-replicated → check `kafka_server_ReplicaManager_UnderReplicatedPartitions`.
4. Transformer blocked on a slow YAML rule (rare) → check `migration_events_processed_total` rate by stage.

**First move.** Check which stage is lagging. `rate(migration_events_processed_total{stage="transformer"}[1m])` vs `{stage="sink"}`. Whichever is *lower* is the bottleneck.

---

## ConsumerLagHigh

**Symptom.** A consumer group's lag exceeds 10k records for 2 minutes.

**Likely causes.**

1. Crash-recovery in progress - lag should shrink within one `max.poll.interval.ms` (5 min default).
2. A stuck consumer thread - check logs for `Rebalance in progress`.
3. Downstream write failure loop - correlate with `migration_write_errors_total`.

**Do NOT** restart blindly. A rebalance storm amplifies lag. Wait 5 min first.

---

## ConsumerLagProbeStale

**Symptom.** `migration_consumer_group_lag_age_seconds > 300` for 5 minutes.

The sink runs an in-process probe (every 30s) that asks Kafka for its
own consumer-group lag. This alert says the probe has not succeeded in
over 5 minutes, even though the metrics endpoint is responding. That
means the sink can serve scrapes but cannot reach the broker's
admin API.

**Likely causes.**

1. Broker ACLs changed and the sink's principal lost `Describe` on the
   consumer group.
2. NetworkPolicy / SecurityGroup tightened and admin port is no longer
   reachable from the sink pod.
3. KRaft controller in a bad state (rare; correlate with
   `kafka_controller_*` metrics).

**Check.**

```bash
kubectl exec -n pg2mongo-cdc deploy/sink -- /bin/sh -c \
  "wget -qO- http://localhost:8080/metrics | grep migration_consumer_group_lag"
```

If `migration_consumer_group_lag_age_seconds` keeps growing, the in-process
probe is failing. Check sink logs for `lag probe:` lines for the broker
error. The data-plane (consume / produce) may still be working - lag
just becomes invisible.

---

## DLQNonEmpty

**Symptom.** At least one message in a `dlq.*` topic.

**First move.** Inspect it:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic dlq.source \
  --from-beginning --max-messages 10 --timeout-ms 5000 \
  --property print.headers=true
```

The headers include: `__dlq_error_reason`, `__dlq_source_offset`, `__dlq_source_topic`. These tell you what, where, and why.

**Reprocessing.** Once the root cause is fixed in code / config, triage first:

```bash
make reprocess-dlq                              # dry-run: counts by reason + source topic
make reprocess-dlq ARGS="dlq.source"            # one topic only
```

When you are confident the root cause is fixed, replay:

```bash
make reprocess-dlq ARGS="dlq.source --replay"   # re-publishes to __dlq_source_topic
```

The replayer preserves key + value bytes verbatim and uses the `__dlq_source_topic` header to route. If the same poison shape repeats it will land back in the DLQ with fresh provenance — never `kafka-console-producer` an old event back without first fixing the underlying schema/code, or you'll just rebuild the same DLQ.

---

## CheckpointStaleness

**Symptom.** `migration_checkpoint_staleness_seconds > 60` for 2 minutes.

Fires when `sink-svc` has stopped writing `_migration_checkpoints` docs. It usually means either:

- The sink process is alive but its consume loop has stalled (deadlock, blocked Mongo write).
- The clock on the sink host is skewed forward.

**First move.** `docker compose logs sink | tail -200` and look for repeated errors around `BulkWrite`. If logs are silent but the process is up, check the goroutine dump at `/debug/pprof`.

---

## WriteErrorRateHigh

**Symptom.** Sink write error rate > 0.1% for 5 minutes.

**Likely causes.**

1. Mongo is in a failover / primary election (usually recovers in < 30s).
2. Schema-registry version mismatch after a producer upgrade.
3. Disk full on Mongo data volume.

**Check.** `docker compose exec mongo mongosh --eval "rs.status()"`.

---

## SinkApplyStuck

**Symptom.** `migration_consecutive_apply_failures > 5` for 2 minutes.

The sink has failed every batch for the last few minutes. It is now
sleeping ~30s between retries (the backoff cap, kept under Kafka's
`max.poll.interval.ms` so the consumer is not kicked out of the group).
Data is NOT being lost — the LSN gate ensures duplicates are no-ops on
recovery — but Mongo (or whatever the immediate downstream is) is
unreachable / rejecting writes.

**First move.** Look at `migration_write_errors_total` by `reason`:

```
sum(increase(migration_write_errors_total[5m])) by (reason)
```

- `reason="connection"` -> Mongo unreachable. Check pod-to-Mongo
  network reachability + DNS.
- `reason="timeout"` -> Mongo overloaded (CPU / IO). Check
  `mongo_op_counters_repl` and `mongo_connections_current`.
- `reason="server"` / `reason="duplicate_key"` (unexpected) -> read
  the sink logs - the underlying Mongo error is wrapped in the log
  line.
- `reason="context"` -> we are shutting down; the alert will clear.

If the sink is healthy but the alert keeps re-firing, investigate
in-flight batch size — `BulkWrite` >16MB silently fails on Mongo and
the batch will redeliver in a tighter loop.

---

## ReplicationSlotInactive

**Symptom.** `pg_replication_slots_active == 0` for 2 minutes.

**Severity: critical.** WAL will accumulate on the source until disk fills. In a real prod migration this is a paging alert.

**First move.**

1. Is `connect` running and healthy? `docker compose ps connect`.
2. Is Debezium's task in FAILED state? `curl localhost:8083/connectors/zdt-postgres-source/status | jq '.tasks[].state'`.
3. If FAILED, read the trace and restart:
   ```bash
   curl -X POST localhost:8083/connectors/zdt-postgres-source/restart
   ```
4. If the slot was dropped externally (someone ran `pg_drop_replication_slot`), you lost data from the WAL window since Debezium's last confirmed LSN. Full resnapshot required:
   ```bash
   curl -X DELETE localhost:8083/connectors/zdt-postgres-source
   # re-register with snapshot.mode=initial - incurs a full table scan
   ```

---

## Discovery / debugging generics

- **Connector status in one line:** `curl -sS localhost:8083/connectors?expand=status | jq '.[].status | {name, state: .connector.state, tasks: [.tasks[].state]}'`
- **Postgres slot state:** `SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal FROM pg_replication_slots;`
- **Kafka topic offsets:** `docker compose exec kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic-pattern 'cdc\..*'`
- **Mongo per-collection counts:** `docker compose exec mongo mongosh --quiet mongodb://localhost:27017/migration?replicaSet=rs0 --eval 'db.getCollectionNames().forEach(n => print(n, db[n].countDocuments()))'`
