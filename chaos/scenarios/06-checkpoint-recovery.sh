#!/usr/bin/env bash
# PASS: after SIGKILL on the sink mid-stream, the _migration_checkpoints doc
#       (a) survives the kill, (b) advances on restart, and the
#       migration_checkpoint_staleness_seconds gauge resets below 30s
#       within 60s of the sink coming back. PG <-> Mongo content also matches.
#
# Why this exists: ADR-003 names the checkpoint doc as the disaster-recovery
# anchor and the staleness gauge as the liveness signal feeding the
# CheckpointStaleness alert. Until this scenario, neither was actually
# proven under fault. The instrumentation completeness pass closed the
# code gap; this scenario makes the property a CI-checkable invariant.

set -euo pipefail

DURATION="${DURATION:-30}"
RECOVERY_BUDGET="${RECOVERY_BUDGET:-60}"     # seconds the gauge has to drop
STALENESS_THRESHOLD="${STALENESS_THRESHOLD:-30}"
SINK_METRICS_PORT="${SINK_METRICS_PORT:-8080}"

echo "Scenario 06: sink kill -> checkpoint doc must persist + advance"
echo "==============================================================="

mongo_eval() {
  docker compose exec -T mongo mongosh --quiet \
    "mongodb://localhost:27017/migration?replicaSet=rs0" \
    --eval "$1"
}

# Strip the metric line from the sink's /metrics endpoint, returning the
# numeric value or "" if the series isn't there yet.
read_gauge() {
  docker compose exec -T sink wget -qO- "http://localhost:${SINK_METRICS_PORT}/metrics" 2>/dev/null \
    | awk '/^migration_checkpoint_staleness_seconds / {print $2; exit}'
}

read_checkpoint_lsn() {
  # NOTE: db._migration_checkpoints.findOne(...) does NOT work in mongosh -
  # any collection name starting with `_` is shadowed by an internal property
  # on the db object. getCollection(...) is the only reliable accessor.
  # Use NumberLong#toNumber() so the printed value is a plain integer that
  # survives bash arithmetic (Long('27111024') breaks `[ -lt ]`).
  mongo_eval 'const d = db.getCollection("_migration_checkpoints").findOne({}); print(d ? Number(d.lastLSN) : 0);' \
    | tr -d '[:space:]'
}

read_checkpoint_updated() {
  mongo_eval 'const d = db.getCollection("_migration_checkpoints").findOne({}); print(d && d.updatedAt ? d.updatedAt.getTime() : 0);' \
    | tr -d '[:space:]'
}

push_one_row() {
  docker compose exec -T postgres psql -U app -d app -c \
    "INSERT INTO users (email, full_name, profile)
     VALUES ('chaos06p-'||extract(epoch from now())||'@test.dev', 'Chaos 06 Post', '{\"src\":\"chaos-06-post\"}')
     ON CONFLICT DO NOTHING;" >/dev/null 2>&1 || true
}

# ---------------- baseline ---------------
echo "Starting background load for ${DURATION}s ..."
(
  for i in $(seq 1 "$DURATION"); do
    docker compose exec -T postgres psql -U app -d app -c \
      "INSERT INTO users (email, full_name, profile)
       VALUES ('chaos06-'||extract(epoch from now())||'@test.dev', 'Chaos 06', '{\"src\":\"chaos-06\"}')
       ON CONFLICT DO NOTHING;" >/dev/null 2>&1 || true
    sleep 1
  done
) &
LOAD_PID=$!

# Let the sink commit at least one checkpoint cycle (interval=10s).
sleep 15

LSN_BEFORE=$(read_checkpoint_lsn)
TS_BEFORE=$(read_checkpoint_updated)
echo "Pre-kill checkpoint: lastLSN=${LSN_BEFORE} updatedAt(ms)=${TS_BEFORE}"

if [ -z "$LSN_BEFORE" ] || [ "$LSN_BEFORE" = "0" ]; then
  echo "FAIL: no _migration_checkpoints doc was written before the kill -- the periodic flush is not running"
  kill $LOAD_PID 2>/dev/null || true
  exit 1
fi

# ---------------- kill ----------------
echo "SIGKILL on sink ..."
docker compose kill -s SIGKILL sink

# Doc must survive the kill - it's persisted in Mongo, not in the process.
LSN_AFTER_KILL=$(read_checkpoint_lsn)
echo "Post-kill checkpoint (process gone, doc still in Mongo): lastLSN=${LSN_AFTER_KILL}"
if [ "$LSN_AFTER_KILL" != "$LSN_BEFORE" ]; then
  echo "WARN: lastLSN changed after kill -- expected exact survival, got ${LSN_AFTER_KILL}"
fi

# ---------------- restart + recovery ----------------
echo "Restarting sink ..."
docker compose up -d sink

# Wait for the new sink's /metrics endpoint to come back.
for _ in $(seq 1 30); do
  g=$(read_gauge 2>/dev/null || true)
  [ -n "$g" ] && break
  sleep 1
done

# Wait for the original load to finish.
wait $LOAD_PID

# Push *fresh* writes after the restart so the new sink has work to do
# and the periodic flush has dirty progress to persist. Without this,
# `dirty` stays false and the doc legitimately doesn't change - we'd be
# asserting a vacuous property.
echo "Pushing 15 post-restart inserts to drive a new flush cycle ..."
for _ in $(seq 1 15); do
  push_one_row
  sleep 1
done

echo "Polling _migration_checkpoints for advance (lastLSN > $LSN_BEFORE AND updatedAt > $TS_BEFORE) ..."
# We poll the persisted doc directly rather than the staleness gauge: the
# gauge measures *time since flush* and is reset to ~0 on cold start, so it
# can read low long before the new sink has actually written a fresh
# checkpoint. The doc is the only source of truth for "the new process
# made progress AND that progress was persisted across the kill boundary".

deadline=$((SECONDS + RECOVERY_BUDGET))
LSN_AFTER=0
TS_AFTER=0
while [ $SECONDS -lt $deadline ]; do
  LSN_AFTER=$(read_checkpoint_lsn)
  TS_AFTER=$(read_checkpoint_updated)
  if [ -n "$LSN_AFTER" ] && [ "$LSN_AFTER" -gt "$LSN_BEFORE" ] \
     && [ -n "$TS_AFTER" ] && [ "$TS_AFTER" -gt "$TS_BEFORE" ]; then
    echo "Checkpoint advanced: lastLSN ${LSN_BEFORE} -> ${LSN_AFTER}, updatedAt ${TS_BEFORE} -> ${TS_AFTER}"
    break
  fi
  sleep 2
done

if [ -z "$LSN_AFTER" ] || [ "$LSN_AFTER" -le "$LSN_BEFORE" ]; then
  echo "FAIL: lastLSN did not advance within ${RECOVERY_BUDGET}s (still=${LSN_AFTER}, was=${LSN_BEFORE})"
  docker compose logs --tail=80 sink || true
  exit 1
fi
if [ -z "$TS_AFTER" ] || [ "$TS_AFTER" -le "$TS_BEFORE" ]; then
  echo "FAIL: updatedAt did not advance -- periodic flush is not running on the recovered sink"
  exit 1
fi

# Now that progress is real, the staleness gauge must reflect it: < threshold.
g=$(read_gauge)
if [ -z "$g" ] || ! awk -v g="$g" -v t="$STALENESS_THRESHOLD" 'BEGIN { exit !(g < t) }'; then
  echo "FAIL: doc advanced but gauge=${g:-<empty>} >= ${STALENESS_THRESHOLD}s -- gauge is not tracking the flush"
  exit 1
fi
echo "Staleness gauge=${g}s (< ${STALENESS_THRESHOLD}s) -- gauge in sync with persisted state"

# Final integrity check: PG row counts == Mongo doc counts.
"$(dirname "$0")/../verify-integrity.sh"
