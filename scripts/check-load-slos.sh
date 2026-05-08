#!/usr/bin/env bash
# check-load-slos.sh — assert post-load CDC SLOs against the live Prometheus.
#
# Run AFTER `make load` (or any sustained write load). Queries the
# observability stack for the histogram-based replication-lag SLO and
# the write-error rate, exits non-zero if either is over budget.
#
# This is the CDC-pipeline-specific complement to k6's HTTP-side
# thresholds: k6 cares whether the loadgen API is fast (HTTP latency),
# this cares whether the pipeline is fast (PG -> Mongo lag).
#
# Defaults (override with env):
#   PROM_URL              http://localhost:9090
#   LAG_P99_BUDGET_SECS   5
#   ERROR_RATE_BUDGET     0.001    (0.1%)
#   WINDOW                5m
#
# Exit:
#   0  every SLO inside budget
#   1  one or more SLOs over budget
#   2  Prometheus unreachable / metric missing

set -euo pipefail

PROM_URL="${PROM_URL:-http://localhost:9090}"
LAG_P99_BUDGET_SECS="${LAG_P99_BUDGET_SECS:-5}"
ERROR_RATE_BUDGET="${ERROR_RATE_BUDGET:-0.001}"
WINDOW="${WINDOW:-5m}"

q() {
  # Returns the first value of an instant-query result; empty if no series.
  curl -fsS --get "$PROM_URL/api/v1/query" --data-urlencode "query=$1" \
    | python -c "import sys,json; d=json.load(sys.stdin); print(d['data']['result'][0]['value'][1] if d['data']['result'] else '')" 2>/dev/null \
    || curl -fsS --get "$PROM_URL/api/v1/query" --data-urlencode "query=$1" \
       | node -e "let d=''; process.stdin.on('data',c=>d+=c); process.stdin.on('end',()=>{const j=JSON.parse(d); process.stdout.write(j.data.result.length?String(j.data.result[0].value[1]):'')})"
}

metric_exists() {
  local m="$1"
  curl -fsS "$PROM_URL/api/v1/label/__name__/values" 2>/dev/null | grep -q "\"$m\""
}

require_metric_exists() {
  # Use only for metrics the sink emits unconditionally on any traffic
  # (replication lag, events processed). Counters that only fire under
  # rare conditions (write errors, idempotent skips) are NOT required -
  # absence means "no incidents", not "sink is broken".
  local m="$1"
  if ! metric_exists "$m"; then
    echo "FAIL: metric '$m' is not present in Prometheus. Did the sink emit it during the load run?" >&2
    exit 2
  fi
}

# ---------- 1. Replication lag p99 ----------
require_metric_exists migration_replication_lag_seconds_bucket
LAG_P99="$(q "histogram_quantile(0.99, sum(rate(migration_replication_lag_seconds_bucket[$WINDOW])) by (le))")"

if [[ -z "$LAG_P99" || "$LAG_P99" == "NaN" ]]; then
  echo "WARN: replication-lag p99 query returned no data over $WINDOW. Was load active long enough?"
  LAG_P99_OK="?"
else
  if awk -v g="$LAG_P99" -v b="$LAG_P99_BUDGET_SECS" 'BEGIN { exit !(g <= b) }'; then
    LAG_P99_OK="ok"
  else
    LAG_P99_OK="OVER"
  fi
fi

# ---------- 2. Write error rate ----------
# events_processed_total is required (every batch increments it).
# write_errors_total is OPTIONAL (only fires on a real error). Absence =
# zero errors = within budget, NOT a failure.
require_metric_exists migration_events_processed_total
if metric_exists migration_write_errors_total; then
  ERR_RATE="$(q "sum(rate(migration_write_errors_total[$WINDOW])) / clamp_min(sum(rate(migration_events_processed_total{stage=\"sink\"}[$WINDOW])), 1)")"
else
  ERR_RATE="0"
fi

if [[ -z "$ERR_RATE" || "$ERR_RATE" == "NaN" ]]; then
  ERR_RATE="0"
fi
if awk -v g="$ERR_RATE" -v b="$ERROR_RATE_BUDGET" 'BEGIN { exit !(g <= b) }'; then
  ERR_RATE_OK="ok"
else
  ERR_RATE_OK="OVER"
fi

# ---------- 3. Idempotent skip rate (informational) ----------
SKIP_RATE="$(q "sum(rate(migration_idempotent_skip_total[$WINDOW]))")"

# ---------- Report ----------
echo
echo "Post-load CDC SLO summary (window=$WINDOW)"
echo "  replication_lag_seconds p99   = ${LAG_P99}      (budget ${LAG_P99_BUDGET_SECS}s)    [$LAG_P99_OK]"
echo "  write_error_rate              = ${ERR_RATE}     (budget ${ERROR_RATE_BUDGET})       [$ERR_RATE_OK]"
echo "  idempotent_skip_rate (info)   = ${SKIP_RATE:-0} events/s"

failed=0
if [[ "$LAG_P99_OK" == "OVER" ]]; then failed=$((failed+1)); fi
if [[ "$ERR_RATE_OK" == "OVER" ]]; then failed=$((failed+1)); fi

if [[ $failed -gt 0 ]]; then
  echo
  echo "SLO BREACH: $failed budget(s) exceeded."
  exit 1
fi
echo
echo "All SLOs within budget."
