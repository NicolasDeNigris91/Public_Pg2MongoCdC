#!/usr/bin/env bash
# Drift detector for Prometheus metric names <-> alert rules <-> code.
#
# This is the regression-guard for the gap closed in the
# "instrumentation completeness" pass: alerts referenced
# `migration_replication_lag_seconds` but no Go code observed it, so the
# alert silently could never fire. This script makes that exact failure
# mode break CI on the PR that introduces it.
#
# Two checks:
#
#   A. Every `migration_*` metric token that appears in
#      observability/prometheus/alerts.yml MUST appear at least once in
#      services/. Catches alerts that reference a nonexistent series.
#
#   B. Every metric *field* declared in services/*/internal/metrics/metrics.go
#      (e.g. `ReplicationLag *prometheus.HistogramVec`) MUST be touched by
#      a .Inc / .Add / .Observe / .Set / .WithLabelValues call somewhere
#      outside the metrics package and outside *_test.go. Catches metrics
#      that were registered but are never produced.
#
# Exit code 0 = no drift; 1 = drift; 2 = setup failure.

set -euo pipefail

cd "$(dirname "$0")/.."

ALERTS_FILE="observability/prometheus/alerts.yml"
SERVICES_ROOT="services"

if [[ ! -f "$ALERTS_FILE" ]]; then
  echo "ERROR: $ALERTS_FILE not found" >&2
  exit 2
fi

# All Go files we consider as "code" for production observation purposes.
# Use a glob array so spaces in paths (Windows users) survive.
mapfile -t METRICS_FILES < <(find "$SERVICES_ROOT" -path '*/internal/metrics/metrics.go' -type f)

if [[ ${#METRICS_FILES[@]} -eq 0 ]]; then
  echo "ERROR: no metrics.go files found under $SERVICES_ROOT/" >&2
  exit 2
fi

errors=0

# ---------------------------------------------------------------------
# Check A: alert -> metric existence
# ---------------------------------------------------------------------
echo "==> Check A: alerts.yml metric references must exist somewhere in services/"

# Extract every distinct migration_* token from alerts.yml.
mapfile -t ALERT_REFS < <(grep -oE 'migration_[a-z0-9_]+' "$ALERTS_FILE" | sort -u)

if [[ ${#ALERT_REFS[@]} -eq 0 ]]; then
  echo "WARN: no migration_* metric references found in $ALERTS_FILE — file may be empty"
fi

for m in "${ALERT_REFS[@]}"; do
  # We do not care WHERE in services/ — only that some Go file produces
  # it. Strip _bucket / _count / _sum suffixes that Prometheus auto-adds
  # to histograms; the base name is what's declared in metrics.go.
  base="${m%_bucket}"
  base="${base%_count}"
  base="${base%_sum}"
  if ! grep -rq "\"$base\"" "$SERVICES_ROOT" --include='*.go'; then
    echo "  FAIL  alert references '$m' (base '$base') but no Go file declares it"
    errors=$((errors + 1))
  else
    echo "  ok    $m"
  fi
done

# ---------------------------------------------------------------------
# Check B: declared metric field -> at least one observation
# ---------------------------------------------------------------------
echo
echo "==> Check B: every declared metric field must be observed outside its package"

for f in "${METRICS_FILES[@]}"; do
  service_dir="${f%/internal/metrics/metrics.go}"
  echo "  scanning $f (service: $service_dir)"

  # Pull field names that are assigned to a prometheus.NewXxx constructor.
  # Lines look like:    ReplicationLag: prometheus.NewHistogramVec(
  mapfile -t FIELDS < <(
    grep -E '^[[:space:]]+[A-Z][A-Za-z0-9]+:[[:space:]]+prometheus\.New' "$f" \
      | sed -E 's/^[[:space:]]+([A-Z][A-Za-z0-9]+):.*$/\1/' | sort -u
  )

  if [[ ${#FIELDS[@]} -eq 0 ]]; then
    echo "    (no Metrics{} fields in this file — skipping)"
    continue
  fi

  for field in "${FIELDS[@]}"; do
    # Look for any production observation pattern. We deliberately
    # exclude metrics_test.go (the test that probes the registry by
    # calling Observe/Inc/etc. on every field — that one ALWAYS hits,
    # so it would mask real drift) and metrics.go itself.
    hits=$(
      grep -rE "\.${field}\.(Inc|Add|Observe|Set|WithLabelValues)" \
        --include='*.go' --exclude='*_test.go' \
        "$service_dir" 2>/dev/null \
        | grep -v "/internal/metrics/metrics\.go" || true
    )
    if [[ -z "$hits" ]]; then
      echo "    FAIL  field '$field' is declared but never observed in production code"
      errors=$((errors + 1))
    else
      echo "    ok    $field"
    fi
  done
done

echo
if [[ $errors -gt 0 ]]; then
  echo "DRIFT DETECTED: $errors error(s). See above."
  exit 1
fi
echo "OK: alerts <-> declared metrics <-> observation are all consistent."
