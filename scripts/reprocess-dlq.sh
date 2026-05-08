#!/usr/bin/env bash
# reprocess-dlq.sh — triage and replay Connect DLQ topics.
#
# Usage:
#   scripts/reprocess-dlq.sh [TOPIC] [--replay] [--max N]
#
# Defaults:
#   TOPIC = dlq.source AND dlq.sink (one run per topic)
#   mode  = dry-run (no records re-published)
#
# Examples:
#   scripts/reprocess-dlq.sh                       # triage both DLQs
#   scripts/reprocess-dlq.sh dlq.sink              # triage one
#   scripts/reprocess-dlq.sh dlq.source --replay   # actually re-publish
#
# Runs the dlqtool Go binary inside an ephemeral container that joins the
# compose network so it can reach kafka:29092. Builds on first run.
#
# Env: KAFKA_BROKERS overrides the bootstrap list (default kafka:29092).

set -euo pipefail

cd "$(dirname "$0")/.."

POSITIONAL=()
EXTRA_FLAGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --replay) EXTRA_FLAGS+=("-replay"); shift ;;
    --max)    EXTRA_FLAGS+=("-max" "$2"); shift 2 ;;
    --max=*)  EXTRA_FLAGS+=("-max" "${1#*=}"); shift ;;
    -h|--help)
      sed -n '2,18p' "$0"
      exit 0
      ;;
    -*)
      echo "unknown flag: $1" >&2; exit 64 ;;
    *)
      POSITIONAL+=("$1"); shift ;;
  esac
done

if [[ ${#POSITIONAL[@]} -eq 0 ]]; then
  TOPICS=("dlq.source" "dlq.sink")
else
  TOPICS=("${POSITIONAL[@]}")
fi

# We run the Go tool from inside the kafka container's network namespace so
# the broker DNS name "kafka" resolves the same way the services see it.
# Build a small one-shot image on demand.
IMAGE_TAG="pg2mongo/dlqtool:latest"

if ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
  echo "Building $IMAGE_TAG ..."
  docker build -q -t "$IMAGE_TAG" -f - services/sink <<'DOCKERFILE'
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/dlqtool ./cmd/dlqtool

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/dlqtool /dlqtool
ENTRYPOINT ["/dlqtool"]
DOCKERFILE
fi

NETWORK="$(docker compose ps --format json kafka 2>/dev/null | head -1 | grep -oE '"Networks":"[^"]*"' | head -1 | cut -d'"' -f4 || true)"
if [[ -z "${NETWORK:-}" ]]; then
  # Fall back to the default compose network name.
  NETWORK="$(basename "$(pwd)")_default"
fi

BROKERS="${KAFKA_BROKERS:-kafka:29092}"

EXIT=0
for T in "${TOPICS[@]}"; do
  echo
  echo "=== $T ==="
  if ! docker run --rm \
        --network "$NETWORK" \
        -e KAFKA_BROKERS="$BROKERS" \
        "$IMAGE_TAG" \
        -topic "$T" \
        "${EXTRA_FLAGS[@]}"; then
    rc=$?
    if [[ $rc -eq 2 ]]; then
      echo "(topic does not exist; skipping)"
    else
      EXIT=$rc
    fi
  fi
done

exit $EXIT
