# syntax=docker/dockerfile:1.7
# Multi-stage: small runtime image, reproducible build.
# Built from ./services/sink context, so go.mod is at the build-context root.
#
# OCI labels surface provenance + license at the registry. Pinned base
# images (alpine 3.21) deterministic CVE surface; CI's trivy step
# enforces HIGH+CRITICAL=zero on every push.

ARG GO_VERSION=1.26-alpine
ARG ALPINE_VERSION=3.21

FROM golang:${GO_VERSION} AS builder
WORKDIR /src
# Use BuildKit cache mounts so Go's module + build caches survive across
# successive `docker build` invocations - measurable speedup on dev iter.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/sink ./cmd/sink

FROM alpine:${ALPINE_VERSION}
# busybox provides wget out of the box - sufficient for the /healthz GET
# in compose. ca-certificates is the only real install we need (TLS to
# Mongo Atlas / managed Kafka).
RUN apk add --no-cache ca-certificates \
 && adduser -S -D -u 1000 sink

USER sink
COPY --from=builder /out/sink /usr/local/bin/sink
EXPOSE 8080

# In-image healthcheck so `docker run` alone reports healthy (the compose
# file declares its own with the same probe). Kubernetes uses its own
# httpGet probe and ignores this clause.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/healthz || exit 1

LABEL org.opencontainers.image.title="pg2mongo-cdc-sink"
LABEL org.opencontainers.image.description="Postgres -> MongoDB CDC sink (LSN-gated upserts, idempotent BulkWrite)"
LABEL org.opencontainers.image.source="https://github.com/NicolasDeNigris91/Public_Pg2MongoCdC"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.documentation="https://github.com/NicolasDeNigris91/Public_Pg2MongoCdC/blob/main/docs/architecture.md"

ENTRYPOINT ["/usr/local/bin/sink"]
