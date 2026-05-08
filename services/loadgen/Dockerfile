# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.26-alpine
ARG ALPINE_VERSION=3.21

FROM golang:${GO_VERSION} AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen

FROM alpine:${ALPINE_VERSION}
RUN apk add --no-cache ca-certificates \
 && adduser -S -D -u 1000 loadgen

USER loadgen
COPY --from=builder /out/loadgen /usr/local/bin/loadgen
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/healthz || exit 1

LABEL org.opencontainers.image.title="pg2mongo-cdc-loadgen"
LABEL org.opencontainers.image.description="HTTP -> Postgres write generator used by k6 to drive load against the pipeline"
LABEL org.opencontainers.image.source="https://github.com/NicolasDeNigris91/Public_Pg2MongoCdC"
LABEL org.opencontainers.image.licenses="MIT"

ENTRYPOINT ["/usr/local/bin/loadgen"]
