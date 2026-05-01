# Multi-stage: small runtime image, reproducible build.
# Built from ./services/sink context, so go.mod is at the build-context root.

FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/sink ./cmd/sink

FROM alpine:3.23
RUN apk add --no-cache ca-certificates wget \
 && adduser -S -D -u 1000 sink
USER sink
COPY --from=builder /out/sink /usr/local/bin/sink
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/sink"]
