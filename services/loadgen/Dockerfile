FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen

FROM alpine:3.23
RUN apk add --no-cache ca-certificates wget \
 && adduser -S -D -u 1000 loadgen
USER loadgen
COPY --from=builder /out/loadgen /usr/local/bin/loadgen
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/loadgen"]
