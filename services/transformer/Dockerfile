FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/transformer ./cmd/transformer

FROM alpine:3.23
RUN apk add --no-cache ca-certificates wget \
 && adduser -S -D -u 1000 transformer
USER transformer
COPY --from=builder /out/transformer /usr/local/bin/transformer
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/transformer"]
