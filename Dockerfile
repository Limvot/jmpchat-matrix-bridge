# Build stage (CGO required: sqlite3, libolm for E2BE)
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache ca-certificates gcc g++ musl-dev sqlite-dev olm-dev

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -ldflags "-s -w" -o mautrix-jabber ./cmd/mautrix-jabber

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata sqlite-libs olm

# Run as non-root
RUN adduser -D -h /app -s /sbin/nologin bridge
WORKDIR /app

COPY --from=builder /build/mautrix-jabber .
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

USER bridge

# Default appservice port (override in config if needed)
EXPOSE 29318

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["-c", "/data/config.yaml", "-r", "/data/registration.yaml"]
