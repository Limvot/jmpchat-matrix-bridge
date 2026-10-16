# Build stage (CGO required: sqlite3, libolm for E2BE)
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache ca-certificates gcc g++ musl-dev sqlite-dev olm-dev

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Build metadata (.git is excluded from the build context, so pass it in; see build.sh).
ARG GIT_COMMIT=unknown
ARG GIT_TAG=unknown
ARG GIT_REPOSITORY_URL=unknown
ARG BUILD_TIME=unknown
RUN CGO_ENABLED=1 go build -ldflags "-s -w \
	-X main.Commit=${GIT_COMMIT} -X main.Tag=${GIT_TAG} \
	-X main.RepositoryURL=${GIT_REPOSITORY_URL} -X main.BuildTime=${BUILD_TIME}" \
	-o mautrix-jabber ./cmd/mautrix-jabber

# Runtime stage
FROM alpine:3.19

ARG GIT_COMMIT=unknown
ARG GIT_REPOSITORY_URL=unknown
LABEL org.opencontainers.image.revision=${GIT_COMMIT} \
	org.opencontainers.image.source=${GIT_REPOSITORY_URL}

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
