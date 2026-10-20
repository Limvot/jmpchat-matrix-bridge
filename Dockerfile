# Build stage (CGO required: sqlite3, libolm for E2BE, libopus for calls)
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache ca-certificates gcc g++ musl-dev sqlite-dev olm-dev opus-dev pkgconf

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Build metadata (.git is excluded from the build context, so pass it in; see build.sh).
ARG GIT_COMMIT=unknown
ARG GIT_TAG=unknown
ARG GIT_REPOSITORY_URL=unknown
ARG BUILD_TIME=unknown
# sipcalls: SIP call support (off unless network.calls.enabled); nolibopusfile: hraban/opus without opusfile.
RUN CGO_ENABLED=1 go build -tags "sipcalls nolibopusfile" -ldflags "-s -w \
	-X main.Commit=${GIT_COMMIT} -X main.Tag=${GIT_TAG} \
	-X main.RepositoryURL=${GIT_REPOSITORY_URL} -X main.BuildTime=${BUILD_TIME}" \
	-o jmpchat-matrix-bridge ./cmd/jmpchat-matrix-bridge

# Runtime stage
FROM alpine:3.19

ARG GIT_COMMIT=unknown
ARG GIT_REPOSITORY_URL=unknown
LABEL org.opencontainers.image.revision=${GIT_COMMIT} \
	org.opencontainers.image.source=${GIT_REPOSITORY_URL}

RUN apk add --no-cache ca-certificates tzdata sqlite-libs olm opus

# Run as non-root
RUN adduser -D -h /app -s /sbin/nologin bridge && mkdir -p /data && chown bridge:bridge /data
WORKDIR /app

COPY --from=builder /build/jmpchat-matrix-bridge .
# compat: pre-rename binary path, remove in a later major
RUN ln -s jmpchat-matrix-bridge /app/mautrix-jabber
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

USER bridge

# Default appservice port (override in config if needed)
EXPOSE 29318
# SIP (network.calls.sip_port_min..max) and RTP (rtp_port_min..max) defaults; publish what you configure.
EXPOSE 5060-5069/udp 5060-5069/tcp 40000-40100/udp

ENTRYPOINT ["/app/entrypoint.sh"]
