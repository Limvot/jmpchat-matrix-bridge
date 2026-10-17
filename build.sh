#!/usr/bin/env bash
set -e

# Build inside Docker to ensure a repeatable environment with all dependencies
# (CGO, libolm, libopus, sqlite-dev). The binary is extracted to the current directory.
# Extra arguments are passed to docker build (e.g. -t williamkray/mautrix-jabber:v0.1.10).
docker build -t mautrix-jabber $(./build-args.sh) "$@" .
container_id=$(docker create mautrix-jabber)
docker cp "$container_id:/app/mautrix-jabber" ./mautrix-jabber
docker rm "$container_id" > /dev/null
echo "Built: ./mautrix-jabber"
