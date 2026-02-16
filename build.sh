#!/usr/bin/env bash
set -e

# Build inside Docker to ensure a repeatable environment with all dependencies
# (CGO, libolm, sqlite-dev). The binary is extracted to the current directory.
docker build -t mautrix-jabber .
container_id=$(docker create mautrix-jabber)
docker cp "$container_id:/app/mautrix-jabber" ./mautrix-jabber
docker rm "$container_id" > /dev/null
echo "Built: ./mautrix-jabber"
