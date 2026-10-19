#!/usr/bin/env bash
# Prints docker build --build-arg flags with git metadata for the Dockerfile:
#   docker build $(./build-args.sh) -t williamkray/mautrix-jabber:v0.4.0 .
# The repository URL is normalized to https (git@github.com:owner/repo.git -> https://github.com/owner/repo).
set -e
commit=$(git rev-parse HEAD)
tag=$(git describe --exact-match --tags HEAD 2>/dev/null || echo unknown)
repo=$(git config --get remote.origin.url | sed -E 's#^git@([^:]+):#https://\1/#; s#\.git$##')
echo "--build-arg GIT_COMMIT=$commit --build-arg GIT_TAG=$tag --build-arg GIT_REPOSITORY_URL=$repo --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
