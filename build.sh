#!/bin/sh
# Cross-compile CrossSync for Unraid (Linux). The SQLite driver is pure Go
# (modernc.org/sqlite), so CGO_ENABLED=0 yields a fully static binary — no
# libc/glibc dependency on the host. Builds amd64 (most Unraid boxes) and
# arm64.
#
# Usage:
#   ./build.sh            # both arches into ./dist
#   GOARCH=amd64 ./build.sh
set -e
cd "$(dirname "$0")"
mkdir -p dist
arch="${GOARCH:-amd64}"
VERSION="${VERSION:-$(git describe --tags --always 2>/dev/null || echo 0.1.0)}"
echo "building linux/$arch (v$VERSION) ..."
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath \
  -ldflags="-s -w -X main.version=$VERSION" \
  -o "dist/crosssync-linux-$arch" ./cmd/crosssync
echo "done:"
ls -lh "dist/crosssync-linux-$arch"
echo "use ./release.sh to also emit dist/latest.json for self-updating"
