#!/bin/sh
# Cross-compile CrossSync for all platforms and write dist/latest.json —
# the update manifest the daemon polls (update_url). This is the "auto
# rebuild" side of self-updating: run it locally and publish dist/ (or let
# .github/workflows/release.yml run it on every push), then point
# update_url at dist/latest.json.
#
# Usage:
#   UPDATE_BASE_URL=https://example.com/dist ./release.sh
#
# UPDATE_BASE_URL is the public base URL where the dist/ files are served
# (required — it is embedded in the manifest). VERSION defaults to the last
# git tag/commit; override with VERSION=0.2.0 ./release.sh.
set -e
cd "$(dirname "$0")"
mkdir -p dist

VERSION="${VERSION:-$(git describe --tags --always 2>/dev/null || echo 0.1.0)}"
BASE_URL="${UPDATE_BASE_URL:?set UPDATE_BASE_URL to the public base URL where dist/ is served (e.g. https://example.com/crosssync/dist)}"
LDFLAGS="-s -w -X main.version=$VERSION"

echo "building v$VERSION ..."
CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 go build -trimpath -ldflags="$LDFLAGS" -o dist/crosssync-linux-amd64      ./cmd/crosssync
CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 go build -trimpath -ldflags="$LDFLAGS" -o dist/crosssync-linux-arm64      ./cmd/crosssync
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath -ldflags="$LDFLAGS" -o dist/crosssync-windows-amd64.exe ./cmd/crosssync

# Write the manifest with per-platform sha256 checksums.
{
  echo "{\"version\":\"$VERSION\",\"platforms\":{"
  first=1
  for entry in "linux/amd64:crosssync-linux-amd64" "linux/arm64:crosssync-linux-arm64" "windows/amd64:crosssync-windows-amd64.exe"; do
    plat="${entry%%:*}"; file="${entry##*:}"
    sha=$(sha256sum "dist/$file" | cut -d' ' -f1)
    [ "$first" = 1 ] || echo ","
    first=0
    printf '"%s":{"url":"%s/%s","sha256":"%s"}' "$plat" "$BASE_URL" "$file" "$sha"
  done
  echo ""
  echo "}}"
} > dist/latest.json

echo "done:"
ls -lh dist/
echo "manifest: $(cat dist/latest.json)"
