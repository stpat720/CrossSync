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

# Build a monotonic semantic version. Non-semver tags (like the rolling
# "nightly" release tag) are ignored:
#   - newest vX.Y.Z (or X.Y.Z) tag -> use it; commits after the tag bump a
#     4th counter part (v0.1.0 + 3 commits -> 0.1.0.3) so EVERY push is
#     a newer version than the last build.
#   - no semver tag yet -> 0.1.<total-commit-count> (still monotonic).
# Override entirely with VERSION=1.2.3 ./release.sh.
if [ -z "${VERSION:-}" ]; then
  BASE_TAG=""
  for t in $(git tag --list --sort=-version:refname 2>/dev/null); do
    case "$t" in
      v[0-9]*.[0-9]*.[0-9]*|[0-9]*.[0-9]*.[0-9]*) BASE_TAG="$t"; break ;;
    esac
  done
  if [ -n "$BASE_TAG" ]; then
    BASE_VERSION="${BASE_TAG#v}"
    COUNT=$(git rev-list --count "$BASE_TAG..HEAD" 2>/dev/null || echo 0)
    if [ "$COUNT" -gt 0 ]; then
      VERSION="${BASE_VERSION}.${COUNT}"
    else
      VERSION="$BASE_VERSION"
    fi
  else
    COUNT=$(git rev-list --count HEAD 2>/dev/null || echo 0)
    VERSION="0.1.${COUNT}"
  fi
fi

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
