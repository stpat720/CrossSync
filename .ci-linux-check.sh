#!/bin/sh
# Run the suite the way CI does, from a Linux filesystem.
#
# The daemons are Linux, the Windows dev box is not: three real bugs (a staging
# temp that never used the central area, a share-path rule that rejected every
# non-Unraid folder, and two order-dependent tests) only appeared when the suite
# first ran on Linux. Keep this green before pushing:
#
#   wsl -d Ubuntu-24.04 -- bash /mnt/c/Users/<you>/wt-pull/.ci-linux-check.sh
#
# Needs a Go toolchain inside WSL (a tarball under $HOME/go-toolchain works, and
# GOTOOLCHAIN fetches the exact version named in go.mod).
set -e
export PATH="$HOME/go-toolchain/go/bin:$PATH"
rm -rf "$HOME/src/crosssync"
mkdir -p "$HOME/src"
cp -a /mnt/c/Users/Patrick/wt-pull "$HOME/src/crosssync"
cd "$HOME/src/crosssync"
go build ./... || exit 1
echo "=== BUILD OK ==="
go vet ./... || echo "=== VET FAILED ==="
go test ./... -count=1 -timeout 900s | tee /tmp/test.log
grep -E "^(FAIL|--- FAIL)" /tmp/test.log | head -20
