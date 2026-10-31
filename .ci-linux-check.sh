#!/bin/sh
# Runs the suite the way CI does, inside WSL (Linux), from a copy on the ext4 FS.
set -e
export PATH="$HOME/go-toolchain/go/bin:$PATH"
rm -rf "$HOME/src/crosssync"
mkdir -p "$HOME/src"
cp -a /mnt/c/Users/Patrick/wt-pull "$HOME/src/crosssync"
cd "$HOME/src/crosssync"
go build ./... || exit 1
echo "=== BUILD OK ==="
go vet ./... || echo "=== VET FAILED ==="
go test ./... -count=1 -timeout 900s 2>&1 | tee /tmp/test.log
echo "=== EXIT $? ==="
grep -E "^(FAIL|--- FAIL)" /tmp/test.log | head -20
