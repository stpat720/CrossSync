#!/bin/sh
# Reproduce CI's race-detector step locally.
set -e
export PATH="$HOME/go-toolchain/go/bin:$PATH"
cd "$HOME/src/crosssync"
command -v gcc || echo "NO GCC"
CGO_ENABLED=1 go test -race -count=1 -timeout 900s ./internal/transfer/ ./internal/index/ ./internal/hold/ ./internal/watcher/ ./internal/sync/ 2>&1 | tail -40
