#!/bin/sh
# Does this scanner test fail on Linux BEFORE this session's changes?
set -e
export PATH="$HOME/go-toolchain/go/bin:$PATH"
cd "$HOME/src/crosssync"
git checkout -q "$1"
echo "== at $(git log --oneline -1) =="
go test ./internal/scanner/ -run "$2" -count=1 2>&1 | tail -10
git checkout -q main
