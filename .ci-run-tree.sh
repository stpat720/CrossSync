#!/bin/sh
# usage: .ci-run-tree.sh <windows-tree-path> <test-name>
set -e
export PATH="$HOME/go-toolchain/go/bin:$PATH"
rm -rf "$HOME/src/tree"
mkdir -p "$HOME/src"
cp -a "$1" "$HOME/src/tree"
cd "$HOME/src/tree"
go test ./internal/scanner/ -run "$2" -count=1 2>&1 | tail -8
