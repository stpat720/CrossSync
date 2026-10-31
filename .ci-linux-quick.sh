#!/bin/sh
# Sync the working tree into WSL and run the packages given as arguments.
set -e
export PATH="$HOME/go-toolchain/go/bin:$PATH"
rm -rf "$HOME/src/crosssync"
mkdir -p "$HOME/src"
cp -a /mnt/c/Users/Patrick/wt-pull "$HOME/src/crosssync"
cd "$HOME/src/crosssync"
if [ $# -eq 0 ]; then
	set -- ./...
fi
go test -count=1 -timeout 900s "$@" 2>&1 | tail -40
