#!/bin/sh
# Instrument TestHashDriftSkipsFile in a copy and show what the pass produced.
set -e
export PATH="$HOME/go-toolchain/go/bin:$PATH"
cd "$HOME/src/crosssync"
python3 - <<'PY'
p = "internal/scanner/scanner_settle_test.go"
src = open(p).read()
old = """	changes := gateScan(t, s, ix)
	if kindOf(changes, "drift.bin") != -1 {"""
new = """	changes := gateScan(t, s, ix)
	t.Logf("DEBUG changes=%d inflight=%d", len(changes), len(s.LastInFlight()))
	for _, c := range changes {
		t.Logf("DEBUG change kind=%v name=%v", c.Kind, c.Info.Name)
	}
	if kindOf(changes, "drift.bin") != -1 {"""
assert old in src
open(p, "w").write(src.replace(old, new, 1))
PY
go test ./internal/scanner/ -run TestHashDriftSkipsFile -count=1 -v 2>&1 | grep -E "DEBUG|FAIL|PASS|scanner_settle_test" | head -12
