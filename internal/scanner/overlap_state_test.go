package scanner

import (
	"testing"

	"crosssync/internal/ignore"
	"crosssync/internal/index"
)

// TestSettleMergeKeepsConcurrentRows is the unit-level half of the overlapping
// pass contract: a full walk snapshots the settling table at the start and can
// run for twenty minutes, while a watcher pass writes rows in between. The walk's
// final write must keep a row it never saw in its snapshot — otherwise the
// brand-new file the watcher just observed loses its gate evidence and waits for
// the next walk instead of being detected in seconds.
func TestSettleMergeKeepsConcurrentRows(t *testing.T) {
	root, ix := setup(t)
	s := New(root, ix, &ignore.Matcher{})

	// A scoped pass wrote this row while the "walk" was running.
	scoped := map[string]index.SettleState{"x/new.bin": {Size: 1, ModS: 2, Count: 1, First: 2}}
	if err := ix.SettleReplace(scoped); err != nil {
		t.Fatal(err)
	}
	// The walk's snapshot was taken BEFORE that (empty), and its own pass saw
	// nothing to hold.
	s.publishSettle(map[string]index.SettleState{}, map[string]index.SettleState{}, func(string) bool { return true }, false)
	got, err := ix.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	if got["x/new.bin"] != scoped["x/new.bin"] {
		t.Fatalf("the walk dropped a settle row written after its snapshot: got %+v, want the scoped row preserved", got)
	}

	// A scoped pass only moves rows inside its scope: an out-of-scope row that
	// another pass updated after the scoped snapshot must survive too.
	other := index.SettleState{Size: 9, ModS: 9, Count: 1, First: 9}
	if err := ix.SettleReplace(map[string]index.SettleState{"outside/b.bin": other}); err != nil {
		t.Fatal(err)
	}
	prev := map[string]index.SettleState{"outside/b.bin": {Size: 1, ModS: 1, Count: 1, First: 1}}
	next := map[string]index.SettleState{"outside/b.bin": prev["outside/b.bin"]}
	s.publishSettle(prev, next, func(name string) bool { return name == "inside" }, true)
	got, err = ix.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	if got["outside/b.bin"] != other {
		t.Fatalf("a scoped pass overwrote an out-of-scope settle row: got %+v, want %+v", got["outside/b.bin"], other)
	}
}

// TestMissingMergeKeepsOutOfScopeConfirmations: the deletion-confirmation table
// is the other read-modify-write both passes touch. A scoped pass may add or
// advance a row for a path it walked, but a pending confirmation elsewhere must
// come through its write unchanged.
func TestMissingMergeKeepsOutOfScopeConfirmations(t *testing.T) {
	root, ix := setup(t)
	s := New(root, ix, &ignore.Matcher{})

	pending := map[string]index.MissingState{"outside/gone.txt": {Count: 1, First: 5}}
	if err := ix.MissingReplace(pending); err != nil {
		t.Fatal(err)
	}
	prev := map[string]index.MissingState{
		"outside/gone.txt": pending["outside/gone.txt"],
		"inside/two.txt":   {Count: 0, First: 6},
	}
	next := map[string]index.MissingState{
		// The scoped pass preserved the out-of-scope row verbatim and advanced
		// the in-scope one.
		"outside/gone.txt": prev["outside/gone.txt"],
		"inside/two.txt":   {Count: 1, First: 6},
	}
	s.publishMissing(prev, next, func(name string) bool { return name == "inside/two.txt" }, true)
	got, err := ix.MissingList()
	if err != nil {
		t.Fatal(err)
	}
	if got["outside/gone.txt"].Count != 1 {
		t.Fatalf("out-of-scope confirmation = %+v, want the pending miss preserved", got["outside/gone.txt"])
	}
	if got["inside/two.txt"].Count != 1 {
		t.Fatalf("in-scope confirmation = %+v, want one miss recorded by the scoped pass", got["inside/two.txt"])
	}
}
