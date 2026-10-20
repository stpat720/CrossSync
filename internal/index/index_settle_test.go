package index

import (
	"path/filepath"
	"reflect"
	"testing"
)

// TestSettleStateRoundTrip verifies the stability-gate state table: a full
// replace is read back identically, and a later replace with a smaller map
// drops the rows that are gone (the table only ever holds unsettled files).
func TestSettleStateRoundTrip(t *testing.T) {
	ix := openTestIndex(t)

	states := map[string]SettleState{
		"docs/a.txt": {Size: 42, ModS: 1000, ModNs: 123, Count: 2, First: 990},
		"big.bin":    {Size: 1 << 20, ModS: 2000, ModNs: 0, Count: 1, First: 2000},
	}
	if err := ix.SettleReplace(states); err != nil {
		t.Fatal(err)
	}
	got, err := ix.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, states) {
		t.Fatalf("SettleList = %+v, want %+v", got, states)
	}

	// Smaller replace: the missing row is removed (no leaked state for a file
	// that settled or vanished).
	smaller := map[string]SettleState{"big.bin": states["big.bin"]}
	if err := ix.SettleReplace(smaller); err != nil {
		t.Fatal(err)
	}
	got, err = ix.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, smaller) {
		t.Fatalf("after smaller replace SettleList = %+v, want %+v", got, smaller)
	}

	// Empty replace clears the table (the state of a healthy folder).
	if err := ix.SettleReplace(map[string]SettleState{}); err != nil {
		t.Fatal(err)
	}
	got, err = ix.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("after empty replace SettleList = %+v, want empty", got)
	}
}

// TestSettleStatePersistsAcrossReopen verifies the counter survives a restart:
// the gate keeps its progress across daemon restarts instead of restarting the
// count (a file that has been stable for one scan stays at 1).
func TestSettleStatePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "folder.db")
	ix, err := Open(path, "f")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]SettleState{
		"copy-in-progress.bin": {Size: 7, ModS: 1700000000, ModNs: 500, Count: 3, First: 1699999900},
	}
	if err := ix.SettleReplace(want); err != nil {
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}

	ix2, err := Open(path, "f")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix2.Close() })
	got, err := ix2.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after reopen SettleList = %+v, want %+v", got, want)
	}
}
