package node

import (
	"testing"

	"crosssync/internal/index"
)

// TestStatusAggregateIsSkippedWhileAScanRuns pins the fix for a slow box with
// idle disks: the 5 s status refresher recomputed the folder's whole index
// aggregate (a full-table scan) even while a first pass was writing to that same
// index. On a million-entry index the aggregate grew with the pass and competed
// with the pass's own batch commits for one SQLite file, so the scan spent its
// time waiting on its own telemetry.
//
// During a pass the scan publishes its own progress (walked/hash/committed) and
// bumps the cached counts per committed batch, so the aggregate must be skipped
// and come back once the pass is over.
func TestStatusAggregateIsSkippedWhileAScanRuns(t *testing.T) {
	root := t.TempDir()
	n := newRegistryNode(t, map[string]string{"data": root})
	f, ok := n.lookupFolder("data")
	if !ok {
		t.Fatal("fixture folder missing")
	}

	calls := 0
	restore := SetStatusQueryHook(nil, nil, func(*index.Index) (index.Stats, error) {
		calls++
		return index.Stats{}, nil
	})
	defer restore()

	n.startScan("data", 0)
	n.refreshFolderStatusCounts(f)
	if calls != 0 {
		t.Fatalf("the status refresher ran the full-table index aggregate %d time(s) during a scan; on a million-entry index that competes with the pass's own commits", calls)
	}

	n.endScan("data", 0, nil)
	n.refreshFolderStatusCounts(f)
	if calls != 1 {
		t.Fatalf("aggregate calls after the pass finished = %d, want 1 (the cached figures must be republished once the index is no longer being written)", calls)
	}
}
