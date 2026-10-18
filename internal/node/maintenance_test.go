package node

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/index"
)

// TestRunMaintenanceTombstoneFloor verifies the long-running deployment
// invariant: tombstones accumulate in the index until every peer sharing the
// folder has acknowledged past them (persisted marker), then maintenance
// prunes them — and the floor is conservative when it must be (no peers, or
// a peer with no marker yet).
func TestRunMaintenanceTombstoneFloor(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())

	f := n.Folders["data"]
	// One live file, then a deleted file -> a tombstone row.
	chunkWrite(t, root, "live.txt", "keep")
	_, err := n.ScanFolder("data")
	if err != nil {
		t.Fatal(err)
	}
	chunkWrite(t, root, "doomed.txt", "remove")
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(filepath.Join(root, "doomed.txt"))
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	var tomb int
	if err := f.Ix.ListAll(func(fi *index.FileInfo) error {
		if fi.Deleted {
			tomb++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if tomb < 1 {
		t.Fatalf("expected a tombstone after deleting doomed.txt, got %d", tomb)
	}

	// No peers configured: nothing may be pruned.
	if got := n.tombstoneFloor(f); got != 0 {
		t.Fatalf("tombstoneFloor with no peers = %d, want 0", got)
	}
	if n, err := n.RunMaintenance(); err != nil || n != 0 {
		t.Fatalf("RunMaintenance(no peers) = (%d,%v), want (0,nil)", n, err)
	}

	// Peer configured but no marker yet (never synced): still 0.
	n.Cfg.Peers = []config.Peer{{ID: 9, Name: "p", Addresses: []string{"x:1"}}}
	if got := n.tombstoneFloor(f); got != 0 {
		t.Fatalf("tombstoneFloor with unacknowledged peer = %d, want 0", got)
	}
	if n, err := n.RunMaintenance(); err != nil || n != 0 {
		t.Fatalf("RunMaintenance(unacknowledged peer) = (%d,%v), want (0,nil)", n, err)
	}

	// Peer marker past the tombstone sequence: prunable.
	if err := f.Ix.SetPeerIndex(9, "idx", f.Ix.MaxSeq(), "s"); err != nil {
		t.Fatal(err)
	}
	if got := n.tombstoneFloor(f); got == 0 {
		t.Fatal("tombstoneFloor = 0, want > 0 after marker")
	}
	pruned, err := n.RunMaintenance()
	if err != nil {
		t.Fatal(err)
	}
	if pruned < 1 {
		t.Fatalf("RunMaintenance pruned %d, want >= 1", pruned)
	}
	if err := f.Ix.ListAll(func(fi *index.FileInfo) error {
		if fi.Deleted {
			t.Errorf("tombstone %q survived maintenance", fi.Name)
			return nil
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
