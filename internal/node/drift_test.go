package node

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDriftCheckFindsBothDirections: the check must report a file the index has
// forgotten as well as a file it has never seen, and must change neither side
// while doing it.
func TestDriftCheckFindsBothDirections(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	f := n.Folders["data"]
	f.Scanner.SetSettleScans(1)

	writeAt(t, root, "indexed/known.bin", "known")
	writeAt(t, root, "indexed/gone.bin", "will vanish")
	mustScan(t, n)

	// In step after a scan.
	rep, err := n.CheckDrift("data")
	if err != nil {
		t.Fatal(err)
	}
	if rep.MissingCount != 0 || rep.UnknownCount != 0 {
		t.Fatalf("clean tree reported drift: %+v", rep)
	}
	if rep.OnDisk != rep.Indexed {
		t.Fatalf("on disk %d vs indexed %d on a clean tree", rep.OnDisk, rep.Indexed)
	}

	// One file appears without the index knowing, one disappears behind its back.
	writeAt(t, root, "indexed/new.bin", "brand new")
	if err := os.Remove(filepath.Join(root, "indexed", "gone.bin")); err != nil {
		t.Fatal(err)
	}
	rep, err = n.CheckDrift("data")
	if err != nil {
		t.Fatal(err)
	}
	if rep.MissingCount != 1 || len(rep.Missing) != 1 || rep.Missing[0] != "indexed/gone.bin" {
		t.Fatalf("missing side = %+v, want indexed/gone.bin", rep)
	}
	if rep.UnknownCount != 1 || len(rep.Unknown) != 1 || rep.Unknown[0] != "indexed/new.bin" {
		t.Fatalf("unknown side = %+v, want indexed/new.bin", rep)
	}

	// Read-only: the index must be untouched by the check.
	if _, live := liveEntry(t, f.Ix, "indexed/gone.bin"); !live {
		t.Fatal("the drift check removed an index entry")
	}
	if _, live := liveEntry(t, f.Ix, "indexed/new.bin"); live {
		t.Fatal("the drift check indexed a file")
	}

	// And the counts are published for the status/metrics.
	dm, du := n.FolderDrift("data")
	if dm != 1 || du != 1 {
		t.Fatalf("FolderDrift = %d/%d, want 1/1", dm, du)
	}
}
