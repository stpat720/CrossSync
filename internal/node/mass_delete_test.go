package node

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"crosssync/internal/index"
)

// TestMassDeleteConfirmsItself pins the behaviour that made the UI untrustworthy
// after an operator wiped a folder: the deletion gate only SUSPECTS on the first
// pass, and the second observation used to wait for the periodic safety-net walk
// (up to ten minutes). Until then /api/status kept reporting files and bytes that
// were already gone from disk — "3,276 files · 6.5 GB" for an empty folder.
//
// One ScanFolder call must be enough: the pass that suspects deletions queues the
// confirming pass itself, and the index converges to empty with no further
// operator action.
func TestMassDeleteConfirmsItself(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	f := n.Folders["data"]
	f.Scanner.SetSettleScans(1)    // index new files immediately; this test is about deletes
	f.Scanner.SetDeletionScans(2)  // the production default: a delete needs two passes

	rng := rand.New(rand.NewSource(4242))
	for i := 0; i < 40; i++ {
		writePattern(t, root, fmt.Sprintf("d/f%02d.bin", i), pattern(byte(i), 2048, rng))
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if got := liveEntries(t, f); got != 40 {
		t.Fatalf("indexed %d entries, want 40 files (plus the directory itself)", got)
	}

	// The operator deletes the FILES but leaves the directory: a directory
	// tombstone cascades to its children in one pass, so deleting the tree by
	// its parent would not exercise the two-pass gate at all.
	entries, err := os.ReadDir(filepath.Join(root, "d"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := os.Remove(filepath.Join(root, "d", e.Name())); err != nil {
			t.Fatal(err)
		}
	}

	// ONE scan. Then the daemon must converge by itself.
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if pend, err := f.Ix.MissingList(); err == nil {
		t.Logf("after first pass: missing-table=%d live=%d", len(pend), liveEntries(t, f))
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		left := liveEntries(t, f)
		if left == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("index still reports %d file(s) 20s after the deletion; the confirming pass did not run", left)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// liveEntries counts non-deleted entries in the folder's index.
func liveEntries(t *testing.T, f *Folder) int {
	t.Helper()
	n := 0
	if err := f.Ix.List(func(fi *index.FileInfo) error {
		// Only files: the directory entry itself is also in the index and it is
		// dropped with the tree, so counting it would make the numbers fuzzy.
		if !fi.Deleted && fi.Type == index.TypeFile {
			n++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}
