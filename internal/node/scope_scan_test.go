package node

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/index"
)

// TestScanFolderPathsTouchesOnlyTheScope is the node-level contract of the
// watcher fast path: a scoped scan indexes the changed path and leaves the rest
// of the folder (and its index rows) alone.
func TestScanFolderPathsTouchesOnlyTheScope(t *testing.T) {
	root := t.TempDir()
	meta := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, meta)

	// Three files, indexed by a full scan.
	for _, rel := range []string{"one.txt", "two.txt", "sub/three.txt"} {
		chunkWrite(t, root, rel, "initial "+rel)
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	// 3 files + the "sub" directory entry.
	if got := indexCount(t, n); got != 4 {
		t.Fatalf("after the full scan the index has %d entries, want 4", got)
	}

	// A fourth file appears; only its path is scanned.
	chunkWrite(t, root, "sub/four.txt", "new content")
	applied, err := n.ScanFolderPaths("data", []string{"sub/four.txt"})
	if err != nil {
		t.Fatalf("scoped scan: %v", err)
	}
	if applied != 1 {
		t.Fatalf("scoped scan applied %d changes, want 1", applied)
	}
	if got := indexCount(t, n); got != 5 {
		t.Fatalf("index has %d entries, want 5", got)
	}

	// A file the scope did not name is deleted on disk: the scoped scan must not
	// notice, and the index row must survive.
	if err := os.Remove(filepath.Join(root, "one.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ScanFolderPaths("data", []string{"sub/four.txt"}); err != nil {
		t.Fatal(err)
	}
	f := n.Folders["data"]
	if _, ok, _ := f.Ix.Get("one.txt"); !ok {
		t.Fatal("a path outside the scope was dropped from the index")
	}
}

// indexCount returns how many live entries the folder index holds.
func indexCount(t *testing.T, n *Node) int {
	t.Helper()
	f := n.Folders["data"]
	count := 0
	if err := f.Ix.List(func(*index.FileInfo) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	return count
}
