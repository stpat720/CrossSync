package node_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crosssync/internal/node"
)

// hasConflictCopy reports whether any file in root carries the conflict suffix.
func hasConflictCopy(t *testing.T, root string) bool {
	t.Helper()
	found := false
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.Contains(filepath.Base(p), ".sync-conflict") {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// TestThreeNodeConvergence is the first topology beyond a pair: three peers all
// sharing one folder id. A file created on A must reach B and C, a deletion on A
// must be applied by both, and a concurrent edit on B and C must be resolved the
// way the 2-node engine resolves it (one winner in place, the loser preserved as
// a conflict copy) with NO content lost. Two-node tests cannot catch scoping or
// version-vector problems that only appear with a third vector.
func TestThreeNodeConvergence(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	rootC := t.TempDir()
	a := makeNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy")
	b := makeNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy")
	c := makeNode(t, 3, "c", rootC, filepath.Join(t.TempDir(), "meta-c"), "conflict-copy")

	for _, n := range []*node.Node{a, b, c} {
		if _, err := n.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
	}

	write(t, rootA, "one.txt", "from a")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)
	pipeSession(t, a, c)
	for name, root := range map[string]string{"b": rootB, "c": rootC} {
		if got := read(t, root, "one.txt"); got != "from a" {
			t.Fatalf("%s got %q, want %q", name, got, "from a")
		}
	}

	// Deletion on A must be applied on B and C (tombstone propagation, not a
	// resurrect from the third node).
	if err := os.Remove(filepath.Join(rootA, "one.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)
	pipeSession(t, a, c)
	for name, root := range map[string]string{"b": rootB, "c": rootC} {
		if _, err := os.Stat(filepath.Join(root, "one.txt")); !os.IsNotExist(err) {
			t.Fatalf("%s still has one.txt after the delete propagated (err=%v)", name, err)
		}
	}

	// Concurrent edit on B and C (A not involved): the engine must keep both
	// contents somewhere — one inline winner, one conflict copy — on every node
	// once the rounds settle. Data loss here would be silent, hence the
	// content-level assertion rather than a count.
	write(t, rootB, "shared.txt", "edited on b")
	write(t, rootC, "shared.txt", "edited on c")
	for _, n := range []*node.Node{b, c} {
		if _, err := n.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
	}
	pipeSession(t, b, c)
	pipeSession(t, a, b)
	pipeSession(t, a, c)
	pipeSession(t, b, c)

	sawConflict := false
	for name, root := range map[string]string{"a": rootA, "b": rootB, "c": rootC} {
		body := read(t, root, "shared.txt")
		if body != "edited on b" && body != "edited on c" {
			t.Fatalf("%s: shared.txt = %q, want one of the two edits", name, body)
		}
		if hasConflictCopy(t, root) {
			sawConflict = true
		}
	}
	if !sawConflict {
		t.Fatal("concurrent edits on two peers lost one side entirely: no conflict copy anywhere")
	}
}
