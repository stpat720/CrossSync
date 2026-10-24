package scanner

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/ignore"
)

// A scoped scan walks only the paths a change event named. Everything else must
// be left exactly as it was — no deletions, no settle resets, no surprises. That
// is the property that lets the watcher drive cheap scans instead of re-walking
// the whole share.

// TestScopedScanIgnoresOutsideChanges: a file outside the scope that was deleted
// from disk is NOT reported, and its index entry survives.
func TestScopedScanIgnoresOutsideChanges(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "a/one.txt", "1")
	writeFile(t, root, "b/two.txt", "2")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)
	s.SetSettleSeconds(0)
	s.SetDeletionScans(1)
	if changes := gateScan(t, s, ix); len(changes) == 0 {
		t.Fatal("setup scan found nothing")
	}

	// b/two.txt disappears, but we only scan a/.
	if err := os.Remove(filepath.Join(root, "b", "two.txt")); err != nil {
		t.Fatal(err)
	}
	changes, err := s.ScanScope([]string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "b/two.txt") != -1 {
		t.Fatalf("a scoped scan reported a path it never walked: %+v", changes)
	}
	if _, ok, _ := ix.Get("b/two.txt"); !ok {
		t.Fatal("index entry outside the scope was dropped")
	}

	// A full scan then sees it, confirming the entry was live all along.
	if changes, err := s.Scan(); err != nil {
		t.Fatal(err)
	} else if kindOf(changes, "b/two.txt") != Deleted {
		t.Fatalf("full scan should report the deletion, got %+v", changes)
	}
}

// TestScopedScanReportsInsideDeletion: the scoped walk DOES report what it saw
// disappear inside its own scope.
func TestScopedScanReportsInsideDeletion(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "a/one.txt", "1")
	writeFile(t, root, "a/two.txt", "2")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)
	s.SetSettleSeconds(0)
	s.SetDeletionScans(1)
	gateScan(t, s, ix)

	if err := os.Remove(filepath.Join(root, "a", "two.txt")); err != nil {
		t.Fatal(err)
	}
	changes, err := s.ScanScope([]string{"a/two.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "a/two.txt") != Deleted {
		t.Fatalf("scoped scan should report the deletion inside its scope, got %+v", changes)
	}
}

// TestScopedScanOfVanishedDirectory: the scope root itself is gone (a deleted
// directory), so its children are candidates for deletion.
func TestScopedScanOfVanishedDirectory(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "gone/x.txt", "x")
	writeFile(t, root, "gone/y.txt", "y")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)
	s.SetSettleSeconds(0)
	s.SetDeletionScans(1)
	gateScan(t, s, ix)

	if err := os.RemoveAll(filepath.Join(root, "gone")); err != nil {
		t.Fatal(err)
	}
	changes, err := s.ScanScope([]string{"gone"})
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "gone/x.txt") != Deleted || kindOf(changes, "gone/y.txt") != Deleted {
		t.Fatalf("children of a vanished scope root should be deleted, got %+v", changes)
	}
}

// TestScopedScanPreservesOutsideSettleState: rows for files outside the scope
// belong to other scans and must survive verbatim.
func TestScopedScanPreservesOutsideSettleState(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "a/one.txt", "1")
	writeFile(t, root, "b/growing.bin", "chunk")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(2)
	s.SetSettleSeconds(600) // long window: b stays unsettled for the whole test
	s.SetDeletionScans(1)

	if _, err := s.Scan(); err != nil { // first pass: both unsettled
		t.Fatal(err)
	}
	before, err := ix.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := before["b/growing.bin"]; !ok {
		t.Fatalf("expected b/growing.bin to be held back, table = %+v", before)
	}

	// Scan only a/.
	if _, err := s.ScanScope([]string{"a"}); err != nil {
		t.Fatal(err)
	}
	after, err := ix.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := after["b/growing.bin"], before["b/growing.bin"]; got != want {
		t.Fatalf("outside-scope settle state changed: got %+v, want %+v", got, want)
	}
}
