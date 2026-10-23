package scanner

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/ignore"
)

// TestDeletionNeedsConfirmation is the deletion gate's contract: a path that one
// walk missed is NOT reported deleted yet. One transient bad walk (a union/FUSE
// share mid-rebuild, a network mount hiccup, an Unraid /mnt/user cache refresh)
// must never tombstone real data — the peer would delete its copy, and a
// directory tombstone used to take the whole subtree with it.
func TestDeletionNeedsConfirmation(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "keep.txt", "still here")
	writeFile(t, root, "gone.txt", "about to vanish")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)  // additions: immediate
	s.SetDeletionScans(2) // deletions: two misses

	first := gateScan(t, s, ix)
	if kindOf(first, "gone.txt") != Added {
		t.Fatalf("first scan should add gone.txt, got %+v", first)
	}

	if err := os.Remove(filepath.Join(root, "gone.txt")); err != nil {
		t.Fatal(err)
	}

	// First miss: the deletion is suspected, not believed.
	second := gateScan(t, s, ix)
	if k := kindOf(second, "gone.txt"); k != -1 {
		t.Fatalf("first miss reported kind %v, want no change (waiting for confirmation)", k)
	}

	// Second miss: confirmed, reported deleted.
	third := gateScan(t, s, ix)
	if k := kindOf(third, "gone.txt"); k != Deleted {
		t.Fatalf("second miss reported kind %v, want Deleted", k)
	}

	// The surviving file was never touched by any of this.
	if k := kindOf(third, "keep.txt"); k != -1 {
		t.Fatalf("keep.txt reported as kind %v, want no change", k)
	}
	if _, err := os.Stat(filepath.Join(root, "keep.txt")); err != nil {
		t.Fatalf("keep.txt disappeared: %v", err)
	}
}

// TestDeletionConfirmationResetsWhenFileReturns: a path that comes back between
// scans must clear its suspicion, so a later real deletion still needs the full
// confirmation window.
func TestDeletionConfirmationResetsWhenFileReturns(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "flap.txt", "v1")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)
	s.SetDeletionScans(2)
	if changes := gateScan(t, s, ix); kindOf(changes, "flap.txt") != Added {
		t.Fatalf("setup scan = %+v, want Added", changes)
	}

	p := filepath.Join(root, "flap.txt")
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if changes := gateScan(t, s, ix); kindOf(changes, "flap.txt") != -1 {
		t.Fatalf("first miss must not report a deletion, got %+v", changes)
	}

	// Back again (different content, so it is a real change): the deletion
	// suspicion is dropped, and no deletion may be reported for it.
	writeFile(t, root, "flap.txt", "v2")
	if changes := gateScan(t, s, ix); kindOf(changes, "flap.txt") == Deleted {
		t.Fatalf("reappeared file must not be reported deleted, got %+v", changes)
	}
	if st, err := ix.MissingList(); err != nil {
		t.Fatal(err)
	} else if _, still := st["flap.txt"]; still {
		t.Fatal("missing-table entry should be cleared once the file is back")
	}

	// Delete for real: it must take the FULL window again, not one scan.
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if changes := gateScan(t, s, ix); kindOf(changes, "flap.txt") != -1 {
		t.Fatalf("gate did not reset: %+v", changes)
	}
	if changes := gateScan(t, s, ix); kindOf(changes, "flap.txt") != Deleted {
		t.Fatalf("confirmed deletion missing: %+v", changes)
	}
}

// TestRenameNotDelayedByDeletionGate: rename pairing runs before the gate, so a
// move is still detected on the FIRST scan. Gating moves would turn every
// rename into a full re-upload, which is the whole point of the move path.
func TestRenameNotDelayedByDeletionGate(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "old/name.txt", "identical content")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)
	s.SetDeletionScans(2)
	if changes := gateScan(t, s, ix); kindOf(changes, "old/name.txt") != Added {
		t.Fatalf("setup scan = %+v", changes)
	}

	if err := os.MkdirAll(filepath.Join(root, "new"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "old/name.txt"), filepath.Join(root, "new/name.txt")); err != nil {
		t.Fatal(err)
	}

	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	var renamed bool
	for _, c := range changes {
		if c.Kind == Renamed && c.Info != nil && c.Info.Name == "new/name.txt" {
			renamed = true
		}
		if c.Kind == Deleted && c.Info != nil && c.Info.Name == "old/name.txt" {
			t.Fatalf("rename was reported as a deletion: %+v", changes)
		}
	}
	if !renamed {
		t.Fatalf("rename not detected on the first scan: %+v", changes)
	}
}
