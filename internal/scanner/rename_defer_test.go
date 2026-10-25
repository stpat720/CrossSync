package scanner

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/ignore"
	"crosssync/internal/index"
)

// A directory rename is seen by the walk as "old paths gone, new paths here",
// but its two halves sit behind different gates: the deletion needs only
// deletionScans misses, while the added copy must also survive the stability
// gate's QUIET WINDOW. With change-driven scans the deletion is therefore
// confirmed several scans before the add is released. Publishing it alone tells
// peers "these paths are gone" with no move to apply — they destroy or park
// their copies and later download every byte at the new paths. These tests pin
// that the deletion half waits for the add half, and that a genuine deletion is
// never delayed into oblivion.

// TestRenameDeletionWaitsForSettleGate: the deletion half of a rename must not
// be emitted while a (size, mtime)-identical file is still settling. Once the
// gate releases it, the pair must come out as one Renamed change per file.
func TestRenameDeletionWaitsForSettleGate(t *testing.T) {
	root, ix := setup(t)
	const n = 3
	for i := 0; i < n; i++ {
		writeFile(t, root, fmt.Sprintf("src/f%d.bin", i), fmt.Sprintf("content-%d", i))
	}

	s := New(root, ix, &ignore.Matcher{})
	clock := int64(1000)
	s.SetClock(func() int64 { return clock })
	// Production shape: two observations AND the default 10s quiet window; a
	// deletion is confirmed on two misses.
	s.SetDeletionScans(2)

	// Index the files (two scans past the window).
	scan := func() []Change {
		t.Helper()
		changes, err := s.Scan()
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range changes {
			if c.Kind == Added || c.Kind == Modified || c.Kind == Renamed {
				if err := ix.Put(c.Info); err != nil {
					t.Fatal(err)
				}
			}
		}
		return changes
	}
	scan()
	clock += 12
	if changes := scan(); kindOf(changes, "src/f0.bin") != Added {
		t.Fatalf("setup: src/f0.bin should be added once its window passes: %+v", changes)
	}

	if err := os.Rename(filepath.Join(root, "src"), filepath.Join(root, "dst")); err != nil {
		t.Fatal(err)
	}

	// Miss 1. (The new DIRECTORY is created immediately — it carries no
	// content and the move pairing is per file — but no old path is spoken for.)
	clock += 2
	for _, c := range scan() {
		if c.Kind == Deleted {
			t.Fatalf("first miss after a rename must not emit a deletion, got %+v", c)
		}
	}
	// Miss 2: a deletion is now confirmed, but dst/ is still inside the quiet
	// window, so the deletion half must be deferred — otherwise a round in this
	// window tells the peer to delete content it is about to re-download.
	clock += 2
	changes := scan()
	for _, c := range changes {
		if c.Kind == Deleted {
			t.Fatalf("the deletion half of a rename went out before the add half settled: %+v", c)
		}
	}
	if kindOf(changes, "src") != -1 || kindOf(changes, "src/f0.bin") != -1 {
		t.Fatalf("nothing about the old path may be advertised yet: %+v", changes)
	}
	// The confirmation is kept, not restarted: the deferral is not a reset.
	miss, err := ix.MissingList()
	if err != nil {
		t.Fatal(err)
	}
	if st, ok := miss["src/f0.bin"]; !ok || st.Count < 2 {
		t.Fatalf("the deferred deletion lost its confirmation: %+v (want count >= 2)", miss)
	}

	// Window elapsed: the adds are released and pair with the still-live old
	// entries, so each file is ONE move rather than a delete + add.
	clock += 12
	changes = scan()
	for _, c := range changes {
		if c.Kind == Deleted && c.Info != nil && c.Info.Type == index.TypeFile {
			t.Fatalf("old path emitted as a deletion instead of a move: %+v", c)
		}
	}
	for i := 0; i < n; i++ {
		oldRel := fmt.Sprintf("src/f%d.bin", i)
		newRel := fmt.Sprintf("dst/f%d.bin", i)
		found := false
		for _, c := range changes {
			if c.Kind == Renamed && c.From == oldRel && c.Info != nil && c.Info.Name == newRel {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s -> %s was not reported as a rename: %+v", oldRel, newRel, changes)
		}
	}

	// And the rename is drained: nothing more comes out of the next scans.
	clock += 12
	for _, c := range scan() {
		if c.Kind != Skipped {
			t.Fatalf("a converged rename keeps emitting changes: %+v", c)
		}
	}
}

// TestGenuineDeletionStillConfirmedWhileAFileSettles: the deferral is about a
// deletion whose (size, mtime) matches a settling file. An unrelated deletion
// must go through on schedule even while some other file is being written — the
// gate is not a blanket delay.
func TestGenuineDeletionStillConfirmedWhileAFileSettles(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "docs/gone.txt", "delete me")
	writeFile(t, root, "docs/stays.txt", "keeps me")

	s := New(root, ix, &ignore.Matcher{})
	clock := int64(5000)
	s.SetClock(func() int64 { return clock })
	s.SetDeletionScans(2)

	scan := func() []Change {
		t.Helper()
		changes, err := s.Scan()
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range changes {
			if c.Kind == Added || c.Kind == Modified || c.Kind == Renamed {
				if err := ix.Put(c.Info); err != nil {
					t.Fatal(err)
				}
			}
		}
		return changes
	}
	scan()
	clock += 12
	scan()

	// A file being written elsewhere keeps the stability gate busy.
	writeFile(t, root, "docs/incoming.txt", "partial")
	// A genuine deletion: nothing settling shares its (size, mtime).
	if err := os.Remove(filepath.Join(root, "docs", "gone.txt")); err != nil {
		t.Fatal(err)
	}
	clock += 2
	scan()
	clock += 2
	changes := scan()
	if kindOf(changes, "docs/gone.txt") != Deleted {
		t.Fatalf("a genuine deletion was held back by an unrelated settling file: %+v", changes)
	}
	if kindOf(changes, "docs/incoming.txt") != -1 {
		t.Fatalf("the stability gate leaked a file that is still being written: %+v", changes)
	}
}

// TestScopedRenameDeletionDefersToo: a scoped scan that only re-walks the OLD
// directory must still see the settling new copy (from the persisted settling
// table) and defer the deletion, or a scoped scan would reopen the split.
func TestScopedRenameDeletionDefersToo(t *testing.T) {
	root, ix := setup(t)
	for i := 0; i < 2; i++ {
		writeFile(t, root, fmt.Sprintf("src/f%d.bin", i), fmt.Sprintf("body-%d", i))
	}
	s := New(root, ix, &ignore.Matcher{})
	clock := int64(9000)
	s.SetClock(func() int64 { return clock })
	s.SetDeletionScans(2)

	scanAll := func() []Change {
		t.Helper()
		changes, err := s.Scan()
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range changes {
			if c.Kind == Added || c.Kind == Modified || c.Kind == Renamed {
				if err := ix.Put(c.Info); err != nil {
					t.Fatal(err)
				}
			}
		}
		return changes
	}
	scanAll()
	clock += 12
	scanAll()

	if err := os.Rename(filepath.Join(root, "src"), filepath.Join(root, "dst")); err != nil {
		t.Fatal(err)
	}
	// Change-driven cadence: the watcher's queue produces a scoped scan per
	// changed path, in its own order. First miss of the old directory.
	clock += 2
	if ch, err := s.ScanScope([]string{"src"}); err != nil {
		t.Fatal(err)
	} else {
		for _, c := range ch {
			if c.Kind == Deleted && c.Info != nil && c.Info.Type == index.TypeFile {
				t.Fatalf("first miss must not publish a deletion: %+v", c)
			}
		}
	}
	// Observe the new copy (it is now settling). This scan's scope is dst, so
	// the settling rows live OUTSIDE the next scan's scope.
	clock += 2
	if ch, err := s.ScanScope([]string{"dst"}); err != nil {
		t.Fatal(err)
	} else {
		for _, c := range ch {
			if (c.Kind == Added || c.Kind == Modified) && c.Info != nil && c.Info.Type == index.TypeFile {
				t.Fatalf("the settling new copy must not be advertised: %+v", c)
			}
		}
	}
	// Second miss of the old directory: the deletion is confirmed by count,
	// but the new copy is still inside its window — and it is NOT in this
	// scope, so only the persisted settling table can tell us that.
	clock += 2
	ch, err := s.ScanScope([]string{"src"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range ch {
		if c.Kind == Deleted && c.Info != nil && c.Info.Type == index.TypeFile {
			t.Fatalf("a scoped scan published the deletion half of a rename: %+v", c)
		}
	}
	// After the window the scoped pair still resolves as a rename.
	clock += 12
	ch, err = s.ScanScope([]string{"src", "dst"})
	if err != nil {
		t.Fatal(err)
	}
	renamed := 0
	for _, c := range ch {
		if c.Kind == Renamed {
			renamed++
		}
		if c.Kind == Deleted && c.Info != nil && c.Info.Type == index.TypeFile {
			t.Fatalf("scoped rename emitted a deletion: %+v", c)
		}
	}
	if renamed != 2 {
		t.Fatalf("scoped scan reported %d rename(s), want 2: %+v", renamed, ch)
	}
}

// keep index imported for the helper type above.
var _ = index.TypeFile
