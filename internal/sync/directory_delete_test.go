package sync

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/index"
)

// TestDirectoryTombstoneKeepsLocalOnlyChildren is the regression test for a
// directory tombstone recursing into the local tree.
//
// A peer that tombstones directory "d" must not take files with it that it never
// tombstoned: those may be files it never had, or files whose local version
// wins. The old code ran os.RemoveAll("d") for a directory tombstone, which
// destroyed them with no version check, no versioning archive and no hold —
// and because the deletion guard counts names, the whole subtree counted as a
// single deletion, so the hold never engaged either.
func TestDirectoryTombstoneKeepsLocalOnlyChildren(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "d/gone.txt", "the peer deleted this one too")
	n.writeFile(t, "d/keep.txt", "local only — the peer never had this")
	n.scanAndApply(t)
	// The stability gate holds a just-scanned file out of the delete pass
	// until it settles; these files are settled for the purposes of the test.
	n.engine.SetInFlight(nil)

	gone, ok, err := n.ix.Get("d/gone.txt")
	if err != nil || !ok {
		t.Fatalf("index gone.txt: ok=%v err=%v", ok, err)
	}
	dirFi, ok, err := n.ix.Get("d")
	if err != nil || !ok {
		t.Fatalf("index d: ok=%v err=%v", ok, err)
	}

	// The peer deleted the directory and one child — but not keep.txt. Its
	// versions descend from ours (bump ours, then the peer's) so the tombstones
	// dominate rather than racing.
	n.engine.SetPeerIndex(2, []*index.FileInfo{
		{Name: "d", Type: index.TypeDirectory, Deleted: true, Version: dirFi.Version.Bump(1).Bump(2)},
		{Name: "d/gone.txt", Type: index.TypeFile, Deleted: true, Version: gone.Version.Bump(1).Bump(2)},
	}, false)

	dels, err := n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) == 0 {
		t.Fatal("expected the directory and its deleted child to be pending")
	}
	for _, name := range dels {
		if _, err := n.engine.applyDeletion(name, 2, false); err != nil {
			t.Fatalf("applyDeletion(%q): %v", name, err)
		}
	}

	if _, err := os.Stat(filepath.Join(n.root, "d", "keep.txt")); err != nil {
		t.Fatalf("local-only child was destroyed by the directory tombstone: %v", err)
	}
	if fi, ok, _ := n.ix.Get("d/keep.txt"); !ok || fi.Deleted {
		t.Fatal("local-only child was tombstoned")
	}
	if _, err := os.Stat(filepath.Join(n.root, "d", "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("child the peer deleted is still on disk: %v", err)
	}
	if fi, ok, _ := n.ix.Get("d"); !ok || fi.Deleted {
		t.Fatal("directory was tombstoned while a child survives")
	}
	if _, err := os.Stat(filepath.Join(n.root, "d")); err != nil {
		t.Fatalf("directory removed while a child survives: %v", err)
	}
}

// TestDirectoryTombstoneRemovesDirectoryWhenEmpty covers the normal case: the
// peer deleted the whole subtree, every child carries its own tombstone, and the
// directory disappears once nothing is left inside it. Names are applied
// parents-first, so the directory is retried and goes on the second pass.
func TestDirectoryTombstoneRemovesDirectoryWhenEmpty(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "d/a.txt", "a")
	n.writeFile(t, "d/b.txt", "b")
	n.scanAndApply(t)
	n.engine.SetInFlight(nil)

	dirFi, _, _ := n.ix.Get("d")
	aFi, _, _ := n.ix.Get("d/a.txt")
	bFi, _, _ := n.ix.Get("d/b.txt")
	n.engine.SetPeerIndex(2, []*index.FileInfo{
		{Name: "d", Type: index.TypeDirectory, Deleted: true, Version: dirFi.Version.Bump(1).Bump(2)},
		{Name: "d/a.txt", Type: index.TypeFile, Deleted: true, Version: aFi.Version.Bump(1).Bump(2)},
		{Name: "d/b.txt", Type: index.TypeFile, Deleted: true, Version: bFi.Version.Bump(1).Bump(2)},
	}, false)

	for pass := 0; pass < 2; pass++ {
		dels, err := n.engine.PendingDeletions()
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range dels {
			if _, err := n.engine.applyDeletion(name, 2, false); err != nil {
				t.Fatalf("pass %d applyDeletion(%q): %v", pass, name, err)
			}
		}
	}

	if _, err := os.Stat(filepath.Join(n.root, "d")); !os.IsNotExist(err) {
		t.Fatalf("empty deleted directory should be gone, stat err = %v", err)
	}
	fi, ok, err := n.ix.Get("d")
	if err != nil || !ok {
		t.Fatalf("directory entry missing from the index: ok=%v err=%v", ok, err)
	}
	if !fi.Deleted {
		t.Fatal("directory should be tombstoned once it was removed")
	}
}
