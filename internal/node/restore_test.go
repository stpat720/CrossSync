package node

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/index"
)

// TestRestoreFromBackupNeedsNoReAdd is the rehearsal the backup never had: take the
// snapshot that goes into the archive, put it in a brand-new meta directory next to
// an untouched share, and check what the daemon does.
//
// The point is not that it starts — it is that it does NOT decide the share is new.
// A restore that loses version vectors makes both nodes re-send 32 GB, or worse,
// re-add files the peer has tombstoned.
func TestRestoreFromBackupNeedsNoReAdd(t *testing.T) {
	share := t.TempDir()
	metaA := t.TempDir()
	a := chunkTestNode(t, 1, "a", share, metaA)
	writeAt(t, share, "docs/one.txt", "one")
	writeAt(t, share, "docs/two.txt", "two")
	writeAt(t, share, "top.bin", "top")
	mustScan(t, a)

	before := map[string]index.FileInfo{}
	if err := a.Folders["data"].Ix.List(func(fi *index.FileInfo) error {
		before[fi.Name] = *fi
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(before) == 0 {
		t.Fatal("nothing indexed to restore")
	}

	// Exactly what the backup handler writes into the archive.
	archive := filepath.Join(t.TempDir(), "data.db")
	if err := a.Folders["data"].Ix.SnapshotTo(archive); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	// The box is rebuilt: fresh meta dir, the share untouched, the snapshot put
	// where openFolderIndex looks for it.
	metaB := t.TempDir()
	if err := os.WriteFile(filepath.Join(metaB, "data.db"), mustRead(t, archive), 0o644); err != nil {
		t.Fatal(err)
	}
	b := chunkTestNode(t, 1, "a", share, metaB)

	after := map[string]index.FileInfo{}
	if err := b.Folders["data"].Ix.List(func(fi *index.FileInfo) error {
		after[fi.Name] = *fi
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("restored %d entries, archived %d", len(after), len(before))
	}
	for name, was := range before {
		now, ok := after[name]
		if !ok {
			t.Fatalf("%q vanished in the restore", name)
		}
		if now.Version.Compare(was.Version) != 0 {
			t.Fatalf("%q version vector changed across the restore: %v -> %v", name, was.Version, now.Version)
		}
		if now.Size != was.Size || now.Type != was.Type || len(now.Blocks) != len(was.Blocks) {
			t.Fatalf("%q content identity changed across the restore", name)
		}
	}

	// And the daemon must agree there is nothing to do: no re-add, no delete.
	changes := mustScan(t, b)
	if changes != 0 {
		t.Fatalf("restored index wanted %d changes on an untouched share", changes)
	}
	var live int
	if err := b.Folders["data"].Ix.List(func(fi *index.FileInfo) error {
		if !fi.Deleted {
			live++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if live != len(before) {
		t.Fatalf("index has %d live entries after the restore, want %d", live, len(before))
	}
}

func mustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
