package index

import (
	"fmt"
	"path/filepath"
	"testing"

	"crosssync/internal/version"
)

func TestIndexIDStableAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "folder.db")
	ix, err := Open(path, "f")
	if err != nil {
		t.Fatal(err)
	}
	id1, err := ix.IndexID()
	if err != nil {
		t.Fatal(err)
	}
	if id1 == "" || len(id1) != 32 {
		t.Fatalf("index id should be 32 hex chars, got %q", id1)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}
	ix2, err := Open(path, "f")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ix2.Close() }()
	id2, err := ix2.IndexID()
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("index id changed across reopen: %q != %q", id1, id2)
	}
}

func TestMaxSeqAndListAfter(t *testing.T) {
	ix := openTestIndex(t)
	for i := 0; i < 5; i++ {
		fi := &FileInfo{Name: fmt.Sprintf("file-%d", i), Size: int64(i), Type: TypeFile, Version: version.New()}
		_ = ix.Put(fi)
	}
	max := ix.MaxSeq()
	if max != 5 {
		t.Fatalf("MaxSeq = %d, want 5", max)
	}

	// Delete one entry: the engine writes a fresh tombstone (Sequence 0) so
	// Put allocates a new sequence — that is what makes deletions travel in
	// deltas. Mirror that here.
	tombstone := &FileInfo{Name: "file-2", Type: TypeFile, Deleted: true, Version: version.New()}
	if err := ix.Put(tombstone); err != nil {
		t.Fatal(err)
	}

	// ListAfter(max) must include the tombstone and nothing else.
	var names []string
	if err := ix.ListAfter(max, func(fi *FileInfo) error {
		names = append(names, fi.Name)
		if !fi.Deleted {
			t.Fatalf("expected tombstone in delta, got live entry %q", fi.Name)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "file-2" {
		t.Fatalf("ListAfter(max) = %v, want [file-2]", names)
	}
}

func TestPeerIndexMarkerRoundTrip(t *testing.T) {
	ix := openTestIndex(t)
	_, _, _, ok, err := ix.GetPeerIndex(42)
	if err != nil || ok {
		t.Fatalf("expected no marker initially (ok=%v err=%v)", ok, err)
	}
	if err := ix.SetPeerIndex(42, "abc123", 77, "sess-a"); err != nil {
		t.Fatal(err)
	}
	id, seq, sess, ok, err := ix.GetPeerIndex(42)
	if err != nil || !ok {
		t.Fatalf("expected marker after set (ok=%v err=%v)", ok, err)
	}
	if id != "abc123" || seq != 77 || sess != "sess-a" {
		t.Fatalf("marker = (%q,%d,%q), want (abc123,77,sess-a)", id, seq, sess)
	}
	// Different peer has no marker.
	if _, _, _, ok, err := ix.GetPeerIndex(43); err != nil || ok {
		t.Fatalf("peer 43 should have no marker (ok=%v err=%v)", ok, err)
	}
}

func TestPruneTombstonesFloor(t *testing.T) {
	ix := openTestIndex(t)
	// A live file (seq 1) and a tombstone (seq 2).
	live := &FileInfo{Name: "keep.txt", Size: 1, Type: TypeFile, Version: version.New()}
	_ = ix.Put(live)
	tomb := &FileInfo{Name: "gone.txt", Type: TypeFile, Deleted: true, Version: version.New()}
	_ = ix.Put(tomb)

	// floor 0 / negative: nothing pruned.
	if n, err := ix.PruneTombstones(0); err != nil || n != 0 {
		t.Fatalf("PruneTombstones(0) = (%d,%v), want (0,nil)", n, err)
	}
	if n, err := ix.PruneTombstones(-1); err != nil || n != 0 {
		t.Fatalf("PruneTombstones(-1) = (%d,%v), want (0,nil)", n, err)
	}

	// floor == tombstone seq: pruned — the marker means the peer received
	// everything up to AND including that sequence, so the tombstone at it
	// has been delivered.
	if n, err := ix.PruneTombstones(2); err != nil || n != 1 {
		t.Fatalf("PruneTombstones(2) = (%d,%v), want (1,nil)", n, err)
	}
	if _, ok, err := ix.Get("gone.txt"); err != nil || ok {
		t.Fatalf("gone.txt should be hard-deleted (ok=%v err=%v)", ok, err)
	}
	if _, ok, err := ix.Get("keep.txt"); err != nil || !ok {
		t.Fatalf("keep.txt should survive (ok=%v err=%v)", ok, err)
	}
}

func TestRemovePeerIndex(t *testing.T) {
	ix := openTestIndex(t)
	if err := ix.SetPeerIndex(99, "id-99", 10, "s"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, _ := ix.GetPeerIndex(99); !ok {
		t.Fatalf("expected marker before remove")
	}
	if err := ix.RemovePeerIndex(99); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, _ := ix.GetPeerIndex(99); ok {
		t.Fatalf("marker should be gone after remove")
	}
	// Idempotent: removing again is a no-op, not an error.
	if err := ix.RemovePeerIndex(99); err != nil {
		t.Fatal(err)
	}
}
