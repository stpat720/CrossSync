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
	defer ix2.Close()
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
	_, _, ok, err := ix.GetPeerIndex(42)
	if err != nil || ok {
		t.Fatalf("expected no marker initially (ok=%v err=%v)", ok, err)
	}
	if err := ix.SetPeerIndex(42, "abc123", 77); err != nil {
		t.Fatal(err)
	}
	id, seq, ok, err := ix.GetPeerIndex(42)
	if err != nil || !ok {
		t.Fatalf("expected marker after set (ok=%v err=%v)", ok, err)
	}
	if id != "abc123" || seq != 77 {
		t.Fatalf("marker = (%q,%d), want (abc123,77)", id, seq)
	}
	// Different peer has no marker.
	if _, _, ok, err := ix.GetPeerIndex(43); err != nil || ok {
		t.Fatalf("peer 43 should have no marker (ok=%v err=%v)", ok, err)
	}
}
