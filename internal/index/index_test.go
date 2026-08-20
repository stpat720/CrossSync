package index

import (
	"path/filepath"
	"testing"

	"crosssync/internal/hash"
	"crosssync/internal/version"
)

func openTestIndex(t *testing.T) *Index {
	t.Helper()
	ix, err := Open(filepath.Join(t.TempDir(), "folder.db"), "test-folder")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ix.Close() })
	return ix
}

func TestPutGetDelete(t *testing.T) {
	ix := openTestIndex(t)

	fi := &FileInfo{
		Name:      "docs/report.txt",
		Size:      1234,
		ModifiedS: 100,
		Mode:      0o644,
		Type:      TypeFile,
		Version:   version.New().Bump(7).Bump(9),
		BlockSize: 128 * 1024,
		Blocks:    [][]byte{hash.HashBytes([]byte("block"))},
	}
	if err := ix.Put(fi); err != nil {
		t.Fatal(err)
	}
	if fi.Sequence == 0 {
		t.Fatal("Put should assign a sequence")
	}

	got, ok, err := ix.Get("docs/report.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected entry to exist")
	}
	if got.Size != 1234 || got.ModifiedS != 100 || got.Type != TypeFile {
		t.Fatalf("got wrong metadata: %+v", got)
	}
	if !got.Version.Equal(version.Vector{7: 1, 9: 1}) {
		t.Fatalf("version round trip failed: %v", got.Version)
	}
	if len(got.Blocks) != 1 || !equalBytes(got.Blocks[0], hash.HashBytes([]byte("block"))) {
		t.Fatalf("blocks round trip failed")
	}

	if err := ix.Delete("docs/report.txt"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := ix.Get("docs/report.txt"); ok {
		t.Fatal("entry should be gone after Delete")
	}
}

func TestListSkipsDeleted(t *testing.T) {
	ix := openTestIndex(t)
	_ = ix.Put(&FileInfo{Name: "a.txt", Version: version.New(), Type: TypeFile})
	_ = ix.Put(&FileInfo{Name: "b.txt", Version: version.New(), Type: TypeFile})
	// Tombstone: stored as deleted but not removed from the index.
	_ = ix.Put(&FileInfo{Name: "c.txt", Version: version.New(), Type: TypeFile, Deleted: true})

	var names []string
	if err := ix.List(func(fi *FileInfo) error {
		names = append(names, fi.Name)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatalf("List should return 2 non-deleted entries, got %d: %v", len(names), names)
	}
}

func TestSequencesMonotonic(t *testing.T) {
	ix := openTestIndex(t)
	seen := map[int64]bool{}
	for i := 0; i < 50; i++ {
		fi := &FileInfo{Name: filepath.Join("f", itoa(i)), Version: version.New(), Type: TypeFile}
		if err := ix.Put(fi); err != nil {
			t.Fatal(err)
		}
		if seen[fi.Sequence] {
			t.Fatalf("duplicate sequence %d", fi.Sequence)
		}
		seen[fi.Sequence] = true
	}
}

func TestMeta(t *testing.T) {
	ix := openTestIndex(t)
	if _, ok, _ := ix.GetMeta("index_id"); ok {
		t.Fatal("meta should be empty initially")
	}
	if err := ix.SetMeta("index_id", "abc"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := ix.GetMeta("index_id")
	if err != nil || !ok || v != "abc" {
		t.Fatalf("meta get = %q,%v,%v", v, ok, err)
	}
}

func TestIntegrityCheck(t *testing.T) {
	ix := openTestIndex(t)
	if err := ix.Put(&FileInfo{Name: "x", Version: version.New(), Type: TypeFile}); err != nil {
		t.Fatal(err)
	}
	if err := ix.IntegrityCheck(); err != nil {
		t.Fatalf("integrity check failed: %v", err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
