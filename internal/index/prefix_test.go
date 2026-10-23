package index

import (
	"reflect"
	"testing"
)

// TestListPrefixRangeBounds pins the range scan ListPrefix uses: everything
// strictly under "a/" and nothing that merely starts with the same letters
// ("ab/1" must not match). Names come back deepest-first, which is the order the
// directory-tombstone path needs to empty a subtree bottom-up.
func TestListPrefixRangeBounds(t *testing.T) {
	ix, err := Open(t.TempDir()+"/ix.db", "f1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ix.Close() }()

	for _, name := range []string{"a/1.txt", "a/2.txt", "a/sub/3.txt", "ab/1.txt", "b/1.txt"} {
		if err := ix.Put(&FileInfo{Name: name, Type: TypeFile, Size: 1, ModifiedS: 1}); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}
	// A tombstone under the prefix must not be listed.
	if err := ix.Put(&FileInfo{Name: "a/gone.txt", Type: TypeFile, Deleted: true}); err != nil {
		t.Fatal(err)
	}

	var got []string
	if err := ix.ListPrefix("a/", func(fi *FileInfo) error {
		got = append(got, fi.Name)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"a/sub/3.txt", "a/2.txt", "a/1.txt"} // descending
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListPrefix(a/) = %v, want %v", got, want)
	}

	// A prefix without a trailing slash is a programming error, not a silent
	// full-folder scan.
	if err := ix.ListPrefix("a", func(*FileInfo) error { return nil }); err == nil {
		t.Fatal("ListPrefix without a trailing slash should error")
	}
}
