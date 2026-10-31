package sync

import (
	"testing"

	"crosssync/internal/index"
)

// TestPendingCacheIsACopyAndInvalidates: the pending lists are cached against a
// write counter, so an idle round costs nothing. Two properties must hold for that
// to be safe: the caller gets a COPY (mutating it cannot poison the cache) and any
// write invalidates the entry.
func TestPendingCacheIsACopyAndInvalidates(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "one")
	n.scanAndApply(t)
	n.engine.SetPeerIndex(2, nil, false) // peer has nothing: everything is owed

	first, err := n.engine.PendingUploads()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0] != "a.txt" {
		t.Fatalf("PendingUploads = %v, want [a.txt]", first)
	}
	first[0] = "MUTATED"

	second, err := n.engine.PendingUploads()
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0] != "a.txt" {
		t.Fatalf("the cache handed out its own slice: %v", second)
	}

	// A local write must invalidate: the peer still has nothing, so the new file
	// is owed too, and the cached single entry must not survive.
	if err := n.engine.Ix.Put(&index.FileInfo{Name: "b.txt", Size: 3, Type: index.TypeFile}); err != nil {
		t.Fatal(err)
	}
	third, err := n.engine.PendingUploads()
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 2 {
		t.Fatalf("PendingUploads after a write = %v, want both files", third)
	}
}
