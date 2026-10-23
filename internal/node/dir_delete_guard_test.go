package node

import (
	"fmt"
	"testing"

	"crosssync/internal/index"
)

// TestDeletionWeightCountsDirectorySubtrees pins the weighting that makes the
// deletion guard see a directory tombstone for what it costs.
//
// The guard compares "how many deletions" against "how many live entries". A
// directory tombstone is ONE name, so a peer that deleted one directory holding
// 300 of a folder's 1,000 files used to look like a 0.1% delete pass: well under
// the 25% threshold, no hold, no warning — while the deletion itself took the
// whole subtree. deletionWeight adds the live entries under each pending
// directory so the pass is measured at 30%.
func TestDeletionWeightCountsDirectorySubtrees(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	f := n.Folders["data"]

	put := func(name string) {
		t.Helper()
		fi := &index.FileInfo{Name: name, Type: index.TypeFile, Size: 1, ModifiedS: 1}
		if err := f.Ix.Put(fi); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}
	for i := 0; i < 300; i++ {
		put(fmt.Sprintf("d/f%03d.txt", i))
	}
	for i := 0; i < 700; i++ {
		put(fmt.Sprintf("other/f%03d.txt", i))
	}

	live, delCount := deletionWeight(f, []string{"d"})
	if live != 1000 {
		t.Fatalf("live = %d, want 1000", live)
	}
	if delCount != 301 {
		t.Fatalf("weighted deletions = %d, want 301 (the directory plus its 300 files)", delCount)
	}

	// The unweighted count is the old behaviour and must NOT trip the guard;
	// the weighted count must.
	if guardBlocksDeletes(live, 1, 25, 0) {
		t.Fatal("a single unweighted directory deletion must not trip the guard")
	}
	if !guardBlocksDeletes(live, delCount, 25, 0) {
		t.Fatalf("%d deletions of %d live entries (30%%) must trip the 25%% guard", delCount, live)
	}

	// No pending deletions: the weights stay at zero and nothing trips.
	live, delCount = deletionWeight(f, nil)
	if live != 1000 || delCount != 0 {
		t.Fatalf("deletionWeight(nil) = (%d, %d), want (1000, 0)", live, delCount)
	}
	if guardBlocksDeletes(live, delCount, 25, 0) {
		t.Fatal("an empty delete pass must not trip the guard")
	}
}
