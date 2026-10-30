package scanner

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/ignore"
	"crosssync/internal/index"
)

// TestScopedScanPairsARenameWithTheVanishedOldPath: a watcher reports the new
// name the instant a file is renamed, while the old name is only confirmed gone
// by a later full walk plus the deletion gate. The scoped pass therefore used to
// publish the pair as add + delete, and the peer re-wrote every byte locally
// instead of moving the file — measured as 29.5 MiB of needless writes for one
// folder rename.
//
// A scoped pass may pair the two as long as it PROVES the old path is gone with a
// targeted stat and exactly one file added this pass has its content.
func TestScopedScanPairsARenameWithTheVanishedOldPath(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "dir/old.bin", "payload that must not be copied again")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)
	s.SetDeletionScans(2)
	clock := int64(1000)
	s.SetClock(func() int64 { return clock })

	apply := func(changes []Change) {
		t.Helper()
		for _, c := range changes {
			if c.Info == nil {
				continue
			}
			switch c.Kind {
			case Added, Modified:
				if err := ix.Put(c.Info); err != nil {
					t.Fatal(err)
				}
			case Renamed:
				if err := ix.Put(&index.FileInfo{Name: c.From, Type: index.TypeFile, Deleted: true}); err != nil {
					t.Fatal(err)
				}
				if err := ix.Put(c.Info); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	first, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	apply(first)
	if _, ok, _ := ix.Get("dir/old.bin"); !ok {
		t.Fatal("setup: old.bin not indexed")
	}

	// The rename a user makes over SMB: the old name vanishes, the new one appears.
	if err := os.Rename(filepath.Join(root, "dir", "old.bin"), filepath.Join(root, "dir", "new.bin")); err != nil {
		t.Fatal(err)
	}

	clock += 12
	scoped, err := s.ScanScope([]string{"dir/new.bin"})
	if err != nil {
		t.Fatal(err)
	}
	var renamed *Change
	for i := range scoped {
		if scoped[i].Kind == Renamed {
			renamed = &scoped[i]
		}
		if scoped[i].Kind == Added && scoped[i].Info != nil && scoped[i].Info.Name == "dir/new.bin" {
			t.Fatalf("scoped pass published the new name as a plain add, so the peer will copy instead of move: %+v", scoped)
		}
	}
	if renamed == nil {
		t.Fatalf("scoped pass did not pair the rename: %+v", scoped)
	}
	if renamed.From != "dir/old.bin" || renamed.Info == nil || renamed.Info.Name != "dir/new.bin" {
		t.Fatalf("paired the wrong way: from=%q to=%v", renamed.From, renamed.Info)
	}
}
