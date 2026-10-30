package scanner

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/ignore"
	"crosssync/internal/index"
)

// TestFuseHiddenIsNeverIndexed: shfs leaves a .fuse_hidden name behind when a file
// is deleted while a handle is still open. It is not data, and indexing it made
// the daemon sync the deleted file's bytes to the peer under that name — a 1.8 GB
// "local add" the operator never created.
func TestFuseHiddenIsNeverIndexed(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "26013/keep.bin", "real payload")
	writeFile(t, root, "26013/.fuse_hidden002e778400000154", "the deleted file, still open")
	writeFile(t, root, ".smbdeleteAAA123", "samba staging")
	writeFile(t, root, ".nfs0a1b2c", "nfs silly rename")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)
	s.SetDeletionScans(1)

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
	for _, c := range changes {
		if c.Info == nil {
			continue
		}
		switch c.Info.Name {
		case "26013/.fuse_hidden002e778400000154", ".smbdeleteAAA123", ".nfs0a1b2c":
			t.Fatalf("transient name reported as a change: %s (%v)", c.Info.Name, c.Kind)
		}
	}
	// The real file is still indexed: the rule must not swallow ordinary names.
	names := map[string]bool{}
	if err := ix.List(func(fi *index.FileInfo) error {
		if !fi.Deleted {
			names[fi.Name] = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !names["26013/keep.bin"] {
		t.Fatalf("real file missing from the index: %v", names)
	}
	for _, bad := range []string{"26013/.fuse_hidden002e778400000154", ".smbdeleteAAA123", ".nfs0a1b2c"} {
		if names[bad] {
			t.Fatalf("transient name was indexed: %s", bad)
		}
	}
	_ = os.Remove(filepath.Join(root, "26013", "keep.bin"))
}
