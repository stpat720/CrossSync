package scanner

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"crosssync/internal/ignore"
	"crosssync/internal/index"
)

// TestCaseOnlyRenameIsARename: renaming photo.max -> PHOTO.max on a
// case-insensitive share (Windows, SMB) changes only the case. The scanner must
// see a rename, not a delete plus an unrelated add, or the peer would destroy its
// copy and re-fetch the same bytes under a name it already had.
func TestCaseOnlyRenameIsARename(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "photo.max", "the same bytes")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)
	s.SetDeletionScans(1)
	s.SetSettleSeconds(0)

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

	from := filepath.Join(root, "photo.max")
	to := filepath.Join(root, "PHOTO.max")
	if err := os.Rename(from, to); err != nil {
		t.Skipf("this filesystem cannot rename case-only: %v", err)
	}
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	renamed := false
	for _, c := range changes {
		if c.Kind == Renamed && c.From == "photo.max" && c.Info != nil && c.Info.Name == "PHOTO.max" {
			renamed = true
		}
		if c.Kind == Added && c.Info != nil && c.Info.Name == "PHOTO.max" {
			t.Fatalf("a case-only rename was published as a plain add, so the peer re-fetches the bytes: %+v", changes)
		}
	}
	if !renamed {
		t.Fatalf("expected a case-only rename pairing photo.max -> PHOTO.max, got %+v", changes)
	}
}

// TestSymlinkIsIndexedWithoutHashing: a symlink carries no content of its own, so
// it must be recorded as a symlink and never hashed (hashing would read the
// TARGET, reporting content the link does not own, and a dangling link would
// abort the file).
func TestSymlinkIsIndexedWithoutHashing(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "target.bin", "target content")
	target := filepath.Join(root, "target.bin")
	link := filepath.Join(root, "link.bin")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("%s cannot create symlinks here: %v", runtime.GOOS, err)
	}

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)
	s.SetDeletionScans(1)
	s.SetSettleSeconds(0)
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	var linkChange *Change
	for i := range changes {
		if changes[i].Info != nil && changes[i].Info.Name == "link.bin" {
			linkChange = &changes[i]
		}
	}
	if linkChange == nil {
		t.Fatalf("symlink not reported at all: %+v", changes)
	}
	if linkChange.Info.Type != index.TypeSymlink {
		t.Fatalf("symlink indexed as type %v, want TypeSymlink", linkChange.Info.Type)
	}
	if len(linkChange.Info.Blocks) != 0 {
		t.Fatalf("symlink was hashed (%d blocks): it must never carry the target's content", len(linkChange.Info.Blocks))
	}
}

// TestIgnoredBytesAreCounted pins the accounting for names the walk skips: they
// are not data, but they do occupy the share. 1.8 GB of a deleted-but-open file
// once sat in a share with nothing anywhere reporting it.
func TestIgnoredBytesAreCounted(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "keep.bin", "real")
	writeFile(t, root, "sub/.fuse_hidden00aa0001", "0123456789")

	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleScans(1)
	s.SetDeletionScans(1)
	s.SetSettleSeconds(0)

	var last Progress
	_, err := s.ScanWithProgress(func(p Progress) { last = p })
	if err != nil {
		t.Fatal(err)
	}
	if last.IgnoredFiles != 1 {
		t.Fatalf("IgnoredFiles = %d, want 1", last.IgnoredFiles)
	}
	if last.IgnoredBytes != 10 {
		t.Fatalf("IgnoredBytes = %d, want 10", last.IgnoredBytes)
	}
}
