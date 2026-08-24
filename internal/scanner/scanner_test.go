package scanner

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/hash"
	"crosssync/internal/ignore"
	"crosssync/internal/index"
	"crosssync/internal/staging"
)

func setup(t *testing.T) (root string, ix *index.Index) {
	t.Helper()
	root = t.TempDir()
	ix, err := index.Open(filepath.Join(root, ".sfx-index", "folder.db"), "test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ix.Close() })
	// Create the reserved dir so the walk sees it and must skip it.
	if err := os.MkdirAll(filepath.Join(root, staging.ReservedDir), 0o755); err != nil {
		t.Fatal(err)
	}
	return root, ix
}

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func kindOf(changes []Change, rel string) ChangeKind {
	for _, c := range changes {
		if c.Info != nil && c.Info.Name == rel {
			return c.Kind
		}
	}
	return -1
}

func TestScanAddsAndSkipsUnchanged(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "docs/a.txt", "hello")
	writeFile(t, root, "docs/b.txt", "world")
	writeFile(t, root, "big.dat", string(make([]byte, 300*1024)))

	s := New(root, ix, &ignore.Matcher{})
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "docs/a.txt") != Added || kindOf(changes, "docs/b.txt") != Added || kindOf(changes, "big.dat") != Added {
		t.Fatalf("expected Added for all files, got %+v", changes)
	}
	// big.dat (300 KiB) must have been block-hashed.
	for _, c := range changes {
		if c.Info.Name == "big.dat" {
			if len(c.Info.Blocks) < 2 {
				t.Fatalf("big.dat should have >= 2 blocks, got %d", len(c.Info.Blocks))
			}
			if c.Info.BlockSize != 128*1024 {
				t.Fatalf("big.dat block size = %d, want 128KiB", c.Info.BlockSize)
			}
		}
	}

	// Apply to index, then rescan: must be all unchanged (no hashing).
	for _, c := range changes {
		if c.Kind == Added {
			if err := ix.Put(c.Info); err != nil {
				t.Fatal(err)
			}
		}
	}
	changes, err = s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		t.Fatalf("rescan should be empty, got %+v", c)
	}
}

func TestScanDetectsModifyAndDelete(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "a.txt", "one")
	s := New(root, ix, &ignore.Matcher{})
	changes, _ := s.Scan()
	for _, c := range changes {
		if c.Kind == Added {
			ix.Put(c.Info)
		}
	}

	// Modify content (different size).
	writeFile(t, root, "a.txt", "one two three")
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "a.txt") != Modified {
		t.Fatalf("expected Modified, got %+v", changes)
	}
	// Apply, then delete.
	for _, c := range changes {
		if c.Kind == Modified {
			ix.Put(c.Info)
		}
	}
	os.Remove(filepath.Join(root, "a.txt"))
	changes, err = s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "a.txt") != Deleted {
		t.Fatalf("expected Deleted, got %+v", changes)
	}
}

func TestScanDetectsRename(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "a.txt", "identical content")
	s := New(root, ix, &ignore.Matcher{})
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.Kind == Added {
			if err := ix.Put(c.Info); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Rename a.txt -> b.txt (content unchanged). The scan must report a
	// single Renamed change, not a delete+add pair.
	if err := os.Rename(filepath.Join(root, "a.txt"), filepath.Join(root, "b.txt")); err != nil {
		t.Fatal(err)
	}
	changes, err = s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected exactly 1 rename change, got %+v", changes)
	}
	c := changes[0]
	if c.Kind != Renamed || c.From != "a.txt" || c.Info == nil || c.Info.Name != "b.txt" {
		t.Fatalf("expected Renamed a.txt -> b.txt, got kind=%v from=%q info=%+v", c.Kind, c.From, c.Info)
	}
}

func TestScanRenameAmbiguousFallsBack(t *testing.T) {
	root, ix := setup(t)
	// One deleted source with two same-content additions is ambiguous (1
	// source, 2 targets), so it must stay a delete+add pair, never a guess.
	writeFile(t, root, "a.txt", "same")
	s := New(root, ix, &ignore.Matcher{})
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.Kind == Added {
			if err := ix.Put(c.Info); err != nil {
				t.Fatal(err)
			}
		}
	}
	// a.txt deleted, replaced by TWO new identical-content files.
	os.Remove(filepath.Join(root, "a.txt"))
	writeFile(t, root, "c.txt", "same")
	writeFile(t, root, "d.txt", "same")
	changes, err = s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "a.txt") != Deleted {
		t.Fatalf("a.txt should be Deleted (ambiguous rename), got %+v", changes)
	}
	if kindOf(changes, "c.txt") != Added || kindOf(changes, "d.txt") != Added {
		t.Fatalf("c.txt/d.txt should be Added, got %+v", changes)
	}
	for _, c := range changes {
		if c.Kind == Renamed {
			t.Fatalf("ambiguous pairing must not produce a rename, got %+v", c)
		}
	}
}

func TestScanIgnoresByRule(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "keep.txt", "keep")
	writeFile(t, root, "junk.tmp", "junk")
	writeFile(t, root, ".venv/lib/x.py", "py")

	m, err := ignore.Parse([]string{"*.tmp", ".venv/"})
	if err != nil {
		t.Fatal(err)
	}
	s := New(root, ix, m)
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "junk.tmp") != Skipped {
		t.Fatalf("junk.tmp should be Skipped, got %+v", changes)
	}
	if kindOf(changes, ".venv/lib/x.py") != -1 {
		t.Fatalf(".venv tree should be pruned entirely, got %+v", changes)
	}
	if kindOf(changes, "keep.txt") != Added {
		t.Fatalf("keep.txt should be Added, got %+v", changes)
	}
}

func TestScanSkipsReservedNamespace(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, staging.ReservedDir+"/in-flight.part", "partial")
	writeFile(t, root, "real.txt", "real")
	s := New(root, ix, &ignore.Matcher{})
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.Info.Name == staging.ReservedDir+"/in-flight.part" {
			t.Fatalf("reserved temp must never be indexed: %+v", c)
		}
	}
	if kindOf(changes, "real.txt") != Added {
		t.Fatalf("real.txt should be Added, got %+v", changes)
	}
}

func TestScanHashesMatchContent(t *testing.T) {
	root, ix := setup(t)
	content := "deterministic content"
	writeFile(t, root, "f.txt", content)
	s := New(root, ix, &ignore.Matcher{})
	changes, _ := s.Scan()
	for _, c := range changes {
		if c.Info.Name == "f.txt" {
			if len(c.Info.Blocks) != 1 {
				t.Fatalf("expected 1 block")
			}
			if !bytesEqual(c.Info.Blocks[0], hash.HashBytes([]byte(content))) {
				t.Fatalf("hash does not match content")
			}
		}
	}
}

func bytesEqual(a, b []byte) bool {
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
