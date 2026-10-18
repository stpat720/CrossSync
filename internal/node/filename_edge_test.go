package node

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/index"
)

// TestFilenameEdgeCases verifies that sync converges for filenames and
// directory layouts that are legal on every supported OS but tricky in
// practice: unicode names, leading/trailing/multiple spaces, a dot-leading
// hidden file, a deeply nested path, a zero-byte file, and an empty
// subdirectory (which must not appear as a synced file, but the directory
// itself must arrive).
func TestFilenameEdgeCases(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	// rel -> expected content (unicode + tricky names, legal on all OS)
	decoded := map[string]string{
		"café üñí.txt":        "unicode dir + name",
		"\U0001F600 emoji.txt": "emoji file",
		"spa c y.txt":          "has spaces",
		"  leading-trailing.txt": "trims badly elsewhere",
		"trailing   .txt":     "trailing spaces before ext",
		"deep/a/b/c/d/e/f/g/h/i/j/file.txt": "very deep path",
		"empty.txt":           "",
		"zero.dat":            "",
		".hidden-file":        "dotfile",
		"a.b.c.d.txt":         "many dots",
		"UPPER.CASE.TXT":      "case",
	}
	for rel, content := range decoded {
		chunkWrite(t, rootA, rel, content)
	}
	// An empty directory that must arrive as a directory, not a file.
	if err := os.MkdirAll(filepath.Join(rootA, "empty-dir"), 0o755); err != nil {
		t.Fatal(err)
	}

	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}

	// Every file A has must exist on B with identical content.
	var count int
	err := filepath.WalkDir(rootA, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		count++
		rel, err := filepath.Rel(rootA, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		want, ok := decoded[rel]
		if !ok {
			t.Errorf("unexpected file on A: %q", rel)
			return nil
		}
		gotData, err := os.ReadFile(filepath.Join(rootB, filepath.FromSlash(rel)))
		if err != nil {
			t.Errorf("B missing %q: %v", rel, err)
			return nil
		}
		if string(gotData) != want {
			t.Errorf("B[%q] = %q, want %q", rel, gotData, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("no files walked")
	}
	// B's peer view must contain exactly A's indexed entries (files AND
	// directories — the engine indexes both).
	wantView := 0
	if err := a.Folders["data"].Ix.ListAll(func(fi *index.FileInfo) error {
		wantView++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := b.Folders["data"].Engine.PeerCount(a.ID); got != wantView {
		t.Fatalf("B's peer view of A = %d, want %d indexed entries", got, wantView)
	}
	// The empty dir must exist on B.
	if _, err := os.Stat(filepath.Join(rootB, "empty-dir")); err != nil {
		t.Fatalf("empty dir did not arrive on B: %v", err)
	}
}
