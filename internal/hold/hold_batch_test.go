package hold

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBatchDefersTheManifestWrite pins the fix for the quadratic hold path: a
// mass deletion moves thousands of copies into the hold area, and each Move used
// to rewrite AND fsync the entire manifest — measured ~2 s per held file on the
// array, so a 14k backlog crawled at two files a second while the peer's counts
// still showed everything as pending.
//
// Inside a batch the copies must still move immediately (the hold area is the
// preservation mechanism), only the manifest write is deferred to EndBatch.
func TestBatchDefersTheManifestWrite(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	mk := func(rel string) {
		t.Helper()
		abs := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(abs, []byte("payload-"+rel), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	paths := []string{"a/one.bin", "a/two.bin", "b/three.bin"}
	for _, p := range paths {
		mk(p)
	}

	s.BeginBatch()
	for _, p := range paths {
		if err := s.Move(p); err != nil {
			t.Fatalf("move %s: %v", p, err)
		}
		// The copy is out of the synced tree at once, whatever the manifest says.
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(p))); !os.IsNotExist(err) {
			t.Fatalf("%s still in the tree after Move: %v", p, err)
		}
	}
	if raw, err := os.ReadFile(s.manifestPath()); err == nil {
		for _, p := range paths {
			if strings.Contains(string(raw), p) {
				t.Fatalf("manifest written during the batch: %q already lists %s", raw, p)
			}
		}
	}

	if err := s.EndBatch(); err != nil {
		t.Fatalf("end batch: %v", err)
	}
	raw, err := os.ReadFile(s.manifestPath())
	if err != nil {
		t.Fatalf("manifest missing after EndBatch: %v", err)
	}
	for _, p := range paths {
		if !strings.Contains(string(raw), p) {
			t.Fatalf("manifest does not list %s after EndBatch: %q", p, raw)
		}
	}
	list, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != len(paths) {
		t.Fatalf("List reports %d held copies, want %d", len(list), len(paths))
	}
}

// TestFlushWritesABatchThatNeverEnded: a shutdown in the middle of a batch must
// still persist what was moved.
func TestFlushWritesABatchThatNeverEnded(t *testing.T) {
	root := t.TempDir()
	s := New(root)
	abs := filepath.Join(root, "x.bin")
	if err := os.WriteFile(abs, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.BeginBatch()
	if err := s.Move("x.bin"); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	raw, err := os.ReadFile(s.manifestPath())
	if err != nil {
		t.Fatalf("manifest missing after Flush: %v", err)
	}
	if !strings.Contains(string(raw), "x.bin") {
		t.Fatalf("Flush did not persist the held copy: %q", raw)
	}
}
