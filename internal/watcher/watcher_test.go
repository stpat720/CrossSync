package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// drain waits for a change batch containing rel (or all of rels) within a
// timeout.
func drain(t *testing.T, w *Watcher, want ...string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case batch := <-w.Changes():
			seen := map[string]bool{}
			for _, b := range batch {
				seen[b] = true
			}
			all := true
			for _, x := range want {
				if !seen[x] {
					all = false
				}
			}
			if all {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %v in watcher events", want)
		}
	}
}

func TestWatcherDetectsFileChanges(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "docs")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := New(root, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if w.WatchCount() < 2 {
		t.Fatalf("expected watches on root+docs, got %d", w.WatchCount())
	}

	// New file in the root.
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	drain(t, w, "a.txt")

	// Change inside a subdirectory.
	if err := os.WriteFile(filepath.Join(sub, "b.txt"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	drain(t, w, "docs/b.txt")

	// New directory is watched automatically (deeper file is seen).
	nested := filepath.Join(sub, "deep")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	// Give the watcher a moment to register the new directory.
	time.Sleep(300 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(nested, "c.txt"), []byte("z"), 0o644); err != nil {
		t.Fatal(err)
	}
	drain(t, w, "docs/deep/c.txt")
}

func TestWatcherSuppressesReserved(t *testing.T) {
	root := t.TempDir()
	w, err := New(root, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Our own writes: a staging temp and a version archive.
	os.MkdirAll(filepath.Join(root, ".sfx-tmp"), 0o755)
	os.WriteFile(filepath.Join(root, ".sfx-tmp", ".sfx-abc123.part"), []byte("x"), 0o644)
	os.MkdirAll(filepath.Join(root, ".sfx-versions", "trash"), 0o755)
	os.WriteFile(filepath.Join(root, ".sfx-versions", "trash", "old.txt"), []byte("old"), 0o644)
	// And a reserved file at the root.
	os.WriteFile(filepath.Join(root, ".sfx-hidden"), []byte("x"), 0o644)

	select {
	case batch := <-w.Changes():
		t.Fatalf("reserved writes must not emit events, got %v", batch)
	case <-time.After(1500 * time.Millisecond):
		// Correct: nothing emitted.
	}
}
