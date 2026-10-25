package scanner

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"crosssync/internal/index"
)

// TestScanCancelsPromptlyOnPause: a paused folder must stop a scan that is
// already running, and the cancelled pass must return no changes at all.
//
// Why this matters operationally: pausing previously took the folder lock, which
// a scan holds for its whole pass, so a pause request during a large verification
// pass hung until the request timed out — while the disk kept being read. The
// probe has to be checked often enough that a pause takes effect within a file or
// so, and the partial result must never reach the index: a half-walk that saw
// only part of the tree looks exactly like mass deletion.
func TestScanCancelsPromptlyOnPause(t *testing.T) {
	root := t.TempDir()
	ix, err := index.Open(filepath.Join(t.TempDir(), "index.db"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ix.Close() }()
	for i := 0; i < 400; i++ {
		p := filepath.Join(root, fmt.Sprintf("f%04d.txt", i))
		if err := os.WriteFile(p, []byte("payload"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sc := New(root, ix, nil)
	sc.SetSettleScans(1) // no settle gate: this is about cancellation only

	var paused atomic.Bool
	sc.SetCancel(paused.Load)

	// Cancel once the walk is a way in: progress is emitted every 64 entries, so
	// waiting for an event count would outlast a 400-file tree.
	changes, err := sc.ScanWithProgress(func(p Progress) {
		if p.Walked >= 128 {
			paused.Store(true)
		}
	})
	if !errors.Is(err, ErrScanCancelled) {
		t.Fatalf("err = %v, want ErrScanCancelled", err)
	}
	if changes != nil {
		t.Fatalf("cancelled scan returned %d changes, want none", len(changes))
	}

	// Nothing may have been written to the index by the abandoned pass.
	entries := 0
	if err := ix.ListAll(func(*index.FileInfo) error { entries++; return nil }); err != nil {
		t.Fatal(err)
	}
	if entries != 0 {
		t.Fatalf("index has %d entries after a cancelled scan, want 0", entries)
	}

	// Resuming scans normally and indexes everything.
	paused.Store(false)
	changes, err = sc.Scan()
	if err != nil {
		t.Fatalf("scan after resume: %v", err)
	}
	if len(changes) != 400 {
		t.Fatalf("resumed scan found %d changes, want 400", len(changes))
	}
}

// TestCancelFalseNeverInterferes: an installed probe that always returns false
// must not change scan behaviour at all.
func TestCancelFalseNeverInterferes(t *testing.T) {
	root := t.TempDir()
	ix, err := index.Open(filepath.Join(t.TempDir(), "index.db"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ix.Close() }()
	for i := 0; i < 20; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%02d.txt", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sc := New(root, ix, nil)
	sc.SetSettleScans(1)
	sc.SetCancel(func() bool { return false })
	changes, err := sc.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 20 {
		t.Fatalf("got %d changes, want 20", len(changes))
	}
}
