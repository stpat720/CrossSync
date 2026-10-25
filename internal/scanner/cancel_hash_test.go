package scanner

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"crosssync/internal/index"
)

// TestScanCancelledMidHashIsReported pins the hardest half of the pause
// contract: a pause that lands while files are being HASHED must end the pass as
// cancelled.
//
// Why it matters operationally: hashPending used to answer "no changes, no
// error" when it noticed the cancellation probe. scanScoped could not tell that
// apart from "this tree has no changes", so the pass carried on through the
// deletion pass and the settle write, reported itself as a COMPLETED scan, and
// the operator saw hash_done jump from a few files to the full total while the
// folder was paused — the verified rescan they asked for was silently consumed
// and the index kept the content they were trying to re-check.
//
// A paused folder must read no disk and apply no change, so the probe has to be
// reported, not swallowed: ErrScanCancelled is the signal every caller keys on.
func TestScanCancelledMidHashIsReported(t *testing.T) {
	root := t.TempDir()
	ix, err := index.Open(filepath.Join(t.TempDir(), "index.db"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ix.Close() }()

	const files = 200
	for i := 0; i < files; i++ {
		p := filepath.Join(root, fmt.Sprintf("f%03d.bin", i))
		// Real content, not a single byte: these files are queued for hashing.
		if err := os.WriteFile(p, []byte(strings.Repeat("x", 32*1024)), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	sc := New(root, ix, nil)
	sc.SetSettleScans(1) // no settle gate: this test is about cancellation only

	var cancelled atomic.Bool
	sc.SetCancel(cancelled.Load)

	// The hash phase emits one progress event per file, from the worker, right
	// before it reads that file. Flipping the probe there lands the pause inside
	// the hash phase every run — no clock racing.
	changes, err := sc.ScanWithProgress(func(p Progress) {
		if p.Phase == "hashing" && p.HashDone > 0 {
			cancelled.Store(true)
		}
	})
	if !errors.Is(err, ErrScanCancelled) {
		t.Fatalf("scan cancelled during hashing returned err = %v (%d changes), want ErrScanCancelled", err, len(changes))
	}
	if len(changes) != 0 {
		t.Fatalf("cancelled scan returned %d changes, want none", len(changes))
	}

	// Nothing may have been written to the index by the abandoned pass: not an
	// entry, not a settling row, not a half-confirmed deletion.
	entries := 0
	if err := ix.ListAll(func(*index.FileInfo) error { entries++; return nil }); err != nil {
		t.Fatal(err)
	}
	if entries != 0 {
		t.Fatalf("index has %d entries after a cancelled scan, want 0", entries)
	}
	if rows, err := ix.SettleList(); err != nil || len(rows) != 0 {
		t.Fatalf("settling table has %d rows after a cancelled scan (err %v), want 0", len(rows), err)
	}
	if rows, err := ix.MissingList(); err != nil || len(rows) != 0 {
		t.Fatalf("missing table has %d rows after a cancelled scan (err %v), want 0", len(rows), err)
	}

	// The very next pass, with the pause lifted, must find the whole tree: a
	// cancelled pass must leave no state that makes the tree look unchanged.
	cancelled.Store(false)
	changes, err = sc.Scan()
	if err != nil {
		t.Fatalf("scan after resume: %v", err)
	}
	if len(changes) != files {
		t.Fatalf("resumed scan found %d changes, want %d", len(changes), files)
	}
}
