package scanner

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/ignore"
)

// The scan cadence must never be a safety property. These tests drive the clock
// directly to prove that two observations that happen "instantly" (as they will
// when scans are triggered by change events) cannot promote a file that is
// still being written: eligibility needs a QUIET WINDOW, not just a count.

// TestSettleQuietWindowHoldsDespiteTwoObservations: two scans 1s apart agree on
// (size, mtime) but the 10s window has not elapsed, so the file stays withheld.
// It becomes eligible only once the window passes with the stat unchanged.
func TestSettleQuietWindowHoldsDespiteTwoObservations(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "copy.bin", "partial")

	s := New(root, ix, &ignore.Matcher{})
	clock := int64(1000)
	s.SetClock(func() int64 { return clock })
	s.SetDeletionScans(1)
	// settleScans defaults to 2; the quiet window defaults to 10s.

	// t+0: first observation, held.
	if changes, err := s.Scan(); err != nil {
		t.Fatal(err)
	} else if kindOf(changes, "copy.bin") != -1 {
		t.Fatalf("first observation must be held back, got %+v", changes)
	}

	// t+1: second observation, identical stat — the old count-only gate would
	// have released it here. The window must keep it.
	clock += 1
	if changes, err := s.Scan(); err != nil {
		t.Fatal(err)
	} else if kindOf(changes, "copy.bin") != -1 {
		t.Fatalf("two rapid observations must NOT release the file, got %+v", changes)
	}

	// t+5: still inside the window.
	clock += 4
	if changes, err := s.Scan(); err != nil {
		t.Fatal(err)
	} else if kindOf(changes, "copy.bin") != -1 {
		t.Fatalf("file released before the quiet window elapsed: %+v", changes)
	}

	// t+11: window elapsed with the same stat — now it is safe.
	clock += 6
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "copy.bin") != Added {
		t.Fatalf("file should be eligible after the quiet window, got %+v", changes)
	}
}

// TestSettleWindowRestartsWhileFileKeepsGrowing: a copy that keeps growing is
// never released, no matter how much wall-clock time passes.
func TestSettleWindowRestartsWhileFileKeepsGrowing(t *testing.T) {
	root, ix := setup(t)
	p := filepath.Join(root, "growing.bin")
	if err := os.WriteFile(p, []byte("chunk1"), 0o644); err != nil {
		t.Fatal(err)
	}

	s := New(root, ix, &ignore.Matcher{})
	clock := int64(2000)
	s.SetClock(func() int64 { return clock })
	s.SetDeletionScans(1)
	s.SetSettleSeconds(5)

	appendChunk := func(n int) {
		f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		if _, err := f.Write([]byte("chunk")); err != nil {
			t.Fatal(err)
		}
		// Distinct mtime per write: this filesystem rounds timestamps, so the
		// size change is what the scanner must notice.
		_ = n
	}

	for i := 0; i < 8; i++ {
		clock += 3 // 24s total: far beyond the 5s window
		changes, err := s.Scan()
		if err != nil {
			t.Fatal(err)
		}
		if kindOf(changes, "growing.bin") != -1 {
			t.Fatalf("iteration %d released a file that is still growing: %+v", i, changes)
		}
		appendChunk(i)
	}

	// The writer stops. The final append reset the run, so the gate needs a
	// fresh observation AND the window: one scan to see the new stat, then the
	// window elapses with it unchanged, then it syncs.
	clock += 1
	if changes, err := s.Scan(); err != nil {
		t.Fatal(err)
	} else if kindOf(changes, "growing.bin") != -1 {
		t.Fatalf("settled file released before its window: %+v", changes)
	}
	clock += 6
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "growing.bin") != Added {
		t.Fatalf("a finished copy should sync after the window, got %+v", changes)
	}
}

// TestSettleGateOffSkipsTheWindow: settle_scans 1 is documented as pre-gate
// behaviour, so the quiet window must not apply either.
func TestSettleGateOffSkipsTheWindow(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "app.log", "line")

	s := New(root, ix, &ignore.Matcher{})
	clock := int64(3000)
	s.SetClock(func() int64 { return clock })
	s.SetSettleScans(1)
	s.SetDeletionScans(1)

	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "app.log") != Added {
		t.Fatalf("gate-off scanner must settle on first sight, got %+v", changes)
	}
}
