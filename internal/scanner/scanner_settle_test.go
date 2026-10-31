package scanner

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"crosssync/internal/hash"
	"crosssync/internal/ignore"
	"crosssync/internal/index"
)

// gateScan runs one scan and applies every Added/Modified change to the index,
// mirroring what the engine does with them, so a test can drive the gate
// through settle → apply → re-open.
func gateScan(t *testing.T, s *Scanner, ix *index.Index) []Change {
	t.Helper()
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.Kind == Added || c.Kind == Modified {
			if err := ix.Put(c.Info); err != nil {
				t.Fatal(err)
			}
		}
	}
	return changes
}

// inFlightFor returns the in-flight entry for rel, or nil.
func inFlightFor(list []InFlight, rel string) *InFlight {
	for i := range list {
		if list[i].Name == rel {
			return &list[i]
		}
	}
	return nil
}

// TestGateHoldsNewFileThenSettles is the core gate contract: a changed file is
// not hashed, versioned or advertised until its (size, mtime) has been observed
// unchanged for the configured number of consecutive scans.
func TestGateHoldsNewFileThenSettles(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "a.txt", "hello")
	s := New(root, ix, &ignore.Matcher{}) // gate on at the default 2
	s.SetSettleSeconds(0)                 // count-only: the quiet window has its own tests

	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Fatalf("first scan of a new file emitted %d change(s), want 0: %+v", len(changes), changes)
	}
	ifl := s.LastInFlight()
	f := inFlightFor(ifl, "a.txt")
	if len(ifl) != 1 || f == nil {
		t.Fatalf("LastInFlight = %+v, want exactly one entry for a.txt", ifl)
	}
	if f.Scans != 1 || f.Size != 5 || f.Since == 0 {
		t.Fatalf("in-flight entry = %+v, want Scans 1, Size 5, non-zero Since", *f)
	}
	if _, ok, _ := ix.Get("a.txt"); ok {
		t.Fatal("a held-back file must not be indexed — that is what keeps it off the wire")
	}

	changes = gateScan(t, s, ix)
	if kindOf(changes, "a.txt") != Added {
		t.Fatalf("second scan = %+v, want a.txt Added", changes)
	}
	for _, c := range changes {
		if c.Info.Name == "a.txt" {
			if len(c.Info.Blocks) != 1 || !bytesEqual(c.Info.Blocks[0], hash.HashBytes([]byte("hello"))) {
				t.Fatalf("a.txt blocks = %x, want the hash of \"hello\"", c.Info.Blocks)
			}
		}
	}
	if got := s.LastInFlight(); len(got) != 0 {
		t.Fatalf("LastInFlight after settling = %+v, want empty", got)
	}
	// The settling row is gone too: no state leaks for a settled file.
	states, err := ix.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 0 {
		t.Fatalf("settling table = %+v, want empty after the file settled", states)
	}
}

// TestGateReopensOnSizeChange verifies a settled file re-enters the gate on a
// size change and needs a fresh run of stable scans before it is modified.
func TestGateReopensOnSizeChange(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "a.txt", "one")
	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleSeconds(0) // count-only: the quiet window has its own tests
	// This test is about rename pairing against an in-flight target, not about
	// the two-pass deletion gate: pin the gate to one pass so the assertion on
	// gone.txt cannot depend on how many scans happen to have seen it missing.
	s.SetDeletionScans(1)
	gateScan(t, s, ix) // held
	gateScan(t, s, ix) // Added, applied

	writeFile(t, root, "a.txt", "one two three")
	changes := gateScan(t, s, ix)
	if len(changes) != 0 {
		t.Fatalf("scan of a re-opened file = %+v, want no changes", changes)
	}
	if f := inFlightFor(s.LastInFlight(), "a.txt"); f == nil || f.Scans != 1 {
		t.Fatalf("LastInFlight = %+v, want a.txt at Scans 1", s.LastInFlight())
	}
	changes = gateScan(t, s, ix)
	if kindOf(changes, "a.txt") != Modified {
		t.Fatalf("scan after the run re-settled = %+v, want a.txt Modified", changes)
	}
	if fi, ok, _ := ix.Get("a.txt"); !ok || fi.Size != 13 {
		t.Fatalf("indexed a.txt = %+v (ok=%v), want size 13", fi, ok)
	}
}

// TestGateReopensOnMtimeOnlyChange proves the comparison covers all three of
// size, modified_s and modified_ns: a same-size, mtime-only change must reopen
// the gate rather than pass as "unchanged content".
func TestGateReopensOnMtimeOnlyChange(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "a.txt", "same size")
	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleSeconds(0) // count-only: the quiet window has its own tests
	gateScan(t, s, ix) // held
	gateScan(t, s, ix) // Added, applied

	p := filepath.Join(root, "a.txt")
	mt := time.Now().Add(90 * time.Second)
	if err := os.Chtimes(p, mt, mt); err != nil {
		t.Fatal(err)
	}
	changes := gateScan(t, s, ix)
	if len(changes) != 0 {
		t.Fatalf("scan after an mtime-only change = %+v, want no changes", changes)
	}
	if f := inFlightFor(s.LastInFlight(), "a.txt"); f == nil || f.Scans != 1 {
		t.Fatalf("LastInFlight = %+v, want a.txt at Scans 1 (mtime-only change must reopen the gate)", s.LastInFlight())
	}
	changes = gateScan(t, s, ix)
	if kindOf(changes, "a.txt") != Modified {
		t.Fatalf("scan after the run re-settled = %+v, want a.txt Modified", changes)
	}
}

// TestGateDisabledAtOne verifies settle_scans 1 restores the pre-gate
// behaviour: settle on first sight.
func TestGateDisabledAtOne(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "a.txt", "hello")
	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleSeconds(0) // count-only: the quiet window has its own tests
	s.SetSettleScans(1)
	s.SetSettleSeconds(0)
	s.SetDeletionScans(1) // this test is about the add-side gate only

	changes := gateScan(t, s, ix)
	if kindOf(changes, "a.txt") != Added {
		t.Fatalf("first scan with the gate off = %+v, want a.txt Added", changes)
	}
	if got := s.LastInFlight(); len(got) != 0 {
		t.Fatalf("LastInFlight with the gate off = %+v, want empty", got)
	}
}

// TestGateBypass verifies the rebuildFromFS contract: a bypassed scan settles
// on first sight (a quarantined index has no prior content to contradict).
func TestGateBypass(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "a.txt", "hello")
	writeFile(t, root, "docs/b.txt", "world")
	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleSeconds(0) // count-only: the quiet window has its own tests
	s.SetGateBypass(true)

	changes := gateScan(t, s, ix)
	if kindOf(changes, "a.txt") != Added || kindOf(changes, "docs/b.txt") != Added {
		t.Fatalf("bypassed scan = %+v, want both files Added", changes)
	}
	if got := s.LastInFlight(); len(got) != 0 {
		t.Fatalf("LastInFlight after a bypassed scan = %+v, want empty", got)
	}

	// The bypass is scoped to that scan: the gate is back on afterwards.
	s.SetGateBypass(false)
	writeFile(t, root, "c.txt", "later")
	if changes := gateScan(t, s, ix); kindOf(changes, "c.txt") != -1 {
		t.Fatalf("scan after the bypass = %+v, want c.txt held back", changes)
	}
}

// TestInFlightStillSeenByDeletedPass verifies an in-flight file is still
// "seen": it must never be reported as a deletion (which would tombstone it on
// every peer), and a settled neighbour must stay untouched.
func TestInFlightStillSeenByDeletedPass(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "settled.txt", "settled")
	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleSeconds(0) // count-only: the quiet window has its own tests
	gateScan(t, s, ix) // held
	gateScan(t, s, ix) // Added, applied
	before, ok, _ := ix.Get("settled.txt")
	if !ok {
		t.Fatal("settled.txt should be indexed")
	}

	writeFile(t, root, "copying.txt", "partial")
	changes := gateScan(t, s, ix)
	for _, c := range changes {
		if c.Kind == Deleted {
			t.Fatalf("in-flight file must never be reported as Deleted, got %+v", c)
		}
	}
	if kindOf(changes, "copying.txt") != -1 {
		t.Fatalf("in-flight file must emit no change, got %+v", changes)
	}
	after, ok, _ := ix.Get("settled.txt")
	if !ok || after.Deleted {
		t.Fatalf("settled.txt = %+v (ok=%v), want a live untouched entry", after, ok)
	}
	if after.Size != before.Size || after.ModifiedS != before.ModifiedS || after.Sequence != before.Sequence {
		t.Fatalf("settled.txt changed: before=%+v after=%+v", before, after)
	}
}

// TestInFlightNotRenameCandidate verifies an unsettled target is never paired
// as a rename (its content was never hashed, so it cannot be trusted
// byte-identical): the deletion falls back to delete+add, while an unrelated
// settled rename in the same scan still pairs.
func TestInFlightNotRenameCandidate(t *testing.T) {
	root, ix := setup(t)
	// gone.txt will be deleted; its content is being copied to mid.bin.
	writeFile(t, root, "gone.txt", "samecontent")
	writeFile(t, root, "old2.txt", "b-content")
	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleSeconds(0) // count-only: the quiet window has its own tests
	gateScan(t, s, ix) // held
	gateScan(t, s, ix) // Added, applied

	// The copy starts (mid.bin gets gone.txt's content) and old2.txt is
	// renamed to new2.txt. Both new targets are unsettled after this scan.
	if err := os.Remove(filepath.Join(root, "gone.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, "mid.bin", "samecontent")
	if err := os.Rename(filepath.Join(root, "old2.txt"), filepath.Join(root, "new2.txt")); err != nil {
		t.Fatal(err)
	}
	gateScan(t, s, ix) // mid.bin + new2.txt at Scans 1

	// Next scan: mid.bin is written again (still unstable); new2.txt has now
	// been stable for two scans, so it is hashed and can pair as a rename.
	writeFile(t, root, "mid.bin", "samecontent-but-still-growing")
	changes := gateScan(t, s, ix)
	inflightFirst := s.LastInFlight()
	// The deletion may be confirmed on this pass or the next depending on the
	// gate state the previous scans left behind; what matters is that it is a
	// DELETION and never a rename onto the in-flight copy. Take a second pass so
	// the assertion is about behaviour, not about pass bookkeeping.
	if kindOf(changes, "gone.txt") != Deleted {
		changes = append(changes, gateScan(t, s, ix)...)
	}

	for _, c := range changes {
		if c.Kind == Renamed && c.Info.Name == "mid.bin" {
			t.Fatalf("an in-flight target must never be paired as a rename, got %+v", c)
		}
		if c.Kind == Renamed && (c.From == "gone.txt" || c.From == "mid.bin") {
			t.Fatalf("gone.txt -> mid.bin must not be a rename, got %+v", c)
		}
	}
	if kindOf(changes, "gone.txt") != Deleted {
		t.Fatalf("gone.txt = %v, want Deleted (delete+add fallback); changes=%+v", kindOf(changes, "gone.txt"), changes)
	}
	if f := inFlightFor(inflightFirst, "mid.bin"); f == nil || f.Scans != 1 {
		t.Fatalf("LastInFlight = %+v, want mid.bin at Scans 1", inflightFirst)
	}
	// The unrelated, now-settled rename still pairs exactly.
	paired := false
	for _, c := range changes {
		if c.Kind == Renamed && c.From == "old2.txt" && c.Info.Name == "new2.txt" {
			paired = true
		}
	}
	if !paired {
		t.Fatalf("old2.txt -> new2.txt should still pair as a rename; changes=%+v", changes)
	}
}

// driftInfo fakes a file whose size grew after it was read.
type driftInfo struct {
	os.FileInfo
	size int64
}

func (d driftInfo) Size() int64 { return d.size }

// TestHashDriftSkipsFile verifies the scan→hash drift check: a file that
// changed while it was being read is NOT applied (its entry would claim the
// walk's size with hashes of the newer content) and is demoted to in-flight
// with the post-hash stat, without poisoning the rest of the batch.
func TestHashDriftSkipsFile(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "drift.bin", "v1")
	writeFile(t, root, "good.txt", "g1")
	s := New(root, ix, &ignore.Matcher{})
	s.SetSettleSeconds(0) // count-only: the quiet window has its own tests
	s.SetSettleScans(1) // the drift check is independent of the gate
	s.SetSettleSeconds(0)
	s.SetDeletionScans(1)
	gateScan(t, s, ix)
	before, ok, _ := ix.Get("drift.bin")
	if !ok {
		t.Fatal("drift.bin should be indexed")
	}

	writeFile(t, root, "drift.bin", "v2-longer")
	writeFile(t, root, "good.txt", "g2")
	// Deterministic mtimes: the rewrite must be VISIBLY different to the walk, or
	// the pass legitimately reports nothing and the test fails for reasons that
	// have nothing to do with drift handling (seen once on Linux).
	when := time.Now().Add(2 * time.Second)
	for _, rel := range []string{"drift.bin", "good.txt"} {
		if err := os.Chtimes(filepath.Join(root, rel), when, when); err != nil {
			t.Fatal(err)
		}
	}
	old := statForHash
	statForHash = func(p string) (os.FileInfo, error) {
		st, err := os.Stat(p)
		if err != nil {
			return st, err
		}
		if filepath.Base(p) == "drift.bin" {
			return driftInfo{FileInfo: st, size: st.Size() + 10}, nil
		}
		return st, nil
	}
	t.Cleanup(func() { statForHash = old })

	changes := gateScan(t, s, ix)
	if kindOf(changes, "drift.bin") != -1 {
		t.Fatalf("a drifted file must emit no change, got %+v", changes)
	}
	if kindOf(changes, "good.txt") != Modified {
		t.Fatalf("good.txt = %v, want Modified (drift must not poison the batch); changes=%+v", kindOf(changes, "good.txt"), changes)
	}
	f := inFlightFor(s.LastInFlight(), "drift.bin")
	if f == nil || f.Scans != 1 {
		t.Fatalf("LastInFlight = %+v, want drift.bin at Scans 1", s.LastInFlight())
	}
	// The observed (post-hash) stat is the faked one: driftInfo reports the
	// size the file had grown to, not the walk's.
	wantSize := int64(len("v2-longer")) + 10
	if f.Size != wantSize {
		t.Fatalf("in-flight size = %d, want the post-hash observed size %d", f.Size, wantSize)
	}
	after, ok, _ := ix.Get("drift.bin")
	if !ok {
		t.Fatal("drift.bin should still be indexed")
	}
	if after.Size != before.Size || after.ModifiedS != before.ModifiedS || after.Sequence != before.Sequence {
		t.Fatalf("drifted file's index entry changed: before=%+v after=%+v", before, after)
	}
	// The demoted row records the post-hash stat, so the next run starts from
	// what was actually hashed.
	states, err := ix.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	st, ok := states["drift.bin"]
	if !ok {
		t.Fatalf("settling table = %+v, want a row for drift.bin", states)
	}
	if st.Count != 1 || st.Size != wantSize {
		t.Fatalf("settling row for drift.bin = %+v, want Count 1 and the post-hash size", st)
	}
}
