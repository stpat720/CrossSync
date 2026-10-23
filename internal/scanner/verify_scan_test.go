package scanner

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"crosssync/internal/ignore"
)

// TestVerifiedScanCatchesTimestampPreservingChange pins the deep-scan feature.
//
// The fast path trusts (size, mtime). That is right for a normal editor, but it
// is BLIND to a content change that preserved both — rsync -a / restore tools
// keep timestamps, and SMB/exFAT timestamp granularity is coarse. A verified
// scan re-hashes every file, so the change is caught; and a verified scan of
// UNCHANGED content must come back as Touched (adopt the stat, keep the
// version) so a deep scan never re-transfers the whole share to peers.
func TestVerifiedScanCatchesTimestampPreservingChange(t *testing.T) {
	root, ix := setup(t)
	p := filepath.Join(root, "doc.txt")
	if err := os.WriteFile(p, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	if err := os.Chtimes(p, stamp, stamp); err != nil {
		t.Fatal(err)
	}

	ig, err := ignore.Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	sc := newTestScanner(root, ix, ig)
	first, err := sc.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if k := kindOf(first, "doc.txt"); k != Added {
		t.Fatalf("first scan kind = %v, want Added", k)
	}
	for _, c := range first {
		if c.Info != nil {
			if err := ix.Put(c.Info); err != nil {
				t.Fatal(err)
			}
		}
	}
	seeded, ok, err := ix.Get("doc.txt")
	if err != nil || !ok {
		t.Fatalf("index lookup: ok=%v err=%v", ok, err)
	}

	// Same size, SAME mtime, different bytes: the classic rsync -a / SMB case.
	if err := os.WriteFile(p, []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, stamp, stamp); err != nil {
		t.Fatal(err)
	}

	// A normal scan cannot see it — this is the documented fast-path trade-off,
	// asserted here so the test fails loudly if that assumption ever changes.
	missed, err := sc.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if k := kindOf(missed, "doc.txt"); k != -1 {
		t.Fatalf("normal scan reported kind %v for a size+mtime-preserving change, want none (fast path trusts stat)", k)
	}

	// A verified scan must catch it.
	sc.SetForceHash(true)
	deep, err := sc.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if k := kindOf(deep, "doc.txt"); k != Modified {
		t.Fatalf("verified scan kind = %v, want Modified (content changed under identical stat)", k)
	}

	// Second verified scan over content that is now UNCHANGED: Touched, and the
	// recorded version must not move (a no-op, not a re-transfer).
	for _, c := range deep {
		if c.Info != nil {
			fi := *c.Info
			fi.Version = seeded.Version.Bump(1)
			if err := ix.Put(&fi); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Nudge the mtime so the verified pass has something to adopt.
	later := stamp.Add(time.Minute)
	if err := os.Chtimes(p, later, later); err != nil {
		t.Fatal(err)
	}
	sc.SetForceHash(true)
	again, err := sc.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if k := kindOf(again, "doc.txt"); k != Touched {
		t.Fatalf("verified scan of unchanged content kind = %v, want Touched", k)
	}
}
