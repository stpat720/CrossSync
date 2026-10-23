package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDeepScanChangeAccounting pins what a verified scan REPORTS. It ran once on
// the live share and answered "4355 changes" for a folder where nothing had
// changed: Touched (content identical, stat adopted) was being counted as a
// change, which inflates the API response, the scan-status "changed" number the
// UI shows and the deep-scan log line. Touched is not a change and must not be
// counted; a real content change must still be counted and still bump the
// version.
func TestDeepScanChangeAccounting(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	chunkWrite(t, root, "same.txt", "identical bytes")
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	before, ok, err := n.Folders["data"].Ix.Get("same.txt")
	if err != nil || !ok {
		t.Fatalf("index lookup: ok=%v err=%v", ok, err)
	}

	// Re-hash everything with unchanged content: zero changes, no new version.
	applied, err := n.ScanFolderDeep("data")
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 {
		t.Fatalf("verified scan of unchanged content reported %d change(s), want 0", applied)
	}
	after, _, err := n.Folders["data"].Ix.Get("same.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := after.Version.Get(1), before.Version.Get(1); got != want {
		t.Fatalf("version counter moved %d -> %d on an unchanged deep scan (peers would re-download)", want, got)
	}

	// A content change that preserves size AND mtime: invisible to a normal
	// scan, caught by a verified one, and counted exactly once.
	stamp := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	p := filepath.Join(root, "same.txt")
	if err := os.WriteFile(p, []byte("DIFFERENT BYTES"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	// Give it BACK the mtime the index already records, so a normal scan sees
	// (size, mtime) unchanged — the rsync -a / coarse-timestamp scenario.
	mt := time.Unix(after.ModifiedS, int64(after.ModifiedNs))
	if err := os.Chtimes(p, mt, mt); err != nil {
		t.Fatal(err)
	}
	if applied, err := n.ScanFolder("data"); err != nil || applied != 0 {
		t.Fatalf("normal scan of a size+mtime-preserving change = (%d, %v), want (0, nil)", applied, err)
	}
	applied, err = n.ScanFolderDeep("data")
	if err != nil {
		t.Fatal(err)
	}
	if applied != 1 {
		t.Fatalf("verified scan of a real content change reported %d change(s), want 1", applied)
	}
	changed, _, err := n.Folders["data"].Ix.Get("same.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := changed.Version.Get(1), before.Version.Get(1); got != want+1 {
		t.Fatalf("version counter = %d, want %d (one bump for the real change)", got, want+1)
	}
}
