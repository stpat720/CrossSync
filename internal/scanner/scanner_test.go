package scanner

import (
	"os"
	"path/filepath"
	"strings"
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
	t.Cleanup(func() { _ = ix.Close() })
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

// newTestScanner builds a scanner with the stability gate DISABLED
// (settle_scans 1 = pre-gate behaviour). These tests exercise the walk,
// hashing and rename machinery directly on the first scan; the gate itself is
// covered by scanner_settle_test.go, which drives it explicitly.
func newTestScanner(root string, ix *index.Index, ig *ignore.Matcher) *Scanner {
	s := New(root, ix, ig)
	s.SetSettleScans(1)
	return s
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

	s := newTestScanner(root, ix, &ignore.Matcher{})
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
	s := newTestScanner(root, ix, &ignore.Matcher{})
	changes, _ := s.Scan()
	for _, c := range changes {
		if c.Kind == Added {
			_ = ix.Put(c.Info)
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
			_ = ix.Put(c.Info)
		}
	}
	_ = os.Remove(filepath.Join(root, "a.txt"))
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
	s := newTestScanner(root, ix, &ignore.Matcher{})
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
	s := newTestScanner(root, ix, &ignore.Matcher{})
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
	_ = os.Remove(filepath.Join(root, "a.txt"))
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
	s := newTestScanner(root, ix, m)
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
	s := newTestScanner(root, ix, &ignore.Matcher{})
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
	s := newTestScanner(root, ix, &ignore.Matcher{})
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

// TestCheapRenameSkipsHashing verifies the mtime fast path: renaming a
// whole folder (which preserves each file's mtime) must pair the moved
// files as renames WITHOUT re-hashing them — the exact fix for slow folder
// renames.
func TestCheapRenameSkipsHashing(t *testing.T) {
	root, ix := setup(t)
	files := map[string]string{
		"folder/a.dat":         strings.Repeat("a", 100),
		"folder/b.dat":         strings.Repeat("b", 200),
		"folder/sub/c.dat":     strings.Repeat("c", 300),
		"folder/sub/deep/d.dat": strings.Repeat("d", 400),
	}
	for rel, content := range files {
		writeFile(t, root, rel, content)
	}
	s := newTestScanner(root, ix, &ignore.Matcher{})
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
	if err := os.Rename(filepath.Join(root, "folder"), filepath.Join(root, "renamed")); err != nil {
		t.Fatal(err)
	}
	hashTotal := -1
	changes, err = s.ScanWithProgress(func(p Progress) {
		if p.Phase == "hashing" {
			hashTotal = p.HashTotal
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if hashTotal != 0 {
		t.Fatalf("hashed %d files during a folder rename, want 0", hashTotal)
	}
	renamed := 0
	for _, c := range changes {
		if c.Kind == Renamed {
			renamed++
		}
	}
	if renamed != len(files) {
		t.Fatalf("renamed changes = %d, want %d; changes=%+v", renamed, len(files), changes)
	}
	for oldRel := range files {
		newRel := "renamed" + strings.TrimPrefix(oldRel, "folder")
		found := false
		for _, c := range changes {
			if c.Kind == Renamed && c.From == oldRel && c.Info.Name == newRel {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected Renamed %s -> %s, got %+v", oldRel, newRel, changes)
		}
	}
}

// TestCheapRenameWithConcurrentAddDelete verifies the user's race scenario:
// rename a folder AND delete some files and add others before the scan runs.
// The moved files must still be cheap renames (no hash), the deleted ones
// tombstoned, and only the genuinely new file hashed — all in ONE scan.
func TestCheapRenameWithConcurrentAddDelete(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "folder/a.txt", "aaa")
	writeFile(t, root, "folder/b.txt", "bbbb")
	writeFile(t, root, "folder/c.txt", "ccccc")
	s := newTestScanner(root, ix, &ignore.Matcher{})
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
	if err := os.Rename(filepath.Join(root, "folder"), filepath.Join(root, "folder2")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "folder2", "b.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, "folder2/d.txt", "dddddd")
	hashTotal := -1
	changes, err = s.ScanWithProgress(func(p Progress) {
		if p.Phase == "hashing" {
			hashTotal = p.HashTotal
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if hashTotal != 1 {
		t.Fatalf("hashTotal = %d, want 1 (only the new file)", hashTotal)
	}
	if kindOf(changes, "folder2/a.txt") != Renamed {
		t.Fatalf("folder2/a.txt = %v, want Renamed; %+v", kindOf(changes, "folder2/a.txt"), changes)
	}
	if kindOf(changes, "folder2/c.txt") != Renamed {
		t.Fatalf("folder2/c.txt = %v, want Renamed; %+v", kindOf(changes, "folder2/c.txt"), changes)
	}
	// The deleted file is reported under its OLD path (the tombstone).
	if kindOf(changes, "folder/b.txt") != Deleted {
		t.Fatalf("folder/b.txt = %v, want Deleted; %+v", kindOf(changes, "folder/b.txt"), changes)
	}
	if kindOf(changes, "folder2/d.txt") != Added {
		t.Fatalf("folder2/d.txt = %v, want Added; %+v", kindOf(changes, "folder2/d.txt"), changes)
	}
}

// TestCheapRenameDowngradesOnMismatch verifies the safety valve: when a
// (size, mtime) match has DIFFERENT content (a timestamp-preserving
// rewrite), the cheap pass must downgrade to the exact content path so the
// new content is hashed and no wrong rename is adopted.
func TestCheapRenameDowngradesOnMismatch(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "folder/a.txt", "AAAA")
	s := newTestScanner(root, ix, &ignore.Matcher{})
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
	oldInfo, err := os.Stat(filepath.Join(root, "folder", "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	mt := oldInfo.ModTime()
	// New path: same size, same mtime, DIFFERENT content.
	if err := os.MkdirAll(filepath.Join(root, "folder2"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "folder2", "a.txt"), []byte("BBBB"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(root, "folder2", "a.txt"), mt, mt); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "folder", "a.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "folder")); err != nil {
		t.Fatal(err)
	}
	changes, err = s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if kindOf(changes, "folder/a.txt") != Deleted {
		t.Fatalf("folder/a.txt = %v, want Deleted (no cheap rename); %+v", kindOf(changes, "folder/a.txt"), changes)
	}
	addedOK := false
	for _, c := range changes {
		if c.Kind == Added && c.Info.Name == "folder2/a.txt" {
			addedOK = true
			if len(c.Info.Blocks) != 1 || !bytesEqual(c.Info.Blocks[0], hash.HashBytes([]byte("BBBB"))) {
				t.Fatalf("folder2/a.txt blocks do not match BBBB: %x", c.Info.Blocks[0])
			}
		}
	}
	if !addedOK {
		t.Fatalf("folder2/a.txt should be Added (hashed), got %+v", changes)
	}
}

// TestCheapRenameMinorityChurnFallsBack verifies the majority gate: a small
// move amid lots of new content is NOT rename-dominated, so the cheap path
// is skipped and the exact content-based path still detects the move.
func TestCheapRenameMinorityChurnFallsBack(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "folder/a.txt", "aaa")
	s := newTestScanner(root, ix, &ignore.Matcher{})
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
	if err := os.Rename(filepath.Join(root, "folder"), filepath.Join(root, "folder2")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, "folder2/n1.txt", "n1n1n1")
	writeFile(t, root, "folder2/n2.txt", "n2n2n2n2")
	writeFile(t, root, "folder2/n3.txt", "n3n3n3n3")
	hashTotal := -1
	changes, err = s.ScanWithProgress(func(p Progress) {
		if p.Phase == "hashing" {
			hashTotal = p.HashTotal
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	// candidates=1 (a.txt), addedCount=4 -> gate fails -> exact path.
	if hashTotal != 4 {
		t.Fatalf("hashTotal = %d, want 4 (cheap path skipped)", hashTotal)
	}
	if kindOf(changes, "folder2/a.txt") != Renamed {
		t.Fatalf("folder2/a.txt = %v, want Renamed via content path; %+v", kindOf(changes, "folder2/a.txt"), changes)
	}
	for _, name := range []string{"n1.txt", "n2.txt", "n3.txt"} {
		if kindOf(changes, "folder2/"+name) != Added {
			t.Fatalf("folder2/%s = %v, want Added; %+v", name, kindOf(changes, "folder2/"+name), changes)
		}
	}
}

// TestCheapRenameTrustsLargeFiles verifies the size gate: a file above the
// verification cap is trusted on (size, mtime) alone, so a timestamp-
// preserving content rewrite of a LARGE file inside a folder rename is NOT
// hashed (and therefore does not downgrade the pass) — renaming a folder of
// a few huge media files stays free. Small files are still verified.
func TestCheapRenameTrustsLargeFiles(t *testing.T) {
	oldCap, oldTotal := maxVerifyFileSize, maxVerifyTotal
	maxVerifyFileSize, maxVerifyTotal = 1<<20, 8<<20 // 1 MiB cap for the test
	t.Cleanup(func() { maxVerifyFileSize, maxVerifyTotal = oldCap, oldTotal })
	root, ix := setup(t)
	big := strings.Repeat("X", 2<<20) // 2 MiB > 1 MiB cap
	small := "small content"
	writeFile(t, root, "folder/big.bin", big)
	writeFile(t, root, "folder/small.txt", small)
	s := newTestScanner(root, ix, &ignore.Matcher{})
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
	// "Rename": big.bin is rewritten with DIFFERENT content at the new path
	// (same size + mtime); small.txt is a true move.
	st, err := os.Stat(filepath.Join(root, "folder", "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	bigMtime := st.ModTime()
	st, err = os.Stat(filepath.Join(root, "folder", "small.txt"))
	if err != nil {
		t.Fatal(err)
	}
	smallMtime := st.ModTime()
	if err := os.MkdirAll(filepath.Join(root, "folder2"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "folder2", "big.bin"), []byte(strings.Repeat("Y", 2<<20)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(root, "folder2", "big.bin"), bigMtime, bigMtime); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "folder2", "small.txt"), []byte(small), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(root, "folder2", "small.txt"), smallMtime, smallMtime); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "folder")); err != nil {
		t.Fatal(err)
	}
	changes, err = s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	// big.bin is trusted (not verified): the content rewrite does not
	// downgrade the pass, so it stays a Renamed change.
	if kindOf(changes, "folder2/big.bin") != Renamed {
		t.Fatalf("folder2/big.bin = %v, want Renamed (trusted, not hashed); %+v", kindOf(changes, "folder2/big.bin"), changes)
	}
	// small.txt is verified (under the cap) and matches -> Renamed.
	if kindOf(changes, "folder2/small.txt") != Renamed {
		t.Fatalf("folder2/small.txt = %v, want Renamed; %+v", kindOf(changes, "folder2/small.txt"), changes)
	}
}

// TestScanIncludeFilter verifies the include (whitelist) gate: with an
// include list only matching paths are in scope — everything else is never
// indexed, never transmitted, and never deleted.
func TestScanIncludeFilter(t *testing.T) {
	root, ix := setup(t)
	writeFile(t, root, "p1/proxy/a.mp4", "a")
	writeFile(t, root, "p1/media/b.mp4", "b")
	writeFile(t, root, "p1/media/c.mov", "c")
	writeFile(t, root, "top.txt", "top")

	inc, err := ignore.Parse([]string{"*/proxy/", "*.mp4"})
	if err != nil {
		t.Fatal(err)
	}
	s := newTestScanner(root, ix, &ignore.Matcher{})
	s.SetInclude(inc)

	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]ChangeKind{
		"p1/proxy":       Added, // the deep dir rule puts it back in scope
		"p1/proxy/a.mp4": Added,
		"p1/media/b.mp4": Added, // an extension rule reaches into an out-of-scope dir
		"p1/media/c.mov": Skipped,
		"top.txt":        Skipped,
	}
	got := map[string]ChangeKind{}
	for _, c := range changes {
		if c.Info == nil {
			t.Fatalf("change without Info: %+v", c)
		}
		got[c.Info.Name] = c.Kind
		if c.Kind == Skipped && c.Rule != "include" {
			t.Fatalf("%s: skipped rule = %q, want \"include\"", c.Info.Name, c.Rule)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("changes = %+v, want exactly %d entries", changes, len(want))
	}
	for name, kind := range want {
		if got[name] != kind {
			t.Fatalf("%s: kind = %v, want %v (changes = %+v)", name, got[name], kind, changes)
		}
	}
	// Out-of-scope paths (including the parent dirs of in-scope files) are
	// never indexed.
	for _, name := range []string{"p1", "p1/media", "p1/media/c.mov", "top.txt"} {
		if _, ok, err := ix.Get(name); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Fatalf("out-of-scope path %s must not be indexed", name)
		}
	}
	for _, c := range changes {
		if c.Kind == Added {
			if err := ix.Put(c.Info); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Rescan of the unchanged tree: only the always-reported skip notices for
	// out-of-scope files, no added/modified/deleted churn.
	changes, err = s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.Kind != Skipped {
			t.Fatalf("rescan should report no real changes, got %+v", c)
		}
	}

	// Removing an out-of-scope file is not a change (it was never in scope).
	if err := os.Remove(filepath.Join(root, filepath.FromSlash("p1/media/c.mov"))); err != nil {
		t.Fatal(err)
	}
	changes, err = s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.Kind != Skipped {
			t.Fatalf("a vanished out-of-scope file must not be reported, got %+v", c)
		}
	}

	// An out-of-scope entry that IS in the index (synced before the include
	// list was added) must not be tombstoned just because the scanner no
	// longer looks at its path.
	if err := ix.Put(&index.FileInfo{Name: "p1/media/c.mov", Size: 1, Type: index.TypeFile}); err != nil {
		t.Fatal(err)
	}
	changes, err = s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.Kind == Deleted {
			t.Fatalf("out-of-scope index entries must never be emitted as deletions, got %+v", c)
		}
	}
}
