package node

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/index"
)

// blockFingerprint is the content identity of an index entry: what a re-hash
// would change and what a (size, mtime) preserving edit cannot.
func blockFingerprint(fi *index.FileInfo) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d/%d:", fi.Size, fi.BlockSize)
	for _, h := range fi.Blocks {
		b.Write(h)
	}
	return b.String()
}

// indexFingerprint renders the live index as name -> (size, mtime, blocks), so a
// test can prove that an abandoned pass wrote nothing at all.
func indexFingerprint(t *testing.T, n *Node) map[string]string {
	t.Helper()
	f, ok := n.Folders["data"]
	if !ok {
		t.Fatal("fixture folder \"data\" is missing")
	}
	out := map[string]string{}
	if err := f.Ix.ListAll(func(fi *index.FileInfo) error {
		out[fi.Name] = fmt.Sprintf("%d/%d.%09d/del=%v/blocks=%d/%x",
			fi.Size, fi.ModifiedS, fi.ModifiedNs, fi.Deleted, len(fi.Blocks), fi.Blocks)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// sameFingerprint reports the first difference between two index snapshots.
func sameFingerprint(before, after map[string]string) (string, bool) {
	for name, want := range before {
		got, ok := after[name]
		if !ok {
			return fmt.Sprintf("%s disappeared from the index", name), false
		}
		if got != want {
			return fmt.Sprintf("%s: %s -> %s", name, want, got), false
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			return fmt.Sprintf("%s appeared in the index", name), false
		}
	}
	return "", true
}

// scanStatusOf returns the live scan status the control plane reports for a
// folder.
func scanStatusOf(t *testing.T, n *Node, id string) ScanStatus {
	t.Helper()
	for _, st := range n.ScanStatuses() {
		if st.Folder == id {
			return st
		}
	}
	t.Fatalf("no scan status for folder %q", id)
	return ScanStatus{}
}

// TestPauseAbandonsInFlightScanAndStaysQuiet is the core promise of the full
// pause: while it is set the folder reads NO disk — not a walk, not a hash, not
// a held-back re-check, not the safety walk — and a pass that was already
// running is abandoned, applies nothing, and is reported as cancelled instead of
// as a completed empty scan.
//
// The pause lands inside the hash phase through the scanner's per-file hook, so
// this reproduces the live failure deterministically: hashPending used to return
// (nil, nil) on cancellation, scanScoped carried on, and the operator watched
// hash_done jump to the full total while the folder said "paused".
func TestPauseAbandonsInFlightScanAndStaysQuiet(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	root := n.Folders["data"].Root
	f := n.Folders["data"]

	const files = 300
	for i := 0; i < files; i++ {
		writeAt(t, root, fmt.Sprintf("src/f%03d.bin", i), strings.Repeat("q", 32*1024))
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatalf("baseline scan: %v", err)
	}
	before := indexFingerprint(t, n)

	// The first file of the hash phase pauses the folder: everything after it
	// must stay unread.
	var once sync.Once
	var hashed int
	var mu sync.Mutex
	f.Scanner.SetHashHook(func(string) {
		mu.Lock()
		hashed++
		mu.Unlock()
		once.Do(func() {
			if err := n.SetFolderPaused("data", true); err != nil {
				t.Errorf("pause from the hash phase: %v", err)
			}
		})
	})

	type result struct {
		applied int
		err     error
	}
	done := make(chan result, 1)
	go func() {
		applied, err := n.ScanFolderDeep("data")
		done <- result{applied, err}
	}()

	var got result
	select {
	case got = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a scan of a folder paused mid-hash did not abandon within 2s — the disk is still being read")
	}
	if !errors.Is(got.err, ErrScanCancelled) {
		t.Fatalf("scan paused mid-hash returned err = %v (applied %d), want ErrScanCancelled: a cancelled pass must never look like a clean one",
			got.err, got.applied)
	}
	if got.applied != 0 {
		t.Fatalf("abandoned scan applied %d change(s), want 0", got.applied)
	}
	st := scanStatusOf(t, n, "data")
	if st.Phase != "cancelled" {
		t.Fatalf("scan phase = %q after a pause cancelled it, want \"cancelled\" (status %+v)", st.Phase, st)
	}
	if st.Running {
		t.Fatal("scan status still says running after the cancelled pass returned")
	}
	if st.HashDone >= st.HashTotal {
		t.Fatalf("the abandoned pass hashed %d of %d files: it kept reading the disk after the pause", st.HashDone, st.HashTotal)
	}
	mu.Lock()
	hashedAfterPause := hashed
	mu.Unlock()
	if hashedAfterPause >= files {
		t.Fatalf("hashed %d of %d files after the pause, want the pass to stop", hashedAfterPause, files)
	}
	if diff, ok := sameFingerprint(before, indexFingerprint(t, n)); !ok {
		t.Fatalf("the cancelled pass changed the index: %s", diff)
	}

	// No scan may START while paused, from any of the daemon's tickers. Drive
	// every one of them by hand for several held-back ticks (the real ticker is
	// 2s) and watch the status: a started pass would flip phase to "walking".
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if applied := n.ScanHeldBack(); applied != 0 {
			t.Fatalf("held-back re-check applied %d change(s) for a paused folder, want 0", applied)
		}
		if err := n.ScanAll(); err != nil {
			t.Fatalf("safety walk over a paused folder: %v", err)
		}
		if _, err := n.ScanFolder("data"); !errors.Is(err, ErrFolderPaused) {
			t.Fatalf("explicit scan of a paused folder = %v, want ErrFolderPaused", err)
		}
		if _, err := n.ScanFolderPaths("data", []string{"src/f000.bin"}); !errors.Is(err, ErrFolderPaused) {
			t.Fatalf("scoped scan of a paused folder = %v, want ErrFolderPaused", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st := scanStatusOf(t, n, "data"); st.Phase != "cancelled" || st.Running {
		t.Fatalf("a scan ran for a paused folder: phase=%q running=%v", st.Phase, st.Running)
	}
	if diff, ok := sameFingerprint(before, indexFingerprint(t, n)); !ok {
		t.Fatalf("a ticker scan touched the index of a paused folder: %s", diff)
	}

	// Resume: the folder scans normally again.
	f.Scanner.SetHashHook(nil)
	if err := n.SetFolderPaused("data", false); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatalf("scan after resume: %v", err)
	}
}

// TestPausedFolderDoesNoHeldBackDiskWork covers the 2s re-check ticker, the
// quietest path that still touches the disk: while the folder is paused it must
// not re-observe the files the stability gate is holding, so a held copy stays
// exactly where it is (unindexed, unmoved) until the resume.
func TestPausedFolderDoesNoHeldBackDiskWork(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	root := n.Folders["data"].Root
	f := n.Folders["data"]
	// Count-only gate: two consecutive observations release a file, so a
	// re-check that really ran would show up in the index within a few ticks.
	f.Scanner.SetSettleScans(2)
	f.Scanner.SetSettleSeconds(0)

	writeAt(t, root, "src/held.bin", strings.Repeat("h", 8192))
	mustScan(t, n) // the directory is applied; the copy is held by the gate
	if _, ok, _ := f.Ix.Get("src/held.bin"); ok {
		t.Fatal("setup: the held copy is already indexed")
	}
	if len(f.Scanner.LastInFlight()) == 0 {
		t.Fatal("setup: the gate is not holding anything")
	}

	if err := n.SetFolderPaused("data", true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if applied := n.ScanHeldBack(); applied != 0 {
			t.Fatalf("held-back re-check applied %d change(s) while the folder is paused", applied)
		}
		if _, ok, _ := f.Ix.Get("src/held.bin"); ok {
			t.Fatal("the held-back re-check indexed a file while the folder is paused")
		}
	}
	// The hold survives the pause: resume releases it on the next re-check.
	if err := n.SetFolderPaused("data", false); err != nil {
		t.Fatal(err)
	}
	if applied := n.ScanHeldBack(); applied != 1 {
		t.Fatalf("held-back re-check after resume applied %d change(s), want 1", applied)
	}
	if _, ok, _ := f.Ix.Get("src/held.bin"); !ok {
		t.Fatal("the copy stayed held after the resume")
	}
}

// TestRescanRequestedWhilePausedRunsOnceOnResume: an operator's rescan request
// is never silently eaten. Made while the folder is paused, it is remembered and
// runs exactly once when the folder is resumed — the file used here changed
// content while keeping its (size, mtime), so only the verified pass can see it,
// which makes "did the queued request run?" answerable from the index.
func TestRescanRequestedWhilePausedRunsOnceOnResume(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	root := n.Folders["data"].Root
	f := n.Folders["data"]

	writeAt(t, root, "src/x.bin", strings.Repeat("A", 4096))
	mustScan(t, n)
	indexed, ok, err := f.Ix.Get("src/x.bin")
	if err != nil || !ok {
		t.Fatalf("setup: index lookup: ok=%v err=%v", ok, err)
	}
	// Same size, same mtime, different bytes: invisible to the fast path.
	writeAt(t, root, "src/x.bin", strings.Repeat("B", 4096))

	if err := n.SetFolderPaused("data", true); err != nil {
		t.Fatal(err)
	}
	queued, _, err := n.RequestRescan("data", true)
	if err != nil {
		t.Fatalf("rescan requested while paused: %v", err)
	}
	if !queued {
		t.Fatal("a rescan requested while paused must be reported as queued, not consumed")
	}
	// Asking again while paused coalesces into that same one pass.
	for i := 0; i < 2; i++ {
		if again, _, err := n.RequestRescan("data", true); err != nil || !again {
			t.Fatalf("second rescan request while paused: queued=%v err=%v", again, err)
		}
	}
	// A plain rescan queued behind a verified one must not add a second pass.
	if again, _, err := n.RequestRescan("data", false); err != nil || !again {
		t.Fatalf("plain rescan request while paused: queued=%v err=%v", again, err)
	}

	// While paused nothing may read the disk: the edit stays invisible.
	time.Sleep(200 * time.Millisecond)
	if fi, ok, err := f.Ix.Get("src/x.bin"); err != nil || !ok {
		t.Fatalf("index lookup while paused: ok=%v err=%v", ok, err)
	} else if blockFingerprint(fi) != blockFingerprint(indexed) {
		t.Fatal("a rescan ran while the folder was paused")
	}

	// Count hashes so "exactly once" is measurable: the queued VERIFIED pass
	// hashes the file, and a plain walk afterwards has nothing to hash.
	var hashes atomic.Int32
	f.Scanner.SetHashHook(func(string) { hashes.Add(1) })

	if err := n.SetFolderPaused("data", false); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		fi, ok, err := f.Ix.Get("src/x.bin")
		if err == nil && ok && blockFingerprint(fi) != blockFingerprint(indexed) {
			break // the queued verified pass ran and adopted the new content
		}
		if time.Now().After(deadline) {
			t.Fatalf("the verified rescan queued while paused did not run after resume (hashes=%d)", hashes.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.Scanner.SetHashHook(nil)
	if got := hashes.Load(); got != 1 {
		t.Fatalf("the queued verified pass hashed %d file(s), want exactly 1", got)
	}

	// A later resume with nothing queued must not start a scan.
	started := scanStatusOf(t, n, "data").Started
	if err := n.SetFolderPaused("data", true); err != nil {
		t.Fatal(err)
	}
	if err := n.SetFolderPaused("data", false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if now := scanStatusOf(t, n, "data"); now.Started != started || now.Running {
		t.Fatalf("resume started a scan with no queued request: started %d -> %d, running=%v", started, now.Started, now.Running)
	}
	// The queue is consumed, not sticky: the request ran once and is gone.
	queued, _, err = n.RequestRescan("data", false)
	if err != nil || queued {
		t.Fatalf("rescan on a resumed folder: queued=%v err=%v", queued, err)
	}
}
