package node

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"crosssync/internal/events"
)

// TestScopedScanRunsDuringFullWalk is the core contract of the two scan slots:
// a watcher-driven, path-scoped pass must index a new file WHILE a full walk of
// the same folder is in progress. Before the slots existed, a full walk held the
// folder's one scan lock for its whole pass, so the scoped scan waited behind it
// (up to twenty minutes on the operator's 1.17M-file Assets share while the
// watcher had already seen the file).
//
// The walk is parked inside its hash phase by a test hook, the new file is
// created after the walk has read that directory (so the walk itself can never
// see it), and the scoped pass must index it without disturbing the walk's live
// counters.
func TestScopedScanRunsDuringFullWalk(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	f := n.Folders["data"]
	f.Scanner.SetSettleScans(1)
	f.Scanner.SetSettleSeconds(0)
	f.Scanner.SetDeletionScans(1)

	writeAt(t, root, "walk/block.bin", strings.Repeat("b", 8192))
	writeAt(t, root, "walk/other.bin", "other")
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	// The walk has something to hash, so it can be parked in the hash phase.
	// (A different size than the indexed copy makes it a modification even
	// though writeAt pins the mtime.)
	writeAt(t, root, "walk/block.bin", strings.Repeat("B", 9000))

	enteredWalk, walkErr, releaseWalk := parkWalk(t, n, "walk/block.bin")
	defer releaseWalk()

	before := scanStatusOf(t, n, "data")
	if !before.Running || before.Phase != "hashing" || before.HashTotal < 1 || before.Walked == 0 {
		t.Fatalf("walk status while parked = %+v, want a running walk with progress", before)
	}

	// The user's file appears while the walk is parked.
	writeAt(t, root, "walk/new.txt", "new content")
	type result struct {
		applied int
		err     error
	}
	scoped := make(chan result, 1)
	go func() {
		applied, err := n.ScanFolderPaths("data", []string{"walk/new.txt"})
		scoped <- result{applied, err}
	}()

	var got result
	select {
	case got = <-scoped:
	case <-time.After(2 * time.Second):
		releaseWalk()
		<-enteredWalk
		<-scoped
		t.Fatal("a watcher-driven scoped scan did not run while a full walk was in progress: it queued behind the walk (the 20-minute detection hole this change removes)")
	}
	if got.err != nil {
		t.Fatalf("scoped scan during a walk: %v", got.err)
	}
	if _, live := liveEntry(t, f.Ix, "walk/new.txt"); !live {
		t.Fatal("the new file is not indexed while the walk is still parked: the scoped pass ran but applied nothing")
	}
	after := scanStatusOf(t, n, "data")
	if !after.Running || after.Started != before.Started || after.Walked != before.Walked ||
		after.HashTotal != before.HashTotal || after.Phase != before.Phase {
		t.Fatalf("the scoped pass disturbed the walk's live status:\n before %+v\n after  %+v", before, after)
	}

	releaseWalk()
	<-enteredWalk
	if err := *walkErr; err != nil {
		t.Fatalf("walk after the scoped pass: %v", err)
	}
	// The walk must not drop or duplicate the file the scoped pass indexed.
	if _, live := liveEntry(t, f.Ix, "walk/new.txt"); !live {
		t.Fatal("the completed walk dropped the file the scoped pass indexed")
	}
	// walk/ (directory) + block.bin + other.bin + new.txt. One row per path is
	// the index's own contract; this asserts the walk's late apply did not drop
	// or re-create the row the scoped pass wrote.
	if got := indexCount(t, n); got != 4 {
		t.Fatalf("index holds %d rows after both passes, want 4", got)
	}
}

// parkWalk starts a full walk in the background and returns once it is parked
// inside its hash phase on the named file (which must be a modification, so it
// is in the hash set). The returned done channel closes when the walk finishes;
// its error is in errp, readable after done is closed. Call release from a defer
// so a failing test cannot leave the walk holding the folder.
func parkWalk(t *testing.T, n *Node, hashName string) (done chan struct{}, errp *error, release func()) {
	t.Helper()
	f := n.Folders["data"]
	entered := make(chan struct{})
	gate := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	release = func() { releaseOnce.Do(func() { close(gate) }) }
	f.Scanner.SetHashHook(func(name string) {
		if name != hashName {
			return
		}
		enterOnce.Do(func() { close(entered) })
		<-gate
	})
	t.Cleanup(func() { f.Scanner.SetHashHook(nil) })
	done = make(chan struct{})
	errp = new(error)
	go func() {
		_, *errp = n.ScanFolder("data")
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		release()
		<-done
		t.Fatal("the walk never reached its hash phase")
	}
	return done, errp, release
}

// TestScopedScanSameDirectoryWhileWalkHoldsIt covers the same-path race: the
// walk has already read directory X (and is hashing its contents) when a scoped
// pass indexes a NEW file in X. The walk's "absent from my seen set" must not
// become a tombstone: the file exists on disk, so the walk's deletion pass has
// nothing to say about it. The file must appear exactly once and keep the
// version the scoped pass gave it.
func TestScopedScanSameDirectoryWhileWalkHoldsIt(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	f := n.Folders["data"]
	f.Scanner.SetSettleScans(1)
	f.Scanner.SetSettleSeconds(0)
	f.Scanner.SetDeletionScans(1)

	writeAt(t, root, "x/slow.bin", strings.Repeat("s", 4096))
	writeAt(t, root, "x/keep.bin", "keep")
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	writeAt(t, root, "x/slow.bin", strings.Repeat("S", 5000))

	done, walkErr, release := parkWalk(t, n, "x/slow.bin")
	defer release()

	writeAt(t, root, "x/new.txt", "created while the walk owned x")
	if _, err := n.ScanFolderPaths("data", []string{"x/new.txt"}); err != nil {
		t.Fatalf("scoped scan while the walk is in x: %v", err)
	}
	fi, live := liveEntry(t, f.Ix, "x/new.txt")
	if !live {
		t.Fatal("the new file in the walk's directory was not indexed")
	}
	wantVer := fi.Version

	release()
	<-done
	if err := *walkErr; err != nil {
		t.Fatalf("walk: %v", err)
	}
	fi, live = liveEntry(t, f.Ix, "x/new.txt")
	if !live {
		t.Fatal("the walk tombstoned a file that exists on disk, only because it was created after the walk read the directory")
	}
	if fi.Version.Compare(wantVer) != 0 {
		t.Fatalf("the walk re-applied the new file it never saw (version moved from %v to %v): it must not duplicate the scoped pass's row", wantVer, fi.Version)
	}
	if got := indexCount(t, n); got != 4 { // x/, slow.bin, keep.bin, new.txt
		t.Fatalf("index holds %d rows, want 4", got)
	}
	// A second clean walk must agree, not "discover" a duplicate.
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if got := indexCount(t, n); got != 4 {
		t.Fatalf("index holds %d rows after the follow-up walk, want 4", got)
	}
}

// TestScopedScanNeverDeletesOutsideItsScope pins the safety rule for a scoped
// pass: it saw only its own paths, so every other path is unobserved, not gone.
// No tombstone, no new deletion-confirmation row, and no delete event may be
// produced for a path outside the scope — even while another path in the scope
// really did vanish.
func TestScopedScanNeverDeletesOutsideItsScope(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	f := n.Folders["data"]
	f.Scanner.SetSettleScans(1)
	f.Scanner.SetSettleSeconds(0)
	f.Scanner.SetDeletionScans(2) // the gate is on: a first miss only records a suspicion

	writeAt(t, root, "inside/one.txt", "one")
	writeAt(t, root, "inside/two.txt", "two")
	writeAt(t, root, "outside/keep.txt", "keep")
	writeAt(t, root, "outside/gone.txt", "gone")
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	// outside/gone.txt disappears, and a full walk records ONE suspicion.
	if err := os.Remove(filepath.Join(root, "outside", "gone.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	missing, err := f.Ix.MissingList()
	if err != nil {
		t.Fatal(err)
	}
	if missing["outside/gone.txt"].Count != 1 {
		t.Fatalf("setup: deletion suspicion = %+v, want one miss recorded", missing["outside/gone.txt"])
	}
	before, err := n.Events.Query(events.Filter{Folder: "data", Path: "outside/keep.txt"})
	if err != nil {
		t.Fatal(err)
	}

	// A scoped pass over inside/ while inside/two.txt also disappears. The
	// in-scope deletion is observed; the OUTSIDE file must be left alone.
	if err := os.Remove(filepath.Join(root, "inside", "two.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ScanFolderPaths("data", []string{"inside"}); err != nil {
		t.Fatalf("scoped scan: %v", err)
	}

	if _, live := liveEntry(t, f.Ix, "outside/gone.txt"); !live {
		t.Fatal("a scoped pass tombstoned a file outside its scope: it only looked at inside/")
	}
	missing, err = f.Ix.MissingList()
	if err != nil {
		t.Fatal(err)
	}
	if got := missing["outside/gone.txt"].Count; got != 1 {
		t.Fatalf("outside/gone.txt confirmation count = %d after a scoped pass over inside/, want 1 (the scoped pass must not touch out-of-scope confirmations)", got)
	}
	// The in-scope miss WAS recorded (the scoped pass really looked there) and
	// the gate still needs the next observation before tombstoning it.
	if got := missing["inside/two.txt"].Count; got != 1 {
		t.Fatalf("in-scope deletion suspicion = %d, want 1", got)
	}
	if _, live := liveEntry(t, f.Ix, "inside/two.txt"); !live {
		t.Fatal("a scoped pass tombstoned an in-scope path on the FIRST miss: the confirmation gate must apply")
	}
	after, err := n.Events.Query(events.Filter{Folder: "data", Path: "outside/keep.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("a scoped pass over inside/ recorded %d event(s) for outside/keep.txt, want none", len(after)-len(before))
	}
}

// TestSecondWalkRefusedAndScopedQueueCoalesces pins both single-flight rules:
// a second FULL walk answers ErrScanBusy while one runs, and two paths added
// while a scoped pass is busy become ONE follow-up pass (the watcher's
// coalescing queue), not one pass per path.
func TestSecondWalkRefusedAndScopedQueueCoalesces(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	f := n.Folders["data"]
	f.Scanner.SetSettleScans(1)
	f.Scanner.SetSettleSeconds(0)

	writeAt(t, root, "x/slow.bin", strings.Repeat("s", 4096))
	writeAt(t, root, "x/other.bin", "other")
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	writeAt(t, root, "x/slow.bin", strings.Repeat("S", 5000))
	done, walkErr, release := parkWalk(t, n, "x/slow.bin")
	defer release()

	if _, err := n.ScanFolder("data"); !errors.Is(err, ErrScanBusy) {
		t.Fatalf("a second full walk while one runs = %v, want ErrScanBusy", err)
	}
	// The scoped slot is independent of the walk slot.
	if _, err := n.ScanFolderPaths("data", []string{"x/other.bin"}); err != nil {
		t.Fatalf("scoped scan beside a walk: %v", err)
	}
	release()
	<-done
	if err := *walkErr; err != nil {
		t.Fatal(err)
	}

	// Queue-level coalescing: while a pass runs, two more paths arrive and the
	// worker drains them together in ONE pass.
	var mu sync.Mutex
	var calls [][]string
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	q := newPathQueue(func(rels []string) error {
		mu.Lock()
		calls = append(calls, append([]string(nil), rels...))
		first := len(calls) == 1
		mu.Unlock()
		if first {
			started <- struct{}{}
			<-gate
		}
		return nil
	}, nil)
	q.Add([]string{"one.txt"})
	<-started
	q.Add([]string{"two.txt"})
	q.Add([]string{"three.txt"})
	close(gate)
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		got := len(calls)
		var second []string
		if got > 1 {
			second = calls[1]
		}
		mu.Unlock()
		if got == 2 {
			if len(second) != 2 {
				t.Fatalf("second pass covered %v, want the two paths that arrived while the first ran", second)
			}
			break
		}
		if got > 2 {
			t.Fatalf("the queue ran %d passes for three paths: paths that arrive while a pass runs must coalesce into ONE follow-up", got)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the queue did not drain the added paths (calls=%d)", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
