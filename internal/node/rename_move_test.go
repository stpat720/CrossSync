package node

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/events"
)

// renameNode builds a node with the PRODUCTION scan settings: the stability
// gate needs settleScans unchanged observations AND a quiet window of
// settleSeconds, and a deletion needs DeleteConfirmScans consecutive misses.
// The clock is driven by the test (Scanner.SetClock), so the quiet window
// costs no wall time.
func renameNode(t *testing.T, id uint64, name, root, metaDir string, settleScans int) *Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: id, Name: name},
		MetaDir: metaDir,
		Folders: []config.Folder{{
			ID: "data", Path: root, ConflictPolicy: "conflict-copy",
			SettleScans: settleScans, SettleSeconds: 10,
		}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// renameFixture converges two nodes on a "src/" directory of n files written
// with stable mtimes, then returns everything the test needs to watch a
// directory rename.
type renameFixture struct {
	a, b         *Node
	rootA, rootB string
	clock        int64
	files        []string
}

func newRenameFixture(t *testing.T, n int) *renameFixture {
	t.Helper()
	f := &renameFixture{
		rootA: t.TempDir(),
		rootB: t.TempDir(),
		clock: 1_000_000,
	}
	f.a = renameNode(t, 1, "a", f.rootA, filepath.Join(t.TempDir(), "meta-a"), 2)
	f.b = renameNode(t, 2, "b", f.rootB, filepath.Join(t.TempDir(), "meta-b"), 2)
	f.a.Folders["data"].Scanner.SetClock(func() int64 { return f.clock })
	f.b.Folders["data"].Scanner.SetClock(func() int64 { return f.clock })

	for i := 0; i < n; i++ {
		rel := fmt.Sprintf("src/f%02d.bin", i)
		// Distinct, non-trivial content per file: a re-download is visible in
		// bytes_down, and identical content would let a bad pairing hide.
		body := strings.Repeat(fmt.Sprintf("%02d", i), 4096)
		writeAt(t, f.rootA, rel, body)
		f.files = append(f.files, rel)
	}
	// Two scans past the quiet window: the files settle on the second.
	mustScan(t, f.a)
	f.clock += 12
	mustScan(t, f.a)
	mustScan(t, f.b)
	gateSession(t, f.a, f.b)
	for _, rel := range f.files {
		want := strings.Repeat(fmt.Sprintf("%02d", indexOf(t, f, rel)), 4096)
		if got := readFile(t, f.rootB, rel); got != want {
			t.Fatalf("setup: B %s = %d bytes, want %d", rel, len(got), len(want))
		}
	}
	return f
}

func indexOf(t *testing.T, f *renameFixture, rel string) int {
	t.Helper()
	for i, r := range f.files {
		if r == rel {
			return i
		}
	}
	t.Fatalf("%s is not in the fixture", rel)
	return 0
}

// scan drives one scan of node n at the fixture's current clock.
func (f *renameFixture) scan(t *testing.T, n *Node) {
	t.Helper()
	mustScan(t, n)
}

// settle advances past the quiet window and scans, which releases whatever the
// stability gate held.
func (f *renameFixture) settle(t *testing.T, n *Node) {
	t.Helper()
	f.clock += 12
	mustScan(t, n)
}

// renameDir renames old to new on node a's disk.
func (f *renameFixture) renameDir(t *testing.T, old, new string) {
	t.Helper()
	if err := os.Rename(filepath.Join(f.rootA, old), filepath.Join(f.rootA, new)); err != nil {
		t.Fatal(err)
	}
}

// countPulls counts applied "pulled from peer" events on n under prefix.
func countPulls(t *testing.T, n *Node, prefix string) int {
	t.Helper()
	c := 0
	for _, e := range mustQuery(t, n, "data") {
		if e.Category == events.CatApplied && e.Reason == "pulled from peer" && strings.HasPrefix(e.Path, prefix) {
			c++
		}
	}
	return c
}

// countMoves counts the per-file move events on n.
func countMoves(t *testing.T, n *Node) int {
	t.Helper()
	c := 0
	for _, e := range mustQuery(t, n, "data") {
		if e.Category == events.CatApplied && strings.Contains(e.Reason, "moved") && e.Path != "" {
			c++
		}
	}
	return c
}

// TestDirRenameAppliesAsMoveOnPeer is the regression test for a directory
// rename that degraded into a full re-download on the peer plus a silent
// resurrection of the old path on the origin.
//
// The cadence is the production one: change-driven scans arrive far faster
// than the settle gate's quiet window, so the DELETION half of a rename is
// confirmed (and advertised) several scans before the ADD half is released by
// the stability gate. A round that lands in that window tells the peer "these
// paths are gone" and nothing else; the peer destroys/parks its copies, and
// when the adds finally arrive there is nothing left to move, so it downloads
// every byte again.
func TestDirRenameAppliesAsMoveOnPeer(t *testing.T) {
	f := newRenameFixture(t, 5)
	engA := f.a.Folders["data"].Engine
	engB := f.b.Folders["data"].Engine
	bytesA := engA.Stats.BytesDown.Load()
	bytesB := engB.Stats.BytesDown.Load()
	pullsBefore := countPulls(t, f.b, "")

	f.renameDir(t, "src", "dst")

	// t+0: the walk sees dst/ settling (count 1) and src/ missing (count 1).
	f.clock += 2
	f.scan(t, f.a)
	// t+2: src/ is confirmed deleted, dst/ is still inside the quiet window.
	f.clock += 2
	f.scan(t, f.a)
	// A round in this window must not be able to tell the peer anything that
	// makes it destroy the old copies: the rename is not resolved yet.
	gateSession(t, f.a, f.b)

	// t+14: the window has passed, so the adds are released.
	f.settle(t, f.a)
	mustScan(t, f.b)
	gateSession(t, f.a, f.b)

	// A few more rounds to let everything settle and prove there is no
	// resurrection loop.
	for i := 0; i < 3; i++ {
		f.settle(t, f.a)
		mustScan(t, f.b)
		gateSession(t, f.a, f.b)
	}

	for i := range f.files {
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		newRel := fmt.Sprintf("dst/f%02d.bin", i)
		want := strings.Repeat(fmt.Sprintf("%02d", i), 4096)
		if exists(t, f.rootB, oldRel) {
			t.Fatalf("B still holds the old path %s after the rename", oldRel)
		}
		if got := readFile(t, f.rootB, newRel); got != want {
			t.Fatalf("B %s = %d bytes, want %d", newRel, len(got), len(want))
		}
		if exists(t, f.rootA, oldRel) {
			t.Fatalf("A resurrected the old path %s (the rename was applied locally)", oldRel)
		}
		if _, live := liveEntry(t, f.a.Folders["data"].Ix, oldRel); live {
			t.Fatalf("A still tracks the old path %s as live", oldRel)
		}
	}
	if d := engB.Stats.BytesDown.Load() - bytesB; d != 0 {
		t.Fatalf("B re-downloaded %d bytes for a rename, want 0 (%d pull event(s) under dst/)",
			d, countPulls(t, f.b, "dst/"))
	}
	if got := countPulls(t, f.b, "") - pullsBefore; got != 0 {
		t.Fatalf("B pulled %d file(s) for a rename, want 0 (a move transfers no bytes)", got)
	}
	if d := engA.Stats.BytesDown.Load() - bytesA; d != 0 {
		t.Fatalf("A re-downloaded %d bytes (the old path came back)", d)
	}
	if got := countMoves(t, f.b); got != len(f.files) {
		t.Fatalf("B recorded %d move event(s), want %d", got, len(f.files))
	}
	// The peer must never have been told to destroy the old copies as part of
	// the rename: a move is a rename on both sides, not a delete + download.
	for i := range f.files {
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		if n := remoteDeletions(t, f.b, oldRel); n != 0 {
			t.Fatalf("B applied %d remote deletion(s) for %s: the deletion half of the rename went out alone", n, oldRel)
		}
		if _, live := liveEntry(t, f.b.Folders["data"].Ix, oldRel); live {
			t.Fatalf("B still tracks the old path %s as live", oldRel)
		}
	}
	// The old directory is gone on both sides, not left behind as an empty
	// husk the next scan would re-add.
	for _, root := range []string{f.rootA, f.rootB} {
		if exists(t, root, "src") {
			t.Fatalf("the old directory survived the rename on %s", root)
		}
	}
}

// TestDirRenameWhilePeerAwayStillMoves: the peer is unreachable for the whole
// rename, so it never sees an intermediate state. When it comes back the pair
// must still arrive together and be applied as a move — a peer that saw
// nothing may not be handed a delete-only view.
func TestDirRenameWhilePeerAwayStillMoves(t *testing.T) {
	f := newRenameFixture(t, 4)
	engB := f.b.Folders["data"].Engine
	bytesB := engB.Stats.BytesDown.Load()

	f.renameDir(t, "src", "dst")
	f.clock += 2
	f.scan(t, f.a)
	// Rounds fail while the peer is away; nothing may be advertised that would
	// strand the peer's copies if it had been listening.
	for i := 0; i < 3; i++ {
		f.clock += 2
		f.scan(t, f.a)
		gateOfflineRound(t, f.a)
	}
	f.settle(t, f.a)
	f.settle(t, f.a)

	// The peer is back: one round must apply the whole rename as moves.
	mustScan(t, f.b)
	gateSession(t, f.a, f.b)
	for i := 0; i < 2; i++ {
		f.settle(t, f.a)
		mustScan(t, f.b)
		gateSession(t, f.a, f.b)
	}

	if d := engB.Stats.BytesDown.Load() - bytesB; d != 0 {
		t.Fatalf("B re-downloaded %d bytes after the rename happened while it was away, want 0", d)
	}
	for i := range f.files {
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		newRel := fmt.Sprintf("dst/f%02d.bin", i)
		if exists(t, f.rootB, oldRel) {
			t.Fatalf("B still holds %s", oldRel)
		}
		if got := readFile(t, f.rootB, newRel); len(got) != 8192 {
			t.Fatalf("B lost %s (%d bytes)", newRel, len(got))
		}
		if n := remoteDeletions(t, f.b, oldRel); n != 0 {
			t.Fatalf("B applied %d remote deletion(s) for %s instead of moving it", n, oldRel)
		}
	}
}

// TestRenameDoesNotResurrectOldPath is the second half, isolated: once the
// peer has applied the move, further rounds must leave the old path dead on
// BOTH nodes — no pull, no local delete, no new live entry for it.
func TestRenameDoesNotResurrectOldPath(t *testing.T) {
	f := newRenameFixture(t, 3)
	engA := f.a.Folders["data"].Engine
	engB := f.b.Folders["data"].Engine

	f.renameDir(t, "src", "dst")
	// Straight through the gates: two immediate scans, then the window.
	f.clock += 2
	f.scan(t, f.a)
	f.clock += 2
	f.scan(t, f.a)
	f.settle(t, f.a)
	mustScan(t, f.b)
	for i := 0; i < 3; i++ {
		gateSession(t, f.a, f.b)
		f.settle(t, f.a)
		mustScan(t, f.b)
	}
	for i := range f.files {
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		if _, live := liveEntry(t, f.a.Folders["data"].Ix, oldRel); live {
			t.Fatalf("A holds the old path %s live after the move", oldRel)
		}
		if _, live := liveEntry(t, f.b.Folders["data"].Ix, oldRel); live {
			t.Fatalf("B holds the old path %s live after the move", oldRel)
		}
	}

	bytesA := engA.Stats.BytesDown.Load()
	bytesB := engB.Stats.BytesDown.Load()
	evA := len(mustQuery(t, f.a, "data"))
	evB := len(mustQuery(t, f.b, "data"))

	// Three idle rounds: a converged pair adds nothing and transfers nothing.
	for i := 0; i < 3; i++ {
		f.settle(t, f.a)
		mustScan(t, f.b)
		gateSession(t, f.a, f.b)
	}

	if d := engA.Stats.BytesDown.Load() - bytesA; d != 0 {
		t.Fatalf("A pulled %d bytes in idle rounds: the old path was resurrected", d)
	}
	if d := engB.Stats.BytesDown.Load() - bytesB; d != 0 {
		t.Fatalf("B pulled %d bytes in idle rounds", d)
	}
	for _, e := range mustQuery(t, f.a, "data")[0 : len(mustQuery(t, f.a, "data"))-evA] {
		if strings.HasPrefix(e.Path, "src/") {
			t.Fatalf("A recorded an event for the dead old path: %q %q", e.Reason, e.Path)
		}
	}
	if got := len(mustQuery(t, f.b, "data")) - evB; got != 0 {
		t.Fatalf("B recorded %d event(s) in idle rounds, want 0 (churn)", got)
	}
	for i := range f.files {
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		if exists(t, f.rootA, oldRel) || exists(t, f.rootB, oldRel) {
			t.Fatalf("the old path %s came back on disk", oldRel)
		}
		if got := readFile(t, f.rootB, fmt.Sprintf("dst/f%02d.bin", i)); len(got) != 8192 {
			t.Fatalf("B lost dst/f%02d.bin: %d bytes", i, len(got))
		}
	}
}
