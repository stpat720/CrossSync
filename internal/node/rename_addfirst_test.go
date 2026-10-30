package node

import (
	"fmt"
	"strings"
	"testing"

	"crosssync/internal/events"
)

// scanPaths drives one SCOPED scan of node n over the given relative paths —
// the cheap pass the daemon runs for watcher batches and for the held-back
// re-check (cmd/crosssync/main.go's heldTicker calls Node.ScanHeldBack, which
// scopes to the settle gate's own paths).
func (f *renameFixture) scanPaths(t *testing.T, n *Node, paths ...string) {
	t.Helper()
	if _, err := n.ScanFolderPaths("data", paths); err != nil {
		t.Fatal(err)
	}
}

// countLocalCopies counts the per-file events recording a commit that was
// satisfied from a byte-identical local copy instead of the network.
func countLocalCopies(t *testing.T, n *Node) int {
	t.Helper()
	c := 0
	for _, e := range mustQuery(t, n, "data") {
		if e.Category == events.CatApplied && strings.HasPrefix(e.Reason, "copied from local") {
			c++
		}
	}
	return c
}

// TestDirRenameAddAdvertisedFirstCostsNoBytes reproduces the ordering seen on
// the deployed cluster, which the previous fix (and its tests) missed because
// its clock ordering always released the add in a FULL scan.
//
// Production cadence: a watcher event or the held-back ticker runs a SCOPED
// pass over just the paths that changed (the new directory). The OLD paths are
// out of that scope, so they are not even counted as missing there — the
// deletion half is still unconfirmed when the quiet window elapses and the
// same scoped pass releases the ADD half. Nothing in that pass can pair the
// two halves: the walk never looked at the old directory. The add goes out
// alone, the peer downloads every byte at the new path, and the deletion half
// only follows on the next FULL scan (measured live: add at +45s, old path
// gone from the origin's index at +143s, 500 files / 2 GiB re-downloaded).
func TestDirRenameAddAdvertisedFirstCostsNoBytes(t *testing.T) {
	f := newRenameFixture(t, 5)
	// Fallback coverage: with cross-scope pairing on (the default) the scoped
	// pass proves the old path is gone and publishes a rename instead. These
	// tests exist for the case where that proof is unavailable, so the add goes
	// out alone and the peer must still satisfy it from its local copy.
	f.a.Folders["data"].Scanner.SetCrossScopePairing(false)
	engA := f.a.Folders["data"].Engine
	engB := f.b.Folders["data"].Engine
	ixA := f.a.Folders["data"].Ix
	bytesUp := engA.Stats.BytesUp.Load()
	bytesDown := engB.Stats.BytesDown.Load()
	pullsBefore := countPulls(t, f.b, "")

	f.renameDir(t, "src", "dst")

	// t+2: the change-driven pass sees dst/ for the first time (count 1).
	f.clock += 2
	f.scanPaths(t, f.a, "dst")
	// t+14: the quiet window has elapsed, so the same scoped pass releases the
	// adds. src/ was never in scope: its deletion is still unconfirmed.
	f.clock += 12
	f.scanPaths(t, f.a, "dst")

	// The add half is on the wire with no deletion half to pair it with —
	// exactly the state the deployed build reached.
	for i := range f.files {
		newRel := fmt.Sprintf("dst/f%02d.bin", i)
		if _, live := liveEntry(t, ixA, newRel); !live {
			t.Fatalf("A must advertise %s after the quiet window", newRel)
		}
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		if _, live := liveEntry(t, ixA, oldRel); !live {
			t.Fatalf("A dropped %s before its deletion was confirmed: the halves are together anyway", oldRel)
		}
	}

	gateSession(t, f.a, f.b)

	// The peer already holds byte-identical content under the old path, so it
	// must satisfy the new paths from that copy: zero bytes on the wire.
	if d := engB.Stats.BytesDown.Load() - bytesDown; d != 0 {
		t.Fatalf("B re-downloaded %d bytes when the add was advertised first, want 0 (%d pull event(s))",
			d, countPulls(t, f.b, "dst/"))
	}
	if d := engA.Stats.BytesUp.Load() - bytesUp; d != 0 {
		t.Fatalf("A served %d bytes for a rename whose content the peer already held, want 0", d)
	}
	if got := countPulls(t, f.b, "") - pullsBefore; got != 0 {
		t.Fatalf("B pulled %d file(s) for the add half, want 0", got)
	}
	if got := countLocalCopies(t, f.b); got != len(f.files) {
		t.Fatalf("B recorded %d local-copy commit(s), want %d", got, len(f.files))
	}

	// Now let the ordinary full scans confirm the deletion half and converge.
	for i := 0; i < 3; i++ {
		f.clock += 2
		mustScan(t, f.a)
		mustScan(t, f.b)
		gateSession(t, f.a, f.b)
	}

	for i := range f.files {
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		newRel := fmt.Sprintf("dst/f%02d.bin", i)
		want := strings.Repeat(fmt.Sprintf("%02d", i), 4096)
		if got := readFile(t, f.rootB, newRel); got != want {
			t.Fatalf("B %s is not byte-identical after the rename (%d bytes, want %d)", newRel, len(got), len(want))
		}
		if exists(t, f.rootB, oldRel) {
			t.Fatalf("B still holds the old path %s", oldRel)
		}
		if _, live := liveEntry(t, f.b.Folders["data"].Ix, oldRel); live {
			t.Fatalf("B still tracks the old path %s as live", oldRel)
		}
		if exists(t, f.rootA, oldRel) {
			t.Fatalf("A resurrected the old path %s", oldRel)
		}
		if _, live := liveEntry(t, ixA, oldRel); live {
			t.Fatalf("A still tracks the old path %s as live", oldRel)
		}
	}
	if d := engB.Stats.BytesDown.Load() - bytesDown; d != 0 {
		t.Fatalf("B transferred %d bytes in total for the rename, want 0", d)
	}
}

// TestDirRenameDeletionFirstCostsNoBytes is the other ordering: the deletion
// is confirmed and advertised FIRST (a full scan confirms it while the add is
// still inside the quiet window), then the add is released. The two halves must
// still reach the peer together, as a move: no transfer, and no round in which
// the peer is told to destroy the copies it is about to move.
func TestDirRenameDeletionFirstCostsNoBytes(t *testing.T) {
	f := newRenameFixture(t, 4)
	engA := f.a.Folders["data"].Engine
	engB := f.b.Folders["data"].Engine
	ixA := f.a.Folders["data"].Ix
	bytesUp := engA.Stats.BytesUp.Load()
	bytesDown := engB.Stats.BytesDown.Load()

	f.renameDir(t, "src", "dst")

	// t+2 and t+4: two full passes. src/ reaches its confirmation depth while
	// dst/ is still held by the quiet window (the gate state is persisted, so
	// the deletion is deferred rather than advertised).
	f.clock += 2
	f.scan(t, f.a)
	f.clock += 2
	f.scan(t, f.a)

	for i := range f.files {
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		newRel := fmt.Sprintf("dst/f%02d.bin", i)
		if _, live := liveEntry(t, ixA, oldRel); !live {
			t.Fatalf("A advertised the deletion of %s before its add was released", oldRel)
		}
		if _, live := liveEntry(t, ixA, newRel); live {
			t.Fatalf("A advertised %s before the quiet window elapsed", newRel)
		}
	}

	// t+14: the add is released, and the pair must be on the wire together.
	f.settle(t, f.a)
	for i := range f.files {
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		newRel := fmt.Sprintf("dst/f%02d.bin", i)
		if _, live := liveEntry(t, ixA, oldRel); live {
			t.Fatalf("A kept %s live through the release", oldRel)
		}
		if _, live := liveEntry(t, ixA, newRel); !live {
			t.Fatalf("A did not adopt %s", newRel)
		}
	}

	gateSession(t, f.a, f.b)

	if got := countMoves(t, f.b); got != len(f.files) {
		t.Fatalf("B applied %d move(s), want %d", got, len(f.files))
	}
	if got := countLocalCopies(t, f.b); got != 0 {
		t.Fatalf("B materialized %d file(s) from a local copy when the pair arrived together, want 0", got)
	}
	if d := engB.Stats.BytesDown.Load() - bytesDown; d != 0 {
		t.Fatalf("B re-downloaded %d bytes for a delete-first rename, want 0", d)
	}
	if d := engA.Stats.BytesUp.Load() - bytesUp; d != 0 {
		t.Fatalf("A served %d bytes for a delete-first rename, want 0", d)
	}
	for i := range f.files {
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		newRel := fmt.Sprintf("dst/f%02d.bin", i)
		want := strings.Repeat(fmt.Sprintf("%02d", i), 4096)
		if got := readFile(t, f.rootB, newRel); got != want {
			t.Fatalf("B %s is not byte-identical (%d bytes, want %d)", newRel, len(got), len(want))
		}
		if exists(t, f.rootB, oldRel) {
			t.Fatalf("B still holds the old path %s", oldRel)
		}
	}
}

// TestDirRenameAwayPeerSeesAddFirstCostsNoBytes: the peer is unreachable for
// the whole rename, and the origin's only passes meanwhile are the scoped ones
// its watcher and held-back ticker run — so when the peer comes back it is
// handed the ADD half while its own index still holds the old path live (no
// tombstone for the origin to pair against). It must still cost nothing: the
// content it is being offered is the content it already has.
func TestDirRenameAwayPeerSeesAddFirstCostsNoBytes(t *testing.T) {
	f := newRenameFixture(t, 4)
	f.a.Folders["data"].Scanner.SetCrossScopePairing(false) // fallback: add advertised alone
	engB := f.b.Folders["data"].Engine
	bytesDown := engB.Stats.BytesDown.Load()

	f.renameDir(t, "src", "dst")
	f.clock += 2
	f.scanPaths(t, f.a, "dst")
	f.clock += 12
	f.scanPaths(t, f.a, "dst")
	gateOfflineRound(t, f.a)
	gateOfflineRound(t, f.a)

	// The peer comes back: it has never heard of the rename, still holds the
	// old paths live, and is told about the new ones first.
	mustScan(t, f.b)
	gateSession(t, f.a, f.b)

	if d := engB.Stats.BytesDown.Load() - bytesDown; d != 0 {
		t.Fatalf("B re-downloaded %d bytes after seeing the add half first, want 0", d)
	}
	if got := countLocalCopies(t, f.b); got != len(f.files) {
		t.Fatalf("B recorded %d local-copy commit(s), want %d", got, len(f.files))
	}
	for i := range f.files {
		newRel := fmt.Sprintf("dst/f%02d.bin", i)
		want := strings.Repeat(fmt.Sprintf("%02d", i), 4096)
		if got := readFile(t, f.rootB, newRel); got != want {
			t.Fatalf("B %s is not byte-identical (%d bytes, want %d)", newRel, len(got), len(want))
		}
	}
}

// TestCopiedFileReusesLocalCopyWithoutLosingOriginal is the data-loss guard for
// the local-copy path: an ADD whose content this device already holds may be a
// genuine COPY (the source is not going anywhere), so the bytes may be reused
// but the source path must survive untouched — the same file must end up live
// under both names on both nodes, with nothing deleted.
func TestCopiedFileReusesLocalCopyWithoutLosingOriginal(t *testing.T) {
	f := newRenameFixture(t, 2)
	engA := f.a.Folders["data"].Engine
	engB := f.b.Folders["data"].Engine
	bytesUp := engA.Stats.BytesUp.Load()
	bytesDown := engB.Stats.BytesDown.Load()
	evA := len(mustQuery(t, f.a, "data"))

	want := strings.Repeat("00", 4096)
	writeAt(t, f.rootA, "copy/f00.bin", want)

	f.clock += 2
	f.scan(t, f.a)
	f.settle(t, f.a)
	for i := 0; i < 3; i++ {
		gateSession(t, f.a, f.b)
		mustScan(t, f.b)
		f.settle(t, f.a)
	}

	if got := readFile(t, f.rootB, "copy/f00.bin"); got != want {
		t.Fatalf("B copy/f00.bin is not byte-identical (%d bytes, want %d)", len(got), len(want))
	}
	if got := readFile(t, f.rootB, "src/f00.bin"); got != want {
		t.Fatalf("B lost src/f00.bin: %d bytes, want %d", len(got), len(want))
	}
	if got := readFile(t, f.rootA, "src/f00.bin"); got != want {
		t.Fatalf("A lost src/f00.bin: %d bytes, want %d", len(got), len(want))
	}
	if d := engB.Stats.BytesDown.Load() - bytesDown; d != 0 {
		t.Fatalf("B transferred %d bytes for a copy whose content it already held, want 0", d)
	}
	if d := engA.Stats.BytesUp.Load() - bytesUp; d != 0 {
		t.Fatalf("A served %d bytes for a copy, want 0", d)
	}
	// Nothing was deleted: no remote deletion on B, no local deletion on A.
	for _, rel := range []string{"src/f00.bin", "copy/f00.bin"} {
		if n := remoteDeletions(t, f.b, rel); n != 0 {
			t.Fatalf("B applied %d remote deletion(s) for %s", n, rel)
		}
		if _, live := liveEntry(t, f.b.Folders["data"].Ix, rel); !live {
			t.Fatalf("B lost the live entry for %s", rel)
		}
	}
	for _, e := range mustQuery(t, f.a, "data")[0 : len(mustQuery(t, f.a, "data"))-evA] {
		if e.Category == events.CatApplied && strings.Contains(e.Reason, "deleted") {
			t.Fatalf("A applied a deletion for a copy: %q %q", e.Reason, e.Path)
		}
	}
	if needsA, _ := engA.NeedPulls(); len(needsA) != 0 {
		t.Fatalf("not converged on A: %v", needsA)
	}
	if needsB, _ := engB.NeedPulls(); len(needsB) != 0 {
		t.Fatalf("not converged on B: %v", needsB)
	}
}
