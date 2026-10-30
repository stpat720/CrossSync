package node

import (
	"fmt"
	"testing"
)

// TestScopedRenamePairsAndMovesOnPeer pins what the operator actually wants from
// a rename: the scoped pass that sees the new name proves the old one is gone and
// publishes ONE rename, so the peer MOVES its copy instead of re-writing it.
//
// Before this, that pass could not pair the halves (it never walked the old
// directory), so the pair went out as add + delete: the peer satisfied the add
// from its own copy — no bytes on the wire, but a full local re-write of every
// file (measured: 29.5 MiB for one folder rename) and a delete that followed
// minutes later.
func TestScopedRenamePairsAndMovesOnPeer(t *testing.T) {
	f := newRenameFixture(t, 4)
	ixA := f.a.Folders["data"].Ix
	engB := f.b.Folders["data"].Engine
	bytesDown := engB.Stats.BytesDown.Load()
	copiesBefore := engB.Stats.FilesLocalCopy.Load()

	f.renameDir(t, "src", "dst")

	// The production cadence: a watcher pass over the NEW directory, then the
	// same pass once the quiet window elapses and the adds can be released.
	f.clock += 2
	f.scanPaths(t, f.a, "dst")
	f.clock += 12
	f.scanPaths(t, f.a, "dst")

	for i := 0; i < len(f.files); i++ {
		newRel := fmt.Sprintf("dst/f%02d.bin", i)
		oldRel := fmt.Sprintf("src/f%02d.bin", i)
		if _, live := liveEntry(t, ixA, newRel); !live {
			t.Fatalf("A must publish %s", newRel)
		}
		if fi, live := liveEntry(t, ixA, oldRel); live && !fi.Deleted {
			t.Fatalf("%s is still live on A: the halves were not paired, so the peer will COPY every file instead of moving it", oldRel)
		}
	}

	gateSession(t, f.a, f.b)

	// Floor, not the goal: a move must never cost wire bytes.
	if d := engB.Stats.BytesDown.Load() - bytesDown; d != 0 {
		t.Fatalf("B pulled %d bytes for a rename, want 0", d)
	}
	// The goal: the peer MOVED its copies, so it did not rewrite the content.
	if c := engB.Stats.FilesLocalCopy.Load() - copiesBefore; c != 0 {
		t.Fatalf("B recorded %d local-copy commit(s): it re-wrote the files instead of moving them", c)
	}
}
