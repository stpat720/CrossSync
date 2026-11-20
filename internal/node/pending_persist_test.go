package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	syncpkg "crosssync/internal/sync"
)

// TestRoundPersistsPeerViewTime is the wiring proof for the restart fix: the
// exchange time the pending gate publishes is written by a REAL round (the final
// index frame of the session), and a fresh engine over the same folder index --
// i.e. a daemon restart -- reads it back and publishes the download figure
// without waiting for a new exchange.
//
// Without it, a restart blanked the figure for every folder at once: an in-sync
// 741-file folder read "awaiting sync" for 50 minutes because another folder's
// first index was still being built.
func TestRoundPersistsPeerViewTime(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
	if err := os.WriteFile(filepath.Join(rootA, "x.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, n := range []*Node{a, b} {
		if _, err := n.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
	}
	if err := latencyRound(t, a, b, time.Millisecond, nil); err != nil {
		t.Fatalf("round: %v", err)
	}

	f := b.Folders["data"]
	if _, ok, err := f.Ix.GetMeta("peer_view_at:1"); err != nil || !ok {
		t.Fatalf("the round left no peer_view_at:1 in the folder index (ok=%v err=%v)", ok, err)
	}
	before := f.Engine.PendingBasisState()
	if !before.Down || before.AsOf == 0 {
		t.Fatalf("after the round: down=%v as_of=%d (%s), want the figure published", before.Down, before.AsOf, before.Reason)
	}

	// A second engine over the same index DB: exactly what a restart does.
	restarted := syncpkg.New(2, "data", f.Root, f.Engine.Policy, nil, f.Ix, f.Engine.Stager, f.Engine.Ignore, t.Logf)
	if restarted.PeerCount(1) != 0 {
		t.Fatal("the restarted engine holds a peer view; views are not persisted")
	}
	after := restarted.PendingBasisState()
	if !after.Down {
		t.Fatalf("after a restart: download withheld (%q), want it published from the persisted time", after.Reason)
	}
	if after.AsOf != before.AsOf {
		t.Fatalf("after a restart: as_of = %d, want the round's %d", after.AsOf, before.AsOf)
	}
	if after.Up {
		t.Fatal("after a restart: upload published, want withheld (no view is held in this process)")
	}
}
