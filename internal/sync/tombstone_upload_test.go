package sync

import (
	"testing"
	"time"

	"crosssync/internal/index"
	"crosssync/internal/version"
)

// TestTombstonedPathIsNotUploadWork pins the counting rule behind the UI showing
// thousands of files "pending upload" on a peer that was busy deleting exactly
// those files. A local copy the peer has TOMBSTONED is work for us to apply (it
// is already counted in the pending deletions), never upload work — unless our
// version dominates the tombstone, i.e. a genuine local edit after the peer's
// deletion, which resurrects the file and is genuinely owed to the peer.
func TestTombstonedPathIsNotUploadWork(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "gone.txt", "data")
	n.scanAndApply(t)

	local, ok, err := n.engine.Ix.Get("gone.txt")
	if err != nil || !ok {
		t.Fatalf("index read: ok=%v err=%v", ok, err)
	}

	// The peer deleted it, with a version that DOMINATES ours: the deletion wins,
	// so this device must apply it. It is not an upload.
	dominating := local.Version.Bump(2)
	n.engine.SetPeerIndex(2, []*index.FileInfo{
		{Name: "gone.txt", Deleted: true, Version: dominating, Type: index.TypeFile},
	}, false)
	up, err := n.engine.PendingUploads()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range up {
		if name == "gone.txt" {
			t.Fatalf("a path the peer tombstoned is counted as upload work: %v", up)
		}
	}
	// The mirror rule, in the COUNTERS: the file is still listed by NeedPulls
	// because the session's delete pass consults that list, but it must not be
	// counted as a download. Both counters used to include a peer-tombstoned
	// path, which is how the UI showed the same file pending in both directions.
	time.Sleep(550 * time.Millisecond) // clear the refresh floor
	n.engine.RefreshPendingStats()
	if got := n.engine.Stats.PendingPullCount.Load(); got != 0 {
		t.Fatalf("pending download count = %d, want 0: a tombstoned path is not a download", got)
	}

	// ... and it must show up as a pending deletion instead, so the two numbers
	// describe the same reality.
	dels, err := n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, name := range dels {
		if name == "gone.txt" {
			found = true
		}
	}
	if !found {
		t.Fatalf("tombstoned path missing from pending deletions: %v", dels)
	}

	// Now the other side wins: a local edit after the peer's deletion. That file
	// IS owed to the peer, so it must be counted.
	n.engine.SetPeerIndex(2, []*index.FileInfo{
		{Name: "gone.txt", Deleted: true, Version: version.New(), Type: index.TypeFile},
	}, false)
	up, err = n.engine.PendingUploads()
	if err != nil {
		t.Fatal(err)
	}
	owed := false
	for _, name := range up {
		if name == "gone.txt" {
			owed = true
		}
	}
	if !owed {
		t.Fatalf("a local version dominating the peer's tombstone is upload work, got %v", up)
	}
}
