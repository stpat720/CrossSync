package sync

import (
	"testing"
	"time"

	"crosssync/internal/index"
)

// TestPendingBasisRefusesAStaleView pins the invariant that "pending up" on one
// side and "pending down" on the other are ONE fact seen from both ends: a number
// may only be published when both sides still hold the same pair of indexes.
//
// The failure it prevents was measured live: the peer link dropped for nine
// minutes during a 32 GB scan, and one side then reported "741 files pending
// upload" from a copy of the peer's index that predated the peer's walk, while
// the peer reported 0. Two numbers, one folder, contradicting each other.
func TestPendingBasisRefusesAStaleView(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "payload")
	n.scanAndApply(t)

	// No peer view at all: nothing can be verified.
	if ok, reason := n.engine.PendingBasis(); ok || reason == "" {
		t.Fatalf("with no peer view: ok=%v reason=%q, want unverified with a reason", ok, reason)
	}

	// A completed exchange, and the peer has been sent everything we have.
	n.engine.SetPeerIndex(2, []*index.FileInfo{
		{Name: "a.txt", Size: 7, Type: index.TypeFile},
	}, false)
	n.engine.EndPeerViewExchange(2)
	// A real exchange also records what we told the peer; otherwise their "down"
	// is computed against an older copy of this index.
	id, err := n.engine.Ix.IndexID()
	if err != nil {
		t.Fatal(err)
	}
	if err := n.engine.Ix.SetPeerIndex(2, id, n.engine.Ix.MaxSeq(), "sess"); err != nil {
		t.Fatal(err)
	}
	if ok, reason := n.engine.PendingBasis(); !ok {
		t.Fatalf("after a completed exchange: unverified (%s), want verified", reason)
	}

	// The peer view ages past the freshness bound: the peer may have moved on and
	// nothing has refreshed our picture of it, so no number may be published.
	n.engine.mu.Lock()
	n.engine.viewAt[2] = time.Now().Add(-2 * pendingViewMaxAge)
	n.engine.mu.Unlock()
	if ok, reason := n.engine.PendingBasis(); ok || reason == "" {
		t.Fatalf("with a stale view: ok=%v reason=%q, want unverified with a reason", ok, reason)
	}

	// Fresh again, but this index has moved on since we last told the peer, so
	// their "down" is computed against an older copy of us than our "up" uses.
	n.engine.EndPeerViewExchange(2)
	n.writeFile(t, "b.txt", "second")
	n.scanAndApply(t)
	if ok, reason := n.engine.PendingBasis(); ok || reason == "" {
		t.Fatalf("with unsent local changes: ok=%v reason=%q, want unverified with a reason", ok, reason)
	}

	// Mid-exchange: a truncated view is never truth.
	n.engine.BeginPeerViewExchange(2)
	n.engine.mu.Lock()
	n.engine.viewAt[2] = time.Now()
	n.engine.mu.Unlock()
	if ok, reason := n.engine.PendingBasis(); ok || reason == "" {
		t.Fatalf("mid-exchange: ok=%v reason=%q, want unverified with a reason", ok, reason)
	}
}
