package sync

import (
	"strings"
	"testing"
	"time"

	"crosssync/internal/index"
)

// convergedWithPeer puts the test node in the state a healthy pair is in after
// one completed round: a local file, a complete peer view, and this index
// already sent to that peer (sentSeq == ownSeq). It returns when the view was
// completed.
func (n *testNode) convergedWithPeer(t *testing.T, peerID uint64) time.Time {
	t.Helper()
	n.writeFile(t, "a.txt", "payload")
	n.scanAndApply(t)
	n.engine.SetPeerIndex(peerID, []*index.FileInfo{
		{Name: "a.txt", Size: 7, Type: index.TypeFile},
	}, false)
	n.engine.EndPeerViewExchange(peerID)
	// A real exchange also records what we told the peer; otherwise their "down"
	// is computed against an older copy of this index.
	id, err := n.engine.Ix.IndexID()
	if err != nil {
		t.Fatal(err)
	}
	if err := n.engine.Ix.SetPeerIndex(peerID, id, n.engine.Ix.MaxSeq(), "sess"); err != nil {
		t.Fatal(err)
	}
	n.engine.mu.Lock()
	at := n.engine.viewAt[peerID]
	n.engine.mu.Unlock()
	return at
}

// setViewAge moves the recorded completion time of peerID's view back by d, the
// way it would age while no exchange happens.
func (n *testNode) setViewAge(peerID uint64, d time.Duration) time.Time {
	at := time.Now().Add(-d)
	n.engine.mu.Lock()
	n.engine.viewAt[peerID] = at
	n.engine.mu.Unlock()
	return at
}

// TestPendingBasisRefusesAStaleView pins the invariant that "pending up" on one
// side and "pending down" on the other are ONE fact seen from both ends: the
// upload number may only be published when both sides still hold the same pair
// of indexes.
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
	b := n.engine.PendingBasisState()
	if b.Down || b.Up || b.Reason == "" {
		t.Fatalf("with no peer view: down=%v up=%v reason=%q, want both withheld with a reason", b.Down, b.Up, b.Reason)
	}

	// A completed exchange, and the peer has been sent everything we have.
	n.convergedWithPeer(t, 2)
	if b := n.engine.PendingBasisState(); !b.Down || !b.Up {
		t.Fatalf("after a completed exchange: down=%v up=%v (%s), want both published", b.Down, b.Up, b.Reason)
	}

	// The peer view ages past the freshness bound with NO round in flight: the
	// peer may have moved on and nothing has refreshed our picture of it, so no
	// number may be published.
	staleAt := n.setViewAge(2, 2*pendingViewMaxAge)
	b = n.engine.PendingBasisState()
	if b.Down || b.Up {
		t.Fatalf("with a stale view and no round: down=%v up=%v, want both withheld", b.Down, b.Up)
	}
	if !strings.Contains(b.Reason, "old") {
		t.Fatalf("reason = %q, want it to name the view age", b.Reason)
	}
	if b.AsOf != staleAt.Unix() {
		t.Fatalf("as_of = %d, want %d (the stale exchange the numbers rest on)", b.AsOf, staleAt.Unix())
	}

	// Fresh again, but this index has moved on since we last told the peer: the
	// UPLOAD figure would be computed against an older copy of us, so it is
	// withheld — while the DOWNLOAD figure is still an honest (if conservative)
	// answer about what we need from the peer's view.
	n.engine.EndPeerViewExchange(2)
	n.writeFile(t, "b.txt", "second")
	n.scanAndApply(t)
	b = n.engine.PendingBasisState()
	if !b.Down {
		t.Fatalf("with unsent local changes: download withheld (%s), want published", b.Reason)
	}
	if b.Up {
		t.Fatal("with unsent local changes: upload published, want withheld (the peer has not been sent this index)")
	}
	if !strings.Contains(b.Reason, "not been sent") {
		t.Fatalf("reason = %q, want it to explain the withheld upload figure", b.Reason)
	}

	// Mid-exchange: a truncated view is never truth, in either direction.
	n.engine.BeginPeerViewExchange(2)
	n.engine.mu.Lock()
	n.engine.viewAt[2] = time.Now()
	n.engine.mu.Unlock()
	b = n.engine.PendingBasisState()
	if b.Down || b.Up {
		t.Fatalf("mid-exchange: down=%v up=%v, want both withheld", b.Down, b.Up)
	}
	if !strings.Contains(b.Reason, "exchange is in progress") {
		t.Fatalf("reason = %q, want it to say an exchange is in progress", b.Reason)
	}
}

// TestLongRoundPublishesDownloadWithAge is the measured failure this batch
// fixes: during a 74-minute sync round EVERY folder on BOTH nodes reported
// "awaiting sync" (reason "peer view is 1h13m old (no recent exchange)"),
// including folders that were idle and perfectly in sync.
//
// A live round with that peer IS freshness: the peer is demonstrably talking to
// us and the counters are recomputed when the round ends, so the view is "as of
// round start" — publishable with its age, not unknown. The upload figure stays
// gated, because while a round applies received changes sentSeq < ownSeq is
// ALWAYS true — which is precisely why the old flat rule blanked the download
// number exactly when it was most useful.
func TestLongRoundPublishesDownloadWithAge(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.convergedWithPeer(t, 2)
	// The round started long ago and is still running; no exchange has completed
	// since it began.
	at := n.setViewAge(2, 73*time.Minute)
	n.engine.BeginRound(2)
	// Changes this round is applying: not yet sent back to the peer.
	n.writeFile(t, "b.txt", "received-mid-round")
	n.scanAndApply(t)

	b := n.engine.PendingBasisState()
	if !b.Down {
		t.Fatalf("mid-round: download withheld (%s), want it published as of the round start", b.Reason)
	}
	if b.Up {
		t.Fatal("mid-round: upload published, want it withheld (sentSeq < ownSeq while receiving)")
	}
	if b.AsOf != at.Unix() {
		t.Fatalf("as_of = %d, want %d (the exchange the numbers are based on)", b.AsOf, at.Unix())
	}
	if strings.Contains(b.Reason, "no recent exchange") {
		t.Fatalf("reason = %q, still claims no recent exchange while a round is running", b.Reason)
	}

	// The round ends. Now nothing refreshes the picture at all: both figures go
	// back to withheld.
	n.engine.EndRound(2)
	if b := n.engine.PendingBasisState(); b.Down || b.Up {
		t.Fatalf("after the round ended with a stale view: down=%v up=%v, want both withheld", b.Down, b.Up)
	}
}

// TestDeadPeerGatesBoth: no round activity for longer than the freshness bound
// is the case the gate exists for — the peer may have moved on unseen, so
// neither number may be published.
func TestDeadPeerGatesBoth(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.convergedWithPeer(t, 2)
	n.setViewAge(2, pendingViewMaxAge+time.Minute)

	b := n.engine.PendingBasisState()
	if b.Down || b.Up {
		t.Fatalf("dead peer: down=%v up=%v, want both withheld", b.Down, b.Up)
	}
	if b.Reason == "" {
		t.Fatal("dead peer: no reason given for the withheld figures")
	}
	if b.AsOf == 0 {
		t.Fatal("dead peer: as_of = 0, want the last exchange time so the UI can say how old it is")
	}
}

// TestViewPendingTruncationGatesEverything: a view mid-exchange is a PREFIX of
// the peer's index. It must never be read as "the peer lacks these files", and
// this is not a staleness question at all — a fresh timestamp does not help.
func TestViewPendingTruncationGatesEverything(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.convergedWithPeer(t, 2)
	n.engine.BeginPeerViewExchange(2) // the exchange is still appending

	b := n.engine.PendingBasisState()
	if b.Down || b.Up {
		t.Fatalf("view mid-exchange: down=%v up=%v, want both withheld", b.Down, b.Up)
	}
	if !strings.Contains(b.Reason, "in progress") {
		t.Fatalf("reason = %q, want it to say the exchange is in progress", b.Reason)
	}
}

// TestRoundActivityIsPerPeer: a round with peer A must not make a stale view of
// peer B look fresh. Numbers are per folder across all its peers, so B being
// unknown still withholds both.
func TestRoundActivityIsPerPeer(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.convergedWithPeer(t, 2)
	n.engine.SetPeerIndex(3, []*index.FileInfo{{Name: "a.txt", Size: 7, Type: index.TypeFile}}, false)
	n.engine.EndPeerViewExchange(3)
	n.setViewAge(2, 5*time.Minute)
	n.setViewAge(3, 5*time.Minute)
	n.engine.BeginRound(2)

	if b := n.engine.PendingBasisState(); b.Down || b.Up {
		t.Fatalf("peer 3 has no round and a stale view: down=%v up=%v, want both withheld", b.Down, b.Up)
	}
	n.engine.BeginRound(3)
	if b := n.engine.PendingBasisState(); !b.Down {
		t.Fatalf("both peers now mid-round: download withheld (%s), want published", b.Reason)
	}
}
