package sync

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"crosssync/internal/ignore"
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
	// A real session mirrors the marker in memory (node.recordPeerIndex calls
	// NoteIndexSent) so the status path never queries for it.
	n.engine.NoteIndexSent(peerID, n.engine.Ix.MaxSeq())
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

// restartEngine builds a second engine over the same root AND index DB: exactly
// what a daemon restart does to one folder. Nothing in memory survives (no peer
// views, no timestamps); the index does.
func (n *testNode) restartEngine(t *testing.T) *Engine {
	t.Helper()
	ig, _ := ignore.Parse(nil)
	return New(1, "f", n.root, ParseConflictPolicy("conflict-copy"), nil, n.ix, n.stager, ig, t.Logf)
}

// persistViewAge rewrites the recorded exchange time on disk, the way an
// exchange that completed d ago would have left it. It writes the metadata key
// directly on purpose: that key name is what an existing index DB carries, so
// the pinning is part of the contract (see index.peerViewKey).
func (n *testNode) persistViewAge(t *testing.T, peerID uint64, d time.Duration) time.Time {
	t.Helper()
	at := time.Now().Add(-d).Truncate(time.Second)
	if err := n.ix.SetMeta("peer_view_at:"+strconv.FormatUint(peerID, 10), strconv.FormatInt(at.Unix(), 10)); err != nil {
		t.Fatal(err)
	}
	return at
}

// TestPendingBasisGatesUploadOnAStaleView pins the invariant that "pending up"
// on one side and "pending down" on the other are ONE fact seen from both ends:
// the upload number may only be published when both sides still hold the same
// pair of indexes.
//
// The failure it prevents was measured live: the peer link dropped for nine
// minutes during a 32 GB scan, and one side then reported "741 files pending
// upload" from a copy of the peer's index that predated the peer's walk, while
// the peer reported 0. Two numbers, one folder, contradicting each other.
//
// The DOWNLOAD figure is a different claim and is published even then (an older
// view can only make it pull less), carrying its age so nobody reads it as
// current — see PendingBasis.
func TestPendingBasisGatesUploadOnAStaleView(t *testing.T) {
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
	// peer may have moved on and nothing has refreshed our picture of it, so the
	// UPLOAD figure (which would claim work from a stale copy of the peer) is
	// withheld. The download figure stays published, dated: it can under-report
	// but never invent work.
	staleAt := n.setViewAge(2, 2*pendingViewMaxAge)
	b = n.engine.PendingBasisState()
	if b.Up {
		t.Fatal("with a stale view and no round: upload published, want withheld")
	}
	if !b.Down {
		t.Fatalf("with a stale view and no round: download withheld (%s), want it published with its age", b.Reason)
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

	// The round ends. Now nothing refreshes the picture at all: the upload figure
	// goes back to withheld, while the download figure stays published with its
	// (by now old) age.
	n.engine.EndRound(2)
	if b := n.engine.PendingBasisState(); b.Up {
		t.Fatal("after the round ended with a stale view: upload published, want withheld")
	}
	if b := n.engine.PendingBasisState(); !b.Down || b.AsOf != at.Unix() || !strings.Contains(b.Reason, "old") {
		t.Fatalf("after the round ended: down=%v as_of=%d reason=%q, want the dated download figure kept", b.Down, b.AsOf, b.Reason)
	}
}

// TestDeadPeerKeepsDownloadWithAge: no round activity for longer than the
// freshness bound is the case the upload gate exists for — the peer may have
// moved on unseen, so that figure may not be published. The download figure is
// still published, dated with the last exchange.
func TestDeadPeerKeepsDownloadWithAge(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.convergedWithPeer(t, 2)
	n.setViewAge(2, pendingViewMaxAge+time.Minute)

	b := n.engine.PendingBasisState()
	if b.Up {
		t.Fatal("dead peer: upload published, want withheld")
	}
	if !b.Down {
		t.Fatalf("dead peer: download withheld (%s), want it published with its age", b.Reason)
	}
	if b.Reason == "" {
		t.Fatal("dead peer: no reason given for the withheld upload figure")
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
	staleAt := n.setViewAge(3, 5*time.Minute)
	n.engine.BeginRound(2)

	// Peer 2 is mid-round (fresh as of its start); peer 3 is stale with no round.
	// The numbers are per folder across all its peers, so the row is dated by the
	// oldest exchange and the upload figure stays withheld.
	if b := n.engine.PendingBasisState(); b.Up {
		t.Fatal("peer 3 has no round and a stale view: upload published, want withheld")
	}
	if b := n.engine.PendingBasisState(); !b.Down || b.AsOf != staleAt.Unix() {
		t.Fatalf("peer 3 stale: down=%v as_of=%d, want the download figure dated by the oldest exchange (%d)", b.Down, b.AsOf, staleAt.Unix())
	}
	n.engine.BeginRound(3)
	if b := n.engine.PendingBasisState(); !b.Down {
		t.Fatalf("both peers now mid-round: download withheld (%s), want published", b.Reason)
	}
}

// TestPendingBasisSurvivesRestart is the restart half of the fix: the exchange
// timestamp lives in the folder's index DB, so a restart no longer makes every
// folder report "no completed index exchange with a peer yet" until its own
// exchange lands. Measured live: a long first index for one folder left the
// others (an in-sync 741-file one included) showing "awaiting sync" for 50
// minutes.
//
// The download figure is published from the remembered time. The UPLOAD figure
// is not: the time outlived the process, the VIEW did not, and with no view in
// memory every local file would read as "the peer lacks this".
func TestPendingBasisSurvivesRestart(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	at := n.convergedWithPeer(t, 2)

	restarted := n.restartEngine(t)
	if got := restarted.PeerCount(2); got != 0 {
		t.Fatalf("restarted engine holds %d entries of the peer view, want 0 (views are not persisted)", got)
	}
	b := restarted.PendingBasisState()
	if !b.Down {
		t.Fatalf("after a restart: download withheld (%q), want it published from the persisted exchange time", b.Reason)
	}
	if b.AsOf != at.Unix() {
		t.Fatalf("after a restart: as_of = %d, want %d (the exchange the view was completed at)", b.AsOf, at.Unix())
	}
	if b.Up {
		t.Fatal("after a restart: upload published, want withheld (no view is held in this process)")
	}
	if !strings.Contains(b.Reason, "re-exchanged") {
		t.Fatalf("reason = %q, want it to say the view has not been re-exchanged since this process started", b.Reason)
	}

	// A fresh exchange in the new process replaces the remembered time in both
	// places: memory for this poll path, and the index DB for the next restart.
	restarted.SetPeerIndex(2, []*index.FileInfo{{Name: "a.txt", Size: 7, Type: index.TypeFile}}, false)
	restarted.EndPeerViewExchange(2)
	b = restarted.PendingBasisState()
	if !b.Down || b.AsOf < at.Unix() {
		t.Fatalf("after re-exchanging: down=%v as_of=%d, want a published, current figure", b.Down, b.AsOf)
	}
	persisted, ok, err := n.ix.GetMeta("peer_view_at:2")
	if err != nil || !ok {
		t.Fatalf("persisted exchange time: ok=%v err=%v", ok, err)
	}
	if persisted == "" {
		t.Fatal("persisted exchange time is empty after a completed exchange")
	}
}

// TestPendingBasisMidExchangeStillGatesAfterRestart: a truncated view is never
// truth. Remembering WHEN a view was complete must not let an exchange that is
// still appending look finished.
func TestPendingBasisMidExchangeStillGatesAfterRestart(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.convergedWithPeer(t, 2)

	restarted := n.restartEngine(t)
	restarted.BeginPeerViewExchange(2) // the exchange is still appending
	b := restarted.PendingBasisState()
	if b.Down || b.Up {
		t.Fatalf("mid-exchange after a restart: down=%v up=%v, want both withheld", b.Down, b.Up)
	}
	if !strings.Contains(b.Reason, "in progress") {
		t.Fatalf("reason = %q, want it to say the exchange is in progress", b.Reason)
	}
}

// TestPendingBasisUploadGatedAfterRestartWhenIndexAdvanced: the upload figure
// keeps its own condition (sentSeq >= ownSeq) across a restart, including when
// the index moved on while the daemon was down.
func TestPendingBasisUploadGatedAfterRestartWhenIndexAdvanced(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.convergedWithPeer(t, 2)
	// Local changes the peer has never been told about.
	n.writeFile(t, "b.txt", "changed while down")
	n.scanAndApply(t)

	restarted := n.restartEngine(t)
	// No view in this process yet: the persisted timestamp alone must never
	// publish an upload figure (it would claim every local file).
	if b := restarted.PendingBasisState(); b.Up {
		t.Fatal("after a restart with no live view: upload published, want withheld")
	}
	restarted.SetPeerIndex(2, []*index.FileInfo{
		{Name: "a.txt", Size: 7, Type: index.TypeFile},
		{Name: "b.txt", Size: 19, Type: index.TypeFile},
	}, false)
	restarted.EndPeerViewExchange(2)

	b := restarted.PendingBasisState()
	if !b.Down {
		t.Fatalf("download withheld (%s), want it published", b.Reason)
	}
	if b.Up {
		t.Fatal("upload published, want withheld: this index has changes the peer has not been sent")
	}
	if !strings.Contains(b.Reason, "not been sent") {
		t.Fatalf("reason = %q, want it to explain the withheld upload figure", b.Reason)
	}
}

// TestPersistedExchangeTimeKeepsItsAge: a timestamp restored from disk must
// never make a genuinely stale view look fresh. Freshness is measured against the
// wall clock, so a two-hour-old exchange is published WITH its age (and its
// upload figure withheld), never as if it were current.
func TestPersistedExchangeTimeKeepsItsAge(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.convergedWithPeer(t, 2)
	old := n.persistViewAge(t, 2, 2*time.Hour)

	restarted := n.restartEngine(t)
	b := restarted.PendingBasisState()
	if b.AsOf != old.Unix() {
		t.Fatalf("as_of = %d, want the persisted %d (the age must not be reset on load)", b.AsOf, old.Unix())
	}
	if age := time.Since(time.Unix(b.AsOf, 0)); age < pendingViewMaxAge {
		t.Fatalf("loaded as_of is %v old, want the true age (>= %v)", age.Round(time.Second), pendingViewMaxAge)
	}
	if !b.Down {
		t.Fatalf("download withheld (%q), want it published with its age", b.Reason)
	}
	if b.Up {
		t.Fatal("upload published from a two-hour-old view, want withheld")
	}
	if !strings.Contains(b.Reason, "old") {
		t.Fatalf("reason = %q, want it to name the view age", b.Reason)
	}
}
