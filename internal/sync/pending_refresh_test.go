package sync

import (
	"testing"
	"time"
)

// TestPendingRefreshSkipsWhileRoundInFlight pins the fix for the live wedge: a
// sync round and the 5 s pending refresher fighting over the engine lock.
//
// On Tower's 1.06M-file folder the refresher rebuilt the pending lists (three
// full index walks) every tick. The round could not get the lock to apply the
// peer's index frames or plan its pulls, the peer's stall watchdog closed the
// connection ("broken pipe"), and because the sent marker only advances at the
// end of a session, every retry re-sent the whole 2.1M-entry index. Nothing
// converged for hours.
//
// The refresher must therefore do NOTHING while a round is in flight: the round
// computes what it needs itself, and the counters are display data.
func TestPendingRefreshSkipsWhileRoundInFlight(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "payload")
	n.scanAndApply(t)
	e := n.engine

	// A sentinel the refresher must not overwrite, and an invalid cache it must
	// not rebuild while a round is running.
	e.Stats.PendingPullCount.Store(4242)
	e.mu.Lock()
	e.pendingCache.valid = false
	builtAt := e.pendingCache.builtAt
	e.mu.Unlock()

	e.BeginRound(2)
	defer e.EndRound(2)
	e.Stats.lastPendingRefresh.Store(0) // clear the 500 ms floor
	e.RefreshPendingStats()

	if got := e.Stats.PendingPullCount.Load(); got != 4242 {
		t.Fatalf("pending download count = %d with a round in flight, want the untouched 4242: the refresher must not compute during a round", got)
	}
	e.mu.Lock()
	stillInvalid := !e.pendingCache.valid
	sameBuild := e.pendingCache.builtAt.Equal(builtAt)
	e.mu.Unlock()
	if !stillInvalid || !sameBuild {
		t.Fatal("the pending cache was rebuilt while a sync round was in flight: that rebuild is what starved the round")
	}

	// Once the round is over the same tick does its job.
	e.EndRound(2)
	e.Stats.lastPendingRefresh.Store(0)
	e.RefreshPendingStats()
	if got := e.Stats.PendingPullCount.Load(); got != 0 {
		t.Fatalf("pending download count = %d after the round, want 0 (a.txt is in sync)", got)
	}
	e.mu.Lock()
	valid := e.pendingCache.valid
	e.mu.Unlock()
	if !valid {
		t.Fatal("the pending cache was not rebuilt once the round ended")
	}
}

// TestPendingRefreshNeverBlocksOnALockedEngine: the refresher runs on a timer and
// must never wait for a scan or a round. If it did, a queued tick would pile up
// behind a multi-minute pass and then start its own multi-minute walk.
func TestPendingRefreshNeverBlocksOnALockedEngine(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "payload")
	n.scanAndApply(t)
	e := n.engine
	e.Stats.lastPendingRefresh.Store(0)

	e.mu.Lock()
	defer e.mu.Unlock()
	done := make(chan struct{})
	go func() {
		e.RefreshPendingStats()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RefreshPendingStats blocked on the engine mutex: every tick would queue behind a scan or a round")
	}
}

// TestPendingBasisStateNeedsNoIndexQuery pins the other half of the status-path
// promise: the gate answers from memory. It used to read the peer-index marker
// with a SQL query while holding the engine lock, so every /api/status poll
// queued behind the slowest index query — and held the lock a round needed.
// Closing the index makes any remaining query impossible, so a working answer
// proves the answer came from memory.
func TestPendingBasisStateNeedsNoIndexQuery(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "payload")
	n.scanAndApply(t)
	n.convergedWithPeer(t, 2)

	if err := n.ix.Close(); err != nil {
		t.Fatalf("close index: %v", err)
	}
	b := n.engine.PendingBasisState()
	if !b.Down || !b.Up {
		t.Fatalf("after the index was closed: down=%v up=%v reason=%q — the gate must answer from memory, not SQL", b.Down, b.Up, b.Reason)
	}
}
