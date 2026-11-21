package sync

import (
	"testing"
	"time"
)

// TestPendingBasisNeverBlocksOnALockedEngine pins the fix for the dead UI behind
// a hashing pass.
//
// /api/status calls this per folder per poll, and it used to take e.mu. A live
// goroutine dump on Tower (146k-entry folder, 0.1.141) showed 21 of 24 blocked
// goroutines queued on exactly that Lock while a scan held the engine lock: every
// poll stalled for minutes and the page reported it could not reach the API. The
// answer must therefore come from the last snapshot (a slightly old, honest
// figure) instead of waiting.
func TestPendingBasisNeverBlocksOnALockedEngine(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "payload")
	n.scanAndApply(t)
	n.convergedWithPeer(t, 2)
	if b := n.engine.PendingBasisState(); !b.Down {
		t.Fatalf("precondition: a converged pair must publish the download figure, got %+v", b)
	}

	n.engine.mu.Lock()
	defer n.engine.mu.Unlock()
	done := make(chan PendingBasis, 1)
	go func() { done <- n.engine.PendingBasisState() }()
	select {
	case b := <-done:
		if b.Reason == "" {
			t.Fatalf("a busy engine answered without a reason: %+v", b)
		}
		if b.Up {
			t.Fatalf("a busy engine published the upload figure (%+v); it must fall back to the gated answer", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PendingBasisState blocked on the engine mutex: /api/status would hang behind a scan")
	}
}
