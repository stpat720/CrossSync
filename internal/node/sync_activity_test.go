package node

import (
	"errors"
	"testing"
)

// TestSyncActivityClearsOnTheNextRound pins the rule the sync panel depends on:
// the recorded error survives only until the next round finishes, so
// "interrupted" cannot outlive the failure it describes.
func TestSyncActivityClearsOnTheNextRound(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())

	n.beginSync(7)
	if !n.SyncRunning() {
		t.Fatal("a session in flight must report as running")
	}
	n.endSync(errors.New("connection reset by peer"))
	if n.SyncRunning() {
		t.Fatal("session still reported as running after endSync")
	}
	if got := n.SyncActivity().Error; got == "" {
		t.Fatal("a failed round must record its error")
	}
	first := n.SyncActivity().Finished

	n.beginSync(7)
	n.endSync(nil)
	act := n.SyncActivity()
	if act.Error != "" {
		t.Fatalf("a successful round must clear the error, got %q", act.Error)
	}
	if act.Finished < first {
		t.Fatalf("finished went backwards: %d -> %d", first, act.Finished)
	}
}
