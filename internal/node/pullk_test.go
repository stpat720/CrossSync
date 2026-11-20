package node

import (
	"testing"

	"crosssync/internal/config"
)

// TestPullConcurrencyIsSampledPerBatch pins the two halves of "tuning K at
// runtime is safe": the live value is used by the NEXT batch, and a batch that is
// already running keeps the K it was built with.
//
// K sits on the batch (pullBatch.k) and is read once in newPullBatch. That is the
// whole guarantee: a batch admits at most k jobs, so a change can shrink or grow
// the next batch but can never re-admit into one in flight (which would raise
// memory past the K x window budget the cap exists to bound).
func TestPullConcurrencyIsSampledPerBatch(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	f := n.Folders["data"]

	// Config default (0 = default) is what the first batch sees.
	if got := n.PullConcurrency(); got != config.DefaultPullConcurrency {
		t.Fatalf("live K = %d, want the configured default %d", got, config.DefaultPullConcurrency)
	}
	running := n.newPullBatch(nil, f)
	if running.k != config.DefaultPullConcurrency {
		t.Fatalf("batch built at the default has k = %d, want %d", running.k, config.DefaultPullConcurrency)
	}

	// Raise it: the next batch uses the new K, the running one does not.
	if got := n.SetPullConcurrency(config.MaxPullConcurrency); got != config.MaxPullConcurrency {
		t.Fatalf("SetPullConcurrency(%d) reported %d", config.MaxPullConcurrency, got)
	}
	if got := n.PullConcurrency(); got != config.MaxPullConcurrency {
		t.Fatalf("live K = %d after setting %d", got, config.MaxPullConcurrency)
	}
	next := n.newPullBatch(nil, f)
	if next.k != config.MaxPullConcurrency {
		t.Fatalf("the next batch has k = %d, want the new K %d", next.k, config.MaxPullConcurrency)
	}
	if running.k != config.DefaultPullConcurrency {
		t.Fatalf("the in-flight batch's k changed to %d when K was set at runtime: "+
			"a batch must keep the K it was built with", running.k)
	}

	// Bounds are enforced on the way in, so a caller cannot set a K the pull
	// path would then have to trust.
	if got := n.SetPullConcurrency(99); got != config.MaxPullConcurrency {
		t.Fatalf("SetPullConcurrency(99) = %d, want the cap %d", got, config.MaxPullConcurrency)
	}
	if got := n.SetPullConcurrency(0); got != 1 {
		t.Fatalf("SetPullConcurrency(0) = %d, want the floor 1", got)
	}
	if got := n.SetPullConcurrency(-3); got != 1 {
		t.Fatalf("SetPullConcurrency(-3) = %d, want the floor 1", got)
	}
	if got := n.newPullBatch(nil, f).k; got != 1 {
		t.Fatalf("batch k = %d after clamping to 1", got)
	}
}

// TestPullConcurrencyAppliesToARealRound is the other half: the live value is
// actually consumed by the pull path (not just by the batch builder a test calls
// directly). A round run after a runtime change must still converge, and must
// report every file pulled.
func TestPullConcurrencyAppliesToARealRound(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
	for _, name := range []string{"one.bin", "two.bin", "three.bin"} {
		chunkWrite(t, rootA, name, "payload for "+name)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if got := b.SetPullConcurrency(2); got != 2 {
		t.Fatalf("SetPullConcurrency(2) = %d", got)
	}
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}
	if got := b.Folders["data"].Engine.Stats.FilesDown.Load(); got != 3 {
		t.Fatalf("files pulled after a runtime K change = %d, want 3", got)
	}
}
