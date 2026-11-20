package node

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TestPullStageTimingsAreRecorded proves the instrumentation that is supposed to
// explain the ~9-10 s fixed per-file cost actually measures a real pull.
//
// Measured live: a 57.8 GB transfer ran at t_file ~ 9-10 s + size / ~8.6 MB/s per
// stream, so for a small file the fixed part is 75-85 % of its lifetime. The
// stages (wait for first block, transfer, fsync, commit) attribute that cost; if
// they were not wired to the pull path, or summed to more than the round's own
// wall time, the numbers would be a plausible-looking fabrication.
func TestPullStageTimingsAreRecorded(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
	const files = 3
	// 6 MiB each: bigger than the default 4 MiB pull_run_kb, so a file takes
	// two wire runs and the transfer stage (first run's answer -> last run's
	// answer) is a measurable interval. A SMALLER file is asked for in one
	// coalesced run, so its first and last block land in the same frame and
	// "transfer" is legitimately 0 — its whole wire time shows up in
	// wait_first_block instead. That is why this test uses large files.
	//
	// Distinct content per file (pattern, not a repeated byte): identical bytes
	// would let the receiver satisfy the later files from a local copy, which
	// would measure the local-reuse path instead of the transfer.
	rng := rand.New(rand.NewSource(97))
	for i := 0; i < files; i++ {
		writePattern(t, rootA, fmt.Sprintf("f-%d.bin", i), pattern(byte(100+i), 6<<20, rng))
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// K=1 so the three files' lives are SEQUENTIAL: the stage totals then sum to
	// less than the round's own wall time (with K files in flight the lives
	// overlap and the sum legitimately exceeds it). This also exercises the
	// runtime K knob through the real pull path.
	if got := b.SetPullConcurrency(1); got != 1 {
		t.Fatalf("SetPullConcurrency(1) = %d", got)
	}

	start := time.Now()
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}
	wall := time.Since(start)

	st := &b.Folders["data"].Engine.Stats
	if got := st.PullFiles.Load(); got != files {
		t.Fatalf("measured pulls = %d, want %d (files_down = %d)", got, files, st.FilesDown.Load())
	}
	wait := st.PullWaitFirstNs.Load()
	transfer := st.PullTransferNs.Load()
	fsync := st.PullFsyncNs.Load()
	commit := st.PullCommitNs.Load()
	for _, stage := range []struct {
		name string
		ns   int64
	}{
		{"wait_first_block", wait},
		{"transfer", transfer},
		{"fsync", fsync},
		{"commit", commit},
	} {
		if stage.ns <= 0 {
			t.Fatalf("stage %s = %d ns after a real pull, want a positive total: "+
				"the instrument is not measuring that stage", stage.name, stage.ns)
		}
	}

	// The four stages are sub-intervals of each file's life inside the batch, and
	// the batch is inside the round, so their sum cannot exceed the round's wall
	// time. A sum that exceeds it means a stage is being double-counted.
	sum := wait + transfer + fsync + commit
	if sum >= wall.Nanoseconds() {
		t.Fatalf("stage totals sum to %d ns but the whole round took %d ns: the stages must be "+
			"disjoint intervals of the files' lives", sum, wall.Nanoseconds())
	}
	// The batch wall time is the denominator for "how much of a round is
	// per-file cost": it must be recorded, positive, and inside the round.
	if got := st.PullBatches.Load(); got != 1 {
		t.Fatalf("pull batches recorded = %d, want 1", got)
	}
	batch := st.PullBatchNs.Load()
	if batch <= 0 || batch > wall.Nanoseconds() {
		t.Fatalf("batch wall time = %d ns, want positive and within the round's %d ns", batch, wall.Nanoseconds())
	}
	// Log the split so a failing run shows the shape that broke, not just a
	// count: the whole point of the instrument is that the numbers are readable.
	t.Logf("round %s for %d files: wait %.3fs transfer %.3fs fsync %.3fs commit %.3fs (batch %.3fs)",
		wall, files,
		float64(wait)/1e9, float64(transfer)/1e9, float64(fsync)/1e9, float64(commit)/1e9,
		float64(batch)/1e9)
}
