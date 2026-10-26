package node

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"crosssync/internal/hash"
	syncpkg "crosssync/internal/sync"
)

// TestPlanPulls pins run grouping: consecutive blocks coalesce up to the cap,
// a gap (a resumed temp makes needs non-contiguous) always starts a new run, and
// the short tail block is carried exactly.
func TestPlanPulls(t *testing.T) {
	const bs = 128 << 10
	mk := func(index int, size int) syncpkg.BlockNeed {
		return syncpkg.BlockNeed{Index: index, Offset: int64(index) * bs, Size: size}
	}
	seq := func(from, to int) []syncpkg.BlockNeed {
		var out []syncpkg.BlockNeed
		for i := from; i <= to; i++ {
			out = append(out, mk(i, bs))
		}
		return out
	}

	// Four consecutive blocks with a 1 MiB cap: all four fit in one run.
	runs := planPulls(seq(0, 3), 1<<20)
	if len(runs) != 1 || len(runs[0].needs) != 4 || runs[0].size != 4*bs || runs[0].offset != 0 {
		t.Fatalf("4 consecutive blocks should be one run of 512 KiB, got %+v", runs)
	}

	// Cap below one block: every block is its own run (the old behaviour).
	runs = planPulls(seq(0, 2), bs-1)
	if len(runs) != 3 {
		t.Fatalf("cap under one block should give per-block runs, got %d runs", len(runs))
	}

	// A gap must break the run: 0,1 then 5,6.
	gapped := append(seq(0, 1), seq(5, 6)...)
	runs = planPulls(gapped, 1<<20)
	if len(runs) != 2 || len(runs[0].needs) != 2 || len(runs[1].needs) != 2 {
		t.Fatalf("a gap should split runs, got %d runs", len(runs))
	}
	if runs[1].offset != 5*bs {
		t.Fatalf("second run offset = %d, want %d", runs[1].offset, 5*bs)
	}

	// Never over the peer's hard per-request limit, even with a huge cap.
	runs = planPulls(seq(0, 300), hash.MaxBlockSize*4)
	for _, r := range runs {
		if r.size > hash.MaxBlockSize {
			t.Fatalf("run of %d bytes exceeds the %d-byte request limit", r.size, hash.MaxBlockSize)
		}
	}

	// A short tail block stays inside its run, and the sizes still sum exactly.
	tail := append(seq(0, 1), mk(2, 4096))
	runs = planPulls(tail, 1<<20)
	if len(runs) != 1 || runs[0].size != 2*bs+4096 {
		t.Fatalf("tail block not carried: %+v", runs)
	}

	if got := planPulls(nil, 1<<20); got != nil {
		t.Fatalf("no needs should plan no runs, got %+v", got)
	}
}

// TestCoalescedPullIsByteExact proves the wiring end to end: a file is pulled
// with runs small enough to need several requests, every byte still compares,
// and the wire really carried multi-block requests (not one per block).
func TestCoalescedPullIsByteExact(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
	b.Cfg.PullRunKB = 300 // two 128 KiB blocks per run

	const size = 2<<20 + 777 // 17 blocks, short tail
	body := pattern(41, size, rand.New(rand.NewSource(41)))
	writePattern(t, rootA, "coalesced.bin", body)
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	log := &wireLog{}
	if err := latencyRound(t, a, b, time.Millisecond, log); err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(rootB, "coalesced.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("content mismatch after a coalesced pull: %d bytes", len(got))
	}

	fi, ok, err := b.Folders["data"].Ix.Get("coalesced.bin")
	if err != nil || !ok {
		t.Fatalf("missing index entry: ok=%v err=%v", ok, err)
	}
	blocks := len(fi.Blocks)
	if reqs := log.requests(); reqs >= blocks {
		t.Fatalf("%d request(s) for %d blocks: block coalescing did not happen", reqs, blocks)
	}
	if max := log.maxRequestSize(); max <= 128<<10 {
		t.Fatalf("largest request was %d bytes: no multi-block run went over the wire", max)
	}
	t.Logf("pulled %d blocks in %d request(s), largest %d bytes", blocks, log.requests(), log.maxRequestSize())
}

// TestCoalescedPullWithTightBudget: the byte budget must never wedge the batch —
// with a budget barely above one run it still completes, byte-exact.
func TestCoalescedPullWithTightBudget(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
	b.Cfg.PullRunKB = 256
	b.Cfg.PullInFlightMB = 1 // a few runs in flight, then backpressure

	const size = 1<<20 + 3
	body := pattern(43, size, rand.New(rand.NewSource(43)))
	writePattern(t, rootA, "tight.bin", body)
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- chunkPipe(t, a, b) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pull with a tight byte budget failed: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("pull with a tight byte budget wedged (no progress in 60s)")
	}
	got, err := os.ReadFile(filepath.Join(rootB, "tight.bin"))
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("content mismatch with a tight budget (err=%v)", err)
	}
}
