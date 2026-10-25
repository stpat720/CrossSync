package node

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/events"
)

// TestPullConcurrentIntegrity is the corruption test for in-flight file
// concurrency.
//
// Every block response carries only {ID, Data, Code}, and the ID comes from the
// CONNECTION, so once several files have requests outstanding the only thing
// that keeps bytes in the right file is the batch's in-flight map. A mix-up
// would be silent: a valid block of B written into A's staging temp, verified
// as "a block", committed as A. So the corpus is built to make one impossible
// to miss — several files of IDENTICAL size with distinct content, and two
// multi-block files of identical size whose blocks differ — and every byte of
// every file is compared afterwards.
//
// It runs over a latency link, which is what forces the responses of different
// files to interleave instead of arriving grouped per file on loopback, and it
// asserts that interleaving really happened (otherwise the test would be
// green for a pull that had quietly gone back to one file at a time).
func TestPullConcurrentIntegrity(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("randomised corpus seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))

	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
	b.Cfg.PullConcurrency = config.DefaultPullConcurrency

	type spec struct {
		name string
		size int
	}
	// Four files of ONE size (4096) with four different contents, plus two
	// multi-block files of one size with different contents: a block written
	// into the wrong file's temp can never pass verification, and a file
	// assembled from another file's blocks shows up as a byte mismatch.
	specs := []spec{
		{"empty.bin", 0},
		{"one-byte.bin", 1},
		{"exact-block.bin", 128 << 10}, // exactly one full block
		{"pair-a.bin", 4096},
		{"pair-b.bin", 4096},
		{"pair-c.bin", 4096},
		{"pair-d.bin", 4096},
		{"multi-1.bin", 2<<20 + 7},
		{"multi-2.bin", 2<<20 + 7},
	}
	for i := 0; i < 4; i++ { // plus random single-block sizes
		specs = append(specs, spec{fmt.Sprintf("rand-%d.bin", i), rng.Intn(300 << 10)})
	}

	want := map[string][]byte{}
	for i, s := range specs {
		data := pattern(byte(i*37+1), s.size, rng)
		want[s.name] = data
		writePattern(t, rootA, s.name, data)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// 2 ms each way: ~4 ms RTT, enough that responses of different files
	// arrive interleaved instead of in lockstep with the sends.
	log := &wireLog{}
	if err := latencyRound(t, a, b, 2*time.Millisecond, log); err != nil {
		t.Fatalf("round failed: %v", err)
	}

	for _, s := range specs {
		got, err := os.ReadFile(filepath.Join(rootB, filepath.FromSlash(s.name)))
		if err != nil {
			t.Fatalf("%s missing after a concurrent pull: %v", s.name, err)
		}
		if !bytes.Equal(got, want[s.name]) {
			// Name the first differing byte: a mixed-up block shows up as a
			// long identical prefix, a truncated/zeroed pull as a short one.
			diff := -1
			for i := 0; i < len(got) && i < len(want[s.name]); i++ {
				if got[i] != want[s.name][i] {
					diff = i
					break
				}
			}
			t.Fatalf("%s: content mismatch after a concurrent pull (got %d bytes, want %d, first difference at %d) — a block was written into the wrong file",
				s.name, len(got), len(want[s.name]), diff)
		}
		fi, ok, err := b.Folders["data"].Ix.Get(s.name)
		if err != nil || !ok {
			t.Fatalf("%s not indexed on b: ok=%v err=%v", s.name, ok, err)
		}
		if fi.Size != int64(s.size) {
			t.Fatalf("%s indexed with size %d, want %d", s.name, fi.Size, s.size)
		}
	}

	// No conflict copy, and exactly one transfer per file (no double pull).
	if matches, err := filepath.Glob(filepath.Join(rootB, "*.sync-conflict-*")); err == nil && len(matches) > 0 {
		t.Fatalf("concurrent pull produced conflict copies: %v", matches)
	}
	if got := pullEventsFor(t, b); got != len(specs) {
		t.Fatalf("%d pull events for %d files: a name was pulled more than once", got, len(specs))
	}

	// Proof the requests of different files really were outstanding at once.
	if log.distinct() < 2 || log.transitions() == 0 {
		t.Fatalf("block requests never interleaved across files (%d distinct names, %d transitions) — the pull did not run concurrently",
			log.distinct(), log.transitions())
	}
	t.Logf("pulled %d files concurrently: %d block requests, %d interleaved file switches",
		len(specs), len(log.names), log.transitions())
}

// TestPullConcurrentFailureIsolation pins the two per-file failure modes under
// concurrency: a peer-side refusal (code 1) for one name, and a block that
// fails verification for another. Neither may abort the round, and neither may
// take the other files down with it — the failure of one job must not leave
// another job's staging temp short of blocks or drop its responses.
func TestPullConcurrentFailureIsolation(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	good := []string{"good-0.txt", "good-1.txt", "good-2.txt", "good-3.txt", "good-4.txt", "good-5.txt"}
	for i, name := range good {
		chunkWrite(t, rootA, name, fmt.Sprintf("settled bytes %d", i))
	}
	chunkWrite(t, rootA, "refused.txt", "bytes the source disk will refuse")
	chunkWrite(t, rootA, "corrupt.txt", "content the index no longer describes")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// refused.txt: the peer's disk refuses the file (a directory of the same
	// name), so serveBlock answers the generic code 1.
	p := filepath.Join(rootA, "refused.txt")
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p, 0o755); err != nil {
		t.Fatal(err)
	}
	// corrupt.txt: the source bytes changed after it was indexed, so the
	// block that comes back does not match the recorded hash and the puller's
	// verification rejects it. This is the "a block fails verification" path.
	if err := os.WriteFile(filepath.Join(rootA, "corrupt.txt"), []byte("DIFFERENT CONTENT, SAME LENGTH!!! (not what was hashed)"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := latencyRound(t, a, b, time.Millisecond, nil); err != nil {
		t.Fatalf("one refused file and one corrupt block must not abort the round: %v", err)
	}

	for i, name := range good {
		got, err := os.ReadFile(filepath.Join(rootB, filepath.FromSlash(name)))
		if err != nil || string(got) != fmt.Sprintf("settled bytes %d", i) {
			t.Fatalf("%s must still be pulled despite two failing files: %q, %v", name, got, err)
		}
	}
	for _, name := range []string{"refused.txt", "corrupt.txt"} {
		if _, ok, err := b.Folders["data"].Ix.Get(name); err != nil || ok {
			t.Fatalf("%s must not be indexed on b after failing: ok=%v err=%v", name, ok, err)
		}
		evs, err := b.Events.Query(events.Filter{Folder: "data", Path: name, Category: events.CatUnsynced, OpenOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) != 1 {
			t.Fatalf("want exactly 1 open unsynced event for %s, got %d", name, len(evs))
		}
		if !strings.Contains(evs[0].Reason, "retry") && !strings.Contains(evs[0].Reason, "re-request") {
			t.Fatalf("%s: reason must say it is being retried, got %q", name, evs[0].Reason)
		}
	}
}

// TestPullConcurrentSmallFilesThroughput measures the thing the change is for:
// many small files, each costing one round trip, over a link with the same
// latency class as the live Tailscale hop. K=1 is the sequential baseline the
// daemon shipped before; the default K is the same corpus pulled concurrently.
// Both numbers are printed; the assertion is deliberately loose (a shared CI
// box is noisy) but it cannot pass on a pull that did not overlap files.
func TestPullConcurrentSmallFilesThroughput(t *testing.T) {
	const files = 200
	const size = 1024
	const oneWay = 5 * time.Millisecond

	rng := rand.New(rand.NewSource(20261025))
	rootA := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	wantData := make(map[string][]byte, files)
	for i := 0; i < files; i++ {
		name := fmt.Sprintf("small-%03d.bin", i)
		data := pattern(byte(i+1), size, rng)
		wantData[name] = data
		writePattern(t, rootA, name, data)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// pullWith builds a fresh receiver with the given K and times ONE round.
	pullWith := func(peerID uint64, k int) (time.Duration, *Node) {
		rootB := t.TempDir()
		b := chunkTestNode(t, peerID, fmt.Sprintf("b%d", k), rootB, t.TempDir())
		b.Cfg.PullConcurrency = k
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := latencyRound(t, a, b, oneWay, nil); err != nil {
			t.Fatalf("K=%d round failed: %v", k, err)
		}
		elapsed := time.Since(start)
		for name, want := range wantData {
			got, err := os.ReadFile(filepath.Join(rootB, filepath.FromSlash(name)))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("K=%d: %s did not arrive byte-identically (err=%v)", k, name, err)
			}
		}
		if got := pullEventsFor(t, b); got != files {
			t.Fatalf("K=%d: %d pull events for %d files", k, got, files)
		}
		return elapsed, b
	}

	seq, _ := pullWith(2, 1)
	conc, _ := pullWith(3, config.DefaultPullConcurrency)

	seqRate := float64(files) / seq.Seconds()
	concRate := float64(files) / conc.Seconds()
	t.Logf("link: %.0f ms one-way (%.0f ms RTT), %d files of %d B (%d KiB total)", oneWay.Seconds()*1000, 2*oneWay.Seconds()*1000, files, size, files*size/1024)
	t.Logf("sequential (K=1): %8.1f ms  %6.1f files/s  %7.1f KiB/s", seq.Seconds()*1000, seqRate, seqRate*size/1024)
	t.Logf("concurrent (K=%d): %8.1f ms  %6.1f files/s  %7.1f KiB/s", config.DefaultPullConcurrency, conc.Seconds()*1000, concRate, concRate*size/1024)
	t.Logf("speedup: %.2fx", concRate/seqRate)

	if concRate < seqRate*1.5 {
		t.Fatalf("concurrent pull is only %.2fx the sequential rate (%.1f vs %.1f files/s): files are still serialized on the round trip", concRate/seqRate, concRate, seqRate)
	}
}

// TestPullConcurrentDuplicateGuard pins the one-pull-per-name rule: the round's
// name list is unique by construction (NeedPulls walks the index once per
// name), and the batch refuses to start a name that is already in flight even
// if that ever changes — two concurrent pulls of one name would stage into one
// temp file, interleave blocks into it and race for the same commit.
func TestPullConcurrentDuplicateGuard(t *testing.T) {
	b := &pullBatch{k: 4}
	if b.active("a") {
		t.Fatalf("an empty batch reports an in-flight name")
	}
	a := &pullJob{name: "a"}
	c := &pullJob{name: "c"}
	b.jobs = []*pullJob{a, &pullJob{name: "b"}, c}
	if !b.active("a") || !b.active("b") || !b.active("c") {
		t.Fatalf("in-flight names not reported: %v", []string{a.name})
	}
	if b.active("d") {
		t.Fatalf("a name that was never started reports as in flight")
	}
	b.remove(a)
	if b.active("a") {
		t.Fatalf("a finished job still blocks its own name")
	}
	if len(b.jobs) != 2 || b.jobs[0].name != "b" || b.jobs[1].name != "c" {
		t.Fatalf("remove() left jobs = %v", b.jobs)
	}
}
