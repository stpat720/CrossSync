package scanner

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/ignore"
	"crosssync/internal/index"
)

// TestBatchSinkCommitsDuringTheHashPhase: with a sink installed, hashed entries
// leave the scanner DURING the hash phase, in batches of at most the configured
// size — that is what makes a pass over a huge share resumable instead of
// holding every entry in memory until the end.
func TestBatchSinkCommitsDuringTheHashPhase(t *testing.T) {
	restore := SetBatchLimits(4, 0)
	t.Cleanup(restore)

	root, ix := setup(t)
	const files = 20
	for i := 0; i < files; i++ {
		writeFile(t, root, fmt.Sprintf("src/f%02d.bin", i), fmt.Sprintf("content-%02d", i))
	}

	var (
		mu      sync.Mutex
		batches [][]string
	)
	// The sink applies each batch as ONE index transaction, which is what the
	// node does; PutBatch is atomic, so a crash between batches can only ever
	// leave whole batches behind.
	s := newTestScanner(root, ix, &ignore.Matcher{})
	s.SetBatchSink(func(batch []Change) error {
		fi := make([]*index.FileInfo, 0, len(batch))
		names := make([]string, 0, len(batch))
		for _, c := range batch {
			fi = append(fi, &index.FileInfo{
				Name: c.Info.Name, Size: c.Info.Size, ModifiedS: c.Info.ModifiedS,
				ModifiedNs: c.Info.ModifiedNs, Mode: c.Info.Mode, Type: c.Info.Type,
				BlockSize: c.Info.BlockSize, Blocks: c.Info.Blocks,
			})
			names = append(names, c.Info.Name)
		}
		if err := ix.PutBatch(fi); err != nil {
			return err
		}
		mu.Lock()
		batches = append(batches, names)
		mu.Unlock()
		return nil
	})

	changes, err := s.Scan()
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	mu.Lock()
	got := make([][]string, len(batches))
	copy(got, batches)
	mu.Unlock()

	if len(got) != files/4 {
		t.Fatalf("sink called %d time(s), want %d (one per 4 hashed files)", len(got), files/4)
	}
	seen := 0
	for i, b := range got {
		if len(b) != 4 {
			t.Fatalf("batch %d has %d entries, want exactly 4 (bounded batches): %v", i, len(b), b)
		}
		seen += len(b)
	}
	if seen != files {
		t.Fatalf("%d entries committed in batches, want %d", seen, files)
	}
	// Everything the sink committed must be marked so the caller does not apply
	// it a second time (a second apply would bump the version of unchanged
	// content).
	committed := 0
	for _, c := range changes {
		if c.Committed {
			committed++
		}
	}
	if committed != files {
		t.Fatalf("%d change(s) marked Committed, want %d", committed, files)
	}
	// The index really holds them, with hashes.
	if n := liveIndexNames(t, ix); len(n) != files {
		t.Fatalf("index holds %d entries after the pass, want %d: %v", len(n), files, n)
	}
}

// TestBatchSinkDiscardsHalfBuiltBatchOnCancel pins the rule that a cancelled
// pass commits nothing it had not already committed: the batch being built when
// the pause lands is dropped whole, never applied in part.
func TestBatchSinkDiscardsHalfBuiltBatchOnCancel(t *testing.T) {
	restore := SetBatchLimits(4, 0)
	t.Cleanup(restore)

	root, ix := setup(t)
	const files = 40
	for i := 0; i < files; i++ {
		writeFile(t, root, fmt.Sprintf("f%02d.bin", i), fmt.Sprintf("payload-%02d", i))
	}

	var (
		mu        sync.Mutex
		committed int
		partial   int
		paused    atomic.Bool
		hashed    atomic.Int32
	)
	s := newTestScanner(root, ix, &ignore.Matcher{})
	// One worker keeps the ordering deterministic: hash, hand over, hash again.
	s.SetWorkers(1)
	s.SetBatchSink(func(batch []Change) error {
		mu.Lock()
		defer mu.Unlock()
		committed += len(batch)
		if len(batch) != 4 {
			partial = len(batch)
		}
		return nil
	})
	// Pause once six files have been hashed AND the first whole batch has been
	// committed. The writer is asynchronous, so the sixth hash can land while
	// the first four entries are still queued; pausing then would drop the
	// queue without ever proving that a committed batch survives a cancel
	// (TestBatchQueueIsBoundedAndCancellable covers that state). The two files
	// hashed after the first batch are the uncommitted remainder.
	s.SetHashHook(func(string) {
		if hashed.Add(1) != 6 {
			return
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			mu.Lock()
			done := committed > 0
			mu.Unlock()
			if done || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		paused.Store(true)
	})
	s.SetCancel(paused.Load)

	changes, err := s.Scan()
	if !errors.Is(err, ErrScanCancelled) {
		t.Fatalf("scan = %v, want ErrScanCancelled", err)
	}
	if changes != nil {
		t.Fatalf("cancelled pass returned %d change(s), want none", len(changes))
	}
	mu.Lock()
	defer mu.Unlock()
	if partial != 0 {
		t.Fatalf("the sink was handed a partial batch of %d entries, want only full batches", partial)
	}
	if committed == 0 {
		t.Fatal("no batch was committed before the pause: the test would not prove anything")
	}
	if committed >= files {
		t.Fatalf("%d entries committed, want the pass to have stopped short (%d files)", committed, files)
	}
	if committed%4 != 0 {
		t.Fatalf("%d entries committed, want a whole number of 4-entry batches", committed)
	}
}

// TestBatchSinkErrorAbortsThePass: an index write failure stops the pass instead
// of letting it carry on hashing work it cannot bank.
func TestBatchSinkErrorAbortsThePass(t *testing.T) {
	restore := SetBatchLimits(2, 0)
	t.Cleanup(restore)

	root, ix := setup(t)
	for i := 0; i < 8; i++ {
		writeFile(t, root, fmt.Sprintf("f%02d.bin", i), "x")
	}
	boom := errors.New("index write failed")
	s := newTestScanner(root, ix, &ignore.Matcher{})
	s.SetBatchSink(func([]Change) error { return boom })

	if _, err := s.Scan(); !errors.Is(err, boom) {
		t.Fatalf("scan = %v, want the sink error %v", err, boom)
	}
}

// liveIndexNames returns the sorted live (non-tombstone) entry names.
func liveIndexNames(t *testing.T, ix *index.Index) []string {
	t.Helper()
	var out []string
	if err := ix.ListAll(func(fi *index.FileInfo) error {
		if !fi.Deleted {
			out = append(out, fi.Name)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}
