package scanner

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/ignore"
	"crosssync/internal/index"
)

// waitFor polls cond until it holds or the deadline passes. Returns true when
// cond became true. Used by tests that must observe a state reached by another
// goroutine without assuming a scheduling order.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// TestSlowBatchSinkDoesNotStopHashing is the reason the batch writer exists.
// Measured live on a 1.17M-file / 8 TB share, the first pass hashed at 9.3 MB/s
// (0.4 files/s, 27 MB average file) with all 8 hash workers parked on the
// scanner's flushMu for 1-3 minutes at a time while ONE goroutine wrote a batch
// to the index. A commit must never be able to park the readers.
//
// One worker, so any parking is total: with the sink holding the only commit
// for two seconds, every file of the pass must still be hashed before it
// returns. Against the old synchronous flush the single worker stops at the
// first batch boundary (hashed == batch size) for the whole commit.
func TestSlowBatchSinkDoesNotStopHashing(t *testing.T) {
	const batch = 2
	const files = 12
	restore := SetBatchLimits(batch, 0)
	t.Cleanup(restore)

	root, ix := setup(t)
	for i := 0; i < files; i++ {
		writeFile(t, root, fmt.Sprintf("f%02d.bin", i), fmt.Sprintf("payload-%02d", i))
	}

	var (
		once        sync.Once
		sinkEntered = make(chan struct{})
		batches     atomic.Int32
		hashed      atomic.Int32
	)
	s := newTestScanner(root, ix, &ignore.Matcher{})
	s.SetWorkers(1)
	s.SetBatchSink(func(batch []Change) error {
		batches.Add(1)
		once.Do(func() {
			close(sinkEntered)
			// A commit that takes seconds: the hash workers must not care.
			time.Sleep(2 * time.Second)
		})
		fi := make([]*index.FileInfo, 0, len(batch))
		for _, c := range batch {
			fi = append(fi, c.Info)
		}
		return ix.PutBatch(fi)
	})
	s.SetHashHook(func(string) { hashed.Add(1) })

	done := make(chan error, 1)
	go func() {
		_, err := s.Scan()
		done <- err
	}()

	select {
	case <-sinkEntered:
	case err := <-done:
		t.Fatalf("the pass ended before the sink was entered (err=%v)", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the sink was never called: batching is not in effect")
	}

	// The commit is now in flight (the sink is sleeping). Hashing must keep
	// advancing: the whole pass is hashed while that one commit is still
	// running, well before its 2 s are up.
	if !waitFor(1500*time.Millisecond, func() bool { return hashed.Load() == files }) {
		t.Fatalf("only %d of %d file(s) hashed while a batch commit was in flight: "+
			"the sink is still on the hash workers' critical path", hashed.Load(), files)
	}
	if got := batches.Load(); got != 1 {
		t.Fatalf("the sink ran %d time(s) during the check, want 1 (the slow commit must still be in flight)", got)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pass: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the pass did not finish")
	}
	if got := batches.Load(); got != files/batch {
		t.Fatalf("sink called %d time(s), want %d (whole %d-entry batches)", got, files/batch, batch)
	}
	if n := len(liveIndexNames(t, ix)); n != files {
		t.Fatalf("index holds %d entries after the pass, want %d", n, files)
	}
}

// TestBatchQueueIsBoundedAndCancellable: the decoupling must not turn into
// unbounded run-ahead. With a sink that never returns, the FIFO fills and the
// workers stop hashing instead of growing the queue forever; the pass can then
// be cancelled cleanly in exactly that state.
func TestBatchQueueIsBoundedAndCancellable(t *testing.T) {
	const files = 200
	restore := SetBatchLimits(1, 0)
	t.Cleanup(restore)
	prevQueue := batchQueueEntries
	batchQueueEntries = 6
	t.Cleanup(func() { batchQueueEntries = prevQueue })

	root, ix := setup(t)
	for i := 0; i < files; i++ {
		writeFile(t, root, fmt.Sprintf("f%03d.bin", i), fmt.Sprintf("payload-%03d", i))
	}

	var (
		once    sync.Once
		entered = make(chan struct{})
		release = make(chan struct{})
		hashed  atomic.Int32
		paused  atomic.Bool
		commits atomic.Int32
	)
	s := newTestScanner(root, ix, &ignore.Matcher{})
	s.SetWorkers(4)
	s.SetBatchSink(func([]Change) error {
		commits.Add(1)
		once.Do(func() { close(entered) })
		<-release // never returns until the test lets it
		return nil
	})
	s.SetHashHook(func(string) { hashed.Add(1) })
	s.SetCancel(paused.Load)

	done := make(chan error, 1)
	go func() {
		_, err := s.Scan()
		done <- err
	}()

	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("the pass ended before the sink was entered (err=%v)", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the sink was never called: batching is not in effect")
	}

	// Give the workers time to fill the queue behind the blocked commit, then
	// prove the count has stopped moving: bounded memory means they park, not
	// that they keep hashing the whole tree into RAM.
	time.Sleep(150 * time.Millisecond)
	first := hashed.Load()
	time.Sleep(150 * time.Millisecond)
	second := hashed.Load()
	if second != first {
		t.Fatalf("hash count still climbing (%d -> %d) with the first commit never returning: "+
			"the batch queue is not bounded", first, second)
	}
	// The bound: the queue itself, the one batch the writer is holding, and the
	// workers' own in-flight files.
	if max := int32(batchQueueEntries + batchMaxEntries + 4); first > max {
		t.Fatalf("%d file(s) hashed with the sink stuck, want at most %d "+
			"(queue %d + one %d-entry batch + workers)", first, max, batchQueueEntries, batchMaxEntries)
	}
	if first >= files {
		t.Fatalf("all %d files were hashed while the sink was stuck: the queue did not bound anything", files)
	}

	// Cancelling in that state must be clean: the blocked commit finishes, the
	// uncommitted queue is dropped, and the pass reports cancellation.
	paused.Store(true)
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, ErrScanCancelled) {
			t.Fatalf("scan = %v, want ErrScanCancelled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pass did not stop after cancellation")
	}
	if n := len(liveIndexNames(t, ix)); n != 0 {
		t.Fatalf("index holds %d entries after a cancelled pass, want 0: a batch queued but never committed was applied", n)
	}
}

// TestBatchWriterExitsAfterEveryPass: whatever ends the pass (a clean finish, a
// cancellation, or a sink error) the single writer goroutine must have exited
// before the scan returns — no leaked goroutine per pass on a long-lived daemon.
func TestBatchWriterExitsAfterEveryPass(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, s *Scanner, cancel *atomic.Bool) error // returned to validate the scan error
	}{
		{
			name: "finished",
			setup: func(t *testing.T, s *Scanner, cancel *atomic.Bool) error {
				_, err := s.Scan()
				return err
			},
		},
		{
			name: "cancelled",
			setup: func(t *testing.T, s *Scanner, cancel *atomic.Bool) error {
				_, err := s.Scan()
				if !errors.Is(err, ErrScanCancelled) {
					t.Fatalf("scan = %v, want ErrScanCancelled", err)
				}
				return nil
			},
		},
		{
			name: "sink error",
			setup: func(t *testing.T, s *Scanner, cancel *atomic.Bool) error {
				_, err := s.Scan()
				if err == nil {
					t.Fatal("scan = nil, want the sink error")
				}
				return nil
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := SetBatchLimits(2, 0)
			t.Cleanup(restore)

			root, ix := setup(t)
			for i := 0; i < 8; i++ {
				writeFile(t, root, fmt.Sprintf("f%02d.bin", i), fmt.Sprintf("payload-%02d", i))
			}
			var (
				cancel atomic.Bool
				hashed atomic.Int32
				exited = make(chan struct{})
			)
			s := newTestScanner(root, ix, &ignore.Matcher{})
			s.SetCancel(cancel.Load)
			s.batchWriterHook = func() { close(exited) }
			switch tc.name {
			case "cancelled":
				s.SetHashHook(func(string) {
					if hashed.Add(1) == 3 {
						cancel.Store(true)
					}
				})
				s.SetBatchSink(func([]Change) error { return nil })
			case "sink error":
				s.SetBatchSink(func([]Change) error { return errors.New("index write failed") })
			default:
				s.SetBatchSink(func([]Change) error { return nil })
			}
			if err := tc.setup(t, s, &cancel); err != nil {
				t.Fatalf("pass: %v", err)
			}
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Fatal("the batch writer goroutine is still running after the pass returned")
			}
		})
	}
}
