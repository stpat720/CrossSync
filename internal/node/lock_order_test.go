package node

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/scanner"
)

// The engine lock and the durable event store's lock must never be held by one
// goroutine at the same time, in either order. The live failure this pins down:
// a first index pass wedged with 8 hash workers parked on the scan commit's
// flush mutex, one goroutine holding the engine lock inside the event callback
// (engine emit -> Node.recordEvent -> Store.Record) and waiting for the event
// store's lock, and 6 more goroutines (control read, connmgr recovery, ack
// path) queued behind it — 180 s of zero disk I/O and idle CPU, cleared only by
// a restart.
//
// The rule is now structural: the engine queues the events it emits under its
// lock and delivers them after releasing it (Engine.emit /
// Engine.flushEventQueue), and nothing calls into the engine while holding the
// event store's lock. These tests drive the real paths — the scanner's batch
// commit (Node.commitScanBatch -> Engine.ApplyLocalChanges), the real event
// handler (Node.recordEvent with the real SQLite store), and the real
// control/connmgr event reads — so a regression fails here instead of on a
// share. Every wait is bounded by eventWatchdog: a deadlock is an assertion,
// never a hung test run.

// eventWatchdog bounds every wait in these tests. A legitimate local scan of a
// handful of files finishes in milliseconds; 5 s is orders of magnitude of
// headroom, and the failure message names the wedged path.
const eventWatchdog = 5 * time.Second

// batchChanges builds what the scanner's batch sink hands the engine: plain
// "local add" changes whose content is already hashed and indexed-checked by
// the walk (ApplyLocalChanges verifies nothing else on disk).
func batchChanges(names ...string) []scanner.Change {
	out := make([]scanner.Change, 0, len(names))
	for i, name := range names {
		out = append(out, scanner.Change{
			Kind: scanner.Added,
			Info: &index.FileInfo{
				Name: name, Size: int64(10 + i), Type: index.TypeFile,
				ModifiedS: 1770000000, Blocks: [][]byte{[]byte(strings.Repeat("x", 32))},
			},
		})
	}
	return out
}

// TestScanBatchEmitsEventsOutsideEngineLock is the regression pin for the
// inversion itself: while the scanner's batch commit runs (the engine lock is
// held across the whole index transaction), every event the engine emits must
// be delivered with that lock RELEASED.
//
// On the pre-fix code the callback ran inside the critical section, so the
// real handler — Store.Record, which takes the event store's lock — was called
// holding the engine lock, which is exactly the engine -> events edge of the
// live deadlock. The check is a fact about the callback, not a timing
// assumption: no sleeping, no window-widening.
func TestScanBatchEmitsEventsOutsideEngineLock(t *testing.T) {
	n, f := firstScanFixture(t, 12)
	eng := f.Engine

	var (
		mu       sync.Mutex
		emitted  []string
		underMu  []string
	)
	eng.OnEvent = func(ev *events.Event) {
		// The real handler: one durable row (event store lock) plus the SSE
		// broadcast. Before the fix this ran with e.mu held.
		mu.Lock()
		emitted = append(emitted, ev.Path)
		if eng.Locked() {
			underMu = append(underMu, ev.Path)
		}
		mu.Unlock()
		n.recordEvent(ev)
	}

	applied, err := n.ScanFolder("data")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if applied == 0 {
		t.Fatal("scan applied 0 changes: the fixture did not exercise the batch commit")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(emitted) == 0 {
		t.Fatal("the scan recorded no events at all: nothing was measured")
	}
	if len(underMu) > 0 {
		t.Fatalf("%d of %d events were delivered while the engine lock was HELD (first: %q). "+
			"Recording an event takes the event store's lock, so this is the engine -> events "+
			"edge of the live deadlock: 8 hash workers parked on the scan commit, a query holding "+
			"the event store, hash_done frozen for 180 s with idle CPU. Emit must happen after "+
			"Engine.mu is released (Engine.flushEventQueue).", len(underMu), len(emitted), underMu[0])
	}

	// Semantics must be unchanged: every real change produced exactly one
	// durable event, nobody dropped an event, and nobody recorded one twice.
	evs, err := n.Events.Query(events.Filter{Folder: "data", Category: events.CatApplied})
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	seen := map[string]int{}
	for _, ev := range evs {
		if ev.Path != "" {
			seen[ev.Path]++
		}
	}
	for i := 0; i < 12; i++ {
		name := fmt.Sprintf("src/f%03d.bin", i)
		switch seen[name] {
		case 1:
		case 0:
			t.Fatalf("%s has no applied event: the scan dropped it", name)
		default:
			t.Fatalf("%s has %d applied events, want exactly 1 (a duplicate would re-open "+
				"attention and double-count history)", name, seen[name])
		}
	}
}

// TestEngineRespondsWhileEventIsRecorded is the wedge itself, reduced to a
// deterministic rendezvous. The event callback is held open (a slow event store:
// a big attention query decoding rows, a WAL checkpoint, an archiving UI
// request) while another goroutine performs an ordinary engine read. With the
// engine lock held across the callback that read cannot proceed — on the live
// box every control read, the connection manager and the serve path queued
// behind it while hash_done sat frozen.
//
// The callbacks here are held, not slept on: the reader either completes or the
// watchdog fails the test, so the outcome never depends on machine speed.
func TestEngineRespondsWhileEventIsRecorded(t *testing.T) {
	n, f := firstScanFixture(t, 4)
	eng := f.Engine

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var recorded atomic.Int64
	eng.OnEvent = func(ev *events.Event) {
		once.Do(func() { close(entered) })
		<-release
		recorded.Add(1)
		n.recordEvent(ev)
	}

	commitDone := make(chan error, 1)
	go func() {
		_, _, err := eng.ApplyLocalChanges(batchChanges("src/batch-a.bin", "src/batch-b.bin"))
		commitDone <- err
	}()

	select {
	case <-entered:
	case <-time.After(eventWatchdog):
		close(release)
		t.Fatal("the batch commit never reached its event callback: nothing was measured")
	}

	// An engine read: what NeedPulls, the status handler, PendingUploads and
	// every serve goroutine do. It must not wait for an event to be recorded.
	probe := make(chan struct{})
	go func() {
		_, _ = eng.NeedPulls()
		close(probe)
	}()

	select {
	case <-probe:
	case <-time.After(eventWatchdog):
		close(release)
		t.Fatalf("an engine read blocked for %s while an event was being recorded: the engine "+
			"lock is held across the event callback. This is the live wedge (8 hash workers on "+
			"the scan commit, hash_done frozen 180 s, idle CPU, restart required). Events must be "+
			"delivered after the engine lock is released.", eventWatchdog)
	}

	close(release)
	select {
	case err := <-commitDone:
		if err != nil {
			t.Fatalf("batch commit: %v", err)
		}
	case <-time.After(eventWatchdog):
		t.Fatal("the batch commit never returned after its events were released")
	}
	if recorded.Load() == 0 {
		t.Fatal("no event was recorded: the queue dropped them")
	}
}

// TestNoEngineEventStoreLockNesting drives both real sides at once — a scan
// committing batches and emitting events, and the control plane's attention
// query plus the connection manager's peer-recovery check reading the event
// store — and requires the lock-order detector to see zero inversions.
//
// The detector is the production wiring (Node.wireLockOrder pairs every engine
// lock with the event store's and turns checking on), so this fails on any
// future call site that records an event under the engine lock or reads the
// engine under the event store's lock, without needing to reproduce a wedge.
func TestNoEngineEventStoreLockNesting(t *testing.T) {
	n, f := firstScanFixture(t, 60)
	eng := f.Engine
	if eng.LockGuard() == nil || n.Events.LockGuard() == nil {
		t.Fatal("lock-order guards are not wired: the engine and event store must stay paired")
	}

	stop := make(chan struct{})
	var reads atomic.Int64
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			// The control plane's attention/detail read and the ack path.
			_, _ = n.Events.Query(events.Filter{Folder: "data", OpenOnly: true})
			// The connection manager's per-round recovery check (onRecover).
			n.peerRecovered("peer-b")
			reads.Add(1)
		}
	}()

	scanDone := make(chan struct{})
	var applied int
	var scanErr error
	go func() {
		defer close(scanDone)
		applied, scanErr = n.ScanFolder("data")
	}()

	select {
	case <-scanDone:
	case <-time.After(60 * time.Second):
		close(stop)
		t.Fatal("the scan never finished while the control plane was reading events: " +
			"an engine lock is being taken under the event store's lock")
	}
	close(stop)
	<-readerDone

	if scanErr != nil {
		t.Fatalf("scan: %v", scanErr)
	}
	if applied == 0 || reads.Load() == 0 {
		t.Fatalf("nothing was measured (applied=%d, event reads=%d)", applied, reads.Load())
	}
	if v := eng.LockGuard().Violations(); v != 0 {
		t.Fatalf("engine lock: %d inversion(s); first: %s", v, eng.LockGuard().FirstViolation())
	}
	if v := n.Events.LockGuard().Violations(); v != 0 {
		t.Fatalf("event store lock: %d inversion(s); first: %s", v, n.Events.LockGuard().FirstViolation())
	}
}
