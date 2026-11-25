package node

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/scanner"
)

// The scan commit path used to write ONE durable event per applied change, each
// in its own SQLite transaction (UPDATE + INSERT in Store.Record). On a live
// 1.17M-file / 8 TB first pass that made the event-write rate the throughput of
// the whole pipeline — measured 0.5 files/s and 9.0 MB/s on an array that does
// 133 MB/s, with all 8 hash workers parked in "chan send" on the scanner's
// bounded batch FIFO because the single writer was inside Store.Record (a
// goroutine parked on the events store lock, events.go:224) for every file.
//
// The fix keeps the HISTORY exactly as it was — one row per applied file, with
// its path, so an operator can still see, filter and search every file a batch
// touched — and batches only the WRITE: the events a commit batch emits while
// the engine lock is held are queued and delivered as one slice, and the node
// records them with events.Store.RecordBatch in ONE transaction.
//
// These tests pin both halves: N applied changes produce N queryable rows AND
// exactly one event-store transaction for the batch.

// txTally collects one entry per write transaction the event store commits.
type txTally struct {
	mu   sync.Mutex
	rows []int
}

func (t *txTally) hook(rows int) {
	t.mu.Lock()
	t.rows = append(t.rows, rows)
	t.mu.Unlock()
}

func (t *txTally) snapshot() []int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]int(nil), t.rows...)
}

func newTally(t *testing.T, n *Node) *txTally {
	t.Helper()
	tally := &txTally{}
	n.Events.ResetTxCount()
	n.Events.SetTxHook(tally.hook)
	t.Cleanup(func() { n.Events.SetTxHook(nil) })
	return tally
}

func appliedEvents(t *testing.T, n *Node) []events.Event {
	t.Helper()
	evs, err := n.Events.Query(events.Filter{Folder: "data", Category: events.CatApplied})
	if err != nil {
		t.Fatalf("query applied events: %v", err)
	}
	return evs
}

// liveNames lists the folder's live (non-tombstone) file names.
func liveNames(t *testing.T, f *Folder) []string {
	t.Helper()
	var names []string
	if err := f.Ix.ListAll(func(fi *index.FileInfo) error {
		if !fi.Deleted && fi.Type == index.TypeFile {
			names = append(names, fi.Name)
		}
		return nil
	}); err != nil {
		t.Fatalf("list index: %v", err)
	}
	sort.Strings(names)
	return names
}

// TestScanCommitBatchRowsPerFileOneTransaction is requirement (a): 500 applied
// changes produce 500 rows — every file's path present and queryable — in ONE
// event-store transaction. Before the fix this was 500 transactions.
func TestScanCommitBatchRowsPerFileOneTransaction(t *testing.T) {
	const count = 500
	n, f := firstScanFixture(t, 0)
	tally := newTally(t, n)

	names := make([]string, 0, count)
	for i := 0; i < count; i++ {
		names = append(names, fmt.Sprintf("src/bulk/f%04d.bin", i))
	}
	if err := n.commitScanBatch(f, batchChanges(names...)); err != nil {
		t.Fatalf("commit batch: %v", err)
	}

	// ONE durable write for the whole batch.
	if got := n.Events.TxCount(); got != 1 {
		t.Fatalf("event store committed %d transaction(s) for a %d-change batch, want 1 "+
			"(a per-item durable write in the commit path is what paced the live first pass at "+
			"0.5 files/s)", got, count)
	}
	if rows := tally.snapshot(); len(rows) != 1 || rows[0] != count {
		t.Fatalf("write transactions = %v, want exactly one carrying all %d rows", rows, count)
	}

	// Every applied file still has its own row, with its own path.
	evs := appliedEvents(t, n)
	if len(evs) != count {
		t.Fatalf("applied events = %d, want one per applied file (%d)", len(evs), count)
	}
	seen := map[string]events.Event{}
	for _, ev := range evs {
		if ev.Path == "" {
			t.Fatalf("folder-level event %q in a per-file batch: the history must name the file", ev.Reason)
		}
		if ev.Reason != "local add" {
			t.Fatalf("%s reason = %q, want local add", ev.Path, ev.Reason)
		}
		seen[ev.Path] = ev
	}
	for _, name := range names {
		if _, ok := seen[name]; !ok {
			t.Fatalf("%s has no applied event: history dropped a file the batch applied", name)
		}
	}
	// Filterable/searchable per file, which is the operator requirement.
	one, err := n.Events.Query(events.Filter{Folder: "data", Path: names[7]})
	if err != nil {
		t.Fatalf("path filter: %v", err)
	}
	if len(one) != 1 || one[0].Path != names[7] || one[0].Category != events.CatApplied {
		t.Fatalf("path filter for %s = %+v, want its applied row", names[7], one)
	}
}

// TestScanCommitBatchKeepsPerFileDeletionsAndEdits is requirement (b): deletions,
// a real edit (version bump), a refusal and anything in the conflict/error
// categories STILL produce their own per-file events — and the whole flush is
// still one transaction.
func TestScanCommitBatchKeepsPerFileDeletionsAndEdits(t *testing.T) {
	n, f := firstScanFixture(t, 0)
	tally := newTally(t, n)

	batch := batchChanges("src/new-a.bin", "src/new-b.bin", "src/new-c.bin")
	batch = append(batch,
		scanner.Change{Kind: scanner.Deleted, Info: &index.FileInfo{Name: "src/gone-1.bin", Type: index.TypeFile}},
		scanner.Change{Kind: scanner.Deleted, Info: &index.FileInfo{Name: "src/gone-2.bin", Type: index.TypeFile}},
		scanner.Change{Kind: scanner.Modified, Info: &index.FileInfo{
			Name: "src/edited.bin", Size: 99, Type: index.TypeFile,
			ModifiedS: 1770000001, Blocks: [][]byte{[]byte(strings.Repeat("y", 32))},
		}},
		scanner.Change{Kind: scanner.Skipped, Info: &index.FileInfo{Name: "src/junk.tmp", Type: index.TypeFile}, Rule: "*.tmp"},
	)
	if err := n.commitScanBatch(f, batch); err != nil {
		t.Fatalf("commit batch: %v", err)
	}

	evs := appliedEvents(t, n)
	byPath := map[string]events.Event{}
	for _, ev := range evs {
		byPath[ev.Path] = ev
	}
	for _, want := range []string{"src/gone-1.bin", "src/gone-2.bin"} {
		ev, ok := byPath[want]
		if !ok || ev.Reason != "local delete" {
			t.Fatalf("deletion %s = %+v, want its own 'local delete' event", want, byPath[want])
		}
	}
	if ev, ok := byPath["src/edited.bin"]; !ok || ev.Reason != "local modify" {
		t.Fatalf("edit event = %+v, want its own 'local modify' event (a version bump an operator must see)", byPath["src/edited.bin"])
	}
	for _, name := range []string{"src/new-a.bin", "src/new-b.bin", "src/new-c.bin"} {
		if ev, ok := byPath[name]; !ok || ev.Reason != "local add" {
			t.Fatalf("addition %s = %+v, want its own 'local add' event", name, byPath[name])
		}
	}
	skipEvs, err := n.Events.Query(events.Filter{Folder: "data", Category: events.CatSkipped})
	if err != nil {
		t.Fatalf("query skipped: %v", err)
	}
	if len(skipEvs) != 1 || skipEvs[0].Path != "src/junk.tmp" || skipEvs[0].Linked != "*.tmp" {
		t.Fatalf("refusal events = %+v, want one per-file event for junk.tmp linked to *.tmp", skipEvs)
	}
	// 7 changes -> 7 rows, all in one write.
	if rows := tally.snapshot(); len(rows) != 1 || rows[0] != 7 {
		t.Fatalf("write transactions = %v, want one carrying all 7 rows", rows)
	}
}

// TestRecordEventsBatchKeepsConflictAndErrorRows proves the same for the two
// categories that never come from the local-apply path: a conflict and an error
// delivered in one batch are still their own rows, with their own category and
// path (one transaction, not one per row).
func TestRecordEventsBatchKeepsConflictAndErrorRows(t *testing.T) {
	n, _ := firstScanFixture(t, 0)
	tally := newTally(t, n)

	n.recordEvents([]*events.Event{
		{TS: time.Now(), Folder: "data", Path: "src/a.sync-conflict-20260101-1-abcd.txt",
			Category: events.CatConflict, Severity: events.SevWarn, Reason: "conflict copy created"},
		{TS: time.Now(), Folder: "data", Path: "src/locked.bin",
			Category: events.CatError, Severity: events.SevWarn, Reason: "file is locked/in use — retrying"},
	})

	if rows := tally.snapshot(); len(rows) != 1 || rows[0] != 2 {
		t.Fatalf("write transactions = %v, want one carrying both rows", rows)
	}
	conflicts, err := n.Events.Query(events.Filter{Folder: "data", Category: events.CatConflict})
	if err != nil {
		t.Fatalf("query conflicts: %v", err)
	}
	if len(conflicts) != 1 || conflicts[0].Path != "src/a.sync-conflict-20260101-1-abcd.txt" {
		t.Fatalf("conflict rows = %+v, want the per-file conflict event", conflicts)
	}
	errs, err := n.Events.Query(events.Filter{Folder: "data", Category: events.CatError})
	if err != nil {
		t.Fatalf("query errors: %v", err)
	}
	if len(errs) != 1 || errs[0].Path != "src/locked.bin" {
		t.Fatalf("error rows = %+v, want the per-file error event", errs)
	}
}

// TestCancelledPassKeepsBankedEvents is requirement (d): a pass cancelled
// mid-hash keeps the events for exactly the batches it committed — every banked
// file has its row, nothing that was dropped claims one — in O(batches)
// transactions.
func TestCancelledPassKeepsBankedEvents(t *testing.T) {
	const batch = 4
	const files = 160
	restore := scanner.SetBatchLimits(batch, 0)
	t.Cleanup(restore)

	n, f := firstScanFixture(t, files)
	n.SetStartupScanning(true)
	tally := newTally(t, n)

	var hashed atomic.Int32
	f.Scanner.SetHashHook(func(string) {
		if hashed.Add(1) != 3*batch {
			return
		}
		// The batch writer commits a scheduling hop after the worker that
		// produced the entries: wait for the first whole batch to be banked so
		// the cancellation lands with real work committed, not before it.
		deadline := time.Now().Add(5 * time.Second)
		for liveFileCount(f) == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if err := n.SetFolderPaused("data", true); err != nil {
			t.Errorf("pause: %v", err)
		}
	})

	if _, err := n.ScanFolder("data"); err == nil {
		t.Fatal("the pass was not cancelled: nothing to measure")
	}
	f.Scanner.SetHashHook(nil)

	banked := liveNames(t, f)
	if len(banked) == 0 {
		t.Fatal("the cancelled pass banked nothing: batching is not in effect")
	}
	if len(banked) >= files {
		t.Fatalf("the cancelled pass banked all %d files, so cancellation was not measured", files)
	}
	if len(banked)%batch != 0 {
		t.Fatalf("%d entries banked by a cancelled pass, want whole %d-entry batches", len(banked), batch)
	}

	evs := appliedEvents(t, n)
	got := make([]string, 0, len(evs))
	for _, ev := range evs {
		got = append(got, ev.Path)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(banked, ",") {
		t.Fatalf("history names %v but the cancelled pass banked %v: a committed file lost its event "+
			"or a dropped batch kept one", got, banked)
	}
	// One durable write per committed batch, not per file.
	if want := int64(len(banked) / batch); n.Events.TxCount() != want {
		t.Fatalf("event store committed %d transaction(s) for %d banked file(s) in %d-entry batches, "+
			"want %d (one per batch)", n.Events.TxCount(), len(banked), batch, want)
	}
	if rows := tally.snapshot(); len(rows) != len(banked)/batch {
		t.Fatalf("write transactions = %v, want %d", rows, len(banked)/batch)
	}
}

// TestCommitPathDoesNoPerChangeSQL is requirement (e), the throughput guard: the
// commit path must not perform a durable write per change. It counts the event
// store's write transactions for an add-only first-pass batch and for a
// deletion batch — both must be O(1) transactions (one per flush), while the
// per-file rows for deletions are preserved.
func TestCommitPathDoesNoPerChangeSQL(t *testing.T) {
	const adds = 300
	const dels = 200
	n, f := firstScanFixture(t, 0)
	tally := newTally(t, n)

	names := make([]string, 0, adds)
	for i := 0; i < adds; i++ {
		names = append(names, fmt.Sprintf("src/f%03d.bin", i))
	}
	if err := n.commitScanBatch(f, batchChanges(names...)); err != nil {
		t.Fatalf("commit adds: %v", err)
	}
	if got := n.Events.TxCount(); got != 1 {
		t.Fatalf("%d additions cost %d event-store transaction(s), want 1", adds, got)
	}
	if rows := tally.snapshot(); len(rows) != 1 || rows[0] != adds {
		t.Fatalf("add batch transactions = %v, want one carrying %d rows", rows, adds)
	}

	dels2 := make([]scanner.Change, 0, dels)
	for i := 0; i < dels; i++ {
		dels2 = append(dels2, scanner.Change{Kind: scanner.Deleted,
			Info: &index.FileInfo{Name: fmt.Sprintf("src/f%03d.bin", i), Type: index.TypeFile}})
	}
	n.Events.ResetTxCount()
	if err := n.commitScanBatch(f, dels2); err != nil {
		t.Fatalf("commit deletions: %v", err)
	}
	if got := n.Events.TxCount(); got != 1 {
		t.Fatalf("%d deletions cost %d event-store transaction(s), want 1 (the rows are per file, "+
			"the durable write is not)", dels, got)
	}
	rows := tally.snapshot()
	if len(rows) != 2 || rows[1] != dels {
		t.Fatalf("transactions = %v, want [%d %d]: one write per commit batch, every row preserved", rows, adds, dels)
	}
}
