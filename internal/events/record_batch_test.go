package events

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// newBatchStore opens a store and installs a transaction observer.
func newBatchStore(t *testing.T) (*Store, *[]int) {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	var mu sync.Mutex
	rows := []int{}
	s.SetTxHook(func(n int) {
		mu.Lock()
		rows = append(rows, n)
		mu.Unlock()
	})
	return s, &rows
}

// TestRecordBatchIsOneTransaction is the store half of the scan-commit fix: N
// events are ONE write transaction, where N calls to Record are N.
func TestRecordBatchIsOneTransaction(t *testing.T) {
	s, rows := newBatchStore(t)
	const count = 500
	evs := make([]*Event, 0, count)
	for i := 0; i < count; i++ {
		evs = append(evs, &Event{
			TS: time.Now(), Folder: "data", Path: fmt.Sprintf("f%03d.bin", i),
			Category: CatApplied, Severity: SevInfo, Reason: "local delete",
		})
	}
	if err := s.RecordBatch(evs); err != nil {
		t.Fatalf("RecordBatch: %v", err)
	}
	if got := s.TxCount(); got != 1 {
		t.Fatalf("TxCount = %d after RecordBatch(%d), want 1", got, count)
	}
	if len(*rows) != 1 || (*rows)[0] != count {
		t.Fatalf("transactions = %v, want exactly one carrying %d rows", *rows, count)
	}
	got, err := s.Query(Filter{Folder: "data"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != count {
		t.Fatalf("store holds %d rows, want %d", len(got), count)
	}

	// For contrast: single Record calls commit one transaction each. This is
	// the shape the commit path must not use per change.
	s.ResetTxCount()
	for _, e := range evs[:10] {
		if _, err := s.Record(e); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}
	if got := s.TxCount(); got != 10 {
		t.Fatalf("10 Record calls committed %d transaction(s), want 10", got)
	}
}

// TestRecordBatchPreservesEachCategory: batching must not flatten categories,
// severities or paths — a conflict stays a conflict row, an error an error row,
// and /api/events filters keep working.
func TestRecordBatchPreservesEachCategory(t *testing.T) {
	s, _ := newBatchStore(t)
	now := time.Now()
	batch := []*Event{
		{TS: now, Folder: "data", Path: "a.sync-conflict-1-ab.txt", Category: CatConflict, Severity: SevWarn, Reason: "conflict copy created"},
		{TS: now, Folder: "data", Path: "locked.bin", Category: CatError, Severity: SevWarn, Reason: "file is locked/in use — retrying"},
		{TS: now, Folder: "data", Path: "", Category: CatApplied, Severity: SevInfo, Reason: "added 500 file(s) to folder data"},
		{TS: now, Folder: "data", Path: "junk.tmp", Category: CatSkipped, Severity: SevInfo, Reason: "ignored by rule", Linked: "*.tmp"},
	}
	if err := s.RecordBatch(batch); err != nil {
		t.Fatalf("RecordBatch: %v", err)
	}
	for _, tc := range []struct {
		cat  Category
		path string
		sev  Severity
	}{
		{CatConflict, "a.sync-conflict-1-ab.txt", SevWarn},
		{CatError, "locked.bin", SevWarn},
		{CatSkipped, "junk.tmp", SevInfo},
	} {
		got, err := s.Query(Filter{Folder: "data", Category: tc.cat})
		if err != nil {
			t.Fatalf("query %s: %v", tc.cat, err)
		}
		if len(got) != 1 || got[0].Path != tc.path || got[0].Category != tc.cat {
			t.Fatalf("%s rows = %+v, want exactly [%s]", tc.cat, got, tc.path)
		}
		if got[0].Severity != tc.sev {
			t.Fatalf("%s severity = %d, want %d", tc.cat, got[0].Severity, tc.sev)
		}
	}
	// The folder-level summary is findable by folder + category, which is how
	// the UI and an operator look for it.
	sum, err := s.Query(Filter{Folder: "data", Category: CatApplied})
	if err != nil {
		t.Fatal(err)
	}
	if len(sum) != 1 || sum[0].Path != "" {
		t.Fatalf("applied filter = %+v, want the folder-level summary", sum)
	}
	// Attention still counts exactly the open warn+ conditions.
	if n, err := s.CountOpen(); err != nil || n != 2 {
		t.Fatalf("CountOpen = %d (%v), want 2 (the conflict and the error)", n, err)
	}
}

// TestRecordBatchReopensAcknowledgedCondition: per-row semantics are identical
// to Record — an acknowledged condition recorded again in a batch re-opens, so a
// persistent problem can never be permanently dismissed by being batched.
func TestRecordBatchReopensAcknowledgedCondition(t *testing.T) {
	s, _ := newBatchStore(t)
	id, err := s.Record(&Event{TS: time.Now(), Folder: "data", Path: "locked.bin",
		Category: CatError, Severity: SevWarn, Reason: "file is locked/in use — retrying"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Acknowledge(id, "operator"); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.CountOpen(); n != 0 {
		t.Fatalf("CountOpen after ack = %d, want 0", n)
	}
	if err := s.RecordBatch([]*Event{
		{TS: time.Now(), Folder: "data", Path: "locked.bin", Category: CatError, Severity: SevWarn, Reason: "file is locked/in use — retrying"},
	}); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.CountOpen(); n != 1 {
		t.Fatalf("CountOpen after a batched re-record = %d, want 1: the condition must re-open", n)
	}
}

// TestRecordBatchEmptyIsNoOp: an empty batch costs no transaction (the commit
// path calls it with whatever a flush produced, including nothing).
func TestRecordBatchEmptyIsNoOp(t *testing.T) {
	s, rows := newBatchStore(t)
	if err := s.RecordBatch(nil); err != nil {
		t.Fatal(err)
	}
	if got := s.TxCount(); got != 0 {
		t.Fatalf("TxCount = %d after an empty batch, want 0", got)
	}
	if len(*rows) != 0 {
		t.Fatalf("transactions = %v, want none", *rows)
	}
}
