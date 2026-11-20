package control_test

import (
	"sync/atomic"
	"testing"

	"crosssync/internal/control"
	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/node"
)

// TestStatusDoesNotQuerySQL pins the second half of the /api/status cost fix.
//
// With the statfs and the versions-area walk cached, the remainder of that call
// on Tower was per-request SQL against the meta store: a global open-event count,
// one open-event count per folder, and one index aggregate per folder (~83 ms for
// the aggregate alone). The UI polls this endpoint every 2 s, so all three are now
// refreshed by the same 5 s background ticker that refreshes the disk figures.
//
// The three queries are replaced by counters here: the cache is seeded through
// them once (the ticker's path, Node.RefreshStatusCounts), and then 20 Status()
// calls must make ZERO queries while still returning the cached values.
func TestStatusDoesNotQuerySQL(t *testing.T) {
	n, _ := testNode(t)

	var queries atomic.Int64
	restore := node.SetStatusQueryHook(
		func(*events.Store) (int64, error) {
			queries.Add(1)
			return 11, nil
		},
		func(*events.Store, string) (int64, error) {
			queries.Add(1)
			return 3, nil
		},
		func(*index.Index) (index.Stats, error) {
			queries.Add(1)
			return index.Stats{
				Files:         5,
				Directories:   2,
				Tombstones:    1,
				Size:          4096,
				ConflictFiles: 1,
				ConflictBytes: 512,
			}, nil
		},
	)
	t.Cleanup(restore)

	// This is the ticker's path (Node.StartPendingRefresher), not a request:
	// one global count, one per-folder count, one index aggregate.
	n.RefreshStatusCounts()
	if got := queries.Load(); got != 3 {
		t.Fatalf("one refresh made %d quer(ies), want 3 (global open + per-folder open + index stats)", got)
	}
	queries.Store(0)

	svc := control.New(n, "test")
	for i := 0; i < 20; i++ {
		st := svc.Status()
		if st.OpenEvents != 11 {
			t.Fatalf("call %d: top-level open_events = %d, want the cached 11", i, st.OpenEvents)
		}
		if len(st.Folders) != 1 {
			t.Fatalf("call %d: folders = %d, want 1", i, len(st.Folders))
		}
		f := st.Folders[0]
		if f.Files != 5 || f.Directories != 2 || f.Tombstones != 1 || f.Size != 4096 {
			t.Fatalf("call %d: counts = %d files / %d dirs / %d tombstones / %d bytes, want the cached 5 / 2 / 1 / 4096",
				i, f.Files, f.Directories, f.Tombstones, f.Size)
		}
		if f.ConflictFiles != 1 || f.ConflictBytes != 512 {
			t.Fatalf("call %d: conflicts = %d / %d, want the cached 1 / 512", i, f.ConflictFiles, f.ConflictBytes)
		}
		if f.OpenEvents != 3 {
			t.Fatalf("call %d: folder open_events = %d, want the cached 3", i, f.OpenEvents)
		}
	}
	if got := queries.Load(); got != 0 {
		t.Fatalf("/api/status made %d SQL quer(ies) across 20 requests, want 0: "+
			"the handler must read the cache refreshed by the 5 s ticker, never query the meta store", got)
	}
}
