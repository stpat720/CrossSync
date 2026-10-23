package index

import (
	"path/filepath"
	"testing"
)

// TestDurabilityIsNormalNotFull pins the WAL durability setting.
//
// synchronous=FULL fsyncs on every commit; against the index that made each
// applied file cost a disk flush (measured ~10 small files/s). NORMAL keeps WAL
// crash consistency while dropping the per-commit fsync, which is safe because
// this index is a cache of what is on disk: a lost commit only means the next
// scan re-stats and re-hashes the file, and the data plane fsyncs the file
// itself before any entry is written. If someone raises this to FULL again it
// should be a deliberate, measured decision — hence this test.
func TestDurabilityIsNormalNotFull(t *testing.T) {
	ix, err := Open(filepath.Join(t.TempDir(), "ix.db"), "f1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ix.Close() }()

	var synchronous int
	if err := ix.db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil {
		t.Fatalf("read pragma: %v", err)
	}
	// 0=OFF 1=NORMAL 2=FULL 3=EXTRA
	if synchronous != 1 {
		t.Fatalf("index synchronous = %d, want 1 (NORMAL): FULL costs an fsync per commit", synchronous)
	}

	var journalMode string
	if err := ix.db.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal_mode = %q, want wal (NORMAL is only crash-safe with WAL)", journalMode)
	}
}
