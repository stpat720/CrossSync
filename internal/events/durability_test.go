package events

import (
	"path/filepath"
	"testing"
)

// TestEventLogDurabilityIsNormal pins the WAL durability setting for the event
// log. FULL fsynced on every append — and every applied file appends several
// events, so it dominated the cost of syncing many small files. The log is not
// authoritative state (the files and the index are), so dropping the last few
// frames on a power loss is acceptable; WAL keeps the file consistent either way.
func TestEventLogDurabilityIsNormal(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	var synchronous int
	if err := s.db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil {
		t.Fatalf("read pragma: %v", err)
	}
	if synchronous != 1 {
		t.Fatalf("events synchronous = %d, want 1 (NORMAL)", synchronous)
	}
	var journalMode string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", journalMode)
	}
}
