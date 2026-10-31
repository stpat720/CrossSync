package index

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestSchemaVersionIsRecorded: a fresh index is stamped with the layout this
// binary understands, so a later upgrade knows what it is looking at.
func TestSchemaVersionIsRecorded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	ix, err := Open(path, "f")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != schemaVersion {
		t.Fatalf("user_version = %d, want %d", v, schemaVersion)
	}
}

// TestOlderIndexIsMigrated: an index written before the stamp existed (version 0)
// must be adopted, not rejected — that is every index in the field today.
func TestOlderIndexIsMigrated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	ix, err := Open(path, "f")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version=0`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	ix2, err := Open(path, "f")
	if err != nil {
		t.Fatalf("an unstamped index must be migrated, got %v", err)
	}
	if err := ix2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestNewerIndexIsRefused: reading an index from a NEWER build with older code is
// how an index gets silently corrupted, so it must fail with both versions named.
func TestNewerIndexIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	ix, err := Open(path, "f")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version=999`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	_, err = Open(path, "f")
	if err == nil {
		t.Fatal("an index from a newer build must be refused")
	}
	if !strings.Contains(err.Error(), "999") || !strings.Contains(err.Error(), "newer build") {
		t.Fatalf("error should name both versions and the reason, got: %v", err)
	}
}
