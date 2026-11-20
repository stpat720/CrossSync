package sync

import (
	"testing"

	"crosssync/internal/index"
	"crosssync/internal/scanner"
	"crosssync/internal/version"
)

// A pulled file whose index entry has not been written yet (the pull batch
// writes its entries together — see pullBatch.flushIndex) must not be adopted by
// a scan as a new local file.
//
// Without the guard the scan sees a file with no index entry, treats it as an
// add, and records a freshly bumped local vector. That supersedes the version
// just pulled, so identical content would travel the cluster as a local edit —
// and on a share other machines write to over SMB the scoped watcher pass lands
// inside the window routinely, not occasionally.
func TestScanDoesNotAdoptAFileWhoseIndexEntryIsStillPending(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	a.writeFile(t, "tex/pulled.bin", "pulled-bytes")

	// The pulled entry the batch is about to write: version 2 (the peer's), not
	// anything this device bumped.
	pulled := &index.FileInfo{
		Name: "tex/pulled.bin", Size: int64(len("pulled-bytes")),
		ModifiedS: 1700000000, Type: index.TypeFile,
		Version: version.New().Bump(2),
	}
	a.engine.markPendingIndex(pulled.Name)
	if !a.engine.PendingIndex(pulled.Name) {
		t.Fatal("PendingIndex must report the name it was just given")
	}

	// The scan of the tree reports the file as added (no index entry).
	got, err := a.engine.ApplyLocalChange(scanner.Added, pulled.Clone())
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got != nil {
		t.Fatalf("a pending-index file was recorded as a local change: %+v", got)
	}
	if _, ok, err := a.ix.Get(pulled.Name); err != nil || ok {
		t.Fatalf("the index has an entry for a pending-index file (ok=%v err=%v): the scan must leave it to the pull's own write", ok, err)
	}

	// The batch writes its entry; from then on the same name is an ordinary
	// index entry and the scan sees it as unchanged (nothing to adopt).
	a.engine.ClearPendingIndex(pulled.Name)
	if a.engine.PendingIndex(pulled.Name) {
		t.Fatal("ClearPendingIndex did not clear the mark: scans would skip the path forever")
	}
	if err := a.ix.Put(pulled); err != nil {
		t.Fatal(err)
	}
	cur, ok, err := a.ix.Get(pulled.Name)
	if err != nil || !ok {
		t.Fatalf("the pull's entry did not land: ok=%v err=%v", ok, err)
	}
	if !cur.Version.Equal(pulled.Version) {
		t.Fatalf("version = %v, want the pulled %v", cur.Version, pulled.Version)
	}
}

// The batch form of the same rule: one pass over a tree that contains a
// pending-index file must apply every other change and leave that one alone.
func TestApplyLocalChangesSkipsPendingIndexNames(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	pending := &index.FileInfo{
		Name: "pulled.bin", Size: 4, ModifiedS: 1700000000, Type: index.TypeFile,
		Version: version.New().Bump(2),
	}
	other := &index.FileInfo{
		Name: "local.txt", Size: 5, ModifiedS: 1700000001, Type: index.TypeFile,
		Version: version.New().Bump(1),
	}
	a.engine.markPendingIndex(pending.Name)

	applied, failed, err := a.engine.ApplyLocalChanges([]scanner.Change{
		{Kind: scanner.Added, Info: pending.Clone()},
		{Kind: scanner.Added, Info: other.Clone()},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if applied != 1 || len(failed) != 0 {
		t.Fatalf("applied=%d failed=%d, want only the non-pending change applied", applied, len(failed))
	}
	if _, ok, _ := a.ix.Get(pending.Name); ok {
		t.Fatal("the pending-index file was written by the scan")
	}
	if _, ok, _ := a.ix.Get(other.Name); !ok {
		t.Fatal("the unrelated change was dropped with it")
	}
}
