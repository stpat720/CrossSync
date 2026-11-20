package node

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/ignore"
	"crosssync/internal/index"
	"crosssync/internal/scanner"
	"crosssync/internal/staging"
	syncpkg "crosssync/internal/sync"
)

// One index commit per pull batch.
//
// The pull path used to call Ix.Put for every committed file: one SQLite WAL
// commit (and its fsync) per file, part of the measured ~9-10 s fixed per-file
// cost on the live array. The entries are now written with ONE PutBatch per
// batch (pullBatch.flushIndex). These tests pin the three properties that make
// that safe: N files cost one transaction, the index and files_down agree
// afterwards, and a failed transaction falls back to per-file writes instead of
// losing entries.

// newPullIndexFixture builds a real folder (index, staging area, engine) and
// indexes the given files into it, so a Pull can be started and committed
// exactly as the peer path does it.
func newPullIndexFixture(t *testing.T, contents map[string]string) *Folder {
	t.Helper()
	root := t.TempDir()
	ix, err := index.Open(filepath.Join(root, ".sfx-index", "data.db"), "data")
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	stag, err := staging.New(root)
	if err != nil {
		t.Fatalf("staging: %v", err)
	}
	ig, err := ignore.Parse(nil)
	if err != nil {
		t.Fatalf("ignore: %v", err)
	}
	eng := syncpkg.New(1, "data", root, syncpkg.ParseConflictPolicy("conflict-copy"), nil, ix, stag, ig, t.Logf)
	sc := scanner.New(root, ix, ig)
	sc.SetSettleScans(1)   // pre-gate behaviour: one pass indexes everything
	sc.SetDeletionScans(1) //
	for name, body := range contents {
		abs := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(abs, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	changes, err := sc.Scan()
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if _, _, err := eng.ApplyLocalChanges(changes); err != nil {
		t.Fatalf("apply: %v", err)
	}
	return &Folder{Cfg: config.Folder{ID: "data"}, Root: root, Ix: ix, Stager: stag, Engine: eng}
}

// pullFixtureFile pulls one indexed name from "the peer": every block is
// supplied from the file's own bytes, which is what the wire would carry. It
// returns the unflushed job (its file is committed, its entry is not yet in the
// index).
func pullFixtureFile(t *testing.T, f *Folder, name string) *pullJob {
	t.Helper()
	pull, err := f.Engine.StartPull(name, 0)
	if err != nil {
		t.Fatalf("start pull %s: %v", name, err)
	}
	data, err := os.ReadFile(filepath.Join(f.Root, filepath.FromSlash(name)))
	if err != nil {
		t.Fatalf("read source %s: %v", name, err)
	}
	for _, need := range pull.NeedBlocks() {
		if err := pull.ReceiveBlock(need.Index, data[need.Offset:int64(need.Size)+need.Offset]); err != nil {
			t.Fatalf("receive block %d of %s: %v", need.Index, name, err)
		}
	}
	return &pullJob{name: name, pull: pull, admitAt: time.Now()}
}

// TestPullBatchWritesOneIndexCommitForManyFiles: five files committed in one
// batch cost ONE index write, all five entries are in the index afterwards, and
// files_down counts exactly the five the index knows.
func TestPullBatchWritesOneIndexCommitForManyFiles(t *testing.T) {
	const files = 5
	contents := map[string]string{}
	for i := 0; i < files; i++ {
		// Distinct content per file: identical bytes would let a pull be served
		// from a local copy (localCopyDonor) and skip the path under test.
		contents[fmt.Sprintf("tex/f-%d.bin", i)] = fmt.Sprintf("%s-%d", "payload", i)
	}
	f := newPullIndexFixture(t, contents)
	b := &pullBatch{
		n:        &Node{Folders: map[string]*Folder{}, Logf: t.Logf},
		f:        f,
		k:        4,
		inFlight: make(map[uint64]pullRef),
	}

	jobs := make([]*pullJob, 0, files)
	for name := range contents {
		jobs = append(jobs, pullFixtureFile(t, f, name))
	}
	// Nothing is written while the files are committing: that is the whole point.
	writesBefore := f.Ix.WriteSeq()
	for _, job := range jobs {
		b.jobs = append(b.jobs, job)
		if err := b.complete(job); err != nil {
			t.Fatalf("complete %s: %v", job.name, err)
		}
	}
	if got := f.Ix.WriteSeq() - writesBefore; got != 0 {
		t.Fatalf("%d index write(s) during commit, want 0: the entries must wait for the batch's transaction", got)
	}
	if got := len(b.committed); got != files {
		t.Fatalf("batched entries = %d, want %d", got, files)
	}
	// The files are on disk and the index does not know them yet: they must read
	// as pending-index, not as a local add, until the flush lands.
	for name := range contents {
		if !f.Engine.PendingIndex(name) {
			t.Fatalf("%s is committed on disk but not marked pending-index: a scan would adopt it as a local add", name)
		}
	}

	if err := b.flushIndex(); err != nil {
		t.Fatalf("flushIndex: %v", err)
	}
	if got := f.Ix.WriteSeq() - writesBefore; got != 1 {
		t.Fatalf("%d index write(s) for %d committed files, want 1 (one commit per batch, not one per file)", got, files)
	}
	if got := f.Engine.Stats.PullIndexWrites.Load(); got != 1 {
		t.Fatalf("PullIndexWrites = %d, want 1", got)
	}
	if got := f.Engine.Stats.PullIndexEntries.Load(); got != files {
		t.Fatalf("PullIndexEntries = %d, want %d", got, files)
	}
	for name := range contents {
		fi, ok, err := f.Ix.Get(name)
		if err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		if !ok || fi.Deleted {
			t.Fatalf("%s missing from the index after the flush", name)
		}
		if fi.Size != int64(len(contents[name])) {
			t.Fatalf("%s indexed with size %d, want %d", name, fi.Size, len(contents[name]))
		}
		if f.Engine.PendingIndex(name) {
			t.Fatalf("%s still marked pending-index after the flush: scans would skip it forever", name)
		}
	}
	// files_down moves with the index, never ahead of it.
	if got := f.Engine.Stats.FilesDown.Load(); got != files {
		t.Fatalf("files_down = %d, want %d", got, files)
	}
	if got := f.Engine.Stats.PullFiles.Load(); got != files {
		t.Fatalf("pull stage samples = %d, want %d", got, files)
	}
}

// TestPullIndexFlushEmptyBatchIsFree: a batch that committed nothing must not
// open a transaction (an idle folder would otherwise write an empty one per
// turn).
func TestPullIndexFlushEmptyBatchIsFree(t *testing.T) {
	f := newPullIndexFixture(t, map[string]string{"a.txt": "a"})
	b := &pullBatch{n: &Node{Folders: map[string]*Folder{}, Logf: t.Logf}, f: f, inFlight: make(map[uint64]pullRef)}
	before := f.Ix.WriteSeq()
	if err := b.flushIndex(); err != nil {
		t.Fatalf("flushIndex: %v", err)
	}
	if got := f.Ix.WriteSeq() - before; got != 0 {
		t.Fatalf("%d index write(s) for an empty batch, want 0", got)
	}
	if got := f.Engine.Stats.PullIndexWrites.Load(); got != 0 {
		t.Fatalf("PullIndexWrites = %d for an empty batch, want 0", got)
	}
}

// countingIndex is an indexWriter whose batch can be made to fail, so the
// fallback decision can be exercised without a database.
type countingIndex struct {
	batchErr error
	putErr   map[string]error
	batches  int
	puts     int
	written  []string
}

func (c *countingIndex) Put(fi *index.FileInfo) error {
	c.puts++
	if err := c.putErr[fi.Name]; err != nil {
		return err
	}
	c.written = append(c.written, fi.Name)
	return nil
}

func (c *countingIndex) PutBatch(files []*index.FileInfo) error {
	c.batches++
	if c.batchErr != nil {
		return c.batchErr
	}
	for _, fi := range files {
		c.written = append(c.written, fi.Name)
	}
	return nil
}

// TestWriteIndexEntriesUsesOneTransaction: N entries, one PutBatch, no per-file
// puts.
func TestWriteIndexEntriesUsesOneTransaction(t *testing.T) {
	entries := []*index.FileInfo{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	ix := &countingIndex{}
	written, failed, batchErr := writeIndexEntries(ix, entries)
	if batchErr != nil || len(failed) != 0 {
		t.Fatalf("batch path reported an error: %v / %v", batchErr, failed)
	}
	if ix.batches != 1 || ix.puts != 0 {
		t.Fatalf("batches=%d puts=%d, want 1 batch and no puts", ix.batches, ix.puts)
	}
	if len(written) != len(entries) || len(ix.written) != len(entries) {
		t.Fatalf("wrote %d/%d entries, want %d", len(written), len(ix.written), len(entries))
	}
}

// TestWriteIndexEntriesFallsBackToPerFilePuts: a failed transaction must not
// cost an entry — every file is already committed on disk, so the entries are
// retried one by one and the failures are reported per file.
func TestWriteIndexEntriesFallsBackToPerFilePuts(t *testing.T) {
	entries := []*index.FileInfo{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	ix := &countingIndex{batchErr: errors.New("database is locked")}
	written, failed, batchErr := writeIndexEntries(ix, entries)
	if batchErr == nil {
		t.Fatal("the batch error must be reported so the fallback is visible in the log")
	}
	if ix.batches != 1 || ix.puts != len(entries) {
		t.Fatalf("batches=%d puts=%d, want 1 batch followed by %d puts", ix.batches, ix.puts, len(entries))
	}
	if len(written) != len(entries) || len(failed) != 0 {
		t.Fatalf("written=%d failed=%v, want every entry written one by one", len(written), failed)
	}
}

// TestWriteIndexEntriesReportsTheEntriesThatStillFail: an entry that cannot be
// written even alone is named, so the operator sees the file, not the batch.
func TestWriteIndexEntriesReportsTheEntriesThatStillFail(t *testing.T) {
	entries := []*index.FileInfo{{Name: "a"}, {Name: "b"}}
	ix := &countingIndex{
		batchErr: errors.New("database is locked"),
		putErr:   map[string]error{"b": errors.New("disk I/O error")},
	}
	written, failed, _ := writeIndexEntries(ix, entries)
	if len(written) != 1 || written[0].Name != "a" {
		t.Fatalf("written = %v, want just a", written)
	}
	if len(failed) != 1 || failed["b"] == nil {
		t.Fatalf("failed = %v, want b reported with its own error", failed)
	}
}
