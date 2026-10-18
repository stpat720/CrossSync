package sync

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/index"
)

// multiBlockContent builds a deterministic file large enough to span several
// blocks (min block size is 128 KiB, so > 128 KiB yields multiple blocks).
func multiBlockContent(size int) string {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte('a' + (i%26))
	}
	return string(buf)
}

// TestCorruptBlockRejectedAndRepull verifies the mid-transfer corruption
// path end to end at the engine level: a block whose bytes do not match its
// recorded hash is rejected WITHOUT being written (no partial data lands),
// a Finish while a block is missing is a clean abort (no partial commit),
// Abort cleans the staged temp, the file stays pending, and a fresh pull of
// the correct bytes succeeds.
func TestCorruptBlockRejectedAndRepull(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	content := multiBlockContent(300_000) // 3 blocks at 128 KiB
	a.writeFile(t, "big.bin", content)
	a.scanAndApply(t)

	fi, ok, _ := a.ix.Get("big.bin")
	if !ok {
		t.Fatal("a should have indexed the file")
	}
	b.engine.SetPeerIndex(1, []*index.FileInfo{fi}, false)

	pull, err := b.engine.StartPull("big.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	srcc, err := os.Open(filepath.Join(a.root, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srcc.Close() }()

	needs := pull.NeedBlocks()
	if len(needs) < 2 {
		t.Fatalf("expected a multi-block file, got %d blocks", len(needs))
	}

	// Serve the FIRST block corrupted: flip a byte so the hash no longer
	// matches its recorded value.
	need := needs[0]
	buf := make([]byte, need.Size)
	if _, err := srcc.ReadAt(buf, need.Offset); err != nil {
		t.Fatal(err)
	}
	corrupted := make([]byte, need.Size)
	copy(corrupted, buf)
	corrupted[len(corrupted)/2] ^= 0xFF
	if err := pull.ReceiveBlock(need.Index, corrupted); err == nil {
		t.Fatal("corrupted block should be rejected")
	}
	// The rejected block must not be marked written: Finish is a clean
	// abort (missing block), not a partial commit.
	if _, err := pull.Finish(); err == nil {
		t.Fatal("Finish with a missing block should fail (no partial commit)")
	}
	// Abort cleans the staged temp; the target must not exist yet.
	pull.Abort()
	if _, err := os.Stat(filepath.Join(b.root, "big.bin")); !os.IsNotExist(err) {
		t.Fatalf("target must not exist after aborted pull (err=%v)", err)
	}
	if needs, _ := b.engine.NeedPulls(); len(needs) != 1 {
		t.Fatalf("file must remain pending after aborted pull, got %v", needs)
	}

	// A fresh pull with the correct bytes succeeds.
	pull2, err := b.engine.StartPull("big.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, need := range pull2.NeedBlocks() {
		buf := make([]byte, need.Size)
		if _, err := srcc.ReadAt(buf, need.Offset); err != nil {
			t.Fatal(err)
		}
		if err := pull2.ReceiveBlock(need.Index, buf); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pull2.Finish(); err != nil {
		t.Fatal(err)
	}
	if got := b.fileContent(t, "big.bin"); got != content {
		t.Fatalf("pulled content mismatch after re-pull (len %d vs %d)", len(got), len(content))
	}
	if needs, _ := b.engine.NeedPulls(); len(needs) != 0 {
		t.Fatalf("expected no more needs after successful re-pull, got %v", needs)
	}
}

// TestWriteFailureMidPull verifies the disk-full / I/O-failure path: a
// write error mid-pull (simulated by closing the staged file) leaves no
// partial commit, Abort removes the staged temp, the file stays pending, and
// a retry from scratch succeeds.
func TestWriteFailureMidPull(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	content := multiBlockContent(300_000)
	a.writeFile(t, "big.bin", content)
	a.scanAndApply(t)

	fi, ok, _ := a.ix.Get("big.bin")
	if !ok {
		t.Fatal("a should have indexed the file")
	}
	b.engine.SetPeerIndex(1, []*index.FileInfo{fi}, false)

	pull, err := b.engine.StartPull("big.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	srcc, err := os.Open(filepath.Join(a.root, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srcc.Close() }()

	// Write the first block cleanly, then simulate a mid-pull I/O failure by
	// closing the staged file (the next WriteAt errors, like ENOSPC would).
	first := pull.NeedBlocks()[0]
	buf := make([]byte, first.Size)
	if _, err := srcc.ReadAt(buf, first.Offset); err != nil {
		t.Fatal(err)
	}
	if err := pull.ReceiveBlock(first.Index, buf); err != nil {
		t.Fatal(err)
	}
	_ = pull.f.Close()

	next := pull.NeedBlocks()
	if len(next) == 0 {
		t.Fatal("expected remaining blocks")
	}
	buf2 := make([]byte, next[0].Size)
	_, _ = srcc.ReadAt(buf2, next[0].Offset)
	if err := pull.ReceiveBlock(next[0].Index, buf2); err == nil {
		t.Fatal("write to the closed staged file should fail")
	}
	// No partial commit, temp cleaned up, still pending.
	if _, err := os.Stat(filepath.Join(b.root, "big.bin")); !os.IsNotExist(err) {
		t.Fatalf("target must not exist after failed write (err=%v)", err)
	}
	pull.Abort()
	if needs, _ := b.engine.NeedPulls(); len(needs) != 1 {
		t.Fatalf("file must remain pending after failed write, got %v", needs)
	}

	// Retry from scratch succeeds.
	pull2, err := b.engine.StartPull("big.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, need := range pull2.NeedBlocks() {
		buf := make([]byte, need.Size)
		if _, err := srcc.ReadAt(buf, need.Offset); err != nil {
			t.Fatal(err)
		}
		if err := pull2.ReceiveBlock(need.Index, buf); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pull2.Finish(); err != nil {
		t.Fatal(err)
	}
	if got := b.fileContent(t, "big.bin"); got != content {
		t.Fatalf("pulled content mismatch after retry")
	}
}
