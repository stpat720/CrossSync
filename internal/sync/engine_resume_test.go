package sync

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/index"
)

// TestPullResumesAfterAbort verifies block-level resume: an interrupted pull
// leaves a valid staged temp, and a subsequent StartPull of the same file
// reuses it, re-verifying the already-written prefix and fetching only the
// remaining blocks. The test counts how many blocks are actually fetched in
// the second pull to prove the first blocks are not re-sent.
func TestPullResumesAfterAbort(t *testing.T) {
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

	srcc, err := os.Open(filepath.Join(a.root, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srcc.Close() }()

	blockCount := len(fi.Blocks)
	if blockCount < 2 {
		t.Fatalf("expected a multi-block file, got %d blocks", blockCount)
	}

	// First pull: fetch and write the FIRST block, then abort mid-transfer.
	pull1, err := b.engine.StartPull("big.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	needs := pull1.NeedBlocks()
	if len(needs) != blockCount {
		t.Fatalf("fresh pull needs %d blocks, want %d", len(needs), blockCount)
	}
	first := needs[0]
	buf := make([]byte, first.Size)
	if _, err := srcc.ReadAt(buf, first.Offset); err != nil {
		t.Fatal(err)
	}
	if err := pull1.ReceiveBlock(first.Index, buf); err != nil {
		t.Fatal(err)
	}
	pull1.Abort()
	// The staged temp must survive the abort so the next pull can resume.
	if _, err := os.Stat(pull1.tmp); err != nil {
		t.Fatalf("staged temp should survive abort (resume), err=%v", err)
	}

	// Second pull: must resume from the surviving temp and fetch ONLY the
	// remaining blocks (the first block is already valid on disk).
	fetched := 0
	pull2, err := b.engine.StartPull("big.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	needs2 := pull2.NeedBlocks()
	if len(needs2) != blockCount-1 {
		t.Fatalf("resumed pull needs %d blocks, want %d (first should be free)", len(needs2), blockCount-1)
	}
	for _, need := range needs2 {
		if need.Index == 0 {
			t.Fatal("block 0 should have been reused from the staged temp, not re-fetched")
		}
		fetched++
		buf := make([]byte, need.Size)
		if _, err := srcc.ReadAt(buf, need.Offset); err != nil {
			t.Fatal(err)
		}
		if err := pull2.ReceiveBlock(need.Index, buf); err != nil {
			t.Fatal(err)
		}
	}
	if fetched != blockCount-1 {
		t.Fatalf("fetched %d blocks on resume, want %d", fetched, blockCount-1)
	}
	if _, err := pull2.Finish(); err != nil {
		t.Fatal(err)
	}
	if got := b.fileContent(t, "big.bin"); got != content {
		t.Fatalf("resumed pull produced wrong content (len %d vs %d)", len(got), len(content))
	}
	if needs, _ := b.engine.NeedPulls(); len(needs) != 0 {
		t.Fatalf("expected no more needs after resumed pull, got %v", needs)
	}
}

// TestPullResumeReverifiesAgainstNewVersion guards against resuming a stale
// partial temp: if the file's content changes between pulls but keeps the
// same size, the resume must re-verify against the NEW block hashes and
// re-fetch the stale blocks (not trust the old bytes).
func TestPullResumeReverifiesAgainstNewVersion(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	sameSize := 300_000 // 3 blocks
	a.writeFile(t, "big.bin", multiBlockContent(sameSize))
	a.scanAndApply(t)
	fi, _, _ := a.ix.Get("big.bin")
	b.engine.SetPeerIndex(1, []*index.FileInfo{fi}, false)

	srcc, err := os.Open(filepath.Join(a.root, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srcc.Close() }()

	// Start a pull of version 1 and write the first block, then abort.
	pull1, err := b.engine.StartPull("big.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	first := pull1.NeedBlocks()[0]
	buf := make([]byte, first.Size)
	if _, err := srcc.ReadAt(buf, first.Offset); err != nil {
		t.Fatal(err)
	}
	if err := pull1.ReceiveBlock(first.Index, buf); err != nil {
		t.Fatal(err)
	}
	pull1.Abort()
	_ = srcc.Close()

	// The peer now has a DIFFERENT file of the SAME size (block 0 differs).
	var nb []byte
	for i := 0; i < sameSize; i++ {
		nb = append(nb, byte('a'+((i*7+3)%26)))
	}
	newContent := string(nb)
	a.writeFile(t, "big.bin", newContent)
	a.scanAndApply(t)
	fi2, _, _ := a.ix.Get("big.bin")
	b.engine.SetPeerIndex(1, []*index.FileInfo{fi2}, false)

	// Resume: the surviving temp holds v1 bytes but v2 hashes. The resume
	// must re-verify and re-fetch ALL blocks (none of v1 matches v2).
	srcc2, err := os.Open(filepath.Join(a.root, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srcc2.Close() }()
	pull2, err := b.engine.StartPull("big.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pull2.NeedBlocks()) != len(fi2.Blocks) {
		t.Fatalf("resume against new version should re-fetch all %d blocks, need=%d", len(fi2.Blocks), len(pull2.NeedBlocks()))
	}
	for _, need := range pull2.NeedBlocks() {
		buf := make([]byte, need.Size)
		if _, err := srcc2.ReadAt(buf, need.Offset); err != nil {
			t.Fatal(err)
		}
		if err := pull2.ReceiveBlock(need.Index, buf); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pull2.Finish(); err != nil {
		t.Fatal(err)
	}
	if got := b.fileContent(t, "big.bin"); got != newContent {
		t.Fatalf("resumed pull must use the NEW version's content")
	}
}
