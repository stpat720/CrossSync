package node

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestWindowedPullMultiBlockIntegrity pins the pipelined pull end to end.
//
// The pull keeps pullWindowSize block requests in flight, so responses arrive
// interleaved across several blocks of the SAME file and can also be stale
// leftovers from an abandoned file (hence connection-scoped request IDs). This
// test pulls a file of many blocks across the real session harness and compares
// it BYTE FOR BYTE: any mis-ordered, duplicated or mismatched block shows up as
// a content mismatch instead of a quiet corruption.
func TestWindowedPullMultiBlockIntegrity(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	// 2 MiB of deterministic pseudo-random bytes.
	buf := make([]byte, 2<<20)
	x := uint32(12345)
	for i := range buf {
		x = x*1664525 + 1013904223
		buf[i] = byte(x >> 24)
	}
	p := filepath.Join(rootA, "multi.bin")
	if err := os.WriteFile(p, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	if err := chunkPipe(t, a, b); err != nil {
		t.Fatalf("round failed: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(rootB, "multi.bin"))
	if err != nil {
		t.Fatalf("received file missing: %v", err)
	}
	if !bytes.Equal(got, buf) {
		t.Fatalf("content mismatch after a windowed pull: got %d bytes, want %d (identical bytes required)", len(got), len(buf))
	}
	fi, ok, err := b.Folders["data"].Ix.Get("multi.bin")
	if err != nil || !ok {
		t.Fatalf("indexed entry missing on b: ok=%v err=%v", ok, err)
	}
	if len(fi.Blocks) < pullWindowSize {
		t.Fatalf("file has %d blocks; need more than the %d-deep window to exercise pipelining", len(fi.Blocks), pullWindowSize)
	}
}
