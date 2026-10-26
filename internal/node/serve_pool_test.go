package node

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"crosssync/internal/protocol"
)

// TestServeUntilServesConcurrently pins the responder-side worker pool.
//
// Serving one request at a time was the live pair's throughput ceiling: the disk
// read costs ~47 ms for a 128 KiB block on shfs, so a peer could never exceed
// blockSize/47ms no matter how deep its request pipeline was (measured 2.73 MB/s
// with 128 KiB blocks, 20.5 MB/s with 1 MiB blocks — both ~21 blocks/s). With a
// pool, a burst of requests overlaps.
func TestServeUntilServesConcurrently(t *testing.T) {
	oldWorkers := serveWorkers
	serveWorkers = 4
	t.Cleanup(func() { serveWorkers = oldWorkers })

	var cur, max int32
	serveWorkerHook = func(delta int) {
		v := atomic.AddInt32(&cur, int32(delta))
		for {
			m := atomic.LoadInt32(&max)
			if v <= m || atomic.CompareAndSwapInt32(&max, m, v) {
				return
			}
		}
	}
	t.Cleanup(func() { serveWorkerHook = nil })

	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	// 8 MiB = 64 blocks: the puller queues a burst of 8 requests, so a 4-worker
	// pool has work to overlap even on a fast tmpdir.
	body := pattern(21, 8<<20, rand.New(rand.NewSource(21)))
	writePattern(t, rootA, "burst.bin", body)
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(rootB, "burst.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("pulled %d bytes that do not match the source", len(got))
	}
	if m := atomic.LoadInt32(&max); m < 2 {
		t.Fatalf("max concurrent serveBlock = %d, want >= 2 with a 4-worker pool", m)
	}
}

// TestServeUntilPoolKeepsOrderPerRequest: with out-of-order responses the
// integrity of a multi-block pull is the thing that could break, so pull a file
// the size of many blocks and compare every byte (the pool must never answer a
// request with another request's data).
func TestServeUntilPoolKeepsRequestsDistinct(t *testing.T) {
	oldWorkers := serveWorkers
	serveWorkers = 8
	t.Cleanup(func() { serveWorkers = oldWorkers })

	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	const size = 6<<20 + 12345 // 49 blocks, non-uniform tail
	body := pattern(33, size, rand.New(rand.NewSource(33)))
	writePattern(t, rootA, "wide.bin", body)
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(rootB, "wide.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("content mismatch across a pooled serve: got %d bytes, want %d", len(got), len(body))
	}
	fi, ok, err := b.Folders["data"].Ix.Get("wide.bin")
	if err != nil || !ok {
		t.Fatalf("indexed entry missing: ok=%v err=%v", ok, err)
	}
	if len(fi.Blocks) < serveWorkers {
		t.Fatalf("only %d blocks; the pool was never exercised", len(fi.Blocks))
	}
	_ = protocol.RespNoError
}
