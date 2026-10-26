package node

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/protocol"
)

// countServeOpens swaps the cache's open seam for one that counts calls.
func countServeOpens(t *testing.T) *int32 {
	t.Helper()
	// The cache is off on Windows by build tag (a held handle blocks deletion
	// there); force it on so this box exercises the same logic the Linux daemons
	// run in production.
	oldEnabled := serveCacheEnabled
	serveCacheEnabled = true
	t.Cleanup(func() { serveCacheEnabled = oldEnabled })
	var opens int32
	old := serveOpenFile
	serveOpenFile = func(name string) (*os.File, error) {
		atomic.AddInt32(&opens, 1)
		return os.Open(name)
	}
	t.Cleanup(func() { serveOpenFile = old })
	return &opens
}

// blockReq builds a block request for the first `blocks` blocks of a file.
func blockReq(name string, i, size int) *protocol.Request {
	return &protocol.Request{ID: uint64(i + 1), Folder: "data", Name: name,
		Offset: int64(i) * int64(size), Size: size}
}

// TestServeBlockOpensOncePerFile pins the fix for the live pair's 2 MB/s: the
// serve path used to open, read and close the source file for EVERY block
// request, so a 64 MiB pull (512 blocks) paid 512 open() calls on shfs at ~66 ms
// each. The data was always correct — the open was the whole cost.
func TestServeBlockOpensOncePerFile(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())

	const blockSize = 128 << 10
	const blocks = 8
	data := pattern(9, blockSize*blocks, rand.New(rand.NewSource(9)))
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	writePattern(t, root, "served.bin", data)

	opens := countServeOpens(t)

	for i := 0; i < blocks; i++ {
		resp := n.serveBlock(blockReq("served.bin", i, blockSize))
		if resp.Code != protocol.RespNoError {
			t.Fatalf("block %d: code %d, want no error", i, resp.Code)
		}
		want := data[i*blockSize : (i+1)*blockSize]
		if !bytes.Equal(resp.Data, want) {
			t.Fatalf("block %d: served bytes do not match the file", i)
		}
	}
	if got := atomic.LoadInt32(opens); got != 1 {
		t.Fatalf("%d block requests caused %d opens; want 1 (the handle is cached and reused)", blocks, got)
	}
}

// TestServeBlockReopensWhenTheFileChanges: the cache must never serve a stale
// inode. A rewrite (new bytes and mtime) has to be re-opened, and a path that
// disappears has to answer "no such file" rather than serving the old handle —
// the puller reads that code as a converging delete.
func TestServeBlockReopensWhenTheFileChanges(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	const blockSize = 4 << 10

	first := pattern(3, blockSize, rand.New(rand.NewSource(3)))
	writePattern(t, root, "changing.bin", first)
	opens := countServeOpens(t)

	if resp := n.serveBlock(blockReq("changing.bin", 0, blockSize)); resp.Code != protocol.RespNoError || !bytes.Equal(resp.Data, first) {
		t.Fatalf("first read wrong: code=%d", resp.Code)
	}

	// Rewrite in place with new content and a newer mtime: same size, so only
	// the mtime check can catch it.
	second := pattern(4, blockSize, rand.New(rand.NewSource(4)))
	p := filepath.Join(root, "changing.bin")
	if err := os.WriteFile(p, second, 0o644); err != nil {
		t.Fatal(err)
	}
	newer := time.Now().Add(2 * time.Hour)
	if err := os.Chtimes(p, newer, newer); err != nil {
		t.Fatal(err)
	}
	resp := n.serveBlock(blockReq("changing.bin", 0, blockSize))
	if resp.Code != protocol.RespNoError {
		t.Fatalf("second read: code %d", resp.Code)
	}
	if !bytes.Equal(resp.Data, second) {
		t.Fatalf("served the OLD bytes after the file was rewritten — the cached handle was not revalidated")
	}
	if got := atomic.LoadInt32(opens); got != 2 {
		t.Fatalf("%d opens after one rewrite, want 2", got)
	}

	// Delete it: the next request must be "no such file", not stale bytes.
	// Handles are released first because Windows refuses to unlink a file that is
	// still open — on Linux the Stat revalidation below is what catches it.
	n.serveCache.forget(true)
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if resp := n.serveBlock(blockReq("changing.bin", 0, blockSize)); resp.Code != protocol.RespNoSuchFile {
		t.Fatalf("after delete: code %d, want RespNoSuchFile (a cached handle must not outlive the path)", resp.Code)
	}
	// And the dead handle was dropped from the cache.
	if got := n.serveCache.len(); got != 0 {
		t.Fatalf("cache still holds %d handles after the file was deleted", got)
	}
}

// TestServeBlockCacheIsBounded: many distinct files must not accumulate open
// handles (a daemon serving an 8 TB share for weeks cannot leak fds).
func TestServeBlockCacheIsBounded(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	const blockSize = 4 << 10
	countServeOpens(t)

	for i := 0; i < maxServeHandles*3; i++ {
		name := filepath.Join("many", "f"+string(rune('a'+i%26))+string(rune('a'+i/26))+".bin")
		writePattern(t, root, name, pattern(byte(i+1), blockSize, rand.New(rand.NewSource(int64(i+1)))))
		if resp := n.serveBlock(blockReq(name, 0, blockSize)); resp.Code != protocol.RespNoError {
			t.Fatalf("%s: code %d", name, resp.Code)
		}
	}
	if got := n.serveCache.len(); got > maxServeHandles {
		t.Fatalf("cache holds %d handles, cap is %d", got, maxServeHandles)
	}
}
