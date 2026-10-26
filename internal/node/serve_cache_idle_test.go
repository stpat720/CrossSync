package node

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/protocol"
)

// TestServeBlockClosesIdleHandles pins the daily-use side effect of the handle
// cache: without an idle close, a served file stays open until 32 newer files
// push it out — which on Unraid keeps that file "in use" and stops the array
// spinning down. A handle untouched for serveHandleIdleTTL must be closed, and
// the next request for it must simply reopen.
func TestServeBlockClosesIdleHandles(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	const blockSize = 4 << 10

	writePattern(t, root, "idle.bin", pattern(7, blockSize, rand.New(rand.NewSource(7))))
	writePattern(t, root, "other.bin", pattern(8, blockSize, rand.New(rand.NewSource(8))))

	oldTTL := serveHandleIdleTTL
	serveHandleIdleTTL = 40 * time.Millisecond
	t.Cleanup(func() { serveHandleIdleTTL = oldTTL })

	opens := countServeOpens(t)

	if resp := n.serveBlock(blockReq("idle.bin", 0, blockSize)); resp.Code != protocol.RespNoError {
		t.Fatalf("first read failed: %d", resp.Code)
	}
	if got := n.serveCache.len(); got != 1 {
		t.Fatalf("cache holds %d handles, want 1", got)
	}

	time.Sleep(3 * serveHandleIdleTTL)

	// Serving a different file sweeps the idle handle: the array is no longer
	// holding idle.bin open.
	if resp := n.serveBlock(blockReq("other.bin", 0, blockSize)); resp.Code != protocol.RespNoError {
		t.Fatalf("second read failed: %d", resp.Code)
	}
	if got := n.serveCache.len(); got != 1 {
		t.Fatalf("cache holds %d handles after the idle sweep, want only the fresh one", got)
	}

	// And the idle file still serves correctly (a reopen, not a miss).
	before := atomic.LoadInt32(opens)
	resp := n.serveBlock(blockReq("idle.bin", 0, blockSize))
	if resp.Code != protocol.RespNoError {
		t.Fatalf("re-read failed: %d", resp.Code)
	}
	if !bytes.Equal(resp.Data, pattern(7, blockSize, rand.New(rand.NewSource(7)))) {
		t.Fatalf("re-read returned the wrong bytes")
	}
	if got := atomic.LoadInt32(opens) - before; got != 1 {
		t.Fatalf("idle file was not reopened (%d opens)", got)
	}
}
