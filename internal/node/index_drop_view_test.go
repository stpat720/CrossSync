package node

import (
	"fmt"
	"net"
	"testing"
	"time"

	"crosssync/internal/transfer"
)

// TestMidIndexDropKeepsPeerViewIntact pins the peer-view invariant behind the
// "pending ↑" number.
//
// A full index exchange is chunked (indexChunkSize entries per frame). The
// receiver used to sweep its stored view of the peer on the FIRST frame and
// append the rest, so a session that died mid-exchange left a PREFIX of the
// peer's index stored as if it were the whole thing. Everything the local node
// held beyond that prefix then counted as "the peer still needs this from me" —
// tens of thousands of files of phantom upload work, which is exactly the
// number the dashboard showed (84,353 - 54,353 = 30,000 on the live pair).
//
// The sweep must therefore happen only on the FINAL frame: an interrupted
// exchange leaves the last COMPLETE view, so the count never inflates.
func TestMidIndexDropKeepsPeerViewIntact(t *testing.T) {
	old := indexChunkSize
	indexChunkSize = 25
	defer func() { indexChunkSize = old }()

	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	// The same corpus on both nodes: identical content and mtime, added
	// independently (so each side's view of the other is "we hold the same
	// files"), which is what makes any phantom count visible.
	const n = 400
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("doc-%04d.txt", i)
		chunkWrite(t, rootA, name, fmt.Sprintf("identical body %d", i))
		chunkWrite(t, rootB, name, fmt.Sprintf("identical body %d", i))
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// Session 1: a clean round with chunked exchange in both directions.
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}
	mustPending := func(node *Node) int {
		t.Helper()
		ups, err := node.Folders["data"].Engine.PendingUploads()
		if err != nil {
			t.Fatal(err)
		}
		return len(ups)
	}
	if got := mustPending(b); got != 0 {
		t.Fatalf("after a clean round b reports %d pending uploads, want 0", got)
	}
	if got := b.peerCountFor(t, "data", a.ID); got != n {
		t.Fatalf("b's view of a = %d entries after a clean round, want %d", got, n)
	}

	// Session 2: B restarts (the deploy / reconnect case) so its in-memory view
	// of A is EMPTY and A must stream its FULL index. The stream is cut
	// mid-way, which is exactly how the live pair ended up with a truncated
	// view: a fresh process, one interrupted full exchange.
	metaB := t.TempDir()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = chunkTestNode(t, 2, "b", rootB, metaB)
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	serveDone := make(chan error, 1)
	go func() {
		conn, err := transfer.Accept(ln)
		if err != nil {
			serveDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		serveDone <- a.Serve(conn)
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	wrapped := transfer.NewConn(&dropAfterBytes{Conn: raw, remaining: 8 * 1024})
	if _, err := b.SyncOnce(wrapped, a.ID); err == nil {
		t.Fatalf("the round should have failed when the link was cut mid-exchange")
	}
	_ = raw.Close()
	select {
	case err := <-serveDone:
		_ = err
	case <-time.After(30 * time.Second):
		t.Fatal("server side did not finish after the drop")
	}

	// A truncated view must never be reported as "the peer lacks these files":
	// while the exchange is incomplete the honest answer is unknown, so the
	// upload count stays 0 instead of counting the 300+ files past the cut.
	if got := mustPending(b); got != 0 {
		t.Fatalf("after an interrupted full exchange b reports %d pending uploads; a truncated peer view must not be read as truth", got)
	}

	// And a follow-up clean round still converges (the incomplete exchange left
	// no half-applied state behind).
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatalf("the round after an interrupted exchange failed: %v", err)
	}
	if got := mustPending(b); got != 0 {
		t.Fatalf("b reports %d pending uploads after recovery, want 0", got)
	}
}
