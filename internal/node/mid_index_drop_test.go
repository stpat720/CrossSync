package node

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"crosssync/internal/index"
	"crosssync/internal/transfer"
)

// dropAfterBytes closes the connection once `remaining` bytes have been
// read, simulating a network drop mid-session.
type dropAfterBytes struct {
	net.Conn
	remaining int64
	mu        sync.Mutex
}

func (c *dropAfterBytes) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remaining <= 0 {
		_ = c.Close()
		return 0, errors.New("simulated mid-index drop")
	}
	n, err := c.Conn.Read(p)
	c.remaining -= int64(n)
	return n, err
}

// TestMidIndexDropDoesNotAdvanceMarker verifies the mid-index-exchange drop
// path: a session interrupted while the full index is still streaming must
// NOT persist an index marker past what the peer actually received. The
// marker only advances when a session completes end-to-end, so after the
// drop the stored marker still describes the last COMPLETED session and a
// follow-up session re-sends everything the peer missed.
func TestMidIndexDropDoesNotAdvanceMarker(t *testing.T) {
	old := indexChunkSize
	indexChunkSize = 25
	defer func() { indexChunkSize = old }()

	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	metaB := t.TempDir()
	b := chunkTestNode(t, 2, "b", rootB, metaB)

	const base = 2000
	for i := 0; i < base; i++ {
		chunkWrite(t, rootA, fmt.Sprintf("base-%04d.txt", i), fmt.Sprintf("a base %d", i))
	}
	chunkWrite(t, rootB, "from-b.txt", "hello from b")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// Session 1: clean full exchange in both directions.
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}

	// A's stored marker for peer B after the completed session.
	_, seqBefore, _, ok, err := a.Folders["data"].Ix.GetPeerIndex(b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("A should have recorded a marker for B after session 1")
	}

	// A changes 5 files after the marker.
	for i := 0; i < 5; i++ {
		chunkWrite(t, rootA, fmt.Sprintf("new-%d.txt", i), fmt.Sprintf("a new %d", i))
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// The expected delta: everything in A's index with a sequence past the
	// marker (the 5 new files plus anything A's rescan re-indexed, e.g.
	// from-b.txt which A pulled to disk in session 1).
	expectedDelta := map[string]bool{}
	if err := a.Folders["data"].Ix.ListAfter(seqBefore, func(fi *index.FileInfo) error {
		expectedDelta[fi.Name] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(expectedDelta) == 0 {
		t.Fatal("expected a non-empty delta after the rescan")
	}

	// B restarts (the production reconnect case): fresh session id and wiped
	// in-memory views, so A's stored marker is invalid and A must stream its
	// FULL index (~2000 entries, ~55 KB) in the next session.
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = chunkTestNode(t, 2, "b", rootB, metaB)
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// Session 2: B dials A. B reads its own tiny index, then A's full index.
	// A drop at 25 KB lands mid-way through that stream.
	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	wrapped := transfer.NewConn(&dropAfterBytes{Conn: raw, remaining: 25 * 1024})
	syncErr := func() error {
		_, err := b.SyncOnce(wrapped, a.ID)
		return err
	}()
	_ = raw.Close()
	_ = ln.Close()
	select {
	case serveErr := <-serveDone:
		if syncErr == nil {
			t.Fatalf("B.SyncOnce succeeded despite the mid-index drop (err on serve: %v)", serveErr)
		}
		if serveErr == nil {
			t.Fatalf("A.Serve completed despite the mid-index drop (b err: %v)", syncErr)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("A.Serve did not notice the drop")
	}

	// The marker must NOT have advanced past the last completed session:
	// the delta from the stored marker must be exactly the pre-computed set.
	_, seqAfter, _, ok, err := a.Folders["data"].Ix.GetPeerIndex(b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("marker vanished after interrupted session")
	}
	if seqAfter != seqBefore {
		t.Fatalf("marker advanced across an interrupted session: seq %d -> %d", seqBefore, seqAfter)
	}
	got := map[string]bool{}
	if err := a.Folders["data"].Ix.ListAfter(seqBefore, func(fi *index.FileInfo) error {
		got[fi.Name] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(expectedDelta) {
		t.Fatalf("delta after drop = %d entries, want %d: %v", len(got), len(expectedDelta), got)
	}
	for name := range expectedDelta {
		if !got[name] {
			t.Fatalf("delta after drop missing %s: %v", name, got)
		}
	}
	// B may hold a partial in-memory view of A (frames merged before the
	// drop). That is safe: B's session never completed against A's stored
	// marker, so the next session is a full exchange that replaces the
	// partial view. What must NOT happen is B pulling files from it.
	if _, err := os.Stat(filepath.Join(rootB, "new-0.txt")); !os.IsNotExist(err) {
		t.Fatal("B received new-0.txt from an interrupted session")
	}

	// Session 3: a clean session re-sends everything and converges: B's
	// view of A must exactly match A's actual file set.
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}
	wantView := 0
	if err := a.Folders["data"].Ix.ListAll(func(fi *index.FileInfo) error {
		wantView++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := b.Folders["data"].Engine.PeerCount(a.ID); got != wantView {
		t.Fatalf("B's peer view of A = %d entries after recovery, want %d (A's local count)", got, wantView)
	}
	for i := 0; i < 5; i++ {
		if got := readFile(t, rootB, fmt.Sprintf("new-%d.txt", i)); got != fmt.Sprintf("a new %d", i) {
			t.Fatalf("B[new-%d] = %q after recovery session", i, got)
		}
	}
}
