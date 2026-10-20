package node

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/transfer"
)

// chunkTestNode builds a Node with one folder and returns it plus the folder
// root. In-package so the test can shrink indexChunkSize.
//
// SettleScans 1 disables the stability gate: these tests write a file and
// expect the very next scan to index and transfer it (pre-gate behaviour).
// Gate behaviour at the default depth is covered by the *_settle tests.
func chunkTestNode(t *testing.T, id uint64, name, root, metaDir string) *Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: id, Name: name},
		MetaDir: metaDir,
		Listen:  "",
		Folders: []config.Folder{{ID: "data", Path: root, SettleScans: 1}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func chunkWrite(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(p, time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC), time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC))
}

// TestChunkedIndexExchange forces a tiny chunk size so a folder of 10 files
// is streamed in several frames, then verifies the receiver's peer view holds
// exactly the 10 files (no lost, no duplicated, no infinite send loop).
func TestChunkedIndexExchange(t *testing.T) {
	t.Run("multi-frame", func(t *testing.T) {
		old := indexChunkSize
		indexChunkSize = 3
		defer func() { indexChunkSize = old }()

		rootA := t.TempDir()
		rootB := t.TempDir()
		a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
		b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
		const n = 10
		for i := 0; i < n; i++ {
			chunkWrite(t, rootA, fmt.Sprintf("f-%02d.txt", i), fmt.Sprintf("content %d", i))
		}
		if _, err := a.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}

		if err := chunkPipe(t, a, b); err != nil {
			t.Fatal(err)
		}
		if got := b.peerCountFor(t, "data", 1); got != n {
			t.Fatalf("receiver peer view = %d files, want %d", got, n)
		}
	})

	t.Run("exact-multiple", func(t *testing.T) {
		old := indexChunkSize
		indexChunkSize = 5
		defer func() { indexChunkSize = old }()

		rootA := t.TempDir()
		rootB := t.TempDir()
		a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
		b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
		const n = 10 // exactly 2 chunks
		for i := 0; i < n; i++ {
			chunkWrite(t, rootA, fmt.Sprintf("f-%02d.txt", i), fmt.Sprintf("content %d", i))
		}
		if _, err := a.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if err := chunkPipe(t, a, b); err != nil {
			t.Fatal(err)
		}
		if got := b.peerCountFor(t, "data", 1); got != n {
			t.Fatalf("receiver peer view = %d files, want %d", got, n)
		}
	})

	t.Run("empty-folder", func(t *testing.T) {
		old := indexChunkSize
		indexChunkSize = 3
		defer func() { indexChunkSize = old }()

		rootA := t.TempDir()
		rootB := t.TempDir()
		a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
		b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
		if _, err := a.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		// Must terminate (previously infinite-looped on the empty final frame).
		if err := chunkPipe(t, a, b); err != nil {
			t.Fatal(err)
		}
	})
}

// chunkPipe wires two nodes over loopback with a bounded deadline so a
// regression to the infinite send loop fails the test instead of hanging.
func chunkPipe(t *testing.T, a, b *Node) error {
	t.Helper()
	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()
	done := make(chan error, 1)
	go func() {
		conn, err := transfer.Accept(ln)
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = conn.Close() }()
		done <- a.Serve(conn)
	}()
	conn, err := transfer.Dial(ln.Addr().String())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := b.SyncOnce(conn, a.ID); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("chunked index exchange timed out")
	}
}

func (n *Node) peerCountFor(t *testing.T, folder string, peerID uint64) int {
	t.Helper()
	f, ok := n.Folders[folder]
	if !ok {
		t.Fatalf("folder %q not found", folder)
	}
	return f.Engine.PeerCount(peerID)
}
