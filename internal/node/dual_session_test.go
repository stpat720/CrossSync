package node

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"crosssync/internal/transfer"
)

// TestDualDirectionConcurrentSessions runs two full sessions at once between
// the same pair of nodes: A initiates to B while B simultaneously initiates
// to A, over independent connections. Both directions must complete without
// deadlock or error, and both sides must end with the union of the file
// sets (distinct names, so no conflicts are expected).
func TestDualDirectionConcurrentSessions(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	for i := 0; i < 5; i++ {
		chunkWrite(t, rootA, fmt.Sprintf("only-a-%d.txt", i), fmt.Sprintf("a %d", i))
		chunkWrite(t, rootB, fmt.Sprintf("only-b-%d.txt", i), fmt.Sprintf("b %d", i))
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)

	// Direction 1: b initiates to a.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[0] = dualSession(t, a, b)
	}()
	// Direction 2: a initiates to b, at the same time.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[1] = dualSession(t, b, a)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatal("dual-direction sessions deadlocked")
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("session %d failed: %v", i, err)
		}
	}

	for i := 0; i < 5; i++ {
		if got := readDirCount(t, rootA); got != 10 {
			t.Fatalf("a has %d files, want 10", got)
		}
		if got := readDirCount(t, rootB); got != 10 {
			t.Fatalf("b has %d files, want 10", got)
		}
		if got := readFile(t, rootA, fmt.Sprintf("only-b-%d.txt", i)); got != fmt.Sprintf("b %d", i) {
			t.Fatalf("a only-b-%d = %q", i, got)
		}
		if got := readFile(t, rootB, fmt.Sprintf("only-a-%d.txt", i)); got != fmt.Sprintf("a %d", i) {
			t.Fatalf("b only-a-%d = %q", i, got)
		}
	}
}

// dualSession runs a full session where initiator dials responder.
func dualSession(t *testing.T, responder, initiator *Node) error {
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
		done <- responder.Serve(conn)
	}()
	conn, err := transfer.Dial(ln.Addr().String())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := initiator.SyncOnce(conn, responder.ID); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-time.After(60 * time.Second):
		return fmt.Errorf("serve timed out")
	}
}

func readDirCount(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() {
			n++
		}
	}
	return n
}

func readFile(t *testing.T, root, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
