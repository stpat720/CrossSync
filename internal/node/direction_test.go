package node_test

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/node"
)

// makeNodeDirection builds a node whose single "data" folder carries the
// given per-device sync direction ("" = send-receive, the default).
func makeNodeDirection(t *testing.T, id uint64, name, root, metaDir, direction string) *node.Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: id, Name: name},
		MetaDir: metaDir,
		Folders: []config.Folder{{
			ID: "data", Path: root, ConflictPolicy: "conflict-copy",
			Direction: direction,
			// Gate off: these tests assert per-scan direction behaviour.
			SettleScans: 1,
		}},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// TestSendOnlyFolderOverSession is the end-to-end send-only test: B still
// indexes and SERVES its own files (A pulls them), but B never acquires
// anything from A — no files, no updates — while still merging A's peer view
// so it can serve and keep the global model coherent.
func TestSendOnlyFolderOverSession(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNodeDirection(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "")
	b := makeNodeDirection(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "send-only")

	// A file that exists only on B: send-only still indexes it, so A (plain
	// send-receive) pulls it.
	write(t, rootB, "c.txt", "made on B")
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)
	if got := read(t, rootA, "c.txt"); got != "made on B" {
		t.Fatalf("A content for c.txt = %q, want %q (B still serves)", got, "made on B")
	}

	// A owns files of its own: none of them may ever reach B.
	write(t, rootA, "a.txt", "a v1")
	write(t, rootA, "b.txt", "b v1")
	write(t, rootA, "proj/proxy/p.mxf", "proxy v1")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)

	ixB := b.Folders["data"].Ix
	for _, rel := range []string{"a.txt", "b.txt", "proj/proxy/p.mxf"} {
		if _, err := os.Stat(filepath.Join(rootB, filepath.FromSlash(rel))); !os.IsNotExist(err) {
			t.Fatalf("send-only B must not pull %s (err=%v)", rel, err)
		}
		if _, ok, err := ixB.Get(rel); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Fatalf("send-only B must not index A's %s", rel)
		}
	}
	// The peer view is still merged: B needs A's state to serve and to
	// compute the global model.
	if got := b.Folders["data"].Engine.PeerCount(1); got == 0 {
		t.Fatal("send-only B should still hold A's peer index view")
	}

	// A modifies a.txt: B must not apply the change either.
	write(t, rootA, "a.txt", "a v2")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)
	if _, err := os.Stat(filepath.Join(rootB, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("send-only B must never apply A's change to a.txt (err=%v)", err)
	}

	// B keeps serving new local changes throughout.
	write(t, rootB, "c2.txt", "also from B")
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)
	if got := read(t, rootA, "c2.txt"); got != "also from B" {
		t.Fatalf("A content for c2.txt = %q, want %q", got, "also from B")
	}
}

// TestReceiveOnlyFolderOverSession is the end-to-end receive-only test: C
// never indexes its own new file (so A never learns of it) but still pulls
// A's changes, and the ignored local file stays untouched on disk.
func TestReceiveOnlyFolderOverSession(t *testing.T) {
	rootA := t.TempDir()
	rootC := t.TempDir()
	a := makeNodeDirection(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "")
	c := makeNodeDirection(t, 3, "laptop", rootC, filepath.Join(t.TempDir(), "meta-c"), "receive-only")

	// C's local file is a local change receive-only never records.
	write(t, rootC, "d.txt", "local to C")
	write(t, rootA, "e.txt", "from A")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, c)

	// C never advertised d.txt, so A neither pulled nor indexed it.
	if _, err := os.Stat(filepath.Join(rootA, "d.txt")); !os.IsNotExist(err) {
		t.Fatalf("receive-only C's d.txt must never reach A (err=%v)", err)
	}
	if _, ok, err := a.Folders["data"].Ix.Get("d.txt"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("A's index must not hold C's d.txt")
	}
	// C's local file is ignored, never deleted: it is still on disk.
	if got := read(t, rootC, "d.txt"); got != "local to C" {
		t.Fatalf("C's local d.txt = %q, want %q", got, "local to C")
	}

	// receive-only still pulls.
	if got := read(t, rootC, "e.txt"); got != "from A" {
		t.Fatalf("C content for e.txt = %q, want %q", got, "from A")
	}
	if _, ok, err := c.Folders["data"].Ix.Get("e.txt"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("C should have indexed the pulled e.txt")
	}
}
