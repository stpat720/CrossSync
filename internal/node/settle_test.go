package node_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/node"
)

// partFiles lists staging temp files (.sfx-tmp/*.part) anywhere under root:
// the litter a transfer of an unsettled file would leave behind.
func partFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".part") {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestInFlightNotTransferred is the end-to-end stability-gate contract: a file
// that is still being written is never advertised or transferred, leaves no
// staging litter, and transfers exactly once — after it settles.
func TestInFlightNotTransferred(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNodeGate(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy", 2)
	b := makeNodeGate(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy", 2)

	// A copy lands and keeps growing across scans: each scan sees a different
	// (size, mtime), so the file never becomes eligible.
	write(t, rootA, "big.bin", "v1")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	write(t, rootA, "big.bin", "v2-still-growing")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	engA := a.Folders["data"].Engine
	if !engA.InFlight("big.bin") {
		t.Fatal("big.bin should be held back by the gate while it is being written")
	}
	if n, _ := engA.InFlightStats(); n != 1 {
		t.Fatalf("InFlightStats files = %d, want 1", n)
	}
	if _, ok, err := a.Folders["data"].Ix.Get("big.bin"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("an unsettled file must not be indexed (that is what keeps it off the wire)")
	}

	// A full session must move nothing and leave no staging litter.
	pipeSession(t, a, b)
	if _, err := os.Stat(filepath.Join(rootB, "big.bin")); !os.IsNotExist(err) {
		t.Fatal("an unsettled file must not be transferred")
	}
	if temps := partFiles(t, rootA); len(temps) != 0 {
		t.Fatalf("staging litter under A: %v", temps)
	}
	if temps := partFiles(t, rootB); len(temps) != 0 {
		t.Fatalf("staging litter under B: %v", temps)
	}
	if files := b.Folders["data"].Engine.Stats.FilesDown.Load(); files != 0 {
		t.Fatalf("B downloaded %d file(s) before the source settled, want 0", files)
	}

	// Let it settle (two stable scans), then one session converges it.
	const want = "v2-still-growing"
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if got := a.Folders["data"].Scanner.LastInFlight(); len(got) != 0 {
		t.Fatalf("LastInFlight after settling = %+v, want empty", got)
	}
	pipeSession(t, a, b)

	if got := read(t, rootB, "big.bin"); got != want {
		t.Fatalf("B[big.bin] = %q, want %q", got, want)
	}
	if files := b.Folders["data"].Engine.Stats.FilesDown.Load(); files != 1 {
		t.Fatalf("B downloaded %d file(s), want exactly 1", files)
	}
	// The phantom-pending check: after the transfer B has nothing left to
	// report as an upload.
	up, err := b.Folders["data"].Engine.PendingUploads()
	if err != nil {
		t.Fatal(err)
	}
	if len(up) != 0 {
		t.Fatalf("B pending uploads after converging = %v, want none", up)
	}
	if needs, err := b.Folders["data"].Engine.NeedPulls(); err != nil {
		t.Fatal(err)
	} else if len(needs) != 0 {
		t.Fatalf("B still needs %v after converging", needs)
	}
}

// TestInFlightNotServedOverSession covers the serve gate end to end: a peer
// asks for a file that went in-flight after it was advertised (its index entry
// is the last SETTLED version, but the file on disk is mid-write). The source
// refuses the blocks, the puller skips that one file without failing the
// round, and nothing half-written is ever delivered.
func TestInFlightNotServedOverSession(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNodeGate(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy", 2)
	b := makeNodeGate(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy", 2)

	// A settled, indexed, advertised version.
	write(t, rootA, "doc.txt", "settled-content")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	before, ok, err := a.Folders["data"].Ix.Get("doc.txt")
	if err != nil || !ok {
		t.Fatalf("doc.txt should be indexed: ok=%v err=%v", ok, err)
	}

	// A new write starts and one scan sees it: the index still holds the old
	// settled version, which the next session advertises.
	write(t, rootA, "doc.txt", "settled-content-plus-a-lot-more")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if !a.Folders["data"].Engine.InFlight("doc.txt") {
		t.Fatal("doc.txt should be in flight after the rewrite")
	}

	// The session must complete cleanly (the refusal is a skip, not an
	// error) and B must not end up with the half-written bytes.
	pipeSession(t, a, b)
	if _, err := os.Stat(filepath.Join(rootB, "doc.txt")); !os.IsNotExist(err) {
		got := read(t, rootB, "doc.txt")
		t.Fatalf("B must not receive an unsettled file, got %q", got)
	}
	after, ok, err := a.Folders["data"].Ix.Get("doc.txt")
	if err != nil || !ok {
		t.Fatalf("doc.txt should still be indexed: ok=%v err=%v", ok, err)
	}
	if after.Size != before.Size || !after.Version.Equal(before.Version) {
		t.Fatalf("A's advertised entry changed while the file was in flight: before=%+v after=%+v", before, after)
	}
}

// TestReceiveOnlyGateDisabled verifies the hard requirement from the design:
// a receive-only folder never gates. It never indexes local content, so with
// the gate on every local file would stay in flight forever and the folder
// would stop receiving.
func TestReceiveOnlyGateDisabled(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	metaA := filepath.Join(t.TempDir(), "meta-a")
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: metaA,
		Folders: []config.Folder{{
			ID: "data", Path: rootA, ConflictPolicy: "conflict-copy",
			Direction: "receive-only",
		}},
	}
	a, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b := makeNodeGate(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy", 1)

	// A local write in a receive-only folder must settle on the first scan:
	// the gate is off, so nothing is ever held back.
	write(t, rootA, "local.txt", "written on the receiver")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if got := a.Folders["data"].Scanner.LastInFlight(); len(got) != 0 {
		t.Fatalf("LastInFlight in a receive-only folder = %+v, want empty (gate disabled)", got)
	}

	// And the folder still receives: the peer's file is pulled over.
	write(t, rootB, "peer.txt", "from the peer")
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)
	if got := read(t, rootA, "peer.txt"); got != "from the peer" {
		t.Fatalf("A[peer.txt] = %q, want the peer's content", got)
	}
}
