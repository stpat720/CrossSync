package node

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestPausedFolderStopsScanningAndSyncing pins the pause feature end to end on
// one device: while paused the folder must not scan (no index writes from local
// edits), must not pull or push (so peers neither see nor receive it), and must
// keep its index and files intact so resuming converges again with an ordinary
// round. Pause is per-device: only the paused node stops participating.
func TestPausedFolderStopsScanningAndSyncing(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	chunkWrite(t, rootA, "first.txt", "before pause")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := b.Folders["data"].Ix.Get("first.txt"); err != nil || !ok {
		t.Fatalf("baseline round did not pull first.txt: ok=%v err=%v", ok, err)
	}

	// B pauses its side of the folder.
	if err := b.SetFolderPaused("data", true); err != nil {
		t.Fatal(err)
	}
	if !b.Paused("data") {
		t.Fatal("folder did not report paused")
	}
	if adv := b.advertisedFolders(1); len(adv) != 0 {
		t.Fatalf("a paused folder must not be advertised to peers, got %v", adv)
	}

	// A change on the peer must not arrive while we are paused.
	chunkWrite(t, rootA, "second.txt", "arrived while b was paused")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatalf("a round involving a paused folder must still complete cleanly: %v", err)
	}
	if _, ok, err := b.Folders["data"].Ix.Get("second.txt"); err != nil || ok {
		t.Fatalf("a paused folder pulled second.txt: ok=%v err=%v", ok, err)
	}

	// A local edit must not be scanned while paused, and an explicit rescan
	// says why instead of silently doing nothing.
	chunkWrite(t, rootB, "local.txt", "typed while paused")
	if _, err := b.ScanFolder("data"); !errors.Is(err, ErrFolderPaused) {
		t.Fatalf("explicit scan of a paused folder = %v, want ErrFolderPaused", err)
	}
	if _, ok, err := b.Folders["data"].Ix.Get("local.txt"); err != nil || ok {
		t.Fatalf("a paused folder scanned local.txt: ok=%v err=%v", ok, err)
	}

	// Nothing was deleted by pausing.
	if _, err := os.Stat(filepath.Join(rootB, "first.txt")); err != nil {
		t.Fatalf("pausing must not touch files: %v", err)
	}

	// Resume: the ordinary round reconciles both directions.
	if err := b.SetFolderPaused("data", false); err != nil {
		t.Fatal(err)
	}
	if b.Paused("data") {
		t.Fatal("folder still reported paused after resume")
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatalf("rescan after resume: %v", err)
	}
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := b.Folders["data"].Ix.Get("second.txt"); err != nil || !ok {
		t.Fatalf("resume did not pull second.txt: ok=%v err=%v", ok, err)
	}
	if _, ok, err := a.Folders["data"].Ix.Get("local.txt"); err != nil || !ok {
		t.Fatalf("resume did not push local.txt to the peer: ok=%v err=%v", ok, err)
	}
}
