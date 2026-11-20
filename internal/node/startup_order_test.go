package node

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"crosssync/internal/config"
)

// startupFixture builds a node with the given folders, each root holding one
// small file so a first pass has something to hash.
func startupFixture(t *testing.T, ids ...string) (*Node, map[string]string) {
	t.Helper()
	roots := map[string]string{}
	folders := make([]config.Folder, 0, len(ids))
	for _, id := range ids {
		root := t.TempDir()
		roots[id] = root
		if err := os.WriteFile(filepath.Join(root, "seed.txt"), []byte("seed "+id), 0o644); err != nil {
			t.Fatal(err)
		}
		folders = append(folders, config.Folder{ID: id, Path: root, ConflictPolicy: "conflict-copy", SettleScans: 1})
	}
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: folders,
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n, roots
}

// TestAdvertisedFoldersWithheldUntilFolderFirstPass pins the publication gate
// that made the live incident dangerous: a folder whose first index pass has not
// finished has a PREFIX of its share in the index, and advertising it tells the
// peer "I have nothing" about files we simply have not hashed yet. The gate is
// per folder — a folder becomes visible on its own pass, without waiting for the
// others — and an unarmed node (CLI paths, tests) advertises exactly as before.
func TestAdvertisedFoldersWithheldUntilFolderFirstPass(t *testing.T) {
	n, _ := startupFixture(t, "small", "big")
	both := map[string]bool{"small": true, "big": true}

	// Not armed: today's behaviour, nothing is withheld.
	if got := n.advertisedFolders(7); len(got) != 2 {
		t.Fatalf("unarmed node advertises %v, want both folders", got)
	}

	// Daemon start: every folder's first pass is outstanding.
	n.SetStartupScanning(true)
	if got := n.advertisedFolders(7); len(got) != 0 {
		t.Fatalf("advertised %v during the startup walk, want nothing (partial indexes must not be published)", got)
	}
	if got := n.sharedFolders(both, 7); len(got) != 0 {
		t.Fatalf("shared %v during the startup walk, want nothing: a round would exchange a partial index", got)
	}
	if !n.StartupScanning() {
		t.Fatal("StartupScanning() = false while every first pass is outstanding")
	}

	// The small folder's own pass ends: it becomes visible immediately, while
	// the big folder is still pending.
	if _, err := n.ScanFolder("small"); err != nil {
		t.Fatalf("scan small: %v", err)
	}
	if got := n.advertisedFolders(7); len(got) != 1 || got[0] != "small" {
		t.Fatalf("advertised %v after one folder's first pass, want exactly [small]", got)
	}
	if got := n.sharedFolders(both, 7); len(got) != 1 || got[0] != "small" {
		t.Fatalf("shared %v after one folder's first pass, want exactly [small]", got)
	}
	if !n.StartupScanning() {
		t.Fatal("StartupScanning() = false while the big folder's first pass is outstanding")
	}
	if !n.FolderIndexing("big") || n.FolderIndexing("small") {
		t.Fatalf("FolderIndexing: small=%v big=%v, want false/true", n.FolderIndexing("small"), n.FolderIndexing("big"))
	}

	// The second pass ends: everything is advertised and the flag settles.
	if _, err := n.ScanFolder("big"); err != nil {
		t.Fatalf("scan big: %v", err)
	}
	if got := n.advertisedFolders(7); len(got) != 2 {
		t.Fatalf("advertised %v after the whole walk, want both folders", got)
	}
	if n.StartupScanning() {
		t.Fatal("StartupScanning() = true after every folder's first pass finished")
	}
}

// TestStartupWalkScansFoldersIndependently pins the folder-independence rule on
// the startup path: a big folder blocked mid-hash must not delay a small
// folder's first pass, its advertisement, or its ability to be synced. The
// blocking is a scanner hash hook (restored by the cleanup) rather than a large
// tree, so the window is deterministic.
func TestStartupWalkScansFoldersIndependently(t *testing.T) {
	n, _ := startupFixture(t, "small", "big")

	// BOTH folders' scanners block on their first hash. Whichever folder the
	// walk reaches first is then the "slow" one; the test does not depend on
	// map iteration order. With the passes running concurrently the other folder
	// still finishes and is published; with them serialized the first folder
	// holds the only slot and nothing is ever published.
	entered := make(chan struct{})
	var once sync.Once
	release := make(chan struct{})
	// blocked is written before close(entered), so reading it after <-entered is
	// ordered — no lock needed.
	blocked := ""
	hook := func(who string) func(string) {
		return func(string) {
			mine := false
			once.Do(func() {
				mine = true
				blocked = who
				close(entered)
			})
			if mine {
				<-release
			}
		}
	}
	for id, f := range n.Folders {
		f.Scanner.SetHashHook(hook(id))
	}

	n.SetStartupScanning(true)
	walkDone := make(chan struct{})
	go func() {
		defer close(walkDone)
		_ = n.ScanAllFirstPass()
	}()
	// Cleanups run last-in-first-out, so this releases both hooks and waits for
	// the walk BEFORE the node is closed underneath it.
	t.Cleanup(func() {
		close(release)
		<-walkDone
		for _, f := range n.Folders {
			f.Scanner.SetHashHook(nil)
		}
	})

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no folder's first pass ever started hashing")
	}
	stuck := blocked
	other := "small"
	if stuck == "small" {
		other = "big"
	}

	// The other folder must finish and publish while the first one is stuck.
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := n.advertisedFolders(7)
		if len(got) == 1 && got[0] == other {
			break
		}
		if time.Now().After(deadline) {
			var detail []string
			for _, st := range n.ScanStatuses() {
				detail = append(detail, st.Folder+" running="+boolText(st.Running)+" phase="+st.Phase)
			}
			t.Fatalf("folder %q was not advertised while %q was still hashing (one folder delayed another): advertised=%v scans=%s",
				other, stuck, got, strings.Join(detail, "; "))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n.FolderIndexing(other) {
		t.Fatalf("folder %q still counts as indexing after its pass finished", other)
	}
	if !n.FolderIndexing(stuck) {
		t.Fatalf("folder %q does not count as indexing while its pass is blocked", stuck)
	}

	// A rescan for the finished folder is allowed; for the blocked one it is a
	// busy answer, not a queued duplicate walk (see RequestRescan).
	if _, _, err := n.RequestRescan(stuck, false); !errors.Is(err, ErrScanBusy) {
		t.Fatalf("rescan of the folder being walked = %v, want ErrScanBusy", err)
	}
	if _, _, err := n.RequestRescan(other, false); err != nil {
		t.Fatalf("rescan of the finished folder = %v, want it to be allowed while another folder hashes", err)
	}
}

func boolText(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
