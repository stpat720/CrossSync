package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"crosssync/internal/config"
)

// The folder watcher registry (see watchers.go).
//
// watcher.New used to be called only from the boot loop in main.go, so a folder
// added at runtime had NO watcher until the daemon restarted and its changes
// were only caught by the safety walk. The lifecycle now lives on the Node:
// armed at boot (one watcher per folder), started by AddFolder while armed, closed
// by RemoveFolder, closed for all by Close.

// newRegistryNode builds a node with the given folders (id -> root), each already
// created on disk.
func newRegistryNode(t *testing.T, folders map[string]string) *Node {
	t.Helper()
	cfgs := make([]config.Folder, 0, len(folders))
	for id, root := range folders {
		cfgs = append(cfgs, config.Folder{ID: id, Path: root, ConflictPolicy: "conflict-copy", SettleScans: 1})
	}
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: cfgs,
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// waitFolderScan polls until a scan of id has completed, or fails.
func waitFolderScan(t *testing.T, n *Node, id string, wait time.Duration) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		if _, ok := n.FolderScanAge(id); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no scan ran for folder %q within %s", id, wait)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestBootPathRegistersOneWatcherPerFolder: StartFolderWatchers (what main.go
// calls) gives every configured folder a watcher, and a change in a folder makes
// the daemon scan it — the behaviour the boot loop provided inline before.
func TestBootPathRegistersOneWatcherPerFolder(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	n := newRegistryNode(t, map[string]string{"a": rootA, "b": rootB})

	// Building the node must NOT open watchers: config load, CLI commands and
	// tests all construct nodes that never watch anything.
	if got := n.WatchingFolders(); got != 0 {
		t.Fatalf("%d watcher(s) before the registry was armed, want 0", got)
	}
	n.StartFolderWatchers()
	if got, want := n.WatchingFolders(), 2; got != want {
		t.Fatalf("%d watcher(s) after StartFolderWatchers, want %d (one per folder)", got, want)
	}
	for _, id := range []string{"a", "b"} {
		if !n.Watching(id) {
			t.Fatalf("folder %q has no watcher", id)
		}
	}
	// Idempotent: arming twice must not double the watchers on a tree.
	n.StartFolderWatchers()
	if got := n.WatchingFolders(); got != 2 {
		t.Fatalf("%d watcher(s) after a second StartFolderWatchers, want 2", got)
	}

	// Behavioural half: a file written into a watched folder triggers a scoped
	// scan without anyone asking for one.
	if err := os.WriteFile(filepath.Join(rootA, "watched.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFolderScan(t, n, "a", 10*time.Second)
}

// TestAddFolderStartsAWatcherAndRemoveClosesIt: a folder added through the node
// AFTER the registry is armed gets its own watcher (so its changes are seen
// without a restart), and removing it closes that watcher.
func TestAddFolderStartsAWatcherAndRemoveClosesIt(t *testing.T) {
	root := t.TempDir()
	n := newRegistryNode(t, map[string]string{"data": root})
	n.StartFolderWatchers()
	if got := n.WatchingFolders(); got != 1 {
		t.Fatalf("%d watcher(s) after arming, want 1", got)
	}

	// A folder added while the node is UNARMED (config load) starts nothing;
	// that is covered by the check above, and re-asserted here by adding while
	// armed, which must start one.
	late := t.TempDir()
	if _, err := n.AddFolder(config.Folder{ID: "late", Path: late, ConflictPolicy: "conflict-copy", SettleScans: 1}); err != nil {
		t.Fatalf("add folder: %v", err)
	}
	if !n.Watching("late") {
		t.Fatal("a folder added at runtime has no watcher: its changes would wait for the safety walk (up to 15 minutes)")
	}
	if got := n.WatchingFolders(); got != 2 {
		t.Fatalf("%d watcher(s) after adding a folder, want 2", got)
	}
	// Its changes are seen too, which is the point of having a watcher.
	if err := os.WriteFile(filepath.Join(late, "late.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFolderScan(t, n, "late", 10*time.Second)

	// Removing the folder closes its watcher (and leaves the other one alone).
	if err := n.RemoveFolder("late"); err != nil {
		t.Fatalf("remove folder: %v", err)
	}
	if n.Watching("late") {
		t.Fatal("the removed folder still has a watcher: it would keep scanning a folder the node no longer has")
	}
	if got := n.WatchingFolders(); got != 1 {
		t.Fatalf("%d watcher(s) after removing a folder, want 1", got)
	}
	if !n.Watching("data") {
		t.Fatal("removing one folder closed another folder's watcher")
	}

	// Shutdown closes whatever is left.
	n.CloseWatchers()
	if got := n.WatchingFolders(); got != 0 {
		t.Fatalf("%d watcher(s) after CloseWatchers, want 0", got)
	}
}

// TestAddFolderBeforeArmingStartsNoWatcher: AddFolder is also the CONFIG-LOAD
// path (Node.New). A node that is merely being constructed must not open inotify
// instances — on a box with many folders that is a hard limit — and the boot path
// picks them up when the daemon actually starts.
func TestAddFolderBeforeArmingStartsNoWatcher(t *testing.T) {
	root := t.TempDir()
	n := newRegistryNode(t, map[string]string{"data": root})
	late := t.TempDir()
	if _, err := n.AddFolder(config.Folder{ID: "late", Path: late, ConflictPolicy: "conflict-copy", SettleScans: 1}); err != nil {
		t.Fatalf("add folder: %v", err)
	}
	if got := n.WatchingFolders(); got != 0 {
		t.Fatalf("%d watcher(s) opened by a node that was never armed, want 0", got)
	}
	// Arming later still covers it: every folder gets a watcher, including the
	// ones added before.
	n.StartFolderWatchers()
	if !n.Watching("data") || !n.Watching("late") {
		t.Fatal("StartFolderWatchers did not cover every folder")
	}
}
