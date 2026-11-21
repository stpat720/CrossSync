package node

import (
	"testing"
	"time"

	"crosssync/internal/config"
)

// TestAddFolderDoesNotWaitForTheWatcher pins the bug that made a folder added at
// runtime work perfectly until the daemon restarted and then disappear:
// StartFolderWatcher walks every directory of the share (minutes on a big one)
// and used to run INLINE inside Node.AddFolder. That call sits inside the
// control plane's add-folder request, so the request never returned, the caller
// never reached `s.n.Cfg.Folders = append(...)` or persistFolders, and the
// config file never gained the folder while its index was already on disk.
//
// Measured live on a 400k+ file share: the POST hung past a 60 s client timeout
// and both nodes ended up with a live folder that was not in config.yaml.
func TestAddFolderDoesNotWaitForTheWatcher(t *testing.T) {
	root := t.TempDir()
	n := newRegistryNode(t, map[string]string{"data": root})
	n.StartFolderWatchers()

	blocked := make(chan struct{})
	started := make(chan struct{})
	folderWatcherStart = func(*Node, string) error {
		close(started)
		<-blocked // stands in for a watcher walk that takes minutes
		return nil
	}
	defer func() {
		folderWatcherStart = nil
		close(blocked)
	}()

	late := t.TempDir()
	done := make(chan error, 1)
	go func() {
		_, err := n.AddFolder(config.Folder{ID: "late", Path: late, ConflictPolicy: "conflict-copy", SettleScans: 1})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("add folder: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AddFolder blocked on watcher registration: the add-folder request would never return, so the folder would never be persisted and would vanish at the next restart")
	}

	// The watcher must still be started, just not waited for.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the watcher was never started, so a runtime-added folder would only be seen by the safety walk")
	}
}
