package node

// The folder watcher registry.
//
// watcher.New used to be called in exactly ONE place — the boot loop in
// cmd/crosssync/main.go — so a folder added at runtime had no filesystem watcher
// until the daemon restarted, and its changes were only caught by the periodic
// safety walk (up to scan.full_max, fifteen minutes). Watching a folder is part of
// having the folder, so the lifecycle lives here: started on add (boot or runtime),
// closed on remove, closed for all on shutdown.
//
// Watchers stay an OPTIMISATION: a watcher that cannot start is logged and the
// daemon carries on (the safety walk is the net). Nothing here may turn a folder
// add or a shutdown into an error.

import (
	"errors"
	std "sync"

	"crosssync/internal/watcher"
)

// folderWatch is one folder's watcher plus the stop channel its consumer
// goroutine reads, so closing it ends BOTH the fsnotify watcher and the goroutine
// draining its change batches (Changes is never closed, so a plain range would
// leak the goroutine for the life of the process).
type folderWatch struct {
	w    *watcher.Watcher
	stop chan struct{}
	once std.Once
}

// close stops the watcher and its consumer exactly once.
func (fw *folderWatch) close() {
	fw.once.Do(func() {
		close(fw.stop)
		_ = fw.w.Close()
	})
}

// SetWatcherKick sets how an applied change requests a sync round. The daemon
// passes its coalescing kicker, so a burst of writes becomes one follow-up round
// instead of one per batch; without it a change kicks a round directly.
func (n *Node) SetWatcherKick(fn func()) {
	n.watchMu.Lock()
	n.watchKick = fn
	n.watchMu.Unlock()
}

// watchKickSync asks for a sync round after a change was applied. Never blocks
// the scanner: the injected kicker is asynchronous, and the fallback runs
// ConnManager.SyncAll in its own goroutine (a round can take minutes).
func (n *Node) watchKickSync() {
	n.watchMu.Lock()
	kick := n.watchKick
	n.watchMu.Unlock()
	if kick != nil {
		kick()
		return
	}
	if n.ConnMgr != nil {
		go n.ConnMgr.SyncAll()
	}
}

// StartFolderWatchers arms the registry and starts a watcher for every folder the
// node has. This is the boot path: called once by the daemon after the node is
// built (Node.New must not start watchers — a node constructed for a CLI command
// or a test has no business walking shares with inotify).
//
// Idempotent: a folder that is already watched is left alone, so calling it twice
// cannot double the watchers on a tree.
func (n *Node) StartFolderWatchers() {
	n.watchMu.Lock()
	n.watchArmed = true
	n.watchMu.Unlock()
	n.foldersMu.RLock()
	ids := make([]string, 0, len(n.Folders))
	for id := range n.Folders {
		ids = append(ids, id)
	}
	n.foldersMu.RUnlock()
	for _, id := range ids {
		if err := n.StartFolderWatcher(id); err != nil {
			n.Logf("watcher: %s: %v", id, err)
		}
	}
}

// folderWatcherStart is a test seam. It stands in for StartFolderWatcher so a
// test can prove that adding a folder does NOT wait for watcher registration:
// that registration walks every directory of the share (minutes on a large one)
// and used to run inline in Node.AddFolder, which is called from the control
// plane's add-folder request. A slow watcher therefore hung the request, so the
// caller never appended the folder to the config or persisted it, and the folder
// vanished at the next restart with its index orphaned. Nil uses the real one.
var folderWatcherStart func(n *Node, id string) error

// StartFolderWatcher starts watching one folder. It is idempotent, and it is a
// no-op before the registry is armed: a folder that exists while the daemon is
// still building itself (config load) must not open an inotify instance, which is
// why Node.AddFolder only calls this once StartFolderWatchers has armed it.
func (n *Node) StartFolderWatcher(id string) error {
	n.watchMu.Lock()
	armed := n.watchArmed
	_, already := n.watchers[id]
	n.watchMu.Unlock()
	if !armed || already {
		return nil
	}
	f, ok := n.lookupFolder(id)
	if !ok {
		return errors.New("unknown folder")
	}
	w, err := watcher.New(f.Root, n.Logf)
	if err != nil {
		return err
	}
	if err := w.Start(); err != nil {
		_ = w.Close()
		return err
	}
	fw := &folderWatch{w: w, stop: make(chan struct{})}
	n.watchMu.Lock()
	if n.watchers == nil {
		n.watchers = map[string]*folderWatch{}
	}
	// A stop that raced the start (the folder was removed while the walk that
	// registers watches was running) must not leave an orphan watcher behind;
	// nor may two starters register two watchers for one folder.
	_, dup := n.watchers[id]
	if dup || n.lookupFolderLocked(id) == nil {
		n.watchMu.Unlock()
		fw.close()
		return nil
	}
	n.watchers[id] = fw
	n.watchMu.Unlock()
	n.Logf("watcher: watching %s (%d dirs)", f.Root, w.WatchCount())
	go n.consumeWatcherChanges(id, fw)
	return nil
}

// lookupFolderLocked reports whether id still exists. Caller holds watchMu and
// must not take foldersMu in the opposite order: watchMu is always the inner
// lock (nothing here holds foldersMu while taking watchMu).
func (n *Node) lookupFolderLocked(id string) *Folder {
	n.foldersMu.RLock()
	f := n.Folders[id]
	n.foldersMu.RUnlock()
	return f
}

// consumeWatcherChanges drains one watcher's batches into a per-folder path
// queue: changed paths are scanned, coalesced, and a change that became syncable
// kicks a round. Exits when the watcher is closed.
func (n *Node) consumeWatcherChanges(id string, fw *folderWatch) {
	q := newPathQueue(func(rels []string) error {
		applied, err := n.ScanFolderPaths(id, rels)
		if errors.Is(err, ErrFolderPaused) || errors.Is(err, ErrScanCancelled) {
			return nil // paused folders are not an error
		}
		// A change that just became syncable should not wait for the next
		// interval tick: kick a round now (the connection manager still honours
		// per-peer backoff and skips a peer that is mid-round).
		if applied > 0 {
			n.watchKickSync()
		}
		return err
	}, n.Logf)
	for {
		select {
		case batch := <-fw.w.Changes():
			q.Add(batch)
		case <-fw.stop:
			return
		}
	}
}

// StopFolderWatcher closes one folder's watcher, if it has one. Safe to call for
// a folder that was never watched or is already stopped.
func (n *Node) StopFolderWatcher(id string) {
	n.watchMu.Lock()
	fw := n.watchers[id]
	delete(n.watchers, id)
	n.watchMu.Unlock()
	if fw != nil {
		fw.close()
	}
}

// CloseWatchers closes every watcher (shutdown). Called from Node.Close BEFORE the
// indexes are closed: the consumer goroutines scan, and a scan of a closed index
// is an error, not a shutdown.
func (n *Node) CloseWatchers() {
	n.watchMu.Lock()
	all := make([]*folderWatch, 0, len(n.watchers))
	for id, fw := range n.watchers {
		all = append(all, fw)
		delete(n.watchers, id)
	}
	n.watchMu.Unlock()
	for _, fw := range all {
		fw.close()
	}
}

// WatchingFolders reports how many folders currently have a watcher (tests and
// the folders API use it; it is three fields of map access, no I/O).
func (n *Node) WatchingFolders() int {
	n.watchMu.Lock()
	defer n.watchMu.Unlock()
	return len(n.watchers)
}

// Watching reports whether a specific folder has a watcher.
func (n *Node) Watching(id string) bool {
	n.watchMu.Lock()
	defer n.watchMu.Unlock()
	_, ok := n.watchers[id]
	return ok
}
