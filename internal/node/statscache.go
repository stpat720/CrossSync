package node

import (
	"crosssync/internal/events"
	"crosssync/internal/index"
	syncpkg "crosssync/internal/sync"
)

// The three SQL queries behind the cached status figures.
//
// GET /api/status is polled every couple of seconds, and after the disk figures
// were cached the remainder of its cost on Tower was per-request SQL against the
// meta store: two open-event counts (global + one per folder) and one index
// aggregate per folder (~83 ms for the aggregate alone on a 34k-file folder).
// The request path must do NO SQL at all — it reads the atomics below, which the
// same 5 s background ticker that refreshes the disk figures recomputes
// (Node.RefreshStatusCounts), plus once when a folder finishes a scan (a scan is
// what moves these numbers).
//
// They are package-level variables so a test can prove the request path runs
// none of them (see TestStatusDoesNotQuerySQL in internal/control), the same way
// the two filesystem probes are proved by TestStatusDoesNotTouchFilesystem.
// Package-level is honest here: there is nothing per-node about a query.
var (
	openEventsProbe = func(st *events.Store) (int64, error) { return st.CountOpen() }
	folderOpenProbe = func(st *events.Store, folder string) (int64, error) {
		return st.CountOpenFolder(folder)
	}
	indexStatsProbe = func(ix *index.Index) (index.Stats, error) {
		return ix.Stats(syncpkg.ConflictSuffix)
	}
)

// SetStatusQueryHook replaces the three SQL queries behind the cached status
// figures and returns a restore function (call it from t.Cleanup). TESTS ONLY:
// it exists so a test can assert that a code path runs no SQL, by installing
// queries that only count their calls. A nil argument leaves that query
// unchanged.
func SetStatusQueryHook(
	openEvents func(*events.Store) (int64, error),
	folderOpenEvents func(*events.Store, string) (int64, error),
	indexStats func(*index.Index) (index.Stats, error),
) (restore func()) {
	prevOpen, prevFolder, prevStats := openEventsProbe, folderOpenProbe, indexStatsProbe
	if openEvents != nil {
		openEventsProbe = openEvents
	}
	if folderOpenEvents != nil {
		folderOpenProbe = folderOpenEvents
	}
	if indexStats != nil {
		indexStatsProbe = indexStats
	}
	return func() {
		openEventsProbe, folderOpenProbe, indexStatsProbe = prevOpen, prevFolder, prevStats
	}
}

// refreshIndexStats recomputes this folder's cached index aggregate (files,
// directories, tombstones, bytes, conflict copies). Never called from a request
// handler: it is one SQL aggregate over the whole index.
func (f *Folder) refreshIndexStats() {
	if f.Ix == nil {
		return
	}
	st, err := indexStatsProbe(f.Ix)
	if err != nil {
		// Keep the previous figures: a temporary SQL failure must not make the
		// status page claim the folder is empty.
		return
	}
	f.ixFiles.Store(int64(st.Files))
	f.ixDirectories.Store(int64(st.Directories))
	f.ixTombstones.Store(int64(st.Tombstones))
	f.ixSize.Store(st.Size)
	f.ixConflictFiles.Store(int64(st.ConflictFiles))
	f.ixConflictBytes.Store(st.ConflictBytes)
}

// StatusCounts returns the cached figures /api/status publishes for this folder:
// the index aggregate and the number of open warn+ events. Both are read from
// atomics, so this is safe on the polled request path (no SQL, no index walk).
func (f *Folder) StatusCounts() (index.Stats, int64) {
	return index.Stats{
		Files:         int(f.ixFiles.Load()),
		Directories:   int(f.ixDirectories.Load()),
		Tombstones:    int(f.ixTombstones.Load()),
		Size:          f.ixSize.Load(),
		ConflictFiles: int(f.ixConflictFiles.Load()),
		ConflictBytes: f.ixConflictBytes.Load(),
	}, f.openEvents.Load()
}

// CachedOpenEvents is the last refreshed count of open warn+ events across all
// folders (the top-level open_events field).
func (n *Node) CachedOpenEvents() int64 { return n.openEvents.Load() }

// RefreshStatusCounts recomputes the cached figures the status page shows:
// the global open-event count, and per folder the index aggregate and its own
// open-event count. Called by the background ticker (Node.StartPendingRefresher)
// and after a scan; never by a request handler.
func (n *Node) RefreshStatusCounts() {
	n.foldersMu.RLock()
	folders := make([]*Folder, 0, len(n.Folders))
	for _, f := range n.Folders {
		folders = append(folders, f)
	}
	n.foldersMu.RUnlock()
	if n.Events != nil {
		if open, err := openEventsProbe(n.Events); err == nil {
			n.openEvents.Store(open)
		}
	}
	for _, f := range folders {
		n.refreshFolderStatusCounts(f)
	}
}

// refreshFolderStatusCounts recomputes one folder's cached status figures. The
// index aggregate is recomputed here; the open-event count needs the store, so a
// Folder cannot do it alone (there is no back-pointer from Folder to Node).
func (n *Node) refreshFolderStatusCounts(f *Folder) {
	// A pass in flight publishes its own progress (scans[] walked/hash/committed)
	// and bumps the cached counts as each batch commits (see commitScanBatch), so
	// do NOT also run the full-table aggregate here. That aggregate is a scan of
	// the whole index, and during a first pass the index is growing towards the
	// size of the share (a million rows on the measured one): every 5 s it grew
	// more expensive and competed with the pass's own commits for the same
	// SQLite file, which is exactly the kind of self-inflicted stall that shows
	// up as a busy box with idle disks.
	if !n.scanSnapshot(f.Cfg.ID).Running {
		f.refreshIndexStats()
	}
	if n.Events == nil {
		return
	}
	if open, err := folderOpenProbe(n.Events, f.Cfg.ID); err == nil {
		f.openEvents.Store(open)
	}
}
