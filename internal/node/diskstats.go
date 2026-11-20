package node

import "crosssync/internal/sync"

// The two filesystem probes behind the cached per-folder disk figures.
//
// They are variables so a test can prove GET /api/status calls NEITHER (see
// TestStatusDoesNotTouchFilesystem in internal/control): the request path reads
// Folder's atomics, and only folder registration plus the background ticker run
// these. Package-level is honest here — the production implementations are
// build-tagged (freespace_unix.go / freespace_windows.go) and there is nothing
// per-node about them.
var (
	freeBytesProbe    = freeBytes
	versionStatsProbe = func(root string) (int, int64) {
		return (&sync.Versioning{}).Stats(root)
	}
)

// SetDiskProbeHook replaces the two filesystem probes used by the disk-figure
// cache and returns a restore function (call it from t.Cleanup). TESTS ONLY: it
// exists so a test can assert that a code path does no filesystem work, by
// installing probes that only count their calls. A nil argument leaves that
// probe unchanged.
func SetDiskProbeHook(free func(string) int64, versions func(string) (int, int64)) (restore func()) {
	prevFree, prevVersions := freeBytesProbe, versionStatsProbe
	if free != nil {
		freeBytesProbe = free
	}
	if versions != nil {
		versionStatsProbe = versions
	}
	return func() {
		freeBytesProbe, versionStatsProbe = prevFree, prevVersions
	}
}

// refreshDiskStats recomputes one folder's cached disk figures.
//
// Both underlying calls are slow on the production targets — a statfs against a
// spinning array and a directory walk of the versions area — and neither may run
// on the request path (measured: 5.8-8.3 s per /api/status on Tower, against
// 0.28 s for the identical call on NAS-02), so this runs at folder registration
// and from the 5 s background ticker that already refreshes the pending
// counters. It deliberately does NOT take f.mu: a scan holds that for minutes,
// and nothing it guards (Root is fixed for a folder's lifetime) changes when a
// scan runs.
func (f *Folder) refreshDiskStats() {
	f.freeBytes.Store(freeBytesProbe(f.Root))
	files, bytes := versionStatsProbe(f.Root)
	f.versionFiles.Store(int64(files))
	f.versionBytes.Store(bytes)
}

// RefreshDiskStats refreshes the cached free space and versions-area size of
// every folder. Called by the background ticker and on folder add; never by a
// request handler.
func (n *Node) RefreshDiskStats() {
	n.foldersMu.RLock()
	folders := make([]*Folder, 0, len(n.Folders))
	for _, f := range n.Folders {
		folders = append(folders, f)
	}
	n.foldersMu.RUnlock()
	for _, f := range folders {
		f.refreshDiskStats()
	}
}
