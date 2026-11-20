package control_test

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"crosssync/internal/control"
	"crosssync/internal/node"
)

// TestStatusDoesNotTouchFilesystem pins the fix for the slowest request the
// daemon serves. /api/status used to make two filesystem calls per folder per
// request: a statfs of the folder root (free_bytes) and a filepath.WalkDir of
// the .sfx-versions area (version_files/version_bytes).
//
// Measured live before this change: /api/status took 5.8-8.3 s on Tower while
// NAS-02, serving the same folders from a faster array, answered the identical
// call in 0.28 s — the difference was purely the disk latency of those two
// calls, repeated for every folder on every poll. The UI polls every 2 s, so
// the calls also stacked (see the poll guard in the UI).
//
// The handler must read ONLY the caches refreshed by the 5 s background ticker.
// Here the two probes are replaced by counters: the cache is seeded through them
// once (the ticker's path), and then 20 Status() calls must make ZERO probe
// calls while still returning the cached figures.
func TestStatusDoesNotTouchFilesystem(t *testing.T) {
	n, _ := testNode(t)

	var probes atomic.Int64
	restore := node.SetDiskProbeHook(
		func(string) int64 {
			probes.Add(1)
			return 42 << 30 // a value the request path can only get from the cache
		},
		func(string) (int, int64) {
			probes.Add(1)
			return 7, 1234
		},
	)
	t.Cleanup(restore)

	// This is the ticker's path (Node.StartPendingRefresher), not a request.
	n.RefreshDiskStats()
	if got := probes.Load(); got != 2 {
		t.Fatalf("one folder refresh made %d probe call(s), want 2 (statfs + versions walk)", got)
	}
	probes.Store(0)

	svc := control.New(n, "test")
	for i := 0; i < 20; i++ {
		st := svc.Status()
		if len(st.Folders) != 1 {
			t.Fatalf("call %d: folders = %d, want 1", i, len(st.Folders))
		}
		f := st.Folders[0]
		if f.FreeBytes != 42<<30 {
			t.Fatalf("call %d: free_bytes = %d, want the cached %d", i, f.FreeBytes, int64(42)<<30)
		}
		if f.VersionFiles != 7 || f.VersionBytes != 1234 {
			t.Fatalf("call %d: versions = %d files / %d bytes, want the cached 7 / 1234",
				i, f.VersionFiles, f.VersionBytes)
		}
	}
	if got := probes.Load(); got != 0 {
		t.Fatalf("/api/status made %d filesystem probe call(s) across 20 requests, want 0: "+
			"the handler must read the cache and never stat or walk the folder", got)
	}
}

// TestDiskRefresherPublishesRealFigures is the other half of the contract: the
// cached figures are not merely stale-but-cheap, they are refreshed. With the
// real probes installed, the background refresh must publish a positive free
// space and the versions area's true size for a real folder.
func TestDiskRefresherPublishesRealFigures(t *testing.T) {
	n, root := testNode(t)
	// An archived version in the reserved area, so the walk has something to
	// count.
	if err := os.MkdirAll(filepath.Join(root, ".sfx-versions"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sfx-versions", "a.txt"), []byte("0123456789"), 0o644); err != nil {
		t.Fatal(err)
	}
	n.RefreshDiskStats()
	if got := n.FreeBytes("data"); got <= 0 {
		t.Fatalf("free_bytes = %d after a refresh, want > 0 for a real temp dir", got)
	}
	files, bytes := n.VersionStats("data")
	if files != 1 || bytes != 10 {
		t.Fatalf("versions = %d / %d, want 1 / 10", files, bytes)
	}
}
