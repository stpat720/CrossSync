package sync

import (
	"testing"

	"crosssync/internal/index"
	"crosssync/internal/version"
)

// TestRefreshPendingStatsCachesTheNameLists pins the contract the attention list
// relies on: the refresher stores the NAME lists, not just the counts, so a page
// open never walks the index. Regression here puts two full index scans back on
// the request path (measured live at 47.7 s on an 84k-entry folder under load).
func TestRefreshPendingStatsCachesTheNameLists(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	e := n.engine

	if _, ok := e.CachedPendingPulls(); ok {
		t.Fatalf("caches must be empty before the first refresh")
	}
	if _, ok := e.CachedPendingUploads(); ok {
		t.Fatalf("caches must be empty before the first refresh")
	}

	v := version.New()
	// One local file the peer lacks (an upload) and one remote file we lack (a pull).
	if err := n.ix.Put(&index.FileInfo{Name: "local.txt", Size: 3, Type: index.TypeFile, Version: v}); err != nil {
		t.Fatal(err)
	}
	e.SetPeerIndex(7, []*index.FileInfo{
		{Name: "remote.txt", Size: 4, Type: index.TypeFile, Version: v},
	}, false)

	e.RefreshPendingStats()

	pulls, ok := e.CachedPendingPulls()
	if !ok || len(pulls) != 1 || pulls[0] != "remote.txt" {
		t.Fatalf("cached pulls = %v (ok=%v), want [remote.txt]", pulls, ok)
	}
	ups, ok := e.CachedPendingUploads()
	if !ok || len(ups) != 1 || ups[0] != "local.txt" {
		t.Fatalf("cached uploads = %v (ok=%v), want [local.txt]", ups, ok)
	}
	// The counters and the lists must agree, or the UI shows "3 pending" with a
	// list of two rows.
	if got := e.Stats.PendingPullCount.Load(); got != int64(len(pulls)) {
		t.Fatalf("pending count %d disagrees with the cached list (%d)", got, len(pulls))
	}
	if got := e.Stats.PendingUpCount.Load(); got != int64(len(ups)) {
		t.Fatalf("pending-up count %d disagrees with the cached list (%d)", got, len(ups))
	}
}
