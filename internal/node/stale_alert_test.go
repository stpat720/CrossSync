package node

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/alert"
	"crosssync/internal/config"
)

// TestCheckStaleSyncPagesOnce pins the push-side watchdog: a folder that used
// to sync and then stops must PAGE (once per cooldown), while a paused folder,
// a never-synced folder and a healthy folder must stay silent.
func TestCheckStaleSyncPagesOnce(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	old := staleSyncAfter
	staleSyncAfter = 90 * time.Minute
	defer func() { staleSyncAfter = old }()

	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	n.Cfg.Peers = []config.Peer{{ID: 2, Name: "b", Addresses: []string{"127.0.0.1:1"}}}
	n.Alert = alert.New(srv.URL)
	n.alertCooldown = time.Hour

	f := n.Folders["data"]
	// Never synced: silent (peer-offline events already cover that).
	n.CheckStaleSync()
	if got := hits.Load(); got != 0 {
		t.Fatalf("never-synced folder paged %d time(s), want 0", got)
	}

	// Freshly synced: silent.
	f.Engine.Stats.LastSync.Store(time.Now().UnixNano())
	n.CheckStaleSync()
	if got := hits.Load(); got != 0 {
		t.Fatalf("healthy folder paged %d time(s), want 0", got)
	}

	// Stalled past the threshold: one page, then the cooldown keeps it quiet.
	f.Engine.Stats.LastSync.Store(time.Now().Add(-3 * time.Hour).UnixNano())
	n.CheckStaleSync()
	n.CheckStaleSync()
	if got := hits.Load(); got != 1 {
		t.Fatalf("stalled folder paged %d time(s), want exactly 1 (cooldown)", got)
	}

	// Paused: no paging even when stale.
	f.Engine.Stats.LastSync.Store(time.Now().Add(-3 * time.Hour).UnixNano())
	n.alertThrottle = map[string]time.Time{}
	if err := n.SetFolderPaused("data", true); err != nil {
		t.Fatal(err)
	}
	n.CheckStaleSync()
	if got := hits.Load(); got != 1 {
		t.Fatalf("paused folder paged %d time(s) in total, want 1", got)
	}

	// No peers configured: nothing to be stale about.
	if err := n.SetFolderPaused("data", false); err != nil {
		t.Fatal(err)
	}
	n.Cfg.Peers = nil
	n.alertThrottle = map[string]time.Time{}
	n.CheckStaleSync()
	if got := hits.Load(); got != 1 {
		t.Fatalf("peerless node paged %d time(s) in total, want 1", got)
	}
}
