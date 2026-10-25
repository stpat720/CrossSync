package control_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"crosssync/internal/control"
)

// TestStatusServesCachedPendingCounters pins the fix for a control-plane stall
// seen at scale: /api/status used to compute pending files by walking the whole
// index (twice) on every request. With the UI polling every couple of seconds and
// a round holding the engine lock while it applies files, a status request could
// block for minutes (measured: 90s timeout on a node hashing a 20 GiB corpus),
// which also breaks health checks.
//
// The API must read the cached counters. Here the cache is set to a value the
// recomputation could never produce (the index has no peer, so NeedPulls is
// empty), so a regression that goes back to computing on the request path fails.
func TestStatusServesCachedPendingCounters(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	f := n.Folders["data"]
	f.Engine.Stats.PendingPullCount.Store(5)
	f.Engine.Stats.PendingPullBytes.Store(12345)
	f.Engine.Stats.PendingUpCount.Store(7)

	// Warm the status path, then read it while a slow round would have held the
	// engine lock: the response must still be immediate and reflect the cache.
	_, _ = getJSON(t, srv.URL+"/api/status")
	var got struct {
		Folders []struct {
			PendingFiles int   `json:"pending_files"`
			PendingBytes int64 `json:"pending_bytes"`
			PendingUp    int   `json:"pending_up"`
		} `json:"folders"`
	}
	code, body := getJSON(t, srv.URL+"/api/status")
	if code != 200 {
		t.Fatalf("status = %d", code)
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if len(got.Folders) != 1 {
		t.Fatalf("folders = %d", len(got.Folders))
	}
	fs := got.Folders[0]
	if fs.PendingFiles != 5 || fs.PendingBytes != 12345 || fs.PendingUp != 7 {
		t.Fatalf("status pending = %d files / %d bytes / %d up, want the cached 5 / 12345 / 7",
			fs.PendingFiles, fs.PendingBytes, fs.PendingUp)
	}

	// /metrics must agree with /api/status.
	code, m := getJSON(t, srv.URL+"/metrics")
	if code != 200 {
		t.Fatalf("metrics = %d", code)
	}
	for _, want := range []string{
		`crosssync_folder_pending_files{folder="data"} 5`,
		`crosssync_folder_pending_up{folder="data"} 7`,
	} {
		if !strings.Contains(string(m), want) {
			t.Fatalf("metrics missing %q\n%s", want, m)
		}
	}
}

