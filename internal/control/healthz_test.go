package control_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/control"
)

// healthBody is the /healthz payload shape these tests assert on.
type healthBody struct {
	Status    string `json:"status"`
	Version   string `json:"version"`
	Folders   int    `json:"folders"`
	Peers     int    `json:"peers"`
	OldestAge int64  `json:"oldest_sync_age_seconds"`
	Reason    string `json:"reason"`
}

// TestHealthzReflectsSyncCurrency pins the production probe: /healthz is 200
// while rounds are completing, 503 once a folder with a configured peer stops
// completing them. That silent-stop case is exactly what a container runtime
// has no other way to see — the process stays alive and every other endpoint
// keeps answering 200.
func TestHealthzReflectsSyncCurrency(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "9.9.9")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	get := func() (int, healthBody) {
		t.Helper()
		code, raw := getJSON(t, srv.URL+"/healthz")
		var b healthBody
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatalf("healthz is not JSON (%d): %s", code, raw)
		}
		return code, b
	}

	// No peers configured: nothing can be stale, so a single node is healthy.
	code, b := get()
	if code != 200 || b.Status != "ok" || b.Version != "9.9.9" || b.Folders != 1 {
		t.Fatalf("single-node healthz = %d %+v, want 200 ok with version and 1 folder", code, b)
	}

	// Add a peer: now a folder that never completed a round is degraded, not OK.
	n.Cfg.Peers = []config.Peer{{ID: 42, Name: "peer", Addresses: []string{"127.0.0.1:1"}}}
	code, b = get()
	if code != 503 || b.Status != "degraded" || b.Peers != 1 {
		t.Fatalf("never-synced folder with a peer = %d %+v, want 503 degraded", code, b)
	}

	// A completed round right now is healthy again.
	n.Folders["data"].Engine.Stats.LastSync.Store(time.Now().UnixNano())
	code, b = get()
	if code != 200 || b.Status != "ok" {
		t.Fatalf("fresh sync = %d %+v, want 200 ok", code, b)
	}
	if b.OldestAge < 0 || b.OldestAge > 60 {
		t.Fatalf("oldest age = %d, want a small non-negative number", b.OldestAge)
	}

	// And staleness past the window flips it back.
	n.Folders["data"].Engine.Stats.LastSync.Store(time.Now().Add(-2 * time.Hour).UnixNano())
	code, b = get()
	if code != 503 || b.Status != "degraded" || b.Reason == "" {
		t.Fatalf("2h-stale folder = %d %+v, want 503 degraded with a reason", code, b)
	}
}
