package control_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/control"
)

// TestMetricsExposition pins the Prometheus surface: a scrape must expose the
// build identity, per-folder state (including pause) and per-peer health in the
// text format, with the labels Prometheus expects. It is the substrate for
// "alert me when nothing has synced for an hour" without parsing JSON.
func TestMetricsExposition(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "9.9.9")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	resp, err := httpGet(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if ct := resp.header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") || !strings.Contains(ct, "version=0.0.4") {
		t.Fatalf("content-type = %q, want the Prometheus text format", ct)
	}
	body := resp.body

	for _, want := range []string{
		"# TYPE crosssync_build_info gauge",
		`crosssync_build_info{version="9.9.9",device="nas-01"`,
		`crosssync_folder_files{folder="data"}`,
		`crosssync_folder_paused{folder="data"} 0`,
		`crosssync_folder_sessions_total{folder="data"}`,
		"# TYPE crosssync_open_events gauge",
		"# TYPE crosssync_sync_running gauge",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output is missing %q:\n%s", want, body)
		}
	}

	// A paused folder reports 1, and a configured peer is exposed by name.
	n.Cfg.Peers = []config.Peer{{ID: 42, Name: "peer-b", Addresses: []string{"127.0.0.1:1"}}}
	svc2 := control.New(n, "9.9.9")
	if _, err := svc2.SetFolderPaused("data", true); err != nil {
		t.Fatal(err)
	}
	srv2 := httptest.NewServer(svc2.Handler())
	defer srv2.Close()
	resp2, err := httpGet(srv2.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`crosssync_folder_paused{folder="data"} 1`,
		`crosssync_peer_connected{peer="peer-b",peer_id="42"} 0`,
		`crosssync_peer_last_sync_timestamp_seconds{peer="peer-b"} 0`,
	} {
		if !strings.Contains(resp2.body, want) {
			t.Fatalf("metrics output is missing %q:\n%s", want, resp2.body)
		}
	}
}

type httpResult struct {
	header http.Header
	body   string
}

func httpGet(url string) (httpResult, error) {
	resp, err := http.Get(url)
	if err != nil {
		return httpResult{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return httpResult{}, err
	}
	return httpResult{header: resp.Header, body: string(b)}, nil
}
