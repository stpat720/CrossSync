package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestCmdHealthContract pins the exit contract of `crosssync health`, which is
// what the container HEALTHCHECK relies on: 0 for a healthy daemon, non-zero
// (with the daemon's own reason in the message) for a degraded one, and
// non-zero when the daemon cannot be reached at all.
func TestCmdHealthContract(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("health probed %q, want /healthz", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","version":"1.2.3"}`))
	}))
	defer ok.Close()
	if err := cmdHealth([]string{"--addr", ok.URL}); err != nil {
		t.Fatalf("healthy daemon reported an error: %v", err)
	}

	degraded := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"degraded","reason":"no completed sync round in over 1h0m0s"}`))
	}))
	defer degraded.Close()
	err := cmdHealth([]string{"--addr", degraded.URL})
	if err == nil || !strings.Contains(err.Error(), "degraded") || !strings.Contains(err.Error(), "no completed sync round") {
		t.Fatalf("degraded daemon = %v, want an error carrying the daemon's reason", err)
	}

	// Nothing listening: an unreachable daemon must fail, not hang or pass.
	if err := cmdHealth([]string{"--addr", "http://127.0.0.1:1", "--timeout", "1s"}); err == nil {
		t.Fatal("unreachable daemon reported healthy")
	}
}
