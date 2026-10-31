package control_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"crosssync/internal/control"
)

func getStatus(t *testing.T, url string, header map[string]string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// TestControlTokenGatesEveryRequest: with a token configured, a request without
// it must be refused, and both the header form (API clients, web UI) and the
// query form (EventSource, download links) must be accepted. Without a token the
// daemon behaves exactly as before, so an existing deployment is unaffected.
func TestControlTokenGatesEveryRequest(t *testing.T) {
	n, _ := testNode(t)

	openSrv := httptest.NewServer(control.New(n, "9.9.9").Handler())
	defer openSrv.Close()
	if code := getStatus(t, openSrv.URL+"/api/status", nil); code != http.StatusOK {
		t.Fatalf("no token configured: status %d, want 200", code)
	}

	svc := control.New(n, "9.9.9")
	svc.ControlToken = "s3cret-token-value"
	gated := httptest.NewServer(svc.Handler())
	defer gated.Close()

	if code := getStatus(t, gated.URL+"/api/status", nil); code != http.StatusUnauthorized {
		t.Fatalf("no token: status %d, want 401", code)
	}
	auth := map[string]string{"Authorization": "Bearer s3cret-token-value"}
	if code := getStatus(t, gated.URL+"/api/status", auth); code != http.StatusOK {
		t.Fatalf("bearer header: status %d, want 200", code)
	}
	if code := getStatus(t, gated.URL+"/api/status?token=s3cret-token-value", nil); code != http.StatusOK {
		t.Fatalf("query token: status %d, want 200", code)
	}
	if code := getStatus(t, gated.URL+"/api/status?token=wrong", nil); code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status %d, want 401", code)
	}
}
