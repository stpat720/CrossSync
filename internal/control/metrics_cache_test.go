package control_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"crosssync/internal/control"
)

// TestMetricsScrapeIsServedFromCache: a scrape must not rebuild the exposition
// on the request path. Building it inline took over a minute while a 32 GB scan
// held the locks it reads, so the endpoint timed out exactly when an operator
// needed it. The first scrape renders; later ones serve the same bytes.
func TestMetricsScrapeIsServedFromCache(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "9.9.9")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	first, err := httpGet(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if first.body == "" {
		t.Fatal("first scrape returned an empty body")
	}
	second, err := httpGet(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if second.body != first.body {
		t.Fatalf("second scrape rebuilt the body instead of serving the cache:\nfirst=%d bytes\nsecond=%d bytes", len(first.body), len(second.body))
	}
	_ = http.StatusOK
}
