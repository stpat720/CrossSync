package control

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestGoroutineDumpAnswers: the endpoint has to work precisely when the rest of
// the daemon does not, so the assertion is on the shape of the response rather
// than on any daemon state: a plain-text dump that includes this request's own
// goroutine (proving it answered while other goroutines exist), served as
// text/plain.
func TestGoroutineDumpAnswers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc((&Service{}).handleGoroutines))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content-type = %q, want text/plain", ct)
	}
	buf := make([]byte, 1<<16)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])
	if !strings.Contains(body, "goroutine ") {
		t.Fatalf("dump does not look like a goroutine stack:\n%.200s", body)
	}
	if !strings.Contains(body, "runtime.Stack") && !strings.Contains(body, "net/http") {
		t.Fatalf("dump does not include the serving goroutine:\n%.200s", body)
	}
}
