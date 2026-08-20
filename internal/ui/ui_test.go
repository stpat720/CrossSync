package ui

import (
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServesIndex(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	Handler("").ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	s := string(body)
	for _, want := range []string{"CrossSync", "/api/status", "/api/events/stream", "Add folder", "Event history", "settingsModal"} {
		if !strings.Contains(s, want) {
			t.Fatalf("index.html missing %q", want)
		}
	}
}

func TestOverrideDirTakesPrecedence(t *testing.T) {
	dir := t.TempDir()
	override := `<!DOCTYPE html><html><head><meta name="crosssync-ui-version" content="9.9.9"></head><body>OVERRIDE-UI</body></html>`
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(override), 0o644); err != nil {
		t.Fatal(err)
	}
	if v := CurrentVersion(dir); v != "9.9.9" {
		t.Fatalf("CurrentVersion = %q, want 9.9.9", v)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	Handler(dir).ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "OVERRIDE-UI") {
		t.Fatalf("override UI not served: %s", body)
	}
	// Dropping the override file falls back to the embedded UI.
	if err := os.Remove(filepath.Join(dir, "index.html")); err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	Handler(dir).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	body, _ = io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "CrossSync") {
		t.Fatalf("embedded fallback failed")
	}
}

func TestWriteOverrideValidates(t *testing.T) {
	dir := t.TempDir()
	good := `<!DOCTYPE html><html><head><meta name="crosssync-ui-version" content="1.2.3"></head><body>x</body></html>`
	v, err := WriteOverride(dir, good)
	if err != nil || v != "1.2.3" {
		t.Fatalf("WriteOverride = %q, %v", v, err)
	}
	if _, err := WriteOverride(dir, "<html>not a ui</html>"); err == nil {
		t.Fatal("WriteOverride accepted a non-CrossSync payload")
	}
	if v := Version(); v == "" {
		t.Fatal("embedded UI should report a version")
	}
}
