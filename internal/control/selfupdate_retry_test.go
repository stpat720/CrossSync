package control

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

// TestStageUpdateRetriesChecksumMismatch covers the publish race that was seen
// live on a rolling release: the freshly published latest.json arrives before
// the CDN edge serves the matching bytes, so the first download fails its
// checksum. The apply must retry (with a fresh manifest) instead of leaving an
// unattended auto-update dead until the next check.
func TestStageUpdateRetriesChecksumMismatch(t *testing.T) {
	good := []byte("the real binary")
	sum := sha256.Sum256(good)

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			_, _ = w.Write([]byte("stale bytes from the previous build"))
			return
		}
		_, _ = w.Write(good)
	}))
	defer srv.Close()

	oldDelay := stageRetryDelay
	stageRetryDelay = 0
	t.Cleanup(func() { stageRetryDelay = oldDelay })

	dir := t.TempDir()
	svc := New(nil, "0.1.9")
	svc.SetConfigPath(filepath.Join(dir, "config.yaml"))
	plat := runtime.GOOS + "/" + runtime.GOARCH
	svc.SetUpdate(writeManifest(t, dir, "0.2.0", plat, srv.URL, hex.EncodeToString(sum[:])), "", false)

	ver, err := svc.StageUpdate()
	if err != nil {
		t.Fatalf("StageUpdate should succeed on the retry, got %v", err)
	}
	if ver != "0.2.0" {
		t.Fatalf("staged version = %q, want 0.2.0", ver)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("download attempts = %d, want 2 (one stale, one good)", got)
	}
	staged, err := os.ReadFile(svc.updateTarget() + ".new")
	if err != nil {
		t.Fatalf("no staged binary: %v", err)
	}
	if string(staged) != string(good) {
		t.Fatalf("staged bytes = %q, want the verified payload", staged)
	}
}

// TestStageUpdateGivesUpAfterChecksumRetries pins the bound: a manifest that
// never matches the served bytes must fail (not loop), leaving no staged file.
func TestStageUpdateGivesUpAfterChecksumRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("always wrong"))
	}))
	defer srv.Close()

	oldDelay, oldAttempts := stageRetryDelay, stageAttempts
	stageRetryDelay = 0
	stageAttempts = 2
	t.Cleanup(func() { stageRetryDelay, stageAttempts = oldDelay, oldAttempts })

	dir := t.TempDir()
	svc := New(nil, "0.1.9")
	svc.SetConfigPath(filepath.Join(dir, "config.yaml"))
	svc.SetUpdate(writeManifest(t, dir, "0.2.0", runtime.GOOS+"/"+runtime.GOARCH, srv.URL, "00"), "", false)

	if _, err := svc.StageUpdate(); err == nil {
		t.Fatal("a permanently mismatched download must fail")
	} else {
		msg := err.Error()
		if !strings.Contains(msg, "checksum mismatch") || !strings.Contains(msg, "retried") {
			t.Fatalf("error should report the mismatch and the retries, got %v", err)
		}
	}
	if _, err := os.Stat(svc.updateTarget() + ".new"); !os.IsNotExist(err) {
		t.Fatal("a failed stage must leave no binary behind")
	}
}
