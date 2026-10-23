package control

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"crosssync/internal/node"
)

// newLogCapture builds a Service whose node records every Logf line, so the
// self-update check's log behaviour can be asserted directly.
func newLogCapture(version string) (*Service, *[]string) {
	lines := &[]string{}
	n := &node.Node{Logf: func(format string, args ...any) {
		*lines = append(*lines, fmt.Sprintf(format, args...))
	}}
	return New(n, version), lines
}

// TestUpdateCheckLogsOnlyOnChange pins the noisy-log fix: the outcome of a
// manifest check is logged once per STATE CHANGE, never once per fetch.
//
// Before this, every fetch wrote a line — and fetches are driven by whichever
// dashboard tab is open (the UI polls /api/update/check every 30s, throttled
// to 5 min), so an idle cluster logged ~288 identical "up to date" lines a
// day and went silent the moment the browser closed. Neither is a signal.
func TestUpdateCheckLogsOnlyOnChange(t *testing.T) {
	svc, lines := newLogCapture("0.1.0")
	dir := t.TempDir()
	svc.SetConfigPath(filepath.Join(dir, "config.yaml"))
	plat := runtime.GOOS + "/" + runtime.GOARCH
	bin := "file://" + filepath.Join(dir, "bin")

	// First check after start: one line, up to date.
	svc.SetUpdate(writeManifest(t, dir, "0.1.0", plat, bin, ""), "", false)
	svc.CheckDaemonUpdate()
	if len(*lines) != 1 || !strings.Contains((*lines)[0], "up to date") {
		t.Fatalf("first check should log exactly one up-to-date line, got %q", *lines)
	}

	// A repeat fetch of the same state must stay silent.
	svc.ForceUpdateCheck()
	svc.CheckDaemonUpdate()
	svc.ForceUpdateCheck()
	svc.CheckDaemonUpdate()
	if len(*lines) != 1 {
		t.Fatalf("unchanged checks must not log again, got %q", *lines)
	}

	// A new release changes the state: exactly one new line.
	svc.ForceUpdateCheck()
	svc.SetUpdate(writeManifest(t, dir, "0.2.0", plat, bin, ""), "", false)
	svc.CheckDaemonUpdate()
	if len(*lines) != 2 || !strings.Contains((*lines)[1], "v0.2.0 available") {
		t.Fatalf("a newly available update must log once, got %q", *lines)
	}
	svc.ForceUpdateCheck()
	svc.CheckDaemonUpdate()
	if len(*lines) != 2 {
		t.Fatalf("still-available update must not re-log, got %q", *lines)
	}

	// A failure is a state change and logs; repeating it does not.
	svc.ForceUpdateCheck()
	svc.SetUpdate("file://"+filepath.Join(dir, "missing.json"), "", false)
	svc.CheckDaemonUpdate()
	svc.ForceUpdateCheck()
	svc.CheckDaemonUpdate()
	if len(*lines) != 3 || !strings.Contains((*lines)[2], "check failed") {
		t.Fatalf("a failed check must log once, got %q", *lines)
	}

	// Recovery back to up to date is worth a line (the operator saw the error).
	svc.ForceUpdateCheck()
	svc.SetUpdate(writeManifest(t, dir, "0.1.0", plat, bin, ""), "", false)
	svc.CheckDaemonUpdate()
	if len(*lines) != 4 || !strings.Contains((*lines)[3], "up to date") {
		t.Fatalf("recovery after a failure must log once, got %q", *lines)
	}
}

// TestUpdateCheckRunsWithoutAuto pins the other half: the daemon-side check
// must happen even when update_auto is off. Before this, MaybeAutoUpdate
// returned early on !UpdateAuto, so an operator-run box (Tower) only ever
// learned about a release while somebody had the dashboard open — the daemon
// log even stopped emitting check lines the moment the browser closed.
func TestUpdateCheckRunsWithoutAuto(t *testing.T) {
	svc, _ := newLogCapture("0.1.0")
	dir := t.TempDir()
	svc.SetConfigPath(filepath.Join(dir, "config.yaml"))
	plat := runtime.GOOS + "/" + runtime.GOARCH
	bin := "file://" + filepath.Join(dir, "bin")
	svc.SetUpdate(writeManifest(t, dir, "0.2.0", plat, bin, ""), "", false) // auto OFF

	svc.MaybeAutoUpdate()

	info := svc.CheckDaemonUpdate()
	if info.CheckedAt == 0 || info.Latest != "0.2.0" || !info.UpdateAvailable {
		t.Fatalf("auto:false must still check the manifest, got %+v", info)
	}
	// ...and must NOT install anything on its own.
	if _, err := os.Stat(svc.updateTarget() + ".new"); err == nil {
		t.Fatal("auto:false must never stage a binary")
	}
}
