package control

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// writeManifest writes a latest.json with one platform entry and returns
// its file:// URL.
func writeManifest(t *testing.T, dir, version, plat, url, sha string) string {
	t.Helper()
	m := map[string]any{
		"version": version,
		"platforms": map[string]any{
			plat: map[string]any{"url": url, "sha256": sha},
		},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "latest.json")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return "file://" + p
}

func shaOf(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestVersionNewer(t *testing.T) {
	cases := []struct{ cur, cand string; want bool }{
		{"0.1.0", "0.2.0", true},
		{"0.1.0", "0.1.1", true},
		{"0.1.0", "1.0.0", true},
		{"0.1.0", "0.1.0", false},
		{"0.2.0", "0.1.0", false},
		{"0.1.10", "0.1.9", false},
		// Multi-part (tag + commit count) versions: 0.1.0.3 > 0.1.0.
		{"0.1.0", "0.1.0.3", true},
		{"0.1.0.3", "0.1.0.4", true},
		{"0.1.0.4", "0.1.0.4", false},
		{"0.1.0.3", "0.1.0", false},
		// Non-semver strings compare as 0.
		{"0.1.0", "bf4f45d", false}, // a commit hash is never "newer"
		{"0.1.0", "nightly", false},
		// v-prefixed / suffixed tags still compare numerically.
		{"0.1.0", "v0.2.0", true},
		{"0.1.0-3-gabc123", "0.1.1", true},
	}
	for _, c := range cases {
		if got := versionNewer(c.cur, c.cand); got != c.want {
			t.Fatalf("versionNewer(%q, %q) = %v, want %v", c.cur, c.cand, got, c.want)
		}
	}
}

func TestSwapBinary(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "crosssync")
	staged := target + ".new"
	_ = os.WriteFile(target, []byte("old binary"), 0o755)
	_ = os.WriteFile(staged, []byte("new binary"), 0o755)

	if err := swapBinary(target, staged); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "new binary" {
		t.Fatalf("target = %q, want new binary", b)
	}
	if _, err := os.Stat(target + ".old"); !os.IsNotExist(err) {
		t.Fatal("backup should be removed after a successful swap")
	}
}

func TestCheckDaemonUpdateManifest(t *testing.T) {
	svc := New(nil, "0.1.0")
	dir := t.TempDir()
	svc.SetConfigPath(filepath.Join(dir, "config.yaml"))
	plat := runtime.GOOS + "/" + runtime.GOARCH

	// Newer version in the manifest -> update available.
	svc.SetUpdate(writeManifest(t, dir, "0.2.0", plat, "file://"+filepath.Join(dir, "bin"), ""), "", false)
	info := svc.CheckDaemonUpdate()
	if !info.Configured || !info.UpdateAvailable {
		t.Fatalf("expected update available, got %+v", info)
	}
	if info.Latest != "0.2.0" || info.Current != "0.1.0" {
		t.Fatalf("unexpected versions: %+v", info)
	}
	if !strings.HasSuffix(filepath.ToSlash(info.Target), "/crosssync") && !strings.HasSuffix(info.Target, ".exe") {
		t.Fatalf("unexpected target %q", info.Target)
	}

	// Older / equal version -> not available.
	svc.ForceUpdateCheck()
	svc.SetUpdate(writeManifest(t, dir, "0.1.0", plat, "file://"+filepath.Join(dir, "bin"), ""), "", false)
	info = svc.CheckDaemonUpdate()
	if info.UpdateAvailable {
		t.Fatalf("expected no update for equal version, got %+v", info)
	}

	// No binary for this platform -> error surfaced, not available.
	svc.ForceUpdateCheck()
	svc.SetUpdate(writeManifest(t, dir, "0.2.0", "plan9/riscv", "file://x", ""), "", false)
	info = svc.CheckDaemonUpdate()
	if info.UpdateAvailable || info.Error == "" {
		t.Fatalf("expected error for missing platform, got %+v", info)
	}
}

func TestStageUpdateDownloadsAndVerifies(t *testing.T) {
	svc := New(nil, "0.1.0")
	dir := t.TempDir()
	svc.SetConfigPath(filepath.Join(dir, "config.yaml"))
	plat := runtime.GOOS + "/" + runtime.GOARCH

	// A tiny "binary" payload, checksummed.
	payload := filepath.Join(dir, "payload")
	_ = os.WriteFile(payload, []byte("#!/bin/sh\necho new version\n"), 0o755)
	good := shaOf(t, payload)
	svc.SetUpdate(writeManifest(t, dir, "0.2.0", plat, "file://"+payload, good), "", false)

	ver, err := svc.StageUpdate()
	if err != nil {
		t.Fatal(err)
	}
	if ver != "0.2.0" {
		t.Fatalf("staged version = %q, want 0.2.0", ver)
	}
	// The staged file exists with the payload content; the running binary
	// was NOT touched.
	target := svc.updateTarget()
	staged := target + ".new"
	b, err := os.ReadFile(staged)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "#!/bin/sh\necho new version\n" {
		t.Fatalf("staged content mismatch: %q", b)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target %s should not exist before commit", target)
	}

	// Wrong checksum -> rejected, no staged file left behind.
	bad := writeManifest(t, dir, "0.2.0", plat, "file://"+payload, strings.Repeat("0", 64))
	svc.SetUpdate(bad, "", false)
	if _, err := svc.StageUpdate(); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Fatal("staged file should be removed after a checksum failure")
	}

	// Missing url -> rejected.
	svc.SetUpdate(writeManifest(t, dir, "0.2.0", plat, "", good), "", false)
	if _, err := svc.StageUpdate(); err == nil {
		t.Fatal("missing url should fail")
	}

	// Bad manifest JSON -> rejected.
	_ = os.WriteFile(filepath.Join(dir, "latest.json"), []byte("not json"), 0o644)
	svc.SetUpdate("file://"+filepath.Join(dir, "latest.json"), "", false)
	if _, err := svc.StageUpdate(); err == nil {
		t.Fatal("malformed manifest should fail")
	}
}

// TestStageUpdateRefusesDowngrade pins the direction check: a manifest that
// offers an OLDER build must be refused (a tampered or mis-published manifest
// is the easiest way to put a fixed bug back on a box). The same-version
// re-stage stays legal — that is the documented "swap the identical binary"
// restart path on machines with no shell.
func TestStageUpdateRefusesDowngrade(t *testing.T) {
	svc := New(nil, "0.2.0")
	dir := t.TempDir()
	svc.SetConfigPath(filepath.Join(dir, "config.yaml"))
	plat := runtime.GOOS + "/" + runtime.GOARCH

	payload := filepath.Join(dir, "payload")
	if err := os.WriteFile(payload, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	good := shaOf(t, payload)

	svc.SetUpdate(writeManifest(t, dir, "0.1.9", plat, "file://"+payload, good), "", false)
	if _, err := svc.StageUpdate(); err == nil || !strings.Contains(err.Error(), "refusing to downgrade") {
		t.Fatalf("older manifest must be refused, got %v", err)
	}
	if _, err := os.Stat(svc.updateTarget() + ".new"); !os.IsNotExist(err) {
		t.Fatal("a refused downgrade must not stage a binary")
	}

	svc.SetUpdate(writeManifest(t, dir, "0.2.0", plat, "file://"+payload, good), "", false)
	if _, err := svc.StageUpdate(); err != nil {
		t.Fatalf("same-version re-stage must stay allowed: %v", err)
	}
}
