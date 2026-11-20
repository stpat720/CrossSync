package control_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/control"
)

func putJSON(t *testing.T, url, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

// syncTestConfig writes a real config.yaml for the settings tests: a folder that
// exists, plus an unrelated key and a comment that a settings write must not
// destroy (persistence edits the YAML DOCUMENT, never re-marshals the struct).
func syncTestConfig(t *testing.T, root, metaDir string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	doc := strings.Join([]string{
		"# operator comment that must survive a settings write",
		"tls: false",
		"device:",
		"  id: 1",
		"  name: nas-01",
		"meta_dir: " + filepath.ToSlash(metaDir),
		"control_token: secret-token",
		"folders:",
		"  - id: data",
		"    path: " + filepath.ToSlash(root),
		"    conflict_policy: conflict-copy",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestRESTSetSyncSettings covers PUT /api/settings/sync end to end: the handler,
// the clamp, "absent means unchanged", the 400s, and the fact that the value is
// really persisted to CONFIG.YAML (the boxes have no shell for us, so a value
// that lives only in memory dies at the next restart — and reads as applied).
func TestRESTSetSyncSettings(t *testing.T) {
	n, root := testNode(t)
	cfgPath := syncTestConfig(t, root, n.Cfg.MetaDir)
	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	type syncResp struct {
		PullConcurrency int `json:"pull_concurrency"`
	}
	set := func(body string) syncResp {
		t.Helper()
		code, raw := putJSON(t, srv.URL+"/api/settings/sync", body)
		if code != http.StatusOK {
			t.Fatalf("PUT %s = %d %s, want 200", body, code, raw)
		}
		var out syncResp
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("PUT %s body %s: %v", body, raw, err)
		}
		return out
	}
	statusK := func() int {
		t.Helper()
		code, raw := getJSON(t, srv.URL+"/api/status")
		if code != http.StatusOK {
			t.Fatalf("status = %d %s", code, raw)
		}
		var st struct {
			PullConcurrency int `json:"pull_concurrency"`
		}
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatalf("status body: %v", err)
		}
		return st.PullConcurrency
	}

	// The unconfigured default is what /api/status reports before any change.
	if got := statusK(); got != config.DefaultPullConcurrency {
		t.Fatalf("pull_concurrency in status = %d, want the default %d", got, config.DefaultPullConcurrency)
	}

	// Clamped at both ends.
	if got := set(`{"pull_concurrency":0}`).PullConcurrency; got != 1 {
		t.Fatalf("pull_concurrency 0 applied as %d, want the clamp 1", got)
	}
	if got := set(`{"pull_concurrency":99}`).PullConcurrency; got != config.MaxPullConcurrency {
		t.Fatalf("pull_concurrency 99 applied as %d, want the cap %d", got, config.MaxPullConcurrency)
	}

	// An in-range value applies and is visible in /api/status immediately.
	if got := set(`{"pull_concurrency":6}`).PullConcurrency; got != 6 {
		t.Fatalf("pull_concurrency 6 applied as %d", got)
	}
	if got := statusK(); got != 6 {
		t.Fatalf("status pull_concurrency = %d after setting 6", got)
	}

	// An absent field means "leave unchanged" — not "reset to the zero value".
	// 0 would clamp to 1, so a regression here would be loud.
	if got := set(`{}`).PullConcurrency; got != 6 {
		t.Fatalf("an empty body changed pull_concurrency to %d, want it left at 6", got)
	}
	if got := statusK(); got != 6 {
		t.Fatalf("status pull_concurrency = %d after an empty body, want 6", got)
	}

	// Non-numeric / malformed bodies are refused, never silently coerced.
	for _, bad := range []string{
		`{"pull_concurrency":`,                  // truncated JSON
		`{"pull_concurrency":"eight"}`,          // wrong type
		`{"pull_concurrency":{"n":8}}`,          // wrong type
		`{"pull_concurrency":4.5}`,              // not an integer
		`not json at all`,                       // not JSON
		`{"pull_concurrency":null,"extra":{} ,`, // malformed after a valid key
	} {
		code, raw := putJSON(t, srv.URL+"/api/settings/sync", bad)
		if code != http.StatusBadRequest {
			t.Fatalf("PUT %s = %d %s, want 400", bad, code, raw)
		}
	}
	if got := statusK(); got != 6 {
		t.Fatalf("a rejected request changed pull_concurrency to %d, want 6", got)
	}

	// Persistence: set 3, then reload the config FILE — the daemon restart path.
	if got := set(`{"pull_concurrency":3}`).PullConcurrency; got != 3 {
		t.Fatalf("pull_concurrency 3 applied as %d", got)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("reload the persisted config: %v", err)
	}
	if got := cfg.PullFiles(); got != 3 {
		t.Fatalf("reloaded config pull_concurrency = %d, want 3: the setting did not survive a restart", got)
	}

	// Every other key and the comments must still be there.
	after, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# operator comment that must survive a settings write",
		"control_token: secret-token",
		"conflict_policy: conflict-copy",
		"name: nas-01",
	} {
		if !strings.Contains(string(after), want) {
			t.Fatalf("config.yaml lost %q after a settings write:\n%s", want, after)
		}
	}
	if len(cfg.Folders) != 1 || cfg.Folders[0].ID != "data" {
		t.Fatalf("reloaded folders = %+v, want the one configured folder", cfg.Folders)
	}
	// No temp file may be left behind by the atomic write.
	entries, err := os.ReadDir(filepath.Dir(cfgPath))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".crosssync-config-") {
			t.Fatalf("atomic config write left a temp file behind: %s", e.Name())
		}
	}

	// A persistence failure (unwritable appdata, full disk) must answer an error
	// AND leave the running value where it was: a 200 for a value that only
	// lives in memory would silently disappear at the next restart.
	svc.SetConfigPath(filepath.Join(t.TempDir(), "missing-dir", "config.yaml"))
	if code, raw := putJSON(t, srv.URL+"/api/settings/sync", `{"pull_concurrency":7}`); code != http.StatusBadRequest {
		t.Fatalf("PUT against an unwritable config path = %d %s, want 400", code, raw)
	}
	if got := statusK(); got != 3 {
		t.Fatalf("pull_concurrency = %d after a failed persist, want the previous 3 (the change must roll back)", got)
	}
}

// TestRESTSetSyncSettingsHashWorkers covers the second knob on the same
// endpoint: scan.hash_workers. It must be clamped like pull_concurrency (0 and
// negatives = the minimum 1, anything above the CPU count = the CPU count),
// honoured independently of pull_concurrency, visible in /api/status, and
// PERSISTED into the `scan:` section of config.yaml — merging into an existing
// section rather than replacing it.
func TestRESTSetSyncSettingsHashWorkers(t *testing.T) {
	n, root := testNode(t)
	cpu := runtime.NumCPU()
	// Values BELOW the CPU count, chosen from it so the test is honest on a
	// small box: CI runners have 4 vCPUs, so a hardcoded 5 would clamp to 4 and
	// look like a bug in the endpoint rather than in the expectation.
	low := 3
	if low > cpu {
		low = cpu
	}
	two := 2
	if two > cpu {
		two = cpu
	}

	// A config that ALREADY has a scan: section, so the write has to merge.
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	doc := strings.Join([]string{
		"# operator comment that must survive a settings write",
		"tls: false",
		"device:",
		"  id: 1",
		"  name: nas-01",
		"meta_dir: " + filepath.ToSlash(n.Cfg.MetaDir),
		"scan:",
		"  full_interval: 5m",
		"folders:",
		"  - id: data",
		"    path: " + filepath.ToSlash(root),
		"    conflict_policy: conflict-copy",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}

	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	type syncResp struct {
		PullConcurrency int `json:"pull_concurrency"`
		HashWorkers     int `json:"hash_workers"`
	}
	set := func(body string) syncResp {
		t.Helper()
		code, raw := putJSON(t, srv.URL+"/api/settings/sync", body)
		if code != http.StatusOK {
			t.Fatalf("PUT %s = %d %s, want 200", body, code, raw)
		}
		var out syncResp
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("PUT %s body %s: %v", body, raw, err)
		}
		return out
	}
	status := func() syncResp {
		t.Helper()
		code, raw := getJSON(t, srv.URL+"/api/status")
		if code != http.StatusOK {
			t.Fatalf("status = %d %s", code, raw)
		}
		var st syncResp
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatalf("status body: %v", err)
		}
		return st
	}

	// The unset default is one reader per CPU (the behaviour before the knob):
	// a node that never sets the key must keep hashing exactly as it used to.
	if got := status().HashWorkers; got != cpu {
		t.Fatalf("hash_workers in status = %d, want one per CPU (%d)", got, cpu)
	}

	// Clamped at both ends, same rule as pull_concurrency.
	if got := set(`{"hash_workers":0}`).HashWorkers; got != 1 {
		t.Fatalf("hash_workers 0 applied as %d, want the clamp 1", got)
	}
	if got := set(`{"hash_workers":-4}`).HashWorkers; got != 1 {
		t.Fatalf("hash_workers -4 applied as %d, want the clamp 1", got)
	}
	if got := set(`{"hash_workers":99}`).HashWorkers; got != cpu {
		t.Fatalf("hash_workers 99 applied as %d, want the CPU clamp %d", got, cpu)
	}

	// In range, alone: it applies, and it shows up in /api/status.
	if got := set(fmt.Sprintf(`{"hash_workers":%d}`, low)).HashWorkers; got != low {
		t.Fatalf("hash_workers %d applied as %d", low, got)
	}
	if got := status().HashWorkers; got != low {
		t.Fatalf("status hash_workers = %d after setting %d", got, low)
	}

	// The two knobs must not disturb each other: pull_concurrency alone leaves
	// hash_workers where it was, and vice versa.
	if out := set(`{"pull_concurrency":6}`); out.PullConcurrency != 6 || out.HashWorkers != low {
		t.Fatalf("pull_concurrency alone gave %+v, want K=6 and hash_workers left at %d", out, low)
	}
	if out := set(fmt.Sprintf(`{"hash_workers":%d}`, cpu)); out.PullConcurrency != 6 || out.HashWorkers != cpu {
		t.Fatalf("hash_workers alone gave %+v, want hash_workers=%d and K left at 6", out, cpu)
	}
	// Both in one body, which is what an operator tuning a slow node sends.
	if out := set(fmt.Sprintf(`{"pull_concurrency":7,"hash_workers":%d}`, two)); out.PullConcurrency != 7 || out.HashWorkers != two {
		t.Fatalf("both knobs gave %+v, want K=7 hash_workers=%d", out, two)
	}
	if st := status(); st.PullConcurrency != 7 || st.HashWorkers != two {
		t.Fatalf("status = %+v after setting both, want K=7 hash_workers=%d", st, two)
	}
	// An absent field still means "leave unchanged" for the new key too.
	if out := set(`{}`); out.PullConcurrency != 7 || out.HashWorkers != two {
		t.Fatalf("an empty body changed settings to %+v, want 7/%d", out, two)
	}

	// Round-trip through config.Load: the daemon restart path. The value must be
	// under `scan:`, and the other keys in that section must survive.
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("reload the persisted config: %v", err)
	}
	if got := cfg.Scan.HashWorkers; got != two {
		t.Fatalf("reloaded scan.hash_workers = %d, want %d: the setting did not survive a restart", got, two)
	}
	if got := cfg.Scan.HashWorkersOrDefault(); got != two {
		t.Fatalf("reloaded effective hash workers = %d, want %d", got, two)
	}
	if got := cfg.Scan.FullIntervalOrDefault(); got != 5*time.Minute {
		t.Fatalf("reloaded scan.full_interval = %v, want 5m: the settings write replaced the scan section", got)
	}
	if got := cfg.PullFiles(); got != 7 {
		t.Fatalf("reloaded pull_concurrency = %d, want 7", got)
	}
	after, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# operator comment that must survive a settings write",
		"full_interval: 5m",
		"conflict_policy: conflict-copy",
		fmt.Sprintf("hash_workers: %d", two),
	} {
		if !strings.Contains(string(after), want) {
			t.Fatalf("config.yaml lost %q after a settings write:\n%s", want, after)
		}
	}

	// A persistence failure must roll BOTH values back: memory and disk agree.
	svc.SetConfigPath(filepath.Join(t.TempDir(), "missing-dir", "config.yaml"))
	if code, raw := putJSON(t, srv.URL+"/api/settings/sync", `{"pull_concurrency":8,"hash_workers":4}`); code != http.StatusBadRequest {
		t.Fatalf("PUT against an unwritable config path = %d %s, want 400", code, raw)
	}
	if st := status(); st.HashWorkers != two || st.PullConcurrency != 7 {
		t.Fatalf("after a failed persist status = %+v, want the previous K=7 hash_workers=%d (the change must roll back)", st, two)
	}
}

// TestRESTSetSyncSettingsFolderSessions covers the third knob on the same
// endpoint: node.max_folder_sessions_per_peer, the cap on how many folders of ONE
// peer may sync at the same time. It must clamp like the others (0 and negatives
// = the minimum 1, anything above 8 = 8), apply independently, show up in
// /api/status, and be PERSISTED into the `node:` section of config.yaml so a
// restart keeps it.
func TestRESTSetSyncSettingsFolderSessions(t *testing.T) {
	n, root := testNode(t)
	// A config that ALREADY has a node: section, so the write has to merge into
	// it and not replace it.
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	doc := strings.Join([]string{
		"# operator comment that must survive a settings write",
		"tls: false",
		"device:",
		"  id: 1",
		"  name: nas-01",
		"meta_dir: " + filepath.ToSlash(n.Cfg.MetaDir),
		"node:",
		"  max_folder_sessions_per_peer: 2",
		"folders:",
		"  - id: data",
		"    path: " + filepath.ToSlash(root),
		"    conflict_policy: conflict-copy",
		"",
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}

	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	type syncResp struct {
		PullConcurrency int `json:"pull_concurrency"`
		HashWorkers     int `json:"hash_workers"`
		FolderSessions  int `json:"max_folder_sessions_per_peer"`
	}
	set := func(body string) syncResp {
		t.Helper()
		code, raw := putJSON(t, srv.URL+"/api/settings/sync", body)
		if code != http.StatusOK {
			t.Fatalf("PUT %s = %d %s, want 200", body, code, raw)
		}
		var out syncResp
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("PUT %s body %s: %v", body, raw, err)
		}
		return out
	}
	status := func() syncResp {
		t.Helper()
		code, raw := getJSON(t, srv.URL+"/api/status")
		if code != http.StatusOK {
			t.Fatalf("status = %d %s", code, raw)
		}
		var st syncResp
		if err := json.Unmarshal(raw, &st); err != nil {
			t.Fatalf("status body: %v", err)
		}
		return st
	}

	// The unset default is the documented 4: an existing config that never sets
	// the key keeps syncing the way it always did.
	if got := status().FolderSessions; got != config.DefaultFolderSessionsPerPeer {
		t.Fatalf("max_folder_sessions_per_peer in status = %d, want the default %d", got, config.DefaultFolderSessionsPerPeer)
	}

	// Clamped at both ends, exactly like pull_concurrency.
	if got := set(`{"max_folder_sessions_per_peer":0}`).FolderSessions; got != 1 {
		t.Fatalf("max_folder_sessions_per_peer 0 applied as %d, want the clamp 1", got)
	}
	if got := set(`{"max_folder_sessions_per_peer":-3}`).FolderSessions; got != 1 {
		t.Fatalf("max_folder_sessions_per_peer -3 applied as %d, want the clamp 1", got)
	}
	if got := set(`{"max_folder_sessions_per_peer":99}`).FolderSessions; got != config.MaxFolderSessionsPerPeer {
		t.Fatalf("max_folder_sessions_per_peer 99 applied as %d, want the cap %d", got, config.MaxFolderSessionsPerPeer)
	}

	// In range: it applies and /api/status reports it immediately.
	if got := set(`{"max_folder_sessions_per_peer":6}`).FolderSessions; got != 6 {
		t.Fatalf("max_folder_sessions_per_peer 6 applied as %d", got)
	}
	if got := status().FolderSessions; got != 6 {
		t.Fatalf("status max_folder_sessions_per_peer = %d after setting 6", got)
	}

	// The three knobs are independent, and an absent field still means "leave
	// unchanged" (0 would clamp to 1, so a regression is loud).
	if out := set(`{"pull_concurrency":6}`); out.FolderSessions != 6 || out.PullConcurrency != 6 {
		t.Fatalf("pull_concurrency alone gave %+v, want folder sessions left at 6", out)
	}
	if out := set(`{}`); out.FolderSessions != 6 {
		t.Fatalf("an empty body changed max_folder_sessions_per_peer to %d, want it left at 6", out.FolderSessions)
	}
	// A malformed value is refused, never coerced.
	if code, raw := putJSON(t, srv.URL+"/api/settings/sync", `{"max_folder_sessions_per_peer":"four"}`); code != http.StatusBadRequest {
		t.Fatalf("a non-numeric max_folder_sessions_per_peer = %d %s, want 400", code, raw)
	}
	if got := status().FolderSessions; got != 6 {
		t.Fatalf("a rejected request changed max_folder_sessions_per_peer to %d, want 6", got)
	}

	// Round-trip through config.Load: the daemon restart path.
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("reload the persisted config: %v", err)
	}
	if got := cfg.Node.MaxFolderSessionsPerPeer; got != 6 {
		t.Fatalf("reloaded node.max_folder_sessions_per_peer = %d, want 6: the setting did not survive a restart", got)
	}
	if got := cfg.FolderSessionsPerPeer(); got != 6 {
		t.Fatalf("reloaded effective folder sessions per peer = %d, want 6", got)
	}
	after, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# operator comment that must survive a settings write",
		"conflict_policy: conflict-copy",
		"max_folder_sessions_per_peer: 6",
	} {
		if !strings.Contains(string(after), want) {
			t.Fatalf("config.yaml lost %q after a settings write:\n%s", want, after)
		}
	}

	// A persistence failure rolls the running value back: memory and disk agree.
	svc.SetConfigPath(filepath.Join(t.TempDir(), "missing-dir", "config.yaml"))
	if code, raw := putJSON(t, srv.URL+"/api/settings/sync", `{"max_folder_sessions_per_peer":8}`); code != http.StatusBadRequest {
		t.Fatalf("PUT against an unwritable config path = %d %s, want 400", code, raw)
	}
	if got := status().FolderSessions; got != 6 {
		t.Fatalf("max_folder_sessions_per_peer = %d after a failed persist, want the previous 6 (the change must roll back)", got)
	}
}
