package control_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
