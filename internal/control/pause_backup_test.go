package control_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crosssync/internal/control"
)

// writeTestConfig writes a minimal config.yaml for the persistence tests and
// returns its path.
func writeTestConfig(t *testing.T, root string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	body := "device:\n    id: 1\n    name: nas-01\nfolders:\n    - id: data\n      path: " + root + "\n"
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestPauseFolderEndpoint covers the share's pause switch over REST: it flips
// the live folder, reports it in /api/status, and PERSISTS it, so a restart
// keeps the folder paused instead of silently resuming transfers.
func TestPauseFolderEndpoint(t *testing.T) {
	n, root := testNode(t)
	cfgPath := writeTestConfig(t, root)
	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	post := func(body string) (int, []byte) {
		return postJSON(t, srv.URL+"/api/folders/data/pause", body)
	}

	// Pause.
	code, raw := post(`{"paused":true}`)
	if code != 200 {
		t.Fatalf("pause: %d %s", code, raw)
	}
	var fs struct {
		Paused bool `json:"paused"`
	}
	if err := json.Unmarshal(raw, &fs); err != nil {
		t.Fatal(err)
	}
	if !fs.Paused {
		t.Fatalf("pause response reports paused=false: %s", raw)
	}
	// Persisted to the config file...
	saved, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(saved), "paused: true") {
		t.Fatalf("config.yaml does not record the pause:\n%s", saved)
	}
	// ...and visible in the status payload.
	code, raw = getJSON(t, srv.URL+"/api/status")
	if code != 200 {
		t.Fatalf("status: %d", code)
	}
	var st struct {
		Folders []struct {
			Paused bool `json:"paused"`
		} `json:"folders"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Folders) != 1 || !st.Folders[0].Paused {
		t.Fatalf("status does not report the paused folder: %s", raw)
	}

	// An explicit rescan on a paused folder is refused with a clear reason.
	code, raw = postJSON(t, srv.URL+"/api/folders/data/rescan", `{}`)
	if code == 200 || !strings.Contains(strings.ToLower(string(raw)), "paused") {
		t.Fatalf("rescan of a paused folder = %d %s, want a non-200 mentioning paused", code, raw)
	}

	// Resume clears both the live state and the persisted key.
	code, raw = post(`{"paused":false}`)
	if code != 200 {
		t.Fatalf("resume: %d %s", code, raw)
	}
	saved, err = os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(saved), "paused: true") {
		t.Fatalf("config.yaml still records the pause after resume:\n%s", saved)
	}

	// A missing "paused" field is a 400, not a silent resume.
	if code, _ := post(`{}`); code != 400 {
		t.Fatalf("empty pause body = %d, want 400", code)
	}
}

// TestBackupArchive covers GET /api/backup: the archive must carry the config
// (the thing whose loss costs a full re-pair plus a full first-time exchange)
// and a README that says what is inside and how to restore it.
func TestBackupArchive(t *testing.T) {
	n, root := testNode(t)
	cfgPath := writeTestConfig(t, root)
	svc := control.New(n, "9.9.9")
	svc.SetConfigPath(cfgPath)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/backup")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("backup: %d %s", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/gzip" {
		t.Fatalf("content-type = %q, want application/gzip", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, "crosssync-backup-") {
		t.Fatalf("content-disposition = %q, want a download filename", cd)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("not gzip: %v", err)
	}
	tr := tar.NewReader(gz)
	files := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar: %v", err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read %s: %v", hdr.Name, err)
		}
		files[hdr.Name] = b
	}

	cfg, ok := files["config.yaml"]
	if !ok || !bytes.Contains(cfg, []byte("nas-01")) {
		t.Fatalf("archive is missing the config: %v", keysOf(files))
	}
	manifest, ok := files["MANIFEST.txt"]
	if !ok {
		t.Fatalf("archive is missing MANIFEST.txt: %v", keysOf(files))
	}
	for _, want := range []string{"Restore", "config.yaml", "9.9.9"} {
		if !bytes.Contains(manifest, []byte(want)) {
			t.Fatalf("manifest does not mention %q:\n%s", want, manifest)
		}
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
