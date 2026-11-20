package control_test

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crosssync/internal/control"
)

// pull_fsync through the API.
//
// The knob is a durability setting with default ON, and the one thing it must
// never do is turn off because a client sent a body without the field. These
// tests drive the real endpoint: omit it (unchanged), set false (persisted as
// `pull_fsync: false`, reported back as false), set true again (key removed,
// reported as true) — and the live folder follows each change, because the pull
// path reads it at commit time.

func pullFsyncTestService(t *testing.T) (*control.Service, string) {
	t.Helper()
	n, root := testNode(t)
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	_ = os.WriteFile(cfgPath, []byte(
		"# keep this comment\n"+
			"device:\n  id: 1\n  name: nas-01\n"+
			"meta_dir: /tmp/m\n"+
			"folders:\n"+
			"  - id: data\n"+
			"    path: "+root+"\n"+
			"    conflict: conflict-copy\n"), 0o600)
	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)
	return svc, cfgPath
}

func folderPullFsync(t *testing.T, body []byte) bool {
	t.Helper()
	var fs struct {
		PullFsync *bool `json:"pull_fsync"`
	}
	if err := json.Unmarshal(body, &fs); err != nil {
		t.Fatalf("decode folder status: %v", err)
	}
	if fs.PullFsync == nil {
		t.Fatalf("pull_fsync missing from the folder status: %s", body)
	}
	return *fs.PullFsync
}

// The default is reported as true and can be changed through PUT, both ways,
// with the config file following the intent.
func TestFolderPullFsyncEndpoint(t *testing.T) {
	svc, cfgPath := pullFsyncTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	// Nothing set: effective value is ON.
	code, body := getJSON(t, srv.URL+"/api/folders")
	if code != 200 || !strings.Contains(string(body), `"pull_fsync":true`) {
		t.Fatalf("default pull_fsync not reported as true (%d): %s", code, body)
	}

	// Off, through the endpoint the UI uses.
	code, body = putJSON(t, srv.URL+"/api/folders/data", `{"name":"data","path":"`+filepath.ToSlash(svc.Folders()[0].Path)+`","pull_fsync":false}`)
	if code != 200 {
		t.Fatalf("PUT pull_fsync=false: %d %s", code, body)
	}
	if folderPullFsync(t, body) {
		t.Fatalf("pull_fsync still true after setting it false: %s", body)
	}
	data, _ := os.ReadFile(cfgPath)
	if !strings.Contains(string(data), "pull_fsync: false") {
		t.Fatalf("pull_fsync: false was not persisted:\n%s", data)
	}
	if !strings.Contains(string(data), "keep this comment") {
		t.Fatal("persisting the knob lost the config's comments")
	}

	// A later edit that does NOT mention the field must leave it alone: an
	// older client cannot disable durability by omission.
	code, body = putJSON(t, srv.URL+"/api/folders/data", `{"name":"Renamed","path":"`+filepath.ToSlash(svc.Folders()[0].Path)+`"}`)
	if code != 200 {
		t.Fatalf("PUT without pull_fsync: %d %s", code, body)
	}
	if folderPullFsync(t, body) {
		t.Fatalf("an edit that omitted pull_fsync turned it back on: %s", body)
	}

	// Back on: reported true AND the key removed from the config, so only a
	// deliberate `false` ever appears in the file.
	code, body = putJSON(t, srv.URL+"/api/folders/data", `{"name":"Renamed","path":"`+filepath.ToSlash(svc.Folders()[0].Path)+`","pull_fsync":true}`)
	if code != 200 {
		t.Fatalf("PUT pull_fsync=true: %d %s", code, body)
	}
	if !folderPullFsync(t, body) {
		t.Fatalf("pull_fsync still false after setting it true: %s", body)
	}
	data, _ = os.ReadFile(cfgPath)
	if strings.Contains(string(data), "pull_fsync") {
		t.Fatalf("the default must not be written back into the config:\n%s", data)
	}
}

// The live folder (which the pull path reads at every commit) follows the
// endpoint, not just the config file — and an edit that resets the folder's
// scanner/engine wiring does not silently revert it.
func TestFolderPullFsyncReachesTheLiveFolder(t *testing.T) {
	svc, _ := pullFsyncTestService(t)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	path := filepath.ToSlash(svc.Folders()[0].Path)
	if code, body := putJSON(t, srv.URL+"/api/folders/data", `{"path":"`+path+`","pull_fsync":false}`); code != 200 {
		t.Fatalf("PUT: %d %s", code, body)
	}
	if svc.Folders()[0].PullFsync {
		t.Fatal("the folder status still reports fsync on")
	}
	// Now edit the folder again (new scanner/engine wiring) without touching the
	// knob: it must stay off.
	if code, body := putJSON(t, srv.URL+"/api/folders/data", `{"name":"data","path":"`+path+`"}`); code != 200 {
		t.Fatalf("PUT: %d %s", code, body)
	}
	fs := svc.Folders()[0]
	if fs.PullFsync {
		t.Fatal("a folder edit re-enabled fsync: the knob must survive it")
	}
}
