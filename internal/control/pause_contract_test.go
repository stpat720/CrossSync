package control_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"net/http/httptest"

	"crosssync/internal/control"
	"crosssync/internal/index"
)

func ctrlWrite(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	_ = os.Chtimes(p, mt, mt)
}

func ctrlBlocks(fi *index.FileInfo) string {
	var b strings.Builder
	for _, h := range fi.Blocks {
		b.Write(h)
	}
	return b.String()
}

// TestPauseEndpointIsMinimalAndFast pins the response contract of the pause
// switch. Pausing exists to stop disk I/O immediately, so the endpoint must not
// answer with a full FolderStatus: that path runs per-folder SQL aggregates, a
// statfs on the share and directory walks, and on the live pair it took ~3.3s
// while the disk kept being read.
func TestPauseEndpointIsMinimalAndFast(t *testing.T) {
	n, root := testNode(t)
	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	for i := 0; i < 20; i++ {
		ctrlWrite(t, root, filepath.Join("src", "f.bin"), strings.Repeat("z", 1024))
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	pause := func(body string) (map[string]any, time.Duration) {
		t.Helper()
		start := time.Now()
		code, raw := postJSON(t, srv.URL+"/api/folders/data/pause", body)
		elapsed := time.Since(start)
		if code != 200 {
			t.Fatalf("pause %s = %d %s", body, code, raw)
		}
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("pause body %s: %v", raw, err)
		}
		return out, elapsed
	}

	body, elapsed := pause(`{"paused":true}`)
	if body["paused"] != true {
		t.Fatalf("pause response reports paused=%v, want true", body["paused"])
	}
	// The heavy FolderStatus fields must not be computed for this request.
	for _, heavy := range []string{"files", "size", "free_bytes", "version_bytes", "open_events", "hold", "pending_files"} {
		if _, ok := body[heavy]; ok {
			t.Fatalf("pause response carries the full folder status field %q — that path runs SQL aggregates on the request that must stop I/O (%s)", heavy, body)
		}
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("pause took %s, want < 500ms", elapsed)
	}

	body, elapsed = pause(`{"paused":false}`)
	if body["paused"] != false {
		t.Fatalf("resume response reports paused=%v, want false", body["paused"])
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("resume took %s, want < 500ms", elapsed)
	}

	// The transfer-only level answers the same minimal shape and does not
	// report the folder as fully paused.
	body, elapsed = pause(`{"transfers":true}`)
	if body["transfer_paused"] != true || body["paused"] != false {
		t.Fatalf("transfer pause response = %s, want transfer_paused=true paused=false", body)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("transfer pause took %s, want < 500ms", elapsed)
	}
}

// TestRescanWhilePausedQueuesAndRunsOnResume: the verified rescan is the
// long-running request an operator is most likely to fire and then pause. It
// must be remembered (202 queued) and run exactly once after the resume, never
// silently consumed while the folder is paused.
func TestRescanWhilePausedQueuesAndRunsOnResume(t *testing.T) {
	n, root := testNode(t)
	f := n.Folders["data"]
	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	ctrlWrite(t, root, "src/x.bin", strings.Repeat("A", 4096))
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	before, ok, err := f.Ix.Get("src/x.bin")
	if err != nil || !ok {
		t.Fatalf("setup index lookup: ok=%v err=%v", ok, err)
	}
	// Same size, same mtime: only a verified pass can notice.
	ctrlWrite(t, root, "src/x.bin", strings.Repeat("B", 4096))

	if code, raw := postJSON(t, srv.URL+"/api/folders/data/pause", `{"paused":true}`); code != 200 {
		t.Fatalf("pause: %d %s", code, raw)
	}
	code, raw := postJSON(t, srv.URL+"/api/folders/data/rescan", `{"verify":true}`)
	if code != 202 {
		t.Fatalf("verified rescan of a paused folder = %d %s, want 202 queued", code, raw)
	}
	var queued struct {
		Queued bool   `json:"queued"`
		Folder string `json:"folder"`
		Verify bool   `json:"verify"`
	}
	if err := json.Unmarshal(raw, &queued); err != nil {
		t.Fatalf("queued body %s: %v", raw, err)
	}
	if !queued.Queued || queued.Folder != "data" || !queued.Verify {
		t.Fatalf("verified rescan of a paused folder = %s, want queued=true folder=data verify=true", raw)
	}

	// Nothing may read the disk while paused.
	time.Sleep(200 * time.Millisecond)
	if fi, ok, err := f.Ix.Get("src/x.bin"); err != nil || !ok {
		t.Fatalf("index lookup while paused: ok=%v err=%v", ok, err)
	} else if ctrlBlocks(fi) != ctrlBlocks(before) {
		t.Fatal("the queued rescan ran while the folder was paused")
	}

	if code, raw := postJSON(t, srv.URL+"/api/folders/data/pause", `{"paused":false}`); code != 200 {
		t.Fatalf("resume: %d %s", code, raw)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		fi, ok, err := f.Ix.Get("src/x.bin")
		if err == nil && ok && ctrlBlocks(fi) != ctrlBlocks(before) {
			return // the queued verified pass ran after the resume
		}
		if time.Now().After(deadline) {
			t.Fatal("the verified rescan queued while paused never ran after the resume")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
