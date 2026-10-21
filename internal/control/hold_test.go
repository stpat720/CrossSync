package control_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/control"
	"crosssync/internal/hold"
)

func postJSON(t *testing.T, url, body string) (int, []byte) {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

// TestFolderStatusHoldSectionAndActions covers the soft-deletion hold section of
// the folder status plus the restore / delete-now endpoints: an over-threshold
// deletion leaves copies in the hold area, the API reports their paths, hold
// times and expiry, and the operator can restore one or purge one.
func TestFolderStatusHoldSectionAndActions(t *testing.T) {
	n, root := testNode(t)
	svc := control.New(n, "test")

	// Two copies parked by a soft deletion (the engine puts them there; the
	// hold package is the same code path).
	for _, rel := range []string{"a.txt", "d/b.txt"} {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("payload of "+rel), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	h := hold.New(root)
	for _, rel := range []string{"a.txt", "d/b.txt"} {
		if err := h.Move(rel); err != nil {
			t.Fatal(err)
		}
	}

	// Folder status carries the hold section.
	folders := svc.Folders()
	if len(folders) != 1 {
		t.Fatalf("folders = %d, want 1", len(folders))
	}
	fh := folders[0].Hold
	if fh == nil {
		t.Fatal("folder status is missing the hold section")
	}
	if fh.Count != 2 || len(fh.Entries) != 2 {
		t.Fatalf("hold = %+v, want 2 entries", fh)
	}
	if fh.Bytes <= 0 {
		t.Fatalf("hold bytes = %d, want > 0", fh.Bytes)
	}
	gotPaths := map[string]bool{}
	for _, e := range fh.Entries {
		if e.Path == "" || e.HeldAt == 0 || e.ExpiresAt <= e.HeldAt {
			t.Fatalf("incomplete hold entry: %+v", e)
		}
		if e.ExpiresAt-e.HeldAt != int64(hold.MaxAge.Seconds()) {
			t.Fatalf("hold entry %s expiry window = %ds, want %v", e.Path, e.ExpiresAt-e.HeldAt, hold.MaxAge)
		}
		gotPaths[e.Path] = true
	}
	if !gotPaths["a.txt"] || !gotPaths["d/b.txt"] {
		t.Fatalf("hold entries = %+v, want the original relative paths", gotPaths)
	}

	// The status endpoint exposes the same section.
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	code, body := getJSON(t, srv.URL+"/api/status")
	if code != 200 {
		t.Fatalf("status code %d: %s", code, body)
	}
	var st struct {
		Folders []struct {
			Hold *struct {
				Count   int `json:"count"`
				Entries []struct {
					Path string `json:"path"`
				} `json:"entries"`
			} `json:"hold"`
		} `json:"folders"`
	}
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Folders) != 1 || st.Folders[0].Hold == nil || st.Folders[0].Hold.Count != 2 {
		t.Fatalf("unexpected hold section over REST: %+v", st.Folders)
	}

	// Restore one held path through the API.
	code, body = postJSON(t, srv.URL+"/api/folders/data/hold/restore", `{"path":"a.txt"}`)
	if code != 200 {
		t.Fatalf("restore failed (%d): %s", code, body)
	}
	if _, err := os.Stat(filepath.Join(root, "a.txt")); err != nil {
		t.Fatalf("restored file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".sfx-hold", "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("restored file is still held (err=%v)", err)
	}

	// Delete the other one now.
	code, body = postJSON(t, srv.URL+"/api/folders/data/hold/delete", `{"path":"d/b.txt"}`)
	if code != 200 {
		t.Fatalf("delete-now failed (%d): %s", code, body)
	}
	if _, err := os.Stat(filepath.Join(root, ".sfx-hold", "d", "b.txt")); !os.IsNotExist(err) {
		t.Fatalf("held file survived delete-now (err=%v)", err)
	}
	if _, err := svc.Hold("data"); err != nil {
		t.Fatalf("Hold: %v", err)
	}
	if h, err := svc.Hold("data"); err != nil || h != nil {
		t.Fatalf("hold section after the actions = %+v (%v), want nil", h, err)
	}
}
