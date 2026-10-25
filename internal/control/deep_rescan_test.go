package control_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"crosssync/internal/control"
)

// TestVerifiedRescanIsAsynchronous pins the contract for the deep scan: it
// answers 202 {"started":true} immediately instead of holding one HTTP request
// open for minutes or hours (a verified scan reads every byte of the share, and
// browsers/proxies time out long before a big share finishes). Progress is
// reported through /api/status scans[].
func TestVerifiedRescanIsAsynchronous(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	code, raw := postJSON(t, srv.URL+"/api/folders/data/rescan", `{"verify":true}`)
	if code != 202 {
		t.Fatalf("verified rescan answered %d (%s), want 202 Accepted", code, raw)
	}
	var body struct {
		Started bool   `json:"started"`
		Folder  string `json:"folder"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if !body.Started || body.Folder != "data" {
		t.Fatalf("verified rescan body = %s, want started=true for folder data", raw)
	}

	// The scan is now in flight (or already finished on a tiny folder): either
	// way /api/status must show a scan record for the folder, not an error.
	code, raw = getJSON(t, srv.URL+"/api/status")
	if code != 200 {
		t.Fatalf("status: %d", code)
	}
	var st struct {
		Scans []struct {
			Folder string `json:"folder"`
		} `json:"scans"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range st.Scans {
		if s.Folder == "data" {
			found = true
		}
	}
	if !found {
		t.Fatalf("status has no scan record after a verified rescan: %s", raw)
	}

	// Validation still applies on the async path: a paused folder is QUEUED (the
	// request is remembered and runs once on resume — it is never silently
	// eaten), and an unknown folder is a 400 rather than a silent goroutine.
	if _, err := svc.SetFolderPaused("data", true); err != nil {
		t.Fatal(err)
	}
	qcode, qbody := postJSON(t, srv.URL+"/api/folders/data/rescan", `{"verify":true}`)
	if qcode != 202 {
		t.Fatalf("verified rescan of a paused folder = %d %s, want 202 queued", qcode, qbody)
	}
	var q struct {
		Queued bool `json:"queued"`
	}
	if err := json.Unmarshal(qbody, &q); err != nil || !q.Queued {
		t.Fatalf("verified rescan of a paused folder = %s, want queued=true", qbody)
	}
	if code, body := postJSON(t, srv.URL+"/api/folders/nope/rescan", `{"verify":true}`); code != 400 {
		t.Fatalf("verified rescan of an unknown folder = %d %s, want 400", code, body)
	}
}
