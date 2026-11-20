package control_test

import (
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crosssync/internal/control"
	"crosssync/internal/node"
)

// Adding a folder must make the daemon LOOK at it.
//
// AddFolder registered and persisted the folder and returned: nothing observed
// it until the next scheduled safety walk (up to scan.full_max, fifteen
// minutes), so a folder added at runtime sat there with an empty index while the
// UI reported it as settled — the "it says in sync but it is not" failure the
// resume path already fixes with RequestRescan. The POST now queues that pass
// asynchronously, exactly like a resume.
//
// These tests pin both halves: the pass happens, and the endpoint does not wait
// for it.

// addFolderTree writes n files into a fresh directory, so the pass that follows
// has something real to walk (an empty directory would finish before the
// assertions could tell a queued pass from an inline one).
func addFolderTree(t *testing.T, n int) string {
	t.Helper()
	root := t.TempDir()
	mt := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		dir := filepath.Join(root, fmt.Sprintf("d%02d", i%20))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, fmt.Sprintf("f%05d.bin", i))
		if err := os.WriteFile(p, []byte(strings.Repeat("x", 2048)), 0o644); err != nil {
			t.Fatal(err)
		}
		_ = os.Chtimes(p, mt, mt)
	}
	return root
}

// postAddFolder POSTs one folder and reports how long the request took.
func postAddFolder(t *testing.T, url, id, path string) (int, []byte, time.Duration) {
	t.Helper()
	start := time.Now()
	code, body := postJSON(t, url+"/api/folders",
		fmt.Sprintf(`{"id":%q,"name":%q,"path":%q}`, id, id, filepath.ToSlash(path)))
	return code, body, time.Since(start)
}

// waitScanFinished polls until a scan of id has completed, or fails.
func waitScanFinished(t *testing.T, n *node.Node, id string, wait time.Duration) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		if _, ok := n.FolderScanAge(id); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no scan ran for folder %q: it would wait for the next safety walk (up to 15 minutes)", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestAddFolderQueuesAScan: after POST /api/folders a pass runs for the new
// folder without anything else asking for one.
func TestAddFolderQueuesAScan(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	root := addFolderTree(t, 40)
	if code, body, _ := postAddFolder(t, srv.URL, "added", root); code != 201 {
		t.Fatalf("POST /api/folders = %d %s", code, body)
	}
	// Before this change nothing ever scanned the folder in this test, so this
	// wait is the assertion: the pass happens without a manual rescan.
	waitScanFinished(t, n, "added", 15*time.Second)
}

// TestAddFolderEndpointDoesNotWaitForTheScan: the endpoint answers the UI button
// immediately, however long the walk of a real share takes.
//
// Two claims, and the FIRST is the load-bearing one because it does not depend on
// how fast this machine is: when the response exists the folder has no completed
// pass, so the walk had not happened yet — an inline pass could only produce that
// response after walking the whole tree. (The stability gate makes a first pass
// HOLD new files instead of hashing them, so the walk's cost here is the 4000
// stats, not hashing: ~70 ms on a fast box, far more on CI.) The second is a
// loose net on the request's own cost, to catch a future version that blocks.
func TestAddFolderEndpointDoesNotWaitForTheScan(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	root := addFolderTree(t, 4000)
	code, body, post := postAddFolder(t, srv.URL, "big", root)
	if code != 201 {
		t.Fatalf("POST /api/folders = %d %s", code, body)
	}
	if _, ok := n.FolderScanAge("big"); ok {
		t.Fatal("the added folder already has a completed scan when POST returned: the pass ran inside the request")
	}
	if post > 200*time.Millisecond {
		t.Fatalf("POST /api/folders took %s with a 4000-file tree: the pass must be queued asynchronously, not run inside the request", post)
	}
	// The queued pass still has to complete.
	start := time.Now()
	waitScanFinished(t, n, "big", 30*time.Second)
	t.Logf("POST answered in %s; the 4000-entry pass finished %s later, in the background", post, time.Since(start))
}
