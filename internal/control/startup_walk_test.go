package control_test

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/control"
	"crosssync/internal/node"
)

// startupWalkNode builds a node with one folder and one configured peer, and a
// scanner hash hook that blocks inside the folder's first pass until the test
// releases it. That is the deterministic stand-in for a 57.8 GB share: the walk
// is definitely in flight, mid-hash, while the assertions run.
type startupWalkNode struct {
	n       *node.Node
	root    string
	entered chan struct{}
	done    chan struct{}
	release chan struct{}

	releaseOnce sync.Once
	// hashed counts files hashed by the folder's scanner: one pass over the two
	// seed files is exactly 2, so a duplicate walk shows up as a bigger number.
	hashed atomic.Int64
}

func newStartupWalkNode(t *testing.T, withPeer bool) *startupWalkNode {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("payload "+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{{ID: "data", Path: root, ConflictPolicy: "conflict-copy", SettleScans: 1}},
	}
	if withPeer {
		cfg.Peers = []config.Peer{{ID: 42, Name: "nas-02", Addresses: []string{"127.0.0.1:1"}}}
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })

	sw := &startupWalkNode{
		n:       n,
		root:    root,
		entered: make(chan struct{}),
		done:    make(chan struct{}),
		release: make(chan struct{}),
	}
	var once sync.Once
	sc := n.Folders["data"].Scanner
	sc.SetHashHook(func(string) {
		once.Do(func() { close(sw.entered) })
		<-sw.release
		sw.hashed.Add(1)
	})
	// Registered after node.New's Close cleanup, so it runs BEFORE it: release
	// the blocked pass, wait for the walk to return, then restore the hook (a
	// scan must never outlive the index Close closes).
	t.Cleanup(func() {
		sw.finish(t)
		sc.SetHashHook(nil)
	})
	return sw
}

// startWalk arms the per-folder first-pass marks and runs the startup walk the
// way cmdServe does, returning once the walk is provably inside the hashing
// phase.
func (sw *startupWalkNode) startWalk(t *testing.T) {
	t.Helper()
	sw.n.SetStartupScanning(true)
	go func() {
		defer close(sw.done)
		_ = sw.n.ScanAllFirstPass()
	}()
	select {
	case <-sw.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the startup walk never reached the hashing phase")
	}
}

// finish releases the blocked hash and waits for the walk to return. Idempotent:
// the test settles the walk itself, and the cleanup does it again if the test
// failed early.
func (sw *startupWalkNode) finish(t *testing.T) {
	t.Helper()
	sw.releaseOnce.Do(func() { close(sw.release) })
	select {
	case <-sw.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the startup walk did not finish after its hash hook was released")
	}
}

// TestHealthzAndStatusDuringStartupWalk pins the observability contract of the
// first index pass: the control API answers while the walk runs, /healthz is 200
// ("indexing" — a container must not be restarted mid-hash), per-folder scans[]
// carry live progress from the very first scan of a start, and /api/status says
// startup_scanning. When the walk ends both settle to their normal values.
func TestHealthzAndStatusDuringStartupWalk(t *testing.T) {
	sw := newStartupWalkNode(t, true)
	svc := control.New(sw.n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	sw.startWalk(t)

	// /healthz: 200 while indexing, even though no sync round can have finished.
	code, raw := getJSON(t, srv.URL+"/healthz")
	if code != 200 {
		t.Fatalf("/healthz during the startup walk = %d %s, want 200 (a restarted container must not read as unhealthy)", code, raw)
	}
	var hb healthBody
	if err := json.Unmarshal(raw, &hb); err != nil {
		t.Fatalf("/healthz is not JSON: %s", raw)
	}
	if hb.Status != "indexing" || hb.Reason == "" {
		t.Fatalf("/healthz body = %+v, want status \"indexing\" with a reason", hb)
	}

	// /api/status: the flag is set and the running folder reports progress.
	code, raw = getJSON(t, srv.URL+"/api/status")
	if code != 200 {
		t.Fatalf("/api/status during the startup walk = %d %s, want 200", code, raw)
	}
	var st struct {
		StartupScanning bool              `json:"startup_scanning"`
		Scans           []node.ScanStatus `json:"scans"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("/api/status is not JSON: %s", raw)
	}
	if !st.StartupScanning {
		t.Fatalf("/api/status startup_scanning = false while the first pass is hashing: %s", raw)
	}
	if len(st.Scans) != 1 || !st.Scans[0].Running {
		t.Fatalf("/api/status scans = %+v, want one RUNNING entry for the folder being indexed", st.Scans)
	}
	if st.Scans[0].Started == 0 || st.Scans[0].Phase == "" {
		t.Fatalf("scan entry = %+v, want a start time and a phase from the first scan of a start", st.Scans[0])
	}

	// Settle: the walk finishes, the folder is indexed, the flag clears and the
	// probe falls back to its normal judgement (a configured peer that has not
	// completed a round yet is degraded — that is the real staleness signal, and
	// it proves the 200 above came from the walk, not from a weaker probe).
	sw.finish(t)
	code, raw = getJSON(t, srv.URL+"/api/status")
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatalf("/api/status after the walk is not JSON: %s", raw)
	}
	if code != 200 || st.StartupScanning {
		t.Fatalf("/api/status after the walk = %d startup_scanning=%v, want 200 false", code, st.StartupScanning)
	}
	if len(st.Scans) != 1 || st.Scans[0].Running {
		t.Fatalf("scans after the walk = %+v, want the folder's pass finished", st.Scans)
	}
	code, raw = getJSON(t, srv.URL+"/healthz")
	if code != 503 {
		t.Fatalf("/healthz after the walk = %d %s, want 503 (peer configured, no round completed yet)", code, raw)
	}
}

// TestRescanDuringStartupWalkStartsNoSecondScan pins requirement 4: a rescan for
// a folder that is being indexed must not start a second concurrent pass, and it
// must not answer with an error that reads as a failure. The folder is asked for
// a plain and a verified rescan, plus the all-folders call; exactly one pass may
// have run, and the caller is told "already running".
func TestRescanDuringStartupWalkStartsNoSecondScan(t *testing.T) {
	sw := newStartupWalkNode(t, false)
	svc := control.New(sw.n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	sw.startWalk(t)
	before := startStamp(svc, "data")

	for _, body := range []string{`{}`, `{"verify":true}`} {
		code, raw := postJSON(t, srv.URL+"/api/folders/data/rescan", body)
		if code != 202 {
			t.Fatalf("rescan %s during the startup walk = %d %s, want 202 (already running, not a failure)", body, code, raw)
		}
		var resp struct {
			Started        bool `json:"started"`
			AlreadyRunning bool `json:"already_running"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			t.Fatalf("rescan response is not JSON: %s", raw)
		}
		if resp.Started || !resp.AlreadyRunning {
			t.Fatalf("rescan %s answered %s, want started=false already_running=true", body, raw)
		}
	}

	// The all-folders call is busy (409), never a 500 that reads as a crash.
	code, raw := postJSON(t, srv.URL+"/api/rescan", `{}`)
	if code != 409 {
		t.Fatalf("POST /api/rescan during the startup walk = %d %s, want 409 busy", code, raw)
	}

	// No second pass: the walk's pass is still the only one, and it has not been
	// restarted behind our back.
	if got := startStamp(svc, "data"); got != before {
		t.Fatalf("the folder's scan start moved %d -> %d: a second pass was started during the startup walk", before, got)
	}

	// Let the walk finish, then the same request must work normally: the guard is
	// temporary and per folder.
	sw.finish(t)
	code, raw = postJSON(t, srv.URL+"/api/folders/data/rescan", `{}`)
	if code != 200 {
		t.Fatalf("rescan after the walk = %d %s, want 200", code, raw)
	}
	// One pass per request: the two seed files are hashed by the startup walk
	// (2) and the post-walk rescan finds them unchanged (0 more).
	if got := sw.hashed.Load(); got != 2 {
		t.Fatalf("files hashed = %d, want 2 (one pass over the seed files, no duplicate walk)", got)
	}
}

// startStamp reads the folder's live scan start time from /api/status.
func startStamp(svc *control.Service, folder string) int64 {
	for _, st := range svc.Status().Scans {
		if st.Folder == folder {
			return st.Started
		}
	}
	return -1
}
