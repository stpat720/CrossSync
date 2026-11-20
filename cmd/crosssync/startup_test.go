package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/control"
	"crosssync/internal/node"
)

// freeAddr returns a loopback address whose port was free a moment ago. The
// tests bind real sockets because the whole point is that the daemon's sockets
// exist while its first index pass runs.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// blockingHashNode is a node whose single folder blocks inside its first pass
// until the test releases it: a deterministic stand-in for a 57.8 GB share.
type blockingHashNode struct {
	n       *node.Node
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
	svc     *control.Service
}

func newBlockingHashNode(t *testing.T, cfg *config.Config) *blockingHashNode {
	t.Helper()
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	n.Logf = t.Logf

	b := &blockingHashNode{n: n, entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	var once bool
	sc := n.Folders["data"].Scanner
	sc.SetHashHook(func(string) {
		if !once {
			once = true
			close(b.entered)
		}
		<-b.release
	})
	t.Cleanup(func() {
		select {
		case <-b.release:
		default:
			close(b.release)
		}
		<-b.done
		sc.SetHashHook(nil)
	})
	return b
}

// startWalk runs the first pass the way cmdServe does and waits until it is
// provably hashing.
func (b *blockingHashNode) startWalk(t *testing.T) {
	t.Helper()
	b.n.SetStartupScanning(true)
	go func() {
		defer close(b.done)
		_ = b.n.ScanAllFirstPass()
	}()
	select {
	case <-b.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the startup walk never reached the hashing phase")
	}
}

func (b *blockingHashNode) finish(t *testing.T) {
	t.Helper()
	select {
	case <-b.release:
	default:
		close(b.release)
	}
	select {
	case <-b.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the startup walk did not finish after its hash hook was released")
	}
}

func startupCfg(t *testing.T) (*config.Config, string) {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("payload "+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &config.Config{
		Device:        config.Device{ID: 1, Name: "nas-01"},
		MetaDir:       t.TempDir(),
		Listen:        freeAddr(t),
		ControlListen: freeAddr(t),
		Folders:       []config.Folder{{ID: "data", Path: root, ConflictPolicy: "conflict-copy", SettleScans: 1}},
	}
	return cfg, root
}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v (is anything listening?)", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// TestStartupWalkRunsBehindLiveListeners is the ordering proof for the live
// incident: the sync listener and the control API must be bound and answering
// WHILE the first index walk is still running. Before the fix the walk ran
// first, so both ports were silent for ~40 minutes and the node was
// indistinguishable from a crashed one.
func TestStartupWalkRunsBehindLiveListeners(t *testing.T) {
	cfg, _ := startupCfg(t)
	b := newBlockingHashNode(t, cfg)

	// The daemon binds first, then starts the walk behind the sockets.
	bs, err := bindServices(b.n, cfg, filepath.Join(cfg.MetaDir, "config.yaml"), "test")
	if err != nil {
		t.Fatalf("bindServices: %v", err)
	}
	defer func() { _ = bs.Close() }()
	b.svc = bs.Svc
	if bs.LN == nil || bs.SRV == nil {
		t.Fatalf("bindServices returned ln=%v srv=%v, want both bound", bs.LN, bs.SRV)
	}

	b.startWalk(t)

	// The walk is mid-hash: the control API answers and reports it as indexing.
	code, body := httpGet(t, "http://"+cfg.ControlListen+"/healthz")
	if code != 200 {
		t.Fatalf("/healthz while the first pass runs = %d %s, want 200", code, body)
	}
	if !strings.Contains(body, `"status":"indexing"`) {
		t.Fatalf("/healthz while the first pass runs = %s, want status \"indexing\"", body)
	}
	code, body = httpGet(t, "http://"+cfg.ControlListen+"/api/status")
	if code != 200 {
		t.Fatalf("/api/status while the first pass runs = %d %s, want 200", code, body)
	}
	var st struct {
		StartupScanning bool `json:"startup_scanning"`
	}
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatalf("/api/status is not JSON: %s", body)
	}
	if !st.StartupScanning {
		t.Fatalf("/api/status while the first pass runs = %s, want startup_scanning true", body)
	}

	// The peer-facing sync port accepts connections in the same window.
	conn, err := net.DialTimeout("tcp", cfg.Listen, 5*time.Second)
	if err != nil {
		t.Fatalf("the sync listener did not accept while the first pass ran: %v", err)
	}
	_ = conn.Close()

	// And the walk was genuinely still running at that point.
	select {
	case <-b.done:
		t.Fatal("the startup walk finished before the listeners were probed; the test proved nothing")
	default:
	}

	// After the walk, the same API reports it as done.
	b.finish(t)
	code, body = httpGet(t, "http://"+cfg.ControlListen+"/api/status")
	if code != 200 {
		t.Fatalf("/api/status after the walk = %d %s", code, body)
	}
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatalf("/api/status is not JSON: %s", body)
	}
	if st.StartupScanning {
		t.Fatalf("/api/status after the walk = %s, want startup_scanning false", body)
	}
}

// TestAbortScansCancelsFirstPass pins the shutdown path: a stop during the
// startup walk abandons the pass instead of waiting out the hash, and a
// cancelled pass applies nothing — so the folder stays unindexed and
// unadvertised rather than being published half-built.
func TestAbortScansCancelsFirstPass(t *testing.T) {
	cfg, _ := startupCfg(t)
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	n.Logf = t.Logf

	// Abort before the pass starts: every scan from here on is cancelled, which
	// is exactly the state a shutdown leaves behind.
	n.AbortScans()
	n.SetStartupScanning(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = n.ScanAllFirstPass()
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("a cancelled first pass did not return promptly")
	}

	svc := control.New(n, "test")
	st := svc.Status()
	if len(st.Scans) != 1 || st.Scans[0].Phase != "cancelled" {
		t.Fatalf("scan status after the abort = %+v, want one cancelled pass", st.Scans)
	}
	// Asserted on the JSON contract, not the Go struct: startup_scanning is what
	// /api/status publishes.
	raw, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		StartupScanning bool `json:"startup_scanning"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if !wire.StartupScanning {
		t.Fatalf("status after a cancelled first pass = %s, want startup_scanning true (a cancelled pass proves nothing)", raw)
	}
	if files := st.Folders[0].Files; files != 0 {
		t.Fatalf("folder indexed %d file(s) from a cancelled pass, want 0 (nothing is applied)", files)
	}
}
