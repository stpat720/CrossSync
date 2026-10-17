package control_test

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crosssync/internal/certs"
	"crosssync/internal/config"
	"crosssync/internal/control"
	"crosssync/internal/node"
)

// testTLSNode builds a Node with TLS enabled (so the peer allowlist is
// live state) and returns it plus the node's own certificate fingerprint.
func testTLSNode(t *testing.T) *node.Node {
	t.Helper()
	meta := t.TempDir()
	cm, err := certs.LoadOrCreate(filepath.Join(meta, "key.pem"),
		filepath.Join(meta, "cert.pem"), "nas-01")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		Device: config.Device{ID: cm.DeviceID(), Name: "nas-01"},
		MetaDir: meta,
	}
	cfg.TLS = true
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func peerInList(t *testing.T, n *node.Node, id uint64) bool {
	t.Helper()
	for _, p := range n.Cfg.Peers {
		if p.ID == id {
			return true
		}
	}
	return false
}

func TestAddUpdateRemovePeerLive(t *testing.T) {
	n := testTLSNode(t)
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfgText := "device:\n  id: " + fmt.Sprint(n.Cfg.Device.ID) +
		"\n  name: nas-01\nmeta_dir: " + n.Cfg.MetaDir + "\nfolders: []\npeers: []\n"
	if err := os.WriteFile(cfgPath, []byte(cfgText), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)

	// Add (id 0 = generated).
	p, err := svc.AddPeer(0, "laptop", []string{"127.0.0.1:19999"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if p.ID == 0 || p.ID == n.Cfg.Device.ID {
		t.Fatalf("generated peer id invalid: %d", p.ID)
	}
	if !peerInList(t, n, p.ID) {
		t.Fatal("peer not applied to live config")
	}

	// Update (address + pin a fingerprint).
	fakeFP := strings.Repeat("ab", 32)
	p2, err := svc.UpdatePeer(p.ID, "laptop", []string{"10.0.0.9:55555"}, fakeFP)
	if err != nil {
		t.Fatal(err)
	}
	if p2.Addresses[0] != "10.0.0.9:55555" || p2.Fingerprint != fakeFP {
		t.Fatalf("update not applied: %+v", p2)
	}

	// Persistence: the peers section landed and the config still loads
	// cleanly (comment + other sections preserved by the yaml.Node path).
	data, _ := os.ReadFile(cfgPath)
	if !strings.Contains(string(data), "laptop") || !strings.Contains(string(data), fakeFP) {
		t.Fatalf("peers not persisted:\n%s", data)
	}
	if _, err := config.Load(cfgPath); err != nil {
		t.Fatalf("config no longer valid after persist: %v", err)
	}

	// Remove.
	if err := svc.RemovePeer(p.ID); err != nil {
		t.Fatal(err)
	}
	if peerInList(t, n, p.ID) {
		t.Fatal("peer still in live config after remove")
	}
	data, _ = os.ReadFile(cfgPath)
	if strings.Contains(string(data), "laptop") {
		t.Fatalf("peer not removed from config:\n%s", data)
	}
}

func TestAddPeerValidation(t *testing.T) {
	n := testTLSNode(t)
	svc := control.New(n, "test")
	if _, err := svc.AddPeer(0, "", []string{"1.2.3.4:5"}, ""); err == nil {
		t.Fatal("empty name should fail")
	}
	if _, err := svc.AddPeer(0, "x", nil, ""); err == nil {
		t.Fatal("no addresses should fail")
	}
	if _, err := svc.AddPeer(0, "x", []string{"1.2.3.4:5"}, "zz"); err == nil {
		t.Fatal("bad fingerprint should fail")
	}
	if _, err := svc.AddPeer(n.Cfg.Device.ID, "self", []string{"1.2.3.4:5"}, ""); err == nil {
		t.Fatal("device's own id should fail")
	}
	// Duplicate id rejected.
	if _, err := svc.AddPeer(7, "a", []string{"1.2.3.4:5"}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddPeer(7, "b", []string{"1.2.3.4:5"}, ""); err == nil {
		t.Fatal("duplicate peer id should fail")
	}
}

// TestApplyPeersUpdatesLiveTLSAllowlist verifies the load-bearing security
// property: a peer pinned via ApplyPeers is accepted by the running TLS
// state (the VerifyPeerCertificate closure sees the allowlist live), and a
// removed peer is rejected again — without rebuilding any tls.Config.
func TestApplyPeersUpdatesLiveTLSAllowlist(t *testing.T) {
	// Server: a second node with its own certificate.
	serverMeta := t.TempDir()
	serverCM, err := certs.LoadOrCreate(filepath.Join(serverMeta, "key.pem"),
		filepath.Join(serverMeta, "cert.pem"), "laptop-01")
	if err != nil {
		t.Fatal(err)
	}
	serverAddr := startTLSTestServer(t, serverCM)

	// Client: the node under test (TLS enabled, no peers pinned).
	n := testTLSNode(t)

	// Baseline: unpinned server is rejected.
	if err := dialHandshakeErr(serverAddr, n.ClientTLS); err == nil {
		t.Fatal("expected handshake rejection for unpinned peer")
	}

	// Pin the server's fingerprint live.
	n.ApplyPeers([]config.Peer{{ID: 9, Name: "laptop", Addresses: []string{serverAddr}, Fingerprint: serverCM.Fingerprint()}})
	if err := dialHandshakeErr(serverAddr, n.ClientTLS); err != nil {
		t.Fatalf("pinned peer should handshake: %v", err)
	}

	// Unpin live (remove the peer): rejected again.
	n.ApplyPeers(nil)
	if err := dialHandshakeErr(serverAddr, n.ClientTLS); err == nil {
		t.Fatal("expected rejection after peer removed")
	}
}

// startTLSTestServer serves TLS 1.3 with cm's certificate on loopback and
// returns the listen address.
func startTLSTestServer(t *testing.T, cm *certs.Manager) string {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cm.TLSCert},
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})}
		_ = srv.Serve(ln)
	}()
	return ln.Addr().String()
}

func dialHandshakeErr(addr string, cfg *tls.Config) error {
	c, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		return err
	}
	_ = c.Close()
	return nil
}

func TestRESTPeerEndpoints(t *testing.T) {
	n := testTLSNode(t)
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfgText := "device:\n  id: " + fmt.Sprint(n.Cfg.Device.ID) +
		"\n  name: nas-01\nmeta_dir: " + n.Cfg.MetaDir + "\nfolders: []\npeers: []\n"
	if err := os.WriteFile(cfgPath, []byte(cfgText), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	// POST add (generated id).
	body, _ := json.Marshal(map[string]any{
		"name": "laptop", "addresses": []string{"127.0.0.1:1"},
	})
	resp := mustDo(t, "POST", srv.URL+"/api/peers", body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 201 {
		t.Fatalf("add status %d", resp.StatusCode)
	}
	var added struct {
		ID          uint64 `json:"id"`
		Name        string `json:"name"`
		Fingerprint string `json:"fingerprint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&added); err != nil {
		t.Fatal(err)
	}
	if added.ID == 0 {
		t.Fatal("expected generated id")
	}

	// GET lists it (status-derived PeerStatus).
	code, b := getJSON(t, srv.URL+"/api/peers")
	if code != 200 {
		t.Fatalf("get peers %d", code)
	}
	var listed []map[string]any
	if err := json.Unmarshal(b, &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0]["name"] != "laptop" {
		t.Fatalf("unexpected peer list: %s", b)
	}

	// PUT update.
	upd, _ := json.Marshal(map[string]any{"name": "laptop-2", "addresses": []string{"10.0.0.1:2"}})
	resp = mustDo(t, "PUT", fmt.Sprintf("%s/api/peers/%d", srv.URL, added.ID), upd)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("update status %d", resp.StatusCode)
	}

	// Bad body → 400.
	resp = mustDo(t, "PUT", fmt.Sprintf("%s/api/peers/%d", srv.URL, added.ID), []byte("not json"))
	_ = resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("bad body should be 400, got %d", resp.StatusCode)
	}

	// DELETE removes.
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/api/peers/%d", srv.URL, added.ID), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("remove status %d", resp.StatusCode)
	}
	code, b = getJSON(t, srv.URL+"/api/peers")
	if code != 200 {
		t.Fatalf("get peers after remove %d", code)
	}
	var after []map[string]any
	if err := json.Unmarshal(b, &after); err != nil {
		t.Fatal(err)
	}
	if len(after) != 0 {
		t.Fatalf("expected no peers after remove: %s", b)
	}
}

func mustDo(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
