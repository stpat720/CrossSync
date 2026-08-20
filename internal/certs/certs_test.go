package certs

import (
	"crypto/tls"
	"net"
	"path/filepath"
	"testing"
)

func TestLoadOrCreatePersists(t *testing.T) {
	dir := t.TempDir()
	key, cert := filepath.Join(dir, "key.pem"), filepath.Join(dir, "cert.pem")

	a, err := LoadOrCreate(key, cert, "nas-01")
	if err != nil {
		t.Fatal(err)
	}
	fpA := a.Fingerprint()
	if len(fpA) != 64 {
		t.Fatalf("fingerprint should be 64 hex chars, got %q (%d)", fpA, len(fpA))
	}

	// Second load must read the same identity back from disk.
	b, err := LoadOrCreate(key, cert, "nas-01")
	if err != nil {
		t.Fatal(err)
	}
	if b.Fingerprint() != fpA {
		t.Fatalf("fingerprint changed across loads: %s != %s", b.Fingerprint(), fpA)
	}
	if b.DeviceID() != a.DeviceID() {
		t.Fatalf("device id changed across loads: %d != %d", b.DeviceID(), a.DeviceID())
	}
}

func TestDistinctDevicesDiffer(t *testing.T) {
	a, err := LoadOrCreate(filepath.Join(t.TempDir(), "k"), filepath.Join(t.TempDir(), "c"), "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadOrCreate(filepath.Join(t.TempDir(), "k"), filepath.Join(t.TempDir(), "c"), "b")
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint() == b.Fingerprint() {
		t.Fatal("two freshly generated devices must not share a fingerprint")
	}
	if a.DeviceID() == b.DeviceID() {
		t.Fatal("two freshly generated devices must not share a device id")
	}
}

// handshake runs a full TLS 1.3 handshake over real loopback TCP between a
// server (presented by serverMgr, pinned to serverAllowed) and a client
// (clientMgr / clientAllowed). It returns the client-side and server-side
// handshake errors. (net.Pipe is not used: its synchronous, unbuffered
// semantics deadlock against TLS 1.3's split server flight.)
func handshake(t *testing.T, serverMgr, clientMgr *Manager, serverAllowed, clientAllowed map[string]bool) (clientErr, serverErr error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer c.Close()
		sc := tls.Server(c, ServerConfig(serverMgr, serverAllowed))
		done <- sc.Handshake()
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cc := tls.Client(c, ClientConfig(clientMgr, clientAllowed))
	clientErr = cc.Handshake()
	serverErr = <-done
	return clientErr, serverErr
}

func TestHandshakePinnedSucceeds(t *testing.T) {
	a, _ := LoadOrCreate(filepath.Join(t.TempDir(), "k"), filepath.Join(t.TempDir(), "c"), "a")
	b, _ := LoadOrCreate(filepath.Join(t.TempDir(), "k"), filepath.Join(t.TempDir(), "c"), "b")

	// Server (a) pins the client (b); client (b) pins the server (a).
	clientErr, serverErr := handshake(t, a, b, map[string]bool{b.Fingerprint(): true}, map[string]bool{a.Fingerprint(): true})
	if clientErr != nil {
		t.Fatalf("client handshake with matching pins failed: %v", clientErr)
	}
	if serverErr != nil {
		t.Fatalf("server handshake with matching pins failed: %v", serverErr)
	}
}

func TestHandshakeRejectsUnpinnedPeer(t *testing.T) {
	a, _ := LoadOrCreate(filepath.Join(t.TempDir(), "k"), filepath.Join(t.TempDir(), "c"), "a")
	b, _ := LoadOrCreate(filepath.Join(t.TempDir(), "k"), filepath.Join(t.TempDir(), "c"), "b")
	c, _ := LoadOrCreate(filepath.Join(t.TempDir(), "k"), filepath.Join(t.TempDir(), "c"), "c")

	// Server pins only a, so it must reject client b's unpinned cert. Note:
	// in TLS 1.3 the client finishes its handshake before the server
	// validates the client cert, so the client side may complete while the
	// server side errors — the rejection is what matters.
	clientErr, serverErr := handshake(t, a, b, map[string]bool{a.Fingerprint(): true}, map[string]bool{a.Fingerprint(): true})
	if serverErr == nil {
		t.Fatalf("server should reject unpinned client cert (client=%v server=%v)", clientErr, serverErr)
	}
	// Server pins a and c: b still fails.
	clientErr, serverErr = handshake(t, a, b, map[string]bool{a.Fingerprint(): true, c.Fingerprint(): true}, map[string]bool{a.Fingerprint(): true})
	if serverErr == nil {
		t.Fatalf("server should reject unpinned client cert (client=%v server=%v)", clientErr, serverErr)
	}
	// Client must also reject a server whose fingerprint it doesn't pin.
	clientErr, serverErr = handshake(t, a, b, map[string]bool{b.Fingerprint(): true}, map[string]bool{c.Fingerprint(): true})
	if clientErr == nil {
		t.Fatalf("client should reject unpinned server cert (client=%v server=%v)", clientErr, serverErr)
	}
}
