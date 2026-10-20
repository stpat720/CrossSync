package transfer

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"crosssync/internal/certs"
	"crosssync/internal/protocol"
)

func TestPipeRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	ca, cb := NewConn(a), NewConn(b)

	go func() {
		_ = ca.Send(&protocol.Message{Type: protocol.MsgHello, Hello: &protocol.Hello{DeviceID: 7, DeviceName: "seven"}})
	}()

	m, err := cb.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != protocol.MsgHello || m.Hello.DeviceID != 7 {
		t.Fatalf("unexpected message: %+v", m)
	}
}

func TestTCPSendRecv(t *testing.T) {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := Accept(ln)
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		m, err := conn.Recv()
		if err != nil {
			serverDone <- err
			return
		}
		if m.Type != protocol.MsgPing {
			serverDone <- io.ErrUnexpectedEOF
			return
		}
		serverDone <- conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "bye"}})
	}()

	client, err := Dial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if err := client.Send(&protocol.Message{Type: protocol.MsgPing}); err != nil {
		t.Fatal(err)
	}
	m, err := client.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != protocol.MsgClose {
		t.Fatalf("unexpected response: %+v", m)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not finish")
	}
}

// tlsPair builds two certificate managers, each pinned to the other.
func tlsPair(t *testing.T) (*certs.Manager, *certs.Manager, *tls.Config, *tls.Config) {
	t.Helper()
	a, err := certs.LoadOrCreate(filepath.Join(t.TempDir(), "key.pem"), filepath.Join(t.TempDir(), "cert.pem"), "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := certs.LoadOrCreate(filepath.Join(t.TempDir(), "key.pem"), filepath.Join(t.TempDir(), "cert.pem"), "b")
	if err != nil {
		t.Fatal(err)
	}
	// The server (a) pins the client's fingerprint (b); the client (b) pins
	// the server's fingerprint (a).
	serverCfg := certs.ServerConfig(a, map[string]bool{b.Fingerprint(): true})
	clientCfg := certs.ClientConfig(b, map[string]bool{a.Fingerprint(): true})
	return a, b, serverCfg, clientCfg
}

func TestTLSSendRecv(t *testing.T) {
	_, _, serverCfg, clientCfg := tlsPair(t)
	ln, err := ListenTLS("127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := Accept(ln)
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		m, err := conn.Recv()
		if err != nil {
			serverDone <- err
			return
		}
		if m.Type != protocol.MsgPing {
			serverDone <- io.ErrUnexpectedEOF
			return
		}
		serverDone <- conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "bye"}})
	}()

	client, err := DialTLS(ln.Addr().String(), clientCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if err := client.Send(&protocol.Message{Type: protocol.MsgPing}); err != nil {
		t.Fatal(err)
	}
	m, err := client.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != protocol.MsgClose {
		t.Fatalf("unexpected response: %+v", m)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not finish")
	}
}

func TestTLSRejectsUnpinnedClient(t *testing.T) {
	_, _, serverCfg, _ := tlsPair(t)
	ln, err := ListenTLS("127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	acceptErr := make(chan error, 1)
	go func() {
		_, err := Accept(ln) // handshake fails: client cert not pinned
		acceptErr <- err
	}()

	// An unrelated cert that the server has NOT pinned.
	stranger, err := certs.LoadOrCreate(filepath.Join(t.TempDir(), "key.pem"), filepath.Join(t.TempDir(), "cert.pem"), "stranger")
	if err != nil {
		t.Fatal(err)
	}
	clientCfg := certs.ClientConfig(stranger, map[string]bool{stranger.Fingerprint(): true})
	if _, err := DialTLS(ln.Addr().String(), clientCfg); err == nil {
		t.Fatal("DialTLS with unpinned client should have failed")
	}
	select {
	case err := <-acceptErr:
		if err == nil {
			t.Fatal("server Accept should have failed the handshake")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server accept did not return")
	}
}

// TestPingLoopKeepsIdleConnAlive covers the application-level keepalive: on a
// connection with no other traffic, PingLoop keeps writing MsgPing frames the
// peer reads without error, and once its context is cancelled it stops for
// good (no frame after the last ping, no goroutine left behind) while the
// connection stays usable for normal messages.
func TestPingLoopKeepsIdleConnAlive(t *testing.T) {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	// The peer reads until it sees the Close the test sends after the ping
	// loop stopped, and reports how many keepalive frames preceded it.
	pingsSeen := make(chan int, 1)
	pinging := make(chan struct{}, 1)
	peerErr := make(chan error, 1)
	go func() {
		conn, err := Accept(ln)
		if err != nil {
			peerErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		n := 0
		for {
			m, err := conn.Recv()
			if err != nil {
				peerErr <- err
				return
			}
			if m.Type == protocol.MsgPing {
				n++
				if n == 3 {
					select {
					case pinging <- struct{}{}:
					default:
					}
				}
				continue
			}
			if m.Type != protocol.MsgClose {
				peerErr <- fmt.Errorf("frame %v arrived before close", m.Type)
				return
			}
			pingsSeen <- n
			return
		}
	}()

	conn, err := Dial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		conn.PingLoop(ctx, 10*time.Millisecond)
	}()

	select {
	case <-pinging:
	case err := <-peerErr:
		t.Fatalf("peer failed before any keepalive: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("idle connection received no keepalive pings")
	}

	cancel()
	select {
	case <-loopDone:
	case <-time.After(10 * time.Second):
		t.Fatal("PingLoop did not stop after its context was cancelled")
	}

	// The loop is gone, so nothing may follow this Close on the wire.
	if err := conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "done"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-pingsSeen:
		if n < 3 {
			t.Fatalf("peer saw %d keepalive pings, want at least 3", n)
		}
	case err := <-peerErr:
		t.Fatalf("peer failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("peer never saw the close frame")
	}
}

// TestPingLoopStopsWhenConnDies checks the failure path: when the peer is gone
// the ping write fails and the loop ends on its own (no cancel needed), so a
// dead session can never leave a goroutine pinging a corpse.
func TestPingLoopStopsWhenConnDies(t *testing.T) {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	conn, err := Dial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	peer, err := Accept(ln)
	if err != nil {
		t.Fatal(err)
	}
	_ = peer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // deliberately not called before the loop returns
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn.PingLoop(ctx, 5*time.Millisecond)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("PingLoop kept running after the connection died")
	}
}
