package transfer

import (
	"crypto/tls"
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
	defer a.Close()
	defer b.Close()
	ca, cb := NewConn(a), NewConn(b)

	go func() {
		ca.Send(&protocol.Message{Type: protocol.MsgHello, Hello: &protocol.Hello{DeviceID: 7, DeviceName: "seven"}})
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
	defer ln.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := Accept(ln)
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
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
	defer client.Close()
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
	defer ln.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := Accept(ln)
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
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
	defer client.Close()
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
	defer ln.Close()

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
