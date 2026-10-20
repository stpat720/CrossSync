package node

import (
	"net"
	"testing"
	"time"

	"crosssync/internal/protocol"
	"crosssync/internal/transfer"
)

// keepaliveDial opens a raw loopback connection to a responder and wraps it in
// a transfer.Conn. The raw net.Conn is returned too because only it can carry
// a read deadline (transfer.Conn deliberately has none: a session never needs
// one, but a test that waits on a silent peer does).
func keepaliveDial(t *testing.T, addr string) (net.Conn, *transfer.Conn) {
	t.Helper()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	return raw, transfer.NewConn(raw)
}

// keepaliveRecv reads frames until one of type want arrives, skipping keepalive
// pings — exactly what a session's read paths do — and fails on anything else
// or on a timeout.
func keepaliveRecv(t *testing.T, raw net.Conn, conn *transfer.Conn, want protocol.MsgType) *protocol.Message {
	t.Helper()
	if err := raw.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	for i := 0; i < 10000; i++ {
		m, err := conn.Recv()
		if err != nil {
			t.Fatalf("waiting for %v: %v", want, err)
		}
		if m.Type == protocol.MsgPing {
			continue
		}
		if m.Type != want {
			t.Fatalf("got %v, want %v", m.Type, want)
		}
		return m
	}
	t.Fatalf("only pings arrived, no %v", want)
	return nil
}

// keepaliveResponder starts a responder node serving one accepted connection
// (the pattern every session test in this package uses) and returns the
// listener plus the channel Serve's result arrives on.
func keepaliveResponder(t *testing.T, a *Node) (net.Listener, chan error) {
	t.Helper()
	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	served := make(chan error, 1)
	go func() {
		conn, err := transfer.Accept(ln)
		if err != nil {
			served <- err
			return
		}
		defer func() { _ = conn.Close() }()
		served <- a.Serve(conn)
	}()
	return ln, served
}

// TestServeKeepalivePingsSilentPeer pins the fix for mid-round EOFs over CGNAT
// / Tailscale links: a sync round reuses one connection, and its data phases
// can leave the wire silent for minutes. Once the cluster exchange is done the
// responder must keep writing keepalive frames while it waits on a silent peer,
// so the flow never looks idle to a NAT middlebox. The peer here goes
// deliberately silent after the cluster exchange — the state a responder sits
// in while the initiator hashes or reads a large file.
func TestServeKeepalivePingsSilentPeer(t *testing.T) {
	old := keepaliveInterval
	keepaliveInterval = 20 * time.Millisecond
	defer func() { keepaliveInterval = old }()

	a := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	ln, served := keepaliveResponder(t, a)
	raw, conn := keepaliveDial(t, ln.Addr().String())

	// Handshake as the initiator, advertising the shared folder so the
	// responder moves on to waiting for our index (a fully silent phase).
	if err := conn.Send(&protocol.Message{Type: protocol.MsgHello, Hello: &protocol.Hello{
		DeviceID: 2, DeviceName: "b", SessionID: "silent-peer", Protocol: protocol.Current,
	}}); err != nil {
		t.Fatal(err)
	}
	keepaliveRecv(t, raw, conn, protocol.MsgHello)
	if err := conn.Send(&protocol.Message{Type: protocol.MsgClusterConfig, Cluster: &protocol.ClusterConfig{
		Folders: []protocol.ClusterFolder{{ID: "data"}},
	}}); err != nil {
		t.Fatal(err)
	}
	keepaliveRecv(t, raw, conn, protocol.MsgClusterConfig)

	// Now say nothing at all. Every frame the responder emits from here on is
	// keepalive traffic; anything else means it left the session early.
	if err := raw.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		m, err := conn.Recv()
		if err != nil {
			t.Fatalf("keepalive ping %d on an idle session: %v", i, err)
		}
		if m.Type != protocol.MsgPing {
			t.Fatalf("silent peer: responder sent %v, want ping", m.Type)
		}
	}

	// Dropping the peer must end the session promptly: Serve returns and its
	// deferred keepalive stop tears the ping goroutine down.
	_ = raw.Close()
	select {
	case err := <-served:
		// The peer vanished mid-session, so Serve must surface an I/O error;
		// the point here is that it returns promptly at all.
		if err == nil {
			t.Fatal("Serve returned nil after the peer went away")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Serve did not return after the peer went away")
	}
}

// TestIncomingPingIgnoredBySession checks the other half of the contract: a
// peer may interleave keepalive frames at any point in the session, including
// between the handshake and cluster frames. They must be ignored — never
// answered and never treated as a protocol error — and the exchange must
// complete cleanly.
func TestIncomingPingIgnoredBySession(t *testing.T) {
	a := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	ln, served := keepaliveResponder(t, a)
	raw, conn := keepaliveDial(t, ln.Addr().String())

	ping := func(where string) {
		t.Helper()
		if err := conn.Send(&protocol.Message{Type: protocol.MsgPing}); err != nil {
			t.Fatalf("send ping (%s): %v", where, err)
		}
	}

	if err := conn.Send(&protocol.Message{Type: protocol.MsgHello, Hello: &protocol.Hello{
		DeviceID: 2, DeviceName: "b", SessionID: "pinger", Protocol: protocol.Current,
	}}); err != nil {
		t.Fatal(err)
	}
	keepaliveRecv(t, raw, conn, protocol.MsgHello)
	// A ping ahead of the cluster frame: recvCluster must skip it.
	ping("before cluster")
	if err := conn.Send(&protocol.Message{Type: protocol.MsgClusterConfig, Cluster: &protocol.ClusterConfig{
		Folders: []protocol.ClusterFolder{{ID: "data"}},
	}}); err != nil {
		t.Fatal(err)
	}
	keepaliveRecv(t, raw, conn, protocol.MsgClusterConfig)

	// A ping during the index exchange, then an empty (but final) full index.
	ping("during index exchange")
	if err := conn.Send(&protocol.Message{Type: protocol.MsgIndex, Index: &protocol.IndexMsg{
		Folder: "data", Update: false, Final: true,
	}}); err != nil {
		t.Fatal(err)
	}
	for {
		m := keepaliveRecv(t, raw, conn, protocol.MsgIndex)
		if m.Index == nil || m.Index.Folder != "data" {
			t.Fatalf("unexpected index frame: %+v", m.Index)
		}
		if m.Index.Final {
			break
		}
	}

	// A ping during the serve phase, then the phase hand-off. Phase 2 has
	// nothing to pull (the peer view is empty), so the responder closes.
	ping("during serve phase")
	if err := conn.Send(&protocol.Message{Type: protocol.MsgPhaseDone}); err != nil {
		t.Fatal(err)
	}
	keepaliveRecv(t, raw, conn, protocol.MsgClose)
	if err := conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "done"}}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("session with interleaved pings failed: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Serve did not finish")
	}
}
