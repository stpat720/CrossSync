// Package transfer provides the reliable transport for peer messages:
// a framed message stream over net.Conn (real TCP or net.Pipe for tests).
package transfer

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"crosssync/internal/protocol"
)

// Keepalive tuning. A sync round holds one connection open for the whole
// folder exchange, and the data phases can be silent for minutes (hashing a
// large file, reading a big block from disk). NAT middleboxes — CGNAT, a
// Tailscale relay path — drop idle flows, and the next read then fails with a
// clean EOF that abandons the round. Two layers keep the flow warm:
//
//   - TCP keepalive on the connect path (tcpKeepAlivePeriod, kernel-level),
//   - an application-level MsgPing every KeepaliveInterval while a session is
//     active (Conn.PingLoop), which also refreshes NAT state on paths that
//     ignore or rewrite TCP keepalive probes.
const (
	// KeepaliveInterval is how often an active session writes a MsgPing.
	// It is a protocol liveness floor, deliberately not user-configurable.
	KeepaliveInterval = 20 * time.Second
	// tcpKeepAlivePeriod is the kernel keepalive period for peer connections.
	tcpKeepAlivePeriod = 30 * time.Second
)

// PingLoop writes a MsgPing frame every interval until ctx is cancelled or a
// write fails. Writes go through Send, which takes the connection's write
// mutex, so a ping can never interleave with (or corrupt) a frame the session
// is writing.
//
// A write error means the link is already dead: the loop returns silently and
// lets the session's own I/O report the real error (logging here would just
// repeat it once per tick). It also returns when interval <= 0.
func (cn *Conn) PingLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := cn.Send(&protocol.Message{Type: protocol.MsgPing}); err != nil {
				return
			}
		}
	}
}

// enableTCPKeepalive turns on kernel keepalives for a peer connection so an
// idle flow keeps its NAT entry even outside the application ping window.
// Best effort: a non-TCP conn (net.Pipe in tests) or an OS that refuses the
// option is left as-is — the app-level ping still covers the session.
func enableTCPKeepalive(c net.Conn) {
	if tc, ok := c.(*tls.Conn); ok {
		c = tc.NetConn()
	}
	tcp, ok := c.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcp.SetKeepAlive(true)
	_ = tcp.SetKeepAlivePeriod(tcpKeepAlivePeriod)
}

// Conn is a framed message stream over a net.Conn.
type Conn struct {
	c  net.Conn
	mu sync.Mutex // serializes writes

	// PeerID is set by the session after the Hello handshake.
	PeerID   uint64
	PeerName string
	// PeerSession is the peer's per-process session id from Hello.
	PeerSession string
	// PeerProtocol is the peer's advertised protocol revision from Hello
	// (0 = legacy single-frame index messages).
	PeerProtocol int

	// reqSeq hands out block-request IDs that are unique for the LIFE of the
	// connection (not per file). With several requests in flight, a request
	// abandoned mid-file can still have responses in the pipe; per-file IDs
	// would let a stale response be mistaken for the next file's block —
	// silent data corruption. Only the pulling goroutine touches it.
	reqSeq uint64
}

// NextRequestID returns the next block-request ID for this connection.
func (cn *Conn) NextRequestID() uint64 {
	cn.reqSeq++
	return cn.reqSeq
}

// NewConn wraps an existing net.Conn.
func NewConn(c net.Conn) *Conn {
	return &Conn{c: c}
}

// Send writes one framed message.
func (cn *Conn) Send(m *protocol.Message) error {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	return protocol.WriteMessage(cn.c, m)
}

// Recv reads one framed message.
func (cn *Conn) Recv() (*protocol.Message, error) {
	return protocol.ReadMessage(cn.c)
}

// Close closes the underlying connection.
func (cn *Conn) Close() error { return cn.c.Close() }

// RemoteAddr returns the remote address.
func (cn *Conn) RemoteAddr() string { return cn.c.RemoteAddr().String() }

// Dial connects to a peer address.
func Dial(addr string) (*Conn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	enableTCPKeepalive(c)
	return NewConn(c), nil
}

// Listen opens a TCP listener.
func Listen(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

// Accept accepts one connection. For a TLS listener, the handshake is run
// here (Go's tls.Listener defers it to first use), so an unpinned peer is
// rejected at accept time and never reaches the session.
func Accept(ln net.Listener) (*Conn, error) {
	c, err := ln.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*tls.Conn); ok {
		if err := tc.Handshake(); err != nil {
			_ = c.Close()
			return nil, err
		}
	}
	enableTCPKeepalive(c)
	return NewConn(c), nil
}

// DialTLS connects to a peer over TLS 1.3, completing the handshake
// (including certificate-fingerprint pinning) before returning.
func DialTLS(addr string, cfg *tls.Config) (*Conn, error) {
	c, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
	enableTCPKeepalive(c)
	return NewConn(c), nil
}

// ListenTLS opens a TCP listener that wraps accepted connections in TLS.
// The handshake runs in Accept, so a rejected peer (wrong fingerprint)
// surfaces there rather than in the session.
func ListenTLS(addr string, cfg *tls.Config) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return tls.NewListener(ln, cfg), nil
}
