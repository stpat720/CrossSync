// Package transfer provides the reliable transport for peer messages:
// a framed message stream over net.Conn (real TCP or net.Pipe for tests).
package transfer

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
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

	// writeTimeout bounds each Send while a round is active (0 = unbounded);
	// see SetWriteTimeout.
	writeTimeout time.Duration

	// stall bounds every Recv while a round is active (0 = unbounded). A
	// half-open socket never delivers a FIN: Recv blocks forever, the peer
	// session slot stays taken and NO further round starts. Measured live as both
	// nodes reporting connected=false for nine minutes while TCP on both ports
	// answered in 18-31ms, cleared only by a restart.
	stallNanos atomic.Int64 // see SetStall

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

// SetReadDeadline bounds how long Recv may wait for the next message. A peer that
// restarts leaves a half-open socket: no FIN arrives, so Recv blocks forever and
// a pull round hangs with files still pending and no error to show for it. The
// caller sets a no-progress deadline before each Recv and clears it (zero time)
// when it stops waiting.
func (cn *Conn) SetReadDeadline(t time.Time) error {
	return cn.c.SetReadDeadline(t)
}

// Send writes one framed message.
func (cn *Conn) Send(m *protocol.Message) error {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	// A write to a half-open socket blocks once the send buffer fills: the peer
	// is gone, no ACK ever arrives, and the frame never leaves. That is the hang
	// that stalled a live round for 20+ minutes — the serve workers were stuck in
	// Send and the round waited on them forever. Bound it when the caller asked.
	if d := cn.writeTimeout; d > 0 {
		_ = cn.c.SetWriteDeadline(time.Now().Add(d))
		defer func() { _ = cn.c.SetWriteDeadline(time.Time{}) }()
	}
	return protocol.WriteMessage(cn.c, m)
}

// SetWriteTimeout bounds every Send on this connection (0 = unbounded). Callers
// arm it while a round is active, so an idle session is never cut off.
func (cn *Conn) SetWriteTimeout(d time.Duration) {
	cn.mu.Lock()
	cn.writeTimeout = d
	cn.mu.Unlock()
}

// Recv reads one framed message.
func (cn *Conn) Recv() (*protocol.Message, error) {
	if n := cn.stallNanos.Load(); n > 0 {
		_ = cn.c.SetReadDeadline(time.Now().Add(time.Duration(n)))
		defer func() { _ = cn.c.SetReadDeadline(time.Time{}) }()
	}
	return protocol.ReadMessage(cn.c)
}

// SetStall arms a no-progress deadline before every Recv (0 clears it), so a
// round can never wait forever on a peer that vanished without closing the
// socket. Set it for the duration of a session and clear it at the end: long
// enough for a slow array, short enough that a dead peer is noticed in minutes.
func (cn *Conn) SetStall(d time.Duration) {
	cn.stallNanos.Store(int64(d))
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
