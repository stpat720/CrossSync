// Package transfer provides the reliable transport for peer messages:
// a framed message stream over net.Conn (real TCP or net.Pipe for tests).
package transfer

import (
	"crypto/tls"
	"net"
	"sync"

	"crosssync/internal/protocol"
)

// Conn is a framed message stream over a net.Conn.
type Conn struct {
	c  net.Conn
	mu sync.Mutex // serializes writes

	// PeerID is set by the session after the Hello handshake.
	PeerID   uint64
	PeerName string
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
			c.Close()
			return nil, err
		}
	}
	return NewConn(c), nil
}

// DialTLS connects to a peer over TLS 1.3, completing the handshake
// (including certificate-fingerprint pinning) before returning.
func DialTLS(addr string, cfg *tls.Config) (*Conn, error) {
	c, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
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
