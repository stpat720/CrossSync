package transfer

import (
	"net"
	"testing"
	"time"

	"crosssync/internal/protocol"
)

// TestSendTimesOutOnAStuckPeer pins the write-side stall guard. A peer that
// accepts and then stops reading fills the send buffer; without a write deadline
// Send blocks forever, and on the serving side that means a worker stuck in Send
// while the round waits for it — measured live as a 20-minute stall with the round
// marked running and no error anywhere.
func TestSendTimesOutOnAStuckPeer(t *testing.T) {
	srv, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := srv.Accept()
		if err != nil {
			return
		}
		accepted <- c // never read: the client's sends must back up
	}()

	c, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	peer := <-accepted
	defer func() { _ = peer.Close() }()

	conn := NewConn(c)
	conn.SetWriteTimeout(300 * time.Millisecond)
	big := &protocol.Message{Type: protocol.MsgResponse, Response: &protocol.Response{Data: make([]byte, 4<<20)}}

	deadline := time.Now().Add(20 * time.Second)
	var sendErr error
	for time.Now().Before(deadline) {
		if sendErr = conn.Send(big); sendErr != nil {
			break
		}
	}
	if sendErr == nil {
		t.Fatalf("sending to a peer that never reads never failed")
	}
	if ne, ok := sendErr.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("error = %v (%T), want a timeout", sendErr, sendErr)
	}
	if waited := time.Since(deadline.Add(-20 * time.Second)); waited > 15*time.Second {
		t.Fatalf("took %s to notice the stuck peer", waited)
	}
}
