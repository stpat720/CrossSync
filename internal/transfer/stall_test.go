package transfer

import (
	"net"
	"testing"
	"time"
)

// TestRecvTimesOutOnASilentPeer pins the fix for the wedged link: a peer that
// accepts TCP and then never speaks must not hold a session (and the peer's
// session slot) forever. Measured live as both nodes reporting connected=false
// for nine minutes while TCP answered in milliseconds.
func TestRecvTimesOutOnASilentPeer(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	cn := NewConn(a)
	cn.SetStall(150 * time.Millisecond)

	done := make(chan error, 1)
	go func() {
		_, err := cn.Recv()
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Recv on a silent peer returned no error")
		}
		if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
			t.Fatalf("want a timeout error, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Recv blocked past its stall deadline")
	}
}

// TestStallClearedStopsBounding: clearing the stall must restore an unbounded
// wait, otherwise an idle connection would be dropped for being quiet.
func TestStallClearedStopsBounding(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	cn := NewConn(a)
	cn.SetStall(50 * time.Millisecond)
	cn.SetStall(0)
	if n := cn.stallNanos.Load(); n != 0 {
		t.Fatalf("stall not cleared: %d", n)
	}
}
