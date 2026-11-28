package node

import (
	"net"
	"testing"
	"time"

	"crosssync/internal/protocol"
	"crosssync/internal/transfer"
)

func dialRaw(addr string) (net.Conn, error) { return net.Dial("tcp", addr) }

// TestIndexAckIsSentAndRecorded pins the mechanism that stops every retry from
// re-streaming a whole folder: the receiving side confirms that it MERGED the
// index, and the sender records its delta marker immediately instead of at
// session end. Measured failure it prevents: a round that spent hours in the
// transfer phase died on a serve watchdog, and because the marker only advanced
// when a session completed, the next round re-sent all 2.1M change records.
func TestIndexAckIsSentAndRecorded(t *testing.T) {
	a := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	chunkWrite(t, a.Folders["data"].Cfg.Path, "one.txt", "one")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	f, ok := a.lookupFolder("data")
	if !ok || f.Ix == nil {
		t.Fatal("folder data has no index")
	}
	id, err := f.Ix.IndexID()
	if err != nil {
		t.Fatal(err)
	}
	seq := f.Ix.MaxSeq()
	if seq <= 0 {
		t.Fatalf("MaxSeq = %d; the scan should have produced entries", seq)
	}

	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan *transfer.Conn, 1)
	go func() {
		c, err := transfer.Accept(ln)
		if err != nil {
			return
		}
		accepted <- c
	}()
	raw, err := dialRaw(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	peer := transfer.NewConn(raw)
	peer.PeerProtocol = protocol.Current
	peer.PeerSession = "live-session"
	peer.PeerID = 2

	srv := <-accepted
	defer func() { _ = srv.Close() }()
	srv.PeerProtocol = protocol.Current
	srv.PeerSession = "live-session"

	// Sender side: acknowledge as if this node had just merged the peer's index.
	if err := a.ackIndex(srv, &protocol.IndexMsg{Folder: "data", IndexID: id, MaxSeq: seq}); err != nil {
		t.Fatal(err)
	}
	if err := raw.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	m, err := peer.Recv()
	if err != nil {
		t.Fatalf("waiting for the ack: %v", err)
	}
	if m.Type != protocol.MsgIndexAck || m.IndexAck == nil {
		t.Fatalf("got %v, want an index-ack", m.Type)
	}
	if m.IndexAck.Folder != "data" || m.IndexAck.IndexID != id || m.IndexAck.MaxSeq != seq {
		t.Fatalf("ack = %+v, want folder data id %s seq %d", *m.IndexAck, id, seq)
	}

	// Receiver side of the ack: it must persist the delta marker now.
	if err := raw.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	a.noteIndexAck(peer, m.IndexAck)
	gotID, gotSeq, gotSession, found, err := f.Ix.GetPeerIndex(peer.PeerID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || gotID != id || gotSeq != seq || gotSession != peer.PeerSession {
		t.Fatalf("stored marker = (%q,%d,%q,%v), want (%q,%d,%q,true)", gotID, gotSeq, gotSession, found, id, seq, peer.PeerSession)
	}
	// The upload figure needs a live peer view as well as the marker (an old or
	// missing view reads every local file as owed), so give this engine one.
	f.Engine.SetPeerIndex(peer.PeerID, nil, false)
	f.Engine.EndPeerViewExchange(peer.PeerID)
	if basis := f.Engine.PendingBasisState(); !basis.Up {
		t.Fatalf("the upload figure is still withheld after an acknowledged index: %s", basis.Reason)
	}

	// A stale ack (different generation) must not move the marker.
	a.noteIndexAck(peer, &protocol.IndexAckMsg{Folder: "data", IndexID: "other-generation", MaxSeq: seq + 1})
	if _, gotSeq2, _, _, _ := f.Ix.GetPeerIndex(peer.PeerID); gotSeq2 != seq {
		t.Fatalf("a stale ack moved the marker to %d, want it left at %d", gotSeq2, seq)
	}
}

// TestIndexAckNotSentToOldPeer: an older peer keeps the end-of-session marker it
// already relies on, so a rolling upgrade must not put a message on the wire
// that it would treat as unknown.
func TestIndexAckNotSentToOldPeer(t *testing.T) {
	a := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan *transfer.Conn, 1)
	go func() {
		c, err := transfer.Accept(ln)
		if err != nil {
			return
		}
		accepted <- c
	}()
	raw, err := dialRaw(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	peer := transfer.NewConn(raw)
	peer.PeerProtocol = protocol.Current - 1 // rev 2: no acks

	srv := <-accepted
	defer func() { _ = srv.Close() }()
	srv.PeerProtocol = protocol.Current - 1
	if err := a.ackIndex(srv, &protocol.IndexMsg{Folder: "data", IndexID: "x", MaxSeq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := raw.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if m, err := peer.Recv(); err == nil {
		t.Fatalf("old peer received %v; no ack may be sent below the ack revision", m.Type)
	}
}
