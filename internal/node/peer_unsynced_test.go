package node

import (
	"fmt"
	"net"
	"testing"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/protocol"
	"crosssync/internal/transfer"
)

// TestPeerUnsyncedStatePropagates covers the end-of-round announcement of
// each device's open unsynced set (MsgEvent, protocol rev 2): after a full
// round the peer must hold A's set (path + reason), and the next round must
// REPLACE it wholesale — an empty set has to arrive and clear the stored
// view, otherwise a peer that caught up would keep showing stale work.
func TestPeerUnsyncedStatePropagates(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
	chunkWrite(t, rootA, "shared.txt", "v1")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	// A is stuck on a path (open warn+ unsynced event, not a live entry, so
	// no round can resolve it): exactly what A must tell its peers about.
	if _, err := a.Events.Record(&events.Event{
		TS: time.Now(), Folder: "data", Path: "stuck.txt",
		Category: events.CatUnsynced, Severity: events.SevWarn,
		Reason: "cannot apply: file is locked/in use — retrying",
	}); err != nil {
		t.Fatal(err)
	}

	// Round 1: B initiates, A responds. A announces its set before closing;
	// B's serveUntil stores it.
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}
	got := b.PeerUnsynced(a.ID, "data")
	if len(got) != 1 {
		t.Fatalf("B's view of A's unsynced set = %+v, want exactly stuck.txt", got)
	}
	if got[0].Path != "stuck.txt" || got[0].Reason == "" {
		t.Fatalf("B's stored entry = %+v, want stuck.txt with A's reason", got[0])
	}
	// A has nothing outstanding, so it must have sent an empty set too.
	if aPeer := a.PeerUnsynced(b.ID, "data"); len(aPeer) != 0 {
		t.Fatalf("A's view of B's unsynced set should be empty, got %+v", aPeer)
	}

	// A recovers: the condition is gone. The next round must replace B's
	// stored set with an empty one (wholesale, not a merge).
	if err := a.Events.ResolveAutoCondition("data", "stuck.txt", events.CatUnsynced, "system"); err != nil {
		t.Fatal(err)
	}
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}
	after := b.PeerUnsynced(a.ID, "data")
	if after == nil {
		t.Fatal("round 2 should have replaced the stored set with an empty one, got nil (never received)")
	}
	if len(after) != 0 {
		t.Fatalf("B's view of A's unsynced set should be empty after A caught up, got %+v", after)
	}
}

// TestOldPeerNeverReceivesUnsyncedState is the rolling-upgrade contract for
// the new frame: a peer that advertises protocol rev 1 must never be sent a
// MsgEvent (it would be an unknown frame at the end of a round), and the
// session must complete exactly as it did before the feature existed.
func TestOldPeerNeverReceivesUnsyncedState(t *testing.T) {
	// A new RESPONDER with an old (rev 1) initiator: the responder must not
	// announce its set, and must still finish with the round's Close.
	t.Run("old-initiator", func(t *testing.T) {
		rootA := t.TempDir()
		a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
		chunkWrite(t, rootA, "f.txt", "x")
		if _, err := a.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		// Ensure the responder would have something to announce, so a
		// missing gate would definitely show up as a frame.
		if _, err := a.Events.Record(&events.Event{
			TS: time.Now(), Folder: "data", Path: "stuck.txt",
			Category: events.CatUnsynced, Severity: events.SevWarn, Reason: "cannot apply: locked",
		}); err != nil {
			t.Fatal(err)
		}

		ln, served := keepaliveResponder(t, a)
		raw, conn := keepaliveDial(t, ln.Addr().String())
		if err := raw.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			t.Fatal(err)
		}

		if err := conn.Send(&protocol.Message{Type: protocol.MsgHello, Hello: &protocol.Hello{
			DeviceID: 2, DeviceName: "old", SessionID: "legacy-1", Protocol: 1,
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

		// The responder reads our index first (Serve: recvFullIndexes then
		// sendIndexes), exactly as the pre-existing exchange does.
		if err := conn.Send(&protocol.Message{Type: protocol.MsgIndex, Index: &protocol.IndexMsg{
			Folder: "data", Update: false, Final: true,
		}}); err != nil {
			t.Fatal(err)
		}
		recvIndexFinal(t, raw, conn, "data")
		if err := conn.Send(&protocol.Message{Type: protocol.MsgPhaseDone}); err != nil {
			t.Fatal(err)
		}

		// Phase 1 is over. A rev-1 initiator expects Close next; an EventMsg
		// here is the frame the gate exists to prevent.
		for {
			m, err := conn.Recv()
			if err != nil {
				t.Fatalf("waiting for close: %v", err)
			}
			if m.Type == protocol.MsgPing {
				continue
			}
			if m.Type == protocol.MsgEvent {
				t.Fatalf("responder sent an EventMsg to a protocol-1 initiator: %+v", m.Event)
			}
			if m.Type != protocol.MsgClose {
				t.Fatalf("got %v, want close", m.Type)
			}
			break
		}
		if err := conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "done"}}); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-served:
			if err != nil {
				t.Fatalf("session with a protocol-1 initiator failed: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("Serve did not finish")
		}
	})

	// A new INITIATOR with an old (rev 1) responder: nothing may be sent to
	// it after the final Close handshake either.
	t.Run("old-responder", func(t *testing.T) {
		rootB := t.TempDir()
		b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
		chunkWrite(t, rootB, "g.txt", "y")
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Events.Record(&events.Event{
			TS: time.Now(), Folder: "data", Path: "stuck.txt",
			Category: events.CatUnsynced, Severity: events.SevWarn, Reason: "cannot apply: locked",
		}); err != nil {
			t.Fatal(err)
		}

		ln, err := transfer.Listen("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ln.Close() }()
		served := make(chan error, 1)
		go func() {
			c, err := transfer.Accept(ln)
			if err != nil {
				served <- err
				return
			}
			defer func() { _ = c.Close() }()
			served <- oldResponderSession(c)
		}()

		conn, err := transfer.Dial(ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		if _, err := b.SyncOnce(conn, 0); err != nil {
			t.Fatalf("round with a protocol-1 responder failed: %v", err)
		}
		select {
		case err := <-served:
			if err != nil {
				t.Fatalf("protocol-1 responder saw an unexpected frame: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("old responder session did not finish")
		}
		// Nothing can have been stored for a peer that never announced.
		if got := b.PeerUnsynced(99, "data"); got != nil {
			t.Fatalf("no state should be stored for a protocol-1 peer, got %+v", got)
		}
	})
}

// recvIndexFinal reads index frames until the one marked Final for folder.
func recvIndexFinal(t *testing.T, raw net.Conn, conn *transfer.Conn, folder string) {
	t.Helper()
	for {
		if err := raw.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
			t.Fatal(err)
		}
		m, err := conn.Recv()
		if err != nil {
			t.Fatalf("waiting for index of %q: %v", folder, err)
		}
		if m.Type != protocol.MsgIndex || m.Index == nil {
			t.Fatalf("got %v, want index", m.Type)
		}
		if m.Index.Final {
			return
		}
	}
}

// oldResponderSession speaks the pre-rev-2 responder sequence (what an old
// daemon does) against a new initiator: handshake, cluster exchange, index
// exchange, serve phase, Close — and then insists the only thing left is the
// initiator's Close.
func oldResponderSession(c *transfer.Conn) error {
	m, err := c.Recv()
	if err != nil {
		return err
	}
	if m.Type != protocol.MsgHello {
		return fmt.Errorf("expected hello, got %v", m.Type)
	}
	if err := c.Send(&protocol.Message{Type: protocol.MsgHello, Hello: &protocol.Hello{
		DeviceID: 99, DeviceName: "old-responder", SessionID: "legacy-2", Protocol: 1,
	}}); err != nil {
		return err
	}
	if _, err := c.Recv(); err != nil { // the initiator's cluster config
		return err
	}
	if err := c.Send(&protocol.Message{Type: protocol.MsgClusterConfig, Cluster: &protocol.ClusterConfig{
		Folders: []protocol.ClusterFolder{{ID: "data"}},
	}}); err != nil {
		return err
	}
	for { // the initiator's index, until the final frame
		m, err := c.Recv()
		if err != nil {
			return err
		}
		if m.Type != protocol.MsgIndex || m.Index == nil {
			return fmt.Errorf("expected index, got %v", m.Type)
		}
		if m.Index.Final {
			break
		}
	}
	if err := c.Send(&protocol.Message{Type: protocol.MsgIndex, Index: &protocol.IndexMsg{
		Folder: "data", Update: false, Final: true,
	}}); err != nil {
		return err
	}
	for { // serve phase
		m, err := c.Recv()
		if err != nil {
			return err
		}
		if m.Type == protocol.MsgPing {
			continue
		}
		if m.Type != protocol.MsgPhaseDone {
			return fmt.Errorf("expected phase-done, got %v", m.Type)
		}
		break
	}
	if err := c.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "done"}}); err != nil {
		return err
	}
	for {
		m, err := c.Recv()
		if err != nil {
			return err
		}
		if m.Type == protocol.MsgPing {
			continue
		}
		if m.Type == protocol.MsgEvent {
			return fmt.Errorf("initiator sent an EventMsg to a protocol-1 responder: %+v", m.Event)
		}
		if m.Type != protocol.MsgClose {
			return fmt.Errorf("expected close, got %v", m.Type)
		}
		return nil
	}
}
