package node

import (
	"errors"
	"net"
	"testing"
	"time"

	"crosssync/internal/protocol"
	"crosssync/internal/transfer"
)

// TestUnsyncedAnnouncementGatedByProtocolRev pins the rolling-upgrade contract:
// the end-of-round unsynced-state announcement (MsgEvent, protocol rev 2) is
// sent ONLY to a peer that advertised rev >= 2. A rev-1 peer (an older daemon
// mid-upgrade) must see a round that is byte-for-byte the old exchange — no
// unexpected frame near the end of the session. Receiving is always accepted,
// which is tested by the peer-unsynced tests.
func TestUnsyncedAnnouncementGatedByProtocolRev(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	// One open unsynced condition gives the announcement something to say.
	n.reportProblem("data", "locked.txt", errors.New("permission denied"))

	for _, tc := range []struct {
		rev  int
		want bool
	}{
		{rev: 1, want: false},
		{rev: 2, want: true},
	} {
		client, peer := net.Pipe()
		conn := transfer.NewConn(client)
		conn.PeerProtocol = tc.rev
		go func() {
			_ = n.sendUnsyncedStates(conn, []string{"data"})
			_ = client.Close()
		}()
		if err := peer.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		m, err := protocol.ReadMessage(peer)
		_ = peer.Close()
		if !tc.want {
			if err == nil {
				t.Fatalf("rev %d peer received a frame (%v), want a silent round", tc.rev, m.Type)
			}
			continue
		}
		if err != nil {
			t.Fatalf("rev %d peer: expected an MsgEvent announcement, got %v", tc.rev, err)
		}
		if m.Type != protocol.MsgEvent || m.Event == nil {
			t.Fatalf("rev %d peer: frame = %v, want MsgEvent", tc.rev, m.Type)
		}
		if m.Event.Folder != "data" || len(m.Event.Entries) != 1 || m.Event.Entries[0].Path != "locked.txt" {
			t.Fatalf("rev %d peer: announcement = %+v, want the locked.txt entry for folder data", tc.rev, m.Event)
		}
	}
}
