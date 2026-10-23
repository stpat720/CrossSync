package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/protocol"
)

// TestServeBlockRejectsBadGeometry pins the bounds check on the serving side.
// Size and Offset arrive from the PEER, and serveBlock allocates
//
//	buf := make([]byte, req.Size)
//
// so an unchecked request for a 2 GB "block" is a one-frame OOM kill of the
// daemon. A legal block is never larger than hash.MaxBlockSize and never starts
// at a negative offset; anything else must be refused before allocating.
func TestServeBlockRejectsBadGeometry(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	chunkWrite(t, root, "f.txt", "hello")
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	bad := []struct {
		name string
		req  *protocol.Request
	}{
		{"huge size", &protocol.Request{Folder: "data", Name: "f.txt", Size: 1 << 30}},
		{"negative size", &protocol.Request{Folder: "data", Name: "f.txt", Size: -5}},
		{"zero size", &protocol.Request{Folder: "data", Name: "f.txt", Size: 0}},
		{"negative offset", &protocol.Request{Folder: "data", Name: "f.txt", Offset: -1, Size: 4}},
	}
	for _, b := range bad {
		resp := n.serveBlock(b.req)
		if resp.Code == protocol.RespNoError || resp.Data != nil {
			t.Fatalf("%s: serveBlock served %d bytes (code %d), want a refusal",
				b.name, len(resp.Data), resp.Code)
		}
	}

	// A well-formed request still works, byte for byte.
	resp := n.serveBlock(&protocol.Request{Folder: "data", Name: "f.txt", Offset: 0, Size: 5})
	if resp.Code != protocol.RespNoError || string(resp.Data) != "hello" {
		t.Fatalf("valid block = code %d, %q; want code 0, %q", resp.Code, resp.Data, "hello")
	}
}

// TestPeerSessionIsExclusive pins the one-round-per-peer slot. Overlapping
// inbound and outbound rounds with the same peer both stage the same paths to
// the same deterministic .part temp; the winner renames it into place and the
// loser's commit/chtimes hits a vanished file (the live chtimes churn). The
// slot must be exclusive per peer, independent across peers, and re-usable
// after release.
func TestPeerSessionIsExclusive(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())

	release7, ok := n.acquirePeerSession(7, 0)
	if !ok {
		t.Fatal("first claim for peer 7 was refused")
	}
	if _, ok := n.acquirePeerSession(7, 0); ok {
		t.Fatal("second concurrent claim for peer 7 was allowed")
	}
	// Another peer is unaffected.
	release8, ok := n.acquirePeerSession(8, 0)
	if !ok {
		t.Fatal("peer 8 must not be blocked by peer 7")
	}

	// A bounded wait gives up instead of blocking forever.
	start := time.Now()
	if _, ok := n.acquirePeerSession(8, 50*time.Millisecond); ok {
		t.Fatal("bounded wait claimed a held slot")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("bounded wait took %s, want ~50ms", d)
	}

	// Release makes the slot claimable again.
	release7()
	release, ok := n.acquirePeerSession(7, 0)
	if !ok {
		t.Fatal("peer 7 stayed locked after release")
	}
	release()
	release8()
}

// TestSyncWithPeerSkipsBusyPeer pins the outbound half: when a round with a
// peer is already running (typically that peer's own inbound session, which
// syncs both directions anyway), the outbound attempt returns cleanly instead
// of dialling. A skip must NOT look like a failure — the peer must not be
// pushed into backoff over it.
func TestSyncWithPeerSkipsBusyPeer(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	// A peer whose address cannot possibly connect: if the skip is missing the
	// call fails instead of returning cleanly.
	n.Cfg.Peers = []config.Peer{{ID: 2, Name: "b", Addresses: []string{"127.0.0.1:1"}}}

	release, ok := n.acquirePeerSession(2, 0)
	if !ok {
		t.Fatal("could not claim the slot for the test")
	}
	defer release()

	synced, err := n.SyncWithPeer(2)
	if err != nil || synced != 0 {
		t.Fatalf("busy peer: SyncWithPeer = (%d, %v), want (0, nil) — a skip is not a failure", synced, err)
	}
	if n.SyncRunning() {
		t.Fatal("a skipped round must not be reported as a running sync")
	}
}

// TestAddFolderRejectsOverlappingRoots pins the folder-root rule: two folders
// over one tree (or one inside the other) index the same files twice, watch
// the same events twice and fight over tombstones. Sibling trees stay legal.
func TestAddFolderRejectsOverlappingRoots(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir()) // owns "data" at root

	if _, err := n.AddFolder(config.Folder{ID: "same", Path: root}); err == nil {
		t.Fatal("a folder over the same root must be refused")
	}
	sub := filepath.Join(root, "sub", "deeper")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := n.AddFolder(config.Folder{ID: "nested", Path: sub}); err == nil {
		t.Fatal("a folder nested inside another folder's root must be refused")
	}
	if _, err := n.AddFolder(config.Folder{ID: "sibling", Path: t.TempDir()}); err != nil {
		t.Fatalf("a sibling tree must be allowed: %v", err)
	}
}
