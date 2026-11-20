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

// TestFolderSessionIsExclusive pins the per-FOLDER session slot. Two rounds on
// the SAME folder with the same peer both stage the same paths to the same
// deterministic .part temp; the winner renames it into place and the loser's
// commit/chtimes hits a vanished file (the live chtimes churn). So that slot must
// stay exclusive, inbound or outbound.
//
// DIFFERENT folders are the point of the change: separate folders are separate
// syncs, each with its own session, connection and pipeline, so a folder must
// never wait for another folder of the same peer. They are independent slots now.
// The slot must also be re-usable after release, and releasing twice must not
// hand the slot to someone else.
func TestFolderSessionIsExclusive(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())

	release7, ok := n.acquireFolderSession(7, "data", 0)
	if !ok {
		t.Fatal("first claim for (peer 7, data) was refused")
	}
	if _, ok := n.acquireFolderSession(7, "data", 0); ok {
		t.Fatal("a second concurrent claim for the SAME folder and peer was allowed")
	}
	// Another folder of the same peer is a different session: it must not wait.
	otherRel, ok := n.acquireFolderSession(7, "media", 0)
	if !ok {
		t.Fatal("a different folder of the same peer must not be blocked by (7, data)")
	}
	// Another peer on the same folder is unaffected too.
	release8, ok := n.acquireFolderSession(8, "data", 0)
	if !ok {
		t.Fatal("peer 8 must not be blocked by peer 7 on the same folder")
	}

	// A bounded wait gives up instead of blocking forever.
	start := time.Now()
	if _, ok := n.acquireFolderSession(8, "data", 50*time.Millisecond); ok {
		t.Fatal("bounded wait claimed a held slot")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("bounded wait took %s, want ~50ms", d)
	}

	// Release makes the slot claimable again, and a double release must not free
	// it twice (which would let two rounds run on it at once).
	release7()
	release7()
	release, ok := n.acquireFolderSession(7, "data", 0)
	if !ok {
		t.Fatal("(peer 7, data) stayed locked after release")
	}
	if _, ok := n.acquireFolderSession(7, "data", 0); ok {
		t.Fatal("a double release freed the same slot twice")
	}
	release()
	otherRel()
	release8()
}

// TestSyncWithPeerSkipsBusyFolder pins the outbound half: when a round on a
// folder is already running with that peer (typically that peer's own inbound
// session on it, which syncs both directions anyway), the outbound attempt for
// that folder returns cleanly instead of dialling. A skip must NOT look like a
// failure — the peer must not be pushed into backoff over it.
//
// The skip is per FOLDER now: with the one folder this node offers held, every
// candidate is busy, which is the shape this test exercises.
func TestSyncWithPeerSkipsBusyFolder(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	// The folder must have finished its first pass, or it is not advertised and
	// there is nothing to skip (the call would fall back to a reachability probe).
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	// A peer whose address cannot possibly connect: if the skip is missing the
	// call fails instead of returning cleanly.
	n.Cfg.Peers = []config.Peer{{ID: 2, Name: "b", Addresses: []string{"127.0.0.1:1"}}}

	release, ok := n.acquireFolderSession(2, "data", 0)
	if !ok {
		t.Fatal("could not claim the slot for the test")
	}
	defer release()

	synced, err := n.SyncWithPeer(2)
	if err != nil || synced != 0 {
		t.Fatalf("busy folder: SyncWithPeer = (%d, %v), want (0, nil) — a skip is not a failure", synced, err)
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
