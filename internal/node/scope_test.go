package node

import (
	"errors"
	"strings"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/events"
)

// TestSharedFoldersScoping verifies that a folder is only shared with a peer
// when its Peers list is empty or contains that peer's device id.
func TestSharedFoldersScoping(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Listen:  "",
		Folders: []config.Folder{
			{ID: "open", Path: t.TempDir(), ConflictPolicy: "conflict-copy"},
			{ID: "scoped", Path: t.TempDir(), ConflictPolicy: "conflict-copy", Peers: []uint64{2}},
		},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	// Peer 2 shares both folders.
	both := map[string]bool{"open": true, "scoped": true}
	got := n.sharedFolders(both, 2)
	if len(got) != 2 {
		t.Fatalf("peer 2 should share both folders, got %v", got)
	}
	// Peer 3 advertises both, but "scoped" excludes it.
	got = n.sharedFolders(both, 3)
	if len(got) != 1 || got[0] != "open" {
		t.Fatalf("peer 3 should only share 'open', got %v", got)
	}
	// Even if the peer does not advertise a folder we hold, nothing leaks.
	got = n.sharedFolders(map[string]bool{"open": true}, 3)
	if len(got) != 1 || got[0] != "open" {
		t.Fatalf("unexpected shared set: %v", got)
	}
}

// TestSendClusterScoping verifies that a folder scoped to another peer is
// never advertised to a peer it excludes.
func TestSendClusterScoping(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Listen:  "",
		Folders: []config.Folder{
			{ID: "open", Path: t.TempDir(), ConflictPolicy: "conflict-copy"},
			{ID: "scoped", Path: t.TempDir(), ConflictPolicy: "conflict-copy", Peers: []uint64{2}},
		},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	advertised := n.advertisedFolders(3)
	if len(advertised) != 1 || advertised[0] != "open" {
		t.Fatalf("peer 3 should only be advertised 'open', got %v", advertised)
	}
	advertised = n.advertisedFolders(2)
	if len(advertised) != 2 {
		t.Fatalf("peer 2 should be advertised both folders, got %v", advertised)
	}
}

// TestPeerFolderTracking verifies the node records which folders a peer
// advertises (from its cluster config) and exposes them for the control
// plane, so peers that lack a folder can be hidden from its peer list.
func TestPeerFolderTracking(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Listen:  "",
		Folders: []config.Folder{{ID: "media", Path: t.TempDir(), ConflictPolicy: "conflict-copy"}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	// Unknown peer: not seen yet.
	if fs, seen := n.PeerFolders(2); seen || fs != nil {
		t.Fatalf("peer 2 should be unknown, got seen=%v folders=%v", seen, fs)
	}

	// nas-02 advertises media + data; nas-03 only data.
	n.RecordPeerFolders(2, map[string]bool{"media": true, "data": true})
	n.RecordPeerFolders(3, map[string]bool{"data": true})

	fs, seen := n.PeerFolders(2)
	if !seen || len(fs) != 2 || fs[0] != "data" || fs[1] != "media" {
		t.Fatalf("peer 2 folders = %v (seen=%v)", fs, seen)
	}
	fs, seen = n.PeerFolders(3)
	if !seen || len(fs) != 1 || fs[0] != "data" {
		t.Fatalf("peer 3 folders = %v (seen=%v)", fs, seen)
	}
}

// TestProblemEventsThrottled verifies that a file failing to apply emits a
// durable unsynced event, that repeated failures with the same reason do
// not spam the store, and that a subsequent success resolves it.
func TestProblemEventsThrottled(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Listen:  "",
		Folders: []config.Folder{{ID: "media", Path: t.TempDir(), ConflictPolicy: "conflict-copy"}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	// First failure -> one unsynced event.
	n.reportProblem("media", "locked.mp4", errors.New("rename: file is being used by another process"))
	// Same reason again -> throttled, no new event.
	n.reportProblem("media", "locked.mp4", errors.New("rename: file is being used by another process"))
	// Different reason -> new event.
	n.reportProblem("media", "locked.mp4", errors.New("rename: permission denied"))

	evs, err := n.Events.Query(events.Filter{Folder: "media", Category: events.CatUnsynced})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 {
		t.Fatalf("expected 2 unsynced events (locked + permission), got %d: %+v", len(evs), evs)
	}
	// Both are open (badge-worthy) and carry actionable reasons.
	for _, e := range evs {
		if e.Resolution != events.ResOpen {
			t.Fatalf("unsynced event should be open: %+v", e)
		}
		if !strings.Contains(e.Reason, "permission") && !strings.Contains(e.Reason, "locked") {
			t.Fatalf("unexpected reason %q", e.Reason)
		}
	}

	// Success clears the condition (badge/attention drop it).
	n.clearProblem("media", "locked.mp4")
	open, err := n.Events.Query(events.Filter{Folder: "media", Category: events.CatUnsynced, OpenOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 0 {
		t.Fatalf("unsynced condition should be resolved after success: %+v", open)
	}
}
