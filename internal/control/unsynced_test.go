package control_test

import (
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/control"
	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/node"
	"crosssync/internal/protocol"
	"crosssync/internal/version"
)

// TestAttentionDirTag verifies the direction tag on attention rows: it is
// derived from the same NeedPulls/PendingUploads data that already sets
// Pending/PendingUp, so the four situations must come out as
// down / up / both / "" (event-only).
func TestAttentionDirTag(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "test")
	ix := n.Folders["data"].Ix
	folder := n.Folders["data"]

	local := version.New().Bump(1)
	for _, name := range []string{"up.txt", "both.txt"} {
		if err := ix.Put(&index.FileInfo{Name: name, Size: 4, Type: index.TypeFile, Version: local}); err != nil {
			t.Fatal(err)
		}
	}
	// Peer 98 knows nothing about our files -> our live files are uploads.
	folder.Engine.SetPeerIndex(98, nil, false)
	// Peer 99 holds a NEWER both.txt -> we need that one from a peer, and
	// 98 still needs our copy: both directions at once. down.txt exists
	// only on 99 -> download only.
	folder.Engine.SetPeerIndex(99, []*index.FileInfo{
		{Name: "both.txt", Size: 9, Type: index.TypeFile, Version: local.Bump(99)},
		{Name: "down.txt", Size: 5, Type: index.TypeFile, Version: version.New().Bump(99)},
	}, false)
	// An event-only row: nothing pending in either direction.
	if _, err := n.Events.Record(&events.Event{
		TS: time.Now(), Folder: "data", Path: "eventonly.txt",
		Category: events.CatUnsynced, Severity: events.SevWarn, Reason: "cannot apply: locked",
	}); err != nil {
		t.Fatal(err)
	}

	att, err := svc.Attention("data")
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]control.AttentionFile{}
	for _, a := range att {
		byName[a.Name] = a
	}
	want := map[string]string{
		"down.txt":      "down",
		"up.txt":        "up",
		"both.txt":      "both",
		"eventonly.txt": "",
	}
	for name, wantDir := range want {
		a, ok := byName[name]
		if !ok {
			t.Fatalf("%s missing from attention list: %+v", name, att)
		}
		if a.Dir != wantDir {
			t.Fatalf("%s dir = %q, want %q (row: %+v)", name, a.Dir, wantDir, a)
		}
	}
}

// TestFolderStatusShowsPeerUnsynced verifies the API surface for Feature 2:
// a folder's status carries the open unsynced set each peer advertised,
// and a peer that reports an empty set disappears from it.
func TestFolderStatusShowsPeerUnsynced(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{{ID: "data", Path: t.TempDir(), ConflictPolicy: "conflict-copy", SettleScans: 1}},
		Peers: []config.Peer{
			{ID: 2, Name: "nas-02", Addresses: []string{"127.0.0.1:1"}},
			{ID: 3, Name: "nas-03", Addresses: []string{"127.0.0.1:2"}},
		},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Close() }()
	svc := control.New(n, "test")

	n.StorePeerUnsynced(2, "data", []protocol.UnsyncedEntry{
		{Path: "b/locked.bin", Reason: "cannot apply: file is locked/in use — retrying", Dir: "down"},
	})
	n.StorePeerUnsynced(3, "data", []protocol.UnsyncedEntry{
		{Path: "a/upload.bin", Reason: "waiting to upload", Dir: "up"},
	})

	fs := svc.Folders()
	if len(fs) != 1 {
		t.Fatalf("unexpected folders: %+v", fs)
	}
	if len(fs[0].PeerUnsynced) != 2 {
		t.Fatalf("peer_unsynced = %+v, want one block per peer", fs[0].PeerUnsynced)
	}
	// Sorted by peer name: nas-02 then nas-03.
	if fs[0].PeerUnsynced[0].PeerName != "nas-02" || fs[0].PeerUnsynced[0].Entries[0].Path != "b/locked.bin" ||
		fs[0].PeerUnsynced[0].Entries[0].Dir != "down" {
		t.Fatalf("unexpected first peer block: %+v", fs[0].PeerUnsynced[0])
	}
	if fs[0].PeerUnsynced[0].Entries[0].Reason == "" {
		t.Fatal("the peer's reason must survive the API mapping")
	}
	if fs[0].PeerUnsynced[1].PeerName != "nas-03" || fs[0].PeerUnsynced[1].Entries[0].Dir != "up" {
		t.Fatalf("unexpected second peer block: %+v", fs[0].PeerUnsynced[1])
	}

	// Wholesale replace with an empty set removes the peer from the view.
	n.StorePeerUnsynced(2, "data", nil)
	fs = svc.Folders()
	if len(fs[0].PeerUnsynced) != 1 || fs[0].PeerUnsynced[0].PeerName != "nas-03" {
		t.Fatalf("peer that caught up should drop out: %+v", fs[0].PeerUnsynced)
	}

	// Removing the peer drops everything we held for it.
	n.CleanupPeerState(3)
	if got := svc.Folders()[0].PeerUnsynced; len(got) != 0 {
		t.Fatalf("removed peer's view should be dropped: %+v", got)
	}
}
