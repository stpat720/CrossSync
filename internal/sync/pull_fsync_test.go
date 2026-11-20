package sync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crosssync/internal/hash"
	"crosssync/internal/index"
	"crosssync/internal/version"
)

// pull_fsync is a DURABILITY knob with default ON, read at commit time.
//
// These tests drive real pulls (peer view -> StartPull -> ReceiveBlock -> Commit)
// and read the fsync counter the commit path bumps, so "the knob is honoured" is
// an assertion about behaviour rather than about a config field: with the knob on
// every committed file fsyncs, with it off none does, and a change lands on the
// next commit instead of requiring the folder to be rebuilt.
func offerPeerFile(t *testing.T, n *testNode, files map[string]string) map[string]*index.FileInfo {
	t.Helper()
	out := make(map[string]*index.FileInfo, len(files))
	list := make([]*index.FileInfo, 0, len(files))
	for name, body := range files {
		probe := filepath.Join(t.TempDir(), "peer.bin")
		if err := os.WriteFile(probe, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		bs := hash.BlockSizeForSize(int64(len(body)))
		_, blocks, err := hash.FileHashes(probe, bs)
		if err != nil {
			t.Fatal(err)
		}
		target := &index.FileInfo{
			Name: name, Type: index.TypeFile,
			Size: int64(len(body)), BlockSize: int32(bs), Blocks: blocks,
			ModifiedS: 1900000000, Version: version.New().Bump(2),
		}
		out[name] = target
		list = append(list, target)
	}
	n.engine.SetPeerIndex(2, list, false)
	return out
}

// pullAndFinish pulls name from peer 2 and commits it, supplying every block
// from body (what the wire would carry).
func pullAndFinish(t *testing.T, n *testNode, name, body string) {
	t.Helper()
	p, err := n.engine.StartPull(name, 2)
	if err != nil {
		t.Fatalf("start pull: %v", err)
	}
	for _, need := range p.NeedBlocks() {
		if err := p.ReceiveBlock(need.Index, []byte(body[need.Offset:int64(need.Size)+need.Offset])); err != nil {
			t.Fatalf("receive block %d: %v", need.Index, err)
		}
	}
	if _, err := p.Finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
}

// An engine with no hook at all — the state of every folder that never touches
// the knob, including one built by code that predates it — fsyncs.
func TestPullFsyncDefaultsOnWithoutAHook(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	body := strings.Repeat("durable-", 400)
	offerPeerFile(t, n, map[string]string{"dst/a.bin": body})

	pullAndFinish(t, n, "dst/a.bin", body)

	if got := n.engine.Stats.PullFsyncs.Load(); got != 1 {
		t.Fatalf("fsyncs = %d for one committed pull with no hook, want 1: an absent hook must mean ON", got)
	}
	if got := n.engine.Stats.FilesDown.Load(); got != 1 {
		t.Fatalf("files_down = %d, want the pull to have committed", got)
	}
}

// The knob off: the commit still happens (file on disk, entry in the index), it
// just does not fsync.
func TestPullFsyncOffSkipsTheFsyncButStillCommits(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	off := false
	n.engine.PullFsync = func() bool { return off }
	body := strings.Repeat("volatile-", 400)
	offerPeerFile(t, n, map[string]string{"dst/a.bin": body})

	pullAndFinish(t, n, "dst/a.bin", body)

	if got := n.engine.Stats.PullFsyncs.Load(); got != 0 {
		t.Fatalf("fsyncs = %d with pull_fsync off, want 0", got)
	}
	// Everything else the commit does must still have happened.
	if got := n.fileContent(t, "dst/a.bin"); got != body {
		t.Fatalf("committed file is %d bytes, want %d", len(got), len(body))
	}
	if _, ok := liveEntryOf(t, n, "dst/a.bin"); !ok {
		t.Fatal("the pull is not in the index: turning fsync off must not change what a commit means")
	}
	if got := n.engine.Stats.FilesDown.Load(); got != 1 {
		t.Fatalf("files_down = %d, want 1", got)
	}
}

// Read at COMMIT time, not cached when the folder was created: flipping the knob
// between two pulls of the same engine changes the second one.
func TestPullFsyncIsReadAtCommitTime(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	on := true
	n.engine.PullFsync = func() bool { return on }
	body := strings.Repeat("either-way-", 400)
	offerPeerFile(t, n, map[string]string{"dst/a.bin": body, "dst/b.bin": body})

	pullAndFinish(t, n, "dst/a.bin", body)
	if got := n.engine.Stats.PullFsyncs.Load(); got != 1 {
		t.Fatalf("fsyncs after the first pull = %d, want 1", got)
	}

	on = false // one PUT /api/folders/{id} later
	pullAndFinish(t, n, "dst/b.bin", body)
	if got := n.engine.Stats.PullFsyncs.Load(); got != 1 {
		t.Fatalf("fsyncs after flipping the knob = %d, want 1 (the second pull must not fsync: the knob is read per commit)", got)
	}
	if got := n.engine.Stats.FilesDown.Load(); got != 2 {
		t.Fatalf("files_down = %d, want both pulls committed", got)
	}
}
