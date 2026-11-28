package control_test

import (
	"strings"
	"testing"

	"crosssync/internal/control"
)

// TestStatusShowsLiveRoundProgress pins the visibility fix for a long round: the
// pending counters are a snapshot taken when the round planned (recomputing them
// reads the whole index, so the refresher stays out of a live round's way), and
// a round on a million-file folder can run for hours. Both nodes were showing
// "0 pending" while 730 MB moved. Status must publish what the round is actually
// doing: remaining files/bytes AND the live round block.
func TestStatusShowsLiveRoundProgress(t *testing.T) {
	n, _ := testNode(t)
	f := n.Folders["data"]
	if f.Engine == nil {
		t.Fatal("folder data has no engine")
	}
	const peer = uint64(2)

	// A round is in flight and has planned 10 files / 5 MB; two are done.
	f.Engine.BeginRound(peer)
	f.Engine.BeginRoundWork(peer, 10, 5<<20)
	f.Engine.RoundFileDone(peer, 1<<20)
	f.Engine.RoundFileDone(peer, 0)

	svc := control.New(n, "test")
	st := svc.Status()
	var fs *control.FolderStatus
	for i := range st.Folders {
		if st.Folders[i].ID == "data" {
			fs = &st.Folders[i]
		}
	}
	if fs == nil {
		t.Fatal("data folder missing from status")
	}
	if fs.Round == nil {
		t.Fatal("status carries no round block while a round is in flight")
	}
	if fs.Round.FilesTotal != 10 || fs.Round.FilesDone != 2 || fs.Round.BytesTotal != 5<<20 || fs.Round.BytesDone != 1<<20 {
		t.Fatalf("round = %+v, want totals 10/5MiB and done 2/1MiB", *fs.Round)
	}
	if fs.PendingFiles != 8 || fs.PendingBytes != 4<<20 {
		t.Fatalf("pending = %d files / %d bytes while a round runs, want 8 / %d (the round's remaining work)", fs.PendingFiles, fs.PendingBytes, 4<<20)
	}
	if !fs.PendingVerified {
		t.Fatal("a live round's own list is authoritative; pending must not be flagged unverified")
	}
	if !strings.Contains(fs.PendingReason, "syncing now: 2 of 10") {
		t.Fatalf("pending_reason = %q, want it to say what the round has moved", fs.PendingReason)
	}

	// The round ending hands the numbers back to the pending snapshot.
	f.Engine.EndRound(peer)
	st = svc.Status()
	for i := range st.Folders {
		if st.Folders[i].ID == "data" {
			fs = &st.Folders[i]
		}
	}
	if fs.Round != nil {
		t.Fatalf("round block still present after the round ended: %+v", *fs.Round)
	}
}
