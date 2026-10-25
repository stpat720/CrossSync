package node

import (
	"strings"
	"testing"

	"crosssync/internal/protocol"
)

// TestTransfersPausedStopsDataNotScanning pins the two pause levels.
//
// Operators need both: "stop using my link" (keep the index current, keep
// monitoring the disk) and "stop touching my disk" (the existing full pause).
// Pausing transfers must therefore leave scanning alone — the file is indexed and
// held in the index, ready to go — and must move no bytes in either direction,
// including refusing to serve blocks to a peer that asks. Resuming must transfer
// without a re-walk and without a conflict copy.
func TestTransfersPausedStopsDataNotScanning(t *testing.T) {
	f := newRenameFixture(t, 3)
	a := f.a.Folders["data"]
	b := f.b.Folders["data"]

	if err := f.a.SetFolderTransfersPaused("data", true); err != nil {
		t.Fatal(err)
	}
	if !a.TransfersPaused() {
		t.Fatal("folder A should report transfers paused")
	}
	downB := b.Engine.Stats.BytesDown.Load()

	// A new file on A while transfers are paused: the scanner must still see it.
	rel := "src/while-paused.bin"
	body := strings.Repeat("PP", 4096)
	writeAt(t, f.rootA, rel, body)
	mustScan(t, f.a)
	f.clock += 12
	mustScan(t, f.a)

	if fi, ok, err := a.Ix.Get(rel); err != nil || !ok || fi.Deleted {
		t.Fatalf("scanning stopped while transfers were paused: %v ok=%v err=%v", rel, ok, err)
	}

	// A round must move nothing at all.
	gateSession(t, f.a, f.b)
	if got := b.Engine.Stats.BytesDown.Load(); got != downB {
		t.Fatalf("B downloaded %d bytes while transfers were paused", got-downB)
	}
	if _, ok, _ := b.Ix.Get(rel); ok {
		t.Fatal("B received the file while transfers were paused")
	}

	// The peer asking for a block must be refused, not served.
	resp := f.a.serveBlock(&protocol.Request{ID: 1, Folder: "data", Name: rel, Offset: 0, Size: 4096})
	if resp.Code == protocol.RespNoError {
		t.Fatal("paused node served a block to a peer")
	}

	// Resume: the pending file must transfer, from the index that already knew
	// about it (no re-walk needed).
	if err := f.a.SetFolderTransfersPaused("data", false); err != nil {
		t.Fatal(err)
	}
	gateSession(t, f.a, f.b)
	gateSession(t, f.a, f.b)
	if got := readFile(t, f.rootB, rel); got != body {
		t.Fatalf("after resume B has %d bytes, want %d", len(got), len(body))
	}
	if a.TransfersPaused() {
		t.Fatal("folder A still reports transfers paused after resume")
	}
}

// TestFullPauseImpliesTransfersPaused: the full pause (no scanning) must also
// stop data movement — otherwise a paused folder could still be served to a peer
// that already knew about its files.
func TestFullPauseImpliesTransfersPaused(t *testing.T) {
	f := newRenameFixture(t, 2)
	a := f.a.Folders["data"]
	if err := f.a.SetFolderPaused("data", true); err != nil {
		t.Fatal(err)
	}
	if !a.TransfersPaused() {
		t.Fatal("a fully paused folder must report transfers paused")
	}
	if err := f.a.SetFolderPaused("data", false); err != nil {
		t.Fatal(err)
	}
	if a.TransfersPaused() {
		t.Fatal("resuming the folder must allow transfers again")
	}
}
