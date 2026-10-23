package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crosssync/internal/events"
)

// TestBlockErrorDoesNotAbortRound pins the second half of the live-share fix.
//
// A per-file difficulty on the WIRE must not be treated as a transport
// failure. serveBlock answers the generic code 1 (RespGeneric) when the
// source's own disk refuses the file — unreadable, a directory where a file
// is indexed, a driver error. The puller must record ONE visible, retrying
// unsynced event for that path and keep pulling everything else:
//
//   - the round must not return an error (an error here propagates to
//     SyncOnce and marks a healthy peer offline + backoff-reconnects it);
//   - the rest of the folder must still converge in the same round;
//   - the failure must stay VISIBLE as an open unsynced event whose reason
//     says what happened and that it is being retried (never a raw OS error).
func TestBlockErrorDoesNotAbortRound(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir()) // serves
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir()) // pulls

	chunkWrite(t, rootA, "good.txt", "settled bytes")
	chunkWrite(t, rootA, "unreadable.txt", "bytes the source disk will refuse")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// Replace the indexed file with a DIRECTORY of the same name: the open
	// succeeds, every block read fails, so serveBlock answers code 1.
	// (Removing it instead exercises the code 2 peer-confirmed path covered
	// by TestBlockNoSuchFileIsTransient.)
	p := filepath.Join(rootA, "unreadable.txt")
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := chunkPipe(t, a, b); err != nil {
		t.Fatalf("one unreadable file must not abort the round (this marks a healthy peer offline): %v", err)
	}

	// Everything else in the folder converged in that same round.
	if _, ok, err := b.Folders["data"].Ix.Get("good.txt"); err != nil || !ok {
		t.Fatalf("good.txt must still be pulled despite unreadable.txt failing: ok=%v err=%v", ok, err)
	}
	if got, err := os.ReadFile(filepath.Join(rootB, "good.txt")); err != nil || string(got) != "settled bytes" {
		t.Fatalf("good.txt content on b = %q, %v; want %q", got, err, "settled bytes")
	}

	// The refused file is not applied locally, but it stays visible.
	if _, ok, err := b.Folders["data"].Ix.Get("unreadable.txt"); err != nil || ok {
		t.Fatalf("unreadable.txt must not be indexed on b: ok=%v err=%v", ok, err)
	}
	evs, err := b.Events.Query(events.Filter{
		Folder: "data", Path: "unreadable.txt", Category: events.CatUnsynced, OpenOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("want exactly 1 open unsynced event for unreadable.txt, got %d: %+v", len(evs), evs)
	}
	reason := evs[0].Reason
	if !strings.Contains(reason, "retry") {
		t.Fatalf("reason must tell the operator it is being retried, got %q", reason)
	}
	if strings.Contains(reason, "code 1") || strings.Contains(reason, "cannot apply") {
		t.Fatalf("reason must be actionable, not a raw wire/OS error: %q", reason)
	}
}
