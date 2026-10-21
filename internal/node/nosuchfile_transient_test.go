package node

import (
	"os"
	"testing"

	"crosssync/internal/events"
)

// TestBlockNoSuchFileIsTransient pins the fix for the two failure modes the
// live share exhibited (a peer going offline mid-transfer, and unsynced
// events that never left the attention list):
//
// The source advertises a file that is already gone from its disk by the time
// the puller asks for blocks (a delete landed under an in-flight pull). The
// source's serveBlock answers RespNoSuchFile (code 2), which is the SAME
// class of transient signal as RespInvalidFile (code 3) — a "retry later",
// not a failure. The puller must skip that one file and KEEP THE ROUND ALIVE:
//
//   - the round must not return an error (an error here aborts the whole
//     SyncOnce, which marks a healthy peer offline and backoff-reconnects it);
//   - the puller must not record a permanent "cannot apply: block request
//     failed: code 2" event for an already-converged path (that is what
//     re-raised forever on the live share).
func TestBlockNoSuchFileIsTransient(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir()) // serves
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir()) // pulls

	// A settles one good file and one file that will be gone by pull time.
	chunkWrite(t, rootA, "good.txt", "settled bytes")
	chunkWrite(t, rootA, "gone.txt", "bytes that vanish under the transfer")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// The delete lands on the source AFTER it was indexed/advertised but
	// before the puller can take the blocks — exactly the in-flight race.
	if err := os.Remove(rootA + "/gone.txt"); err != nil {
		t.Fatal(err)
	}

	// The round must complete cleanly: skipping the gone file is not an error.
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatalf("round must survive a file going gone mid-pull (this aborts the peer and marks it offline): %v", err)
	}

	// The good file must still transfer despite the gone file's refusal.
	if _, ok, err := b.Folders["data"].Ix.Get("good.txt"); err != nil || !ok {
		t.Fatalf("good.txt should be pulled and indexed on b despite the gone.txt refusal: ok=%v err=%v", ok, err)
	}

	// No permanent unsynced event for the vanished path (the live-share bug).
	evs, err := b.Events.Query(events.Filter{
		Folder: "data", Path: "gone.txt", Category: events.CatUnsynced, OpenOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 0 {
		t.Fatalf("gone.txt left %d open unsynced event(s) for an already-converged path: %+v", len(evs), evs)
	}
}
