package node

import (
	"testing"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/scanner"
)

// TestSyncRoundAutoResolvesConvergedUnsyncedEvents pins the fix for the
// unsynced event that never leaves the attention list: an open warn+
// "unsynced" event is only resolved on the success path of pullOne, so a
// path that becomes in sync WITHOUT a later pull of it (already applied,
// then modified locally; or simply nothing left to pull) kept its event
// open forever even though both devices were byte-identical.
//
// The round must resolve exactly the paths that are verifiably converged
// and leave every other one alone:
//   - converged.txt: pulled and live -> auto-resolved (resolution=3, history
//     kept),
//   - outstanding.txt: genuinely still to pull (the peer refuses blocks
//     while it writes) -> stays open,
//   - ghost.txt: not a live local entry at all -> stays open.
//
// It also pins the straggler case: a path that is absent from the index
// because its converged-delete tombstone was already pruned, carrying a
// peer-confirmed "no such file" (code 2) reason, must self-heal — that is
// the delete that used to sit open forever.
func TestSyncRoundAutoResolvesConvergedUnsyncedEvents(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	chunkWrite(t, rootA, "converged.txt", "same bytes on both devices")
	chunkWrite(t, rootA, "outstanding.txt", "still being written on a")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	// A's stability gate refuses blocks for the file it is still writing, so
	// B's pull of outstanding.txt is skipped and the path stays in NeedPulls
	// after the round — a genuinely open condition, not a stale one.
	a.Folders["data"].Engine.SetInFlight([]scanner.InFlight{{Name: "outstanding.txt", Size: 33}})

	for _, p := range []string{"converged.txt", "outstanding.txt", "ghost.txt"} {
		if _, err := b.Events.Record(&events.Event{
			TS: time.Now(), Folder: "data", Path: p,
			Category: events.CatUnsynced, Severity: events.SevWarn,
			Reason: "cannot apply: file is locked/in use — retrying",
		}); err != nil {
			t.Fatal(err)
		}
	}
	// straggler.txt: absent from the index (its converged-delete tombstone
	// was pruned) and the reason is a peer-confirmed code 2 — the delete
	// that used to sit open forever. It must self-heal.
	if _, err := b.Events.Record(&events.Event{
		TS: time.Now(), Folder: "data", Path: "straggler.txt",
		Category: events.CatUnsynced, Severity: events.SevWarn,
		Reason: "cannot apply: block request failed: code 2",
	}); err != nil {
		t.Fatal(err)
	}

	if err := chunkPipe(t, a, b); err != nil {
		t.Fatal(err)
	}

	// Preconditions: the guards must be what kept the open events, not a
	// test that accidentally converged everything.
	if pulls, err := b.Folders["data"].Engine.NeedPulls(); err != nil {
		t.Fatal(err)
	} else if !containsName(pulls, "outstanding.txt") {
		t.Fatalf("outstanding.txt should still be a needed pull, got %v", pulls)
	}
	if _, ok, err := b.Folders["data"].Ix.Get("converged.txt"); err != nil || !ok {
		t.Fatalf("converged.txt should be a live local entry (ok=%v err=%v)", ok, err)
	}

	openUnsynced := func(path string) []events.Event {
		t.Helper()
		evs, err := b.Events.Query(events.Filter{Folder: "data", Path: path,
			Category: events.CatUnsynced, OpenOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		return evs
	}

	// (1) Converged: auto-resolved (resolution=3) and still in history.
	if evs := openUnsynced("converged.txt"); len(evs) != 0 {
		t.Fatalf("converged.txt still has %d open unsynced event(s): %+v", len(evs), evs)
	}
	all, err := b.Events.Query(events.Filter{Folder: "data", Path: "converged.txt",
		Category: events.CatUnsynced})
	if err != nil {
		t.Fatal(err)
	}
	auto := 0
	for _, e := range all {
		if e.Resolution == events.ResAutoResolved {
			auto++
		}
	}
	if auto == 0 {
		t.Fatalf("converged.txt event should be kept as auto-resolved history, got %+v", all)
	}

	// (2) Still needed from the peer: stays open.
	if evs := openUnsynced("outstanding.txt"); len(evs) != 1 {
		t.Fatalf("outstanding.txt should still have 1 open unsynced event, got %d: %+v", len(evs), evs)
	}

	// (3) Not a live local entry with a HARD reason (locked): stays open (a
	// tombstoned/missing path with an unresolved condition is not evidence of
	// being in sync, and must keep being visible).
	if evs := openUnsynced("ghost.txt"); len(evs) != 1 {
		t.Fatalf("ghost.txt should still have 1 open unsynced event, got %d: %+v", len(evs), evs)
	}

	// (4) Straggler: absent from the index but the reason is a peer-confirmed
	// code 2 (a converging delete whose tombstone was pruned) -> self-heals.
	if evs := openUnsynced("straggler.txt"); len(evs) != 0 {
		t.Fatalf("straggler.txt (peer-confirmed code 2) should have self-healed, still %d open: %+v", len(evs), evs)
	}
}

func containsName(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}
