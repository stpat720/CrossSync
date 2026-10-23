package sync

import (
	"testing"

	"crosssync/internal/events"
	"crosssync/internal/index"
)

// TestDirectoryDeleteEmitsOneEventPerPath pins the event accounting of a
// directory delete. A directory tombstone removes its children itself AND the
// children carry their own tombstones, so the same path is applied twice in one
// pass; the second apply must be a silent no-op instead of a second
// "deleted (remote)" event (live: 201 events for 101 paths, every file doubled).
func TestDirectoryDeleteEmitsOneEventPerPath(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "d/a.txt", "a")
	n.writeFile(t, "d/b.txt", "b")
	n.scanAndApply(t)
	n.engine.SetInFlight(nil)

	emitted := map[string]int{}
	n.engine.OnEvent = func(ev *events.Event) { emitted[ev.Path]++ }

	dirFi, _, _ := n.ix.Get("d")
	aFi, _, _ := n.ix.Get("d/a.txt")
	bFi, _, _ := n.ix.Get("d/b.txt")
	n.engine.SetPeerIndex(2, []*index.FileInfo{
		{Name: "d", Type: index.TypeDirectory, Deleted: true, Version: dirFi.Version.Bump(1).Bump(2)},
		{Name: "d/a.txt", Type: index.TypeFile, Deleted: true, Version: aFi.Version.Bump(1).Bump(2)},
		{Name: "d/b.txt", Type: index.TypeFile, Deleted: true, Version: bFi.Version.Bump(1).Bump(2)},
	}, false)

	// Exactly what the session does: apply the pending names in sorted order,
	// parents first, then each child's own tombstone.
	for _, name := range []string{"d", "d/a.txt", "d/b.txt"} {
		if _, err := n.engine.applyDeletion(name, 2, false); err != nil {
			t.Fatalf("applyDeletion(%q): %v", name, err)
		}
	}

	for _, name := range []string{"d", "d/a.txt", "d/b.txt"} {
		if got := emitted[name]; got != 1 {
			t.Fatalf("%s emitted %d event(s), want exactly 1", name, got)
		}
	}
}
