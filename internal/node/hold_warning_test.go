package node

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/events"
)

// TestHoldAreaWarnsWhenItGrows: a folder wipe parks every removed copy in the hold
// area for the undo window. Measured once as 22,739 copies / 7.0 GB with nothing
// anywhere saying so. Passing the threshold must raise a warning; it must never
// delete anything, because the hold area IS the undo window.
func TestHoldAreaWarnsWhenItGrows(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	f := n.Folders["data"]
	if f.Hold == nil {
		t.Skip("no hold store in this fixture")
	}
	if n.Events == nil {
		t.Skip("no event store in this fixture")
	}

	// Hold one real file so the store has bytes to count.
	if err := os.WriteFile(filepath.Join(root, "big.bin"), make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := f.Hold.Move("big.bin"); err != nil {
		t.Fatal(err)
	}

	// Threshold of 1 byte: any held copy trips it.
	f.Cfg.HoldWarnBytes = 1
	n.warnIfHoldIsLarge(f)

	list, err := n.Events.Query(events.Filter{Limit: 10})
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	for _, e := range list {
		if e.Folder == f.Cfg.ID && e.Severity >= events.SevWarn {
			return // warning recorded
		}
	}
	t.Fatalf("no warning event recorded for a hold area over the threshold: %+v", list)
}
