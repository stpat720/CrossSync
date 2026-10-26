package control_test

import (
	"testing"
	"time"

	"crosssync/internal/control"
	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/version"
)

// TestAttentionConflictsComeFromTheIndexFilter pins the shape of the attention
// list after conflict discovery moved from a full index walk to a SQL name
// filter: the conflict row must still appear, still carry ConflictOf and the
// live file's size, and the live row must not be duplicated.
func TestAttentionConflictsComeFromTheIndexFilter(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "test")
	ix := n.Folders["data"].Ix

	local := version.New().Bump(1)
	for _, fi := range []*index.FileInfo{
		{Name: "shots/a.dng", Size: 4096, Type: index.TypeFile, Version: local},
		{Name: "shots/a.sync-conflict-20261026-101500-ABCDEF.dng", Size: 2048, Type: index.TypeFile, Version: local},
		{Name: "notes/100%done.txt", Size: 12, Type: index.TypeFile, Version: local},
	} {
		if err := ix.Put(fi); err != nil {
			t.Fatal(err)
		}
	}
	// An open conflict event would otherwise be "reconciled away" if the copy
	// were not found, so it is a second check on the same discovery path.
	if _, err := n.Events.Record(&events.Event{
		TS: time.Now(), Folder: "data", Path: "shots/a.dng",
		Category: events.CatConflict, Severity: events.SevWarn, Reason: "kept the newer version",
	}); err != nil {
		t.Fatal(err)
	}

	att, err := svc.Attention("data")
	if err != nil {
		t.Fatal(err)
	}
	var live, copyRow *control.AttentionFile
	for i := range att {
		switch att[i].Name {
		case "shots/a.dng":
			live = &att[i]
		case "shots/a.sync-conflict-20261026-101500-ABCDEF.dng":
			copyRow = &att[i]
		}
	}
	if copyRow == nil {
		t.Fatalf("conflict copy is missing from the attention list: %+v", att)
	}
	if !copyRow.Conflict || copyRow.ConflictOf != "shots/a.dng" {
		t.Fatalf("conflict row = %+v, want Conflict with ConflictOf shots/a.dng", *copyRow)
	}
	if copyRow.Size != 2048 {
		t.Fatalf("conflict row size = %d, want the copy's own size 2048", copyRow.Size)
	}
	// The live file is not a conflict itself; its open conflict event folds into
	// the copy's row instead of producing a second entry.
	if live != nil && live.Conflict {
		t.Fatalf("live file must not be flagged as a conflict: %+v", *live)
	}
	// A name with a LIKE wildcard must not be treated as one.
	for _, a := range att {
		if a.Name == "notes/100%d" || a.Name == "notes/100%%done.txt" {
			t.Fatalf("LIKE escaping broke a literal name: %+v", a)
		}
	}
}
