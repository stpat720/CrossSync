package control_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"crosssync/internal/control"
	"crosssync/internal/events"
)

func TestAddRemoveFolderPersistsToConfig(t *testing.T) {
	n, _ := testNode(t)
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	_ = os.WriteFile(cfgPath, []byte(
		"# keep this comment\n"+
			"device:\n  id: 1\n  name: nas-01\n"+
			"meta_dir: /tmp/m\n"+
			"folders: []\n"), 0o600)

	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)

	newRoot := t.TempDir()
	fs, err := svc.AddFolder("", "Media", newRoot, "conflict-copy", nil)
	if err != nil {
		t.Fatal(err)
	}
	if fs.ID == "" || fs.Name != "Media" || fs.Path != newRoot {
		t.Fatalf("unexpected folder status: %+v", fs)
	}
	// Comment preserved and folder persisted (name + generated id).
	data, _ := os.ReadFile(cfgPath)
	if !strings.Contains(string(data), "keep this comment") {
		t.Fatal("comment was lost when persisting config")
	}
	if !strings.Contains(string(data), "Media") || !strings.Contains(string(data), fs.ID) {
		t.Fatalf("folder not persisted to config (id=%s):\n%s", fs.ID, data)
	}

	// Files listing for the new folder.
	_ = os.WriteFile(filepath.Join(newRoot, "a.txt"), []byte("x"), 0o644)
	if _, err := n.ScanFolder(fs.ID); err != nil {
		t.Fatal(err)
	}
	files, err := svc.Files(fs.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Name != "a.txt" {
		t.Fatalf("unexpected files: %+v", files)
	}

	// Remove: gone from node and config.
	if err := svc.RemoveFolder(fs.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Files(fs.ID); err == nil {
		t.Fatal("folder should be gone after removal")
	}
	data, _ = os.ReadFile(cfgPath)
	if strings.Contains(string(data), fs.ID) {
		t.Fatal("folder not removed from config")
	}
}

func TestUpdateFolderRenamesAndMovesPath(t *testing.T) {
	n, root := testNode(t)
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	_ = os.WriteFile(cfgPath, []byte(
		"# keep this comment\n"+
			"device:\n  id: 1\n  name: nas-01\n"+
			"meta_dir: /tmp/m\n"+
			"folders:\n- id: data\n  path: "+root+"\n  conflict: conflict-copy\n"), 0o600)

	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)

	// Seed the folder with a file so a path change + rescan is meaningful.
	_ = os.WriteFile(filepath.Join(root, "a.txt"), []byte("x"), 0o644)
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	newRoot := t.TempDir()
	fs, err := svc.UpdateFolder("data", "Media", newRoot)
	if err != nil {
		t.Fatal(err)
	}
	if fs.ID != "data" || fs.Name != "Media" || fs.Path != newRoot {
		t.Fatalf("unexpected folder status: %+v", fs)
	}
	// The index is preserved and now reflects the (empty) new root: the old
	// file was tombstoned by the rescan, so the live file list is empty.
	files, err := svc.Files("data")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("expected empty file list after moving path, got %+v", files)
	}
	// Config persisted with the new name/path (and comment preserved).
	data, _ := os.ReadFile(cfgPath)
	if !strings.Contains(string(data), "keep this comment") {
		t.Fatal("comment was lost when persisting config")
	}
	if !strings.Contains(string(data), "Media") || !strings.Contains(string(data), newRoot) {
		t.Fatalf("folder update not persisted to config:\n%s", data)
	}

	// Bad updates rejected.
	if _, err := svc.UpdateFolder("data", "X", ""); err == nil {
		t.Fatal("empty path should fail")
	}
	if _, err := svc.UpdateFolder("data", "X", "relative/path"); err == nil {
		t.Fatal("relative path should fail")
	}
	if _, err := svc.UpdateFolder("nope", "X", newRoot); err == nil {
		t.Fatal("unknown folder should fail")
	}
	// A second folder cannot claim the same path.
	otherRoot := t.TempDir()
	if _, err := svc.AddFolder("", "Other", otherRoot, "conflict-copy", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateFolder("data", "X", otherRoot); err == nil {
		t.Fatal("path already in use by another folder should fail")
	}
}

func TestFileAndDirCounts(t *testing.T) {
	n, root := testNode(t)
	// One file at root + one file inside a subdirectory: the index holds 2
	// files and 1 directory, and Stats must keep them distinct (directories
	// and tombstones are not counted as files).
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(root, "a.txt"), []byte("x"), 0o644)
	_ = os.WriteFile(filepath.Join(root, "sub", "b.txt"), []byte("y"), 0o644)
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	svc := control.New(n, "test")
	for _, f := range svc.Folders() {
		if f.ID != "data" {
			continue
		}
		if f.Files != 2 || f.Directories != 1 || f.Tombstones != 0 {
			t.Fatalf("expected 2 files, 1 dir, 0 tombstones, got files=%d dirs=%d tombs=%d", f.Files, f.Directories, f.Tombstones)
		}
	}
}

func TestRenameDetectedLocally(t *testing.T) {
	n, root := testNode(t)
	_ = os.WriteFile(filepath.Join(root, "a.txt"), []byte("hello world"), 0o644)
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	// Rename on disk: the scan must apply it as a rename (single event),
	// not a delete+add pair, and the live file count must stay at 1.
	if err := os.Rename(filepath.Join(root, "a.txt"), filepath.Join(root, "b.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	svc := control.New(n, "test")
	for _, f := range svc.Folders() {
		if f.ID != "data" {
			continue
		}
		if f.Files != 1 {
			t.Fatalf("live files after rename = %d, want 1", f.Files)
		}
	}
	files, err := svc.Files("data")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Name != "b.txt" {
		t.Fatalf("live file list after rename = %+v, want [b.txt]", files)
	}

	// Transparency: a "local rename" event (no delete/add pair for it).
	evs, err := n.Events.Query(events.Filter{Folder: "data", Category: events.CatApplied})
	if err != nil {
		t.Fatal(err)
	}
	renamed, deleted, added := 0, 0, 0
	for _, e := range evs {
		switch e.Reason {
		case "local rename":
			renamed++
		case "local delete":
			deleted++
		case "local add":
			added++
		}
	}
	if renamed != 1 || deleted != 0 || added != 1 {
		t.Fatalf("rename events: renamed=%d delete=%d add=%d (want 1 rename, 0 delete, 1 add — the original add)", renamed, deleted, added)
	}
}

func TestAddFolderRejectsBadInput(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "test")
	if _, err := svc.AddFolder("", "", "", "", nil); err == nil {
		t.Fatal("empty id and name should fail")
	}
	if _, err := svc.AddFolder("dup", "", "/tmp/x", "", nil); err == nil {
		t.Fatal("relative path should fail")
	}
	if _, err := svc.AddFolder("dup", "", "/tmp/x", "bogus-policy", nil); err == nil {
		t.Fatal("bogus policy should fail")
	}
	if _, err := svc.AddFolder("data", "", "/tmp/x", "", nil); err == nil {
		t.Fatal("duplicate folder should fail")
	}
}

func TestEventSinceFilter(t *testing.T) {
	s, err := events.Open(filepath.Join(t.TempDir(), "e.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	old := time.Now().Add(-48 * time.Hour)
	for i, ts := range []time.Time{old, time.Now()} {
		if _, err := s.Record(&events.Event{TS: ts, Path: "p", Category: events.CatApplied,
			Severity: events.SevInfo, Reason: "r" + strconv.Itoa(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Only events in the last 24h (one event).
	all, err := s.Query(events.Filter{Since: time.Now().Add(-24 * time.Hour).Unix()})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("since filter: got %d events, want 1", len(all))
	}
}
