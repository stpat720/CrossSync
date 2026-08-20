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
	os.WriteFile(cfgPath, []byte(
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
	os.WriteFile(filepath.Join(newRoot, "a.txt"), []byte("x"), 0o644)
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
	defer s.Close()
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
