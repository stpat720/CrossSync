package node

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/index"
	"crosssync/internal/protocol"
	"crosssync/internal/scanner"
)

// TestServeBlockRefusesInFlight verifies the serve-side gate: a block request
// for a file the source is still writing is refused with RespInvalidFile (the
// existing, previously unused code — no protocol change), while a settled file
// still serves normally.
func TestServeBlockRefusesInFlight(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	chunkWrite(t, root, "doc.txt", "settled content")
	// Two scans at the default gate depth: the file settles and is indexed.
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	req := &protocol.Request{ID: 7, Folder: "data", Name: "doc.txt", Offset: 0, Size: 15}
	resp := n.serveBlock(req)
	if resp.Code != protocol.RespNoError {
		t.Fatalf("settled file: code = %d, want RespNoError", resp.Code)
	}
	if string(resp.Data) != "settled content" {
		t.Fatalf("settled file: data = %q, want %q", resp.Data, "settled content")
	}

	// The file starts being rewritten: the gate holds it back.
	f := n.Folders["data"]
	f.Engine.SetInFlight([]scanner.InFlight{{Name: "doc.txt", Size: int64(len("settled content")), Scans: 1}})
	resp = n.serveBlock(req)
	if resp.Code != protocol.RespInvalidFile {
		t.Fatalf("in-flight file: code = %d, want RespInvalidFile", resp.Code)
	}
	if len(resp.Data) != 0 {
		t.Fatalf("in-flight file: %d bytes of data served, want none", len(resp.Data))
	}
	if resp.ID != req.ID {
		t.Fatalf("response id = %d, want %d", resp.ID, req.ID)
	}

	// Once it settles again the same request is served.
	f.Engine.SetInFlight(nil)
	if resp := n.serveBlock(req); resp.Code != protocol.RespNoError {
		t.Fatalf("after settling: code = %d, want RespNoError", resp.Code)
	}

	// An unknown folder is still RespNoSuchFile, not the gate's code.
	if resp := n.serveBlock(&protocol.Request{ID: 8, Folder: "nope", Name: "doc.txt"}); resp.Code != protocol.RespNoSuchFile {
		t.Fatalf("unknown folder: code = %d, want RespNoSuchFile", resp.Code)
	}
}

// TestRebuildBypassesGate verifies the rebuildFromFS contract: a quarantined
// index is repopulated on the FIRST scan (the gate is bypassed), so the folder
// is never left looking empty for a scan interval — which would make peers pull
// the whole folder back.
func TestRebuildBypassesGate(t *testing.T) {
	meta := t.TempDir()
	root := t.TempDir()
	// Gate at its default depth: without the bypass nothing would settle on the
	// first rebuild scan.
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: meta,
		Folders: []config.Folder{{ID: "data", Path: root}},
	}
	if err := os.WriteFile(filepath.Join(root, "one.txt"), []byte("aaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "two.txt"), []byte("bbb"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Seed a populated index, then destroy it so opening the node rebuilds.
	n1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n1.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := n1.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	_ = n1.Close()

	dbPath := filepath.Join(meta, "data.db")
	if err := os.WriteFile(dbPath, []byte("this is definitely not a sqlite database"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")

	n2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n2.Close() })
	f := n2.Folders["data"]
	files := 0
	if err := f.Ix.List(func(fi *index.FileInfo) error {
		if !fi.Deleted && fi.Type == index.TypeFile {
			files++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if files != 2 {
		t.Fatalf("rebuilt index holds %d live files, want 2 (rebuild must bypass the stability gate)", files)
	}
	states, err := f.Ix.SettleList()
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 0 {
		t.Fatalf("settling table after a rebuild = %+v, want empty", states)
	}
	// Nothing is held back either, so the folder immediately advertises
	// everything it holds.
	if held := f.Scanner.LastInFlight(); len(held) != 0 {
		t.Fatalf("LastInFlight after a rebuild = %+v, want empty", held)
	}
}
