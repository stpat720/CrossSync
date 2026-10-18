package node_test

import (
	"os"
	"path/filepath"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/node"
)

// makeNodeInclude builds a node whose single folder carries an include
// (whitelist) list, so the per-device scope filter can be exercised.
func makeNodeInclude(t *testing.T, id uint64, name, root, metaDir string, include []string) *node.Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: id, Name: name},
		MetaDir: metaDir,
		Folders: []config.Folder{{
			ID: "data", Path: root, ConflictPolicy: "conflict-copy",
			Include: include,
		}},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// TestIncludeFilterOverSession is the end-to-end include test: A syncs
// everything, B opts into "*/proxy/" + "*.mxf" only, so B acquires the proxy
// MXF (and its directory) but never full.mp4 or notes.txt. Once B owns an
// entry it keeps following A: a peer deletion and a later re-create both
// propagate.
func TestIncludeFilterOverSession(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNodeInclude(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), nil)
	b := makeNodeInclude(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), []string{"*/proxy/", "*.mxf"})

	write(t, rootA, "proj/proxy/p.mxf", "proxy media v1")
	write(t, rootA, "proj/full.mp4", "full media")
	write(t, rootA, "notes.txt", "notes")

	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)

	// B acquired the in-scope file and its (deep-matched) directory.
	if got := read(t, rootB, "proj/proxy/p.mxf"); got != "proxy media v1" {
		t.Fatalf("B content = %q, want %q", got, "proxy media v1")
	}
	ixB := b.Folders["data"].Ix
	for _, name := range []string{"proj/proxy", "proj/proxy/p.mxf"} {
		if _, ok, err := ixB.Get(name); err != nil {
			t.Fatal(err)
		} else if !ok {
			t.Fatalf("B index should hold in-scope entry %s", name)
		}
	}
	// Out-of-scope paths never reached B, on disk or in the index.
	for _, rel := range []string{"proj/full.mp4", "notes.txt"} {
		if _, err := os.Stat(filepath.Join(rootB, filepath.FromSlash(rel))); !os.IsNotExist(err) {
			t.Fatalf("out-of-scope %s must not be pulled (err=%v)", rel, err)
		}
		if _, ok, err := ixB.Get(rel); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Fatalf("out-of-scope %s must not be indexed on B", rel)
		}
	}

	// A deletes the file: B owns it, so the deletion still follows.
	if err := os.Remove(filepath.Join(rootA, filepath.FromSlash("proj/proxy/p.mxf"))); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)
	if _, err := os.Stat(filepath.Join(rootB, filepath.FromSlash("proj/proxy/p.mxf"))); !os.IsNotExist(err) {
		t.Fatalf("peer deletion of an owned file should propagate (err=%v)", err)
	}

	// A re-creates it with new content: B pulls the new bytes.
	write(t, rootA, "proj/proxy/p.mxf", "proxy media v2 (regenerated)")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)
	if got := read(t, rootB, "proj/proxy/p.mxf"); got != "proxy media v2 (regenerated)" {
		t.Fatalf("B content after re-create = %q, want %q", got, "proxy media v2 (regenerated)")
	}
}
