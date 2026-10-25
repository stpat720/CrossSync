package testgen

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestSameSeedProducesIdenticalCorpora is the whole point: two nodes generating
// "the same data" from one spec must produce identical bytes AND identical
// mtimes, otherwise a mostly-in-sync pair would see every file as changed.
func TestSameSeedProducesIdenticalCorpora(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	spec := Spec{Kind: "base", Dir: "testdata/base", Files: 12, Size: 4096, Seed: 7}
	ra, err := Run(a, spec)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := Run(b, spec)
	if err != nil {
		t.Fatal(err)
	}
	if ra.Files != 12 || rb.Files != 12 || ra.Bytes != rb.Bytes {
		t.Fatalf("results differ: %+v vs %+v", ra, rb)
	}
	for i := 0; i < 12; i++ {
		name := filepath.Join("testdata", "base", fmt.Sprintf("f%07d.bin", i))
		ba, err := os.ReadFile(filepath.Join(a, name))
		if err != nil {
			t.Fatal(err)
		}
		bb, err := os.ReadFile(filepath.Join(b, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ba, bb) {
			t.Fatalf("file %d differs between nodes", i)
		}
		sa, _ := os.Stat(filepath.Join(a, name))
		sb, _ := os.Stat(filepath.Join(b, name))
		if !sa.ModTime().Equal(sb.ModTime()) {
			t.Fatalf("file %d mtime differs: %s vs %s", i, sa.ModTime(), sb.ModTime())
		}
	}

	// A different seed must produce different content (so "newer version" really
	// is a content change, not the same bytes rewritten).
	c := t.TempDir()
	if _, err := Run(c, Spec{Kind: "base", Dir: "testdata/base", Files: 1, Size: 4096, Seed: 8}); err != nil {
		t.Fatal(err)
	}
	bc, err := os.ReadFile(filepath.Join(c, "testdata/base/f0000000.bin"))
	if err != nil {
		t.Fatal(err)
	}
	ba, _ := os.ReadFile(filepath.Join(a, "testdata/base/f0000000.bin"))
	if bytes.Equal(ba, bc) {
		t.Fatal("different seeds produced identical content")
	}
}

// TestJailRefusesPathsOutsideTestdata: the generator must never write to (or
// delete) anything outside <root>/testdata.
func TestJailRefusesPathsOutsideTestdata(t *testing.T) {
	root := t.TempDir()
	bad := []string{"", ".", "..", "../x", "/etc", "userdata/x", "testdatax/y", "testdata/../x"}
	for _, dir := range bad {
		if _, err := Run(root, Spec{Kind: "base", Dir: dir, Files: 1, Size: 16}); err == nil {
			t.Fatalf("dir %q should have been refused", dir)
		}
	}
	// The legitimate prefix is accepted, and nothing outside it is ever created.
	if _, err := Run(root, Spec{Kind: "base", Dir: "testdata/ok", Files: 1, Size: 16, Seed: 1}); err != nil {
		t.Fatalf("testdata/ok should be accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "userdata")); !os.IsNotExist(err) {
		t.Fatal("generator created something outside testdata/")
	}
}

// TestModifyMakesNewerVersion: "newer version of the same file" must change the
// bytes AND move the mtime forward, or conflict resolution cannot pick a winner.
func TestModifyMakesNewerVersion(t *testing.T) {
	root := t.TempDir()
	if _, err := Run(root, Spec{Kind: "base", Dir: "testdata/c", Files: 1, Size: 1024, Seed: 3}); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "testdata/c/f0000000.bin")
	before, _ := os.ReadFile(p)
	st0, _ := os.Stat(p)

	if _, err := Run(root, Spec{Kind: "modify", Dir: "testdata/c", Files: 1, Seed: 3}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(p)
	st1, _ := os.Stat(p)
	if bytes.Equal(before, after) {
		t.Fatal("modify did not change content")
	}
	if !st1.ModTime().After(st0.ModTime()) {
		t.Fatalf("modify did not move the mtime forward: %s -> %s", st0.ModTime(), st1.ModTime())
	}
}

// TestRenameAndCleanupStayInsideTestdata.
func TestRenameAndCleanupStayInsideTestdata(t *testing.T) {
	root := t.TempDir()
	if _, err := Run(root, Spec{Kind: "base", Dir: "testdata/old", Files: 3, Size: 64, Seed: 1}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "keep"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "keep", "user.txt"), []byte("mine"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Run(root, Spec{Kind: "rename", Dir: "testdata/old", To: "testdata/new"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "testdata/new/f0000000.bin")); err != nil {
		t.Fatalf("rename did not move the tree: %v", err)
	}
	if _, err := Run(root, Spec{Kind: "cleanup", Dir: "testdata/new"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "testdata/new")); !os.IsNotExist(err) {
		t.Fatal("cleanup left the tree behind")
	}
	// User data outside testdata/ is untouched.
	if _, err := os.Stat(filepath.Join(root, "keep", "user.txt")); err != nil {
		t.Fatalf("cleanup touched user data: %v", err)
	}
	// Renaming to an existing target is refused, not merged.
	if _, err := Run(root, Spec{Kind: "base", Dir: "testdata/a", Files: 1, Size: 8, Seed: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(root, Spec{Kind: "base", Dir: "testdata/b", Files: 1, Size: 8, Seed: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(root, Spec{Kind: "rename", Dir: "testdata/a", To: "testdata/b"}); err == nil {
		t.Fatal("rename onto an existing target must be refused")
	}
}
