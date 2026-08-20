package staging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCommitCentral(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := s.TempPathFor("docs/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(tmp) != filepath.Join(root, ReservedDir) {
		t.Fatalf("expected central temp, got %s", tmp)
	}
	if err := os.WriteFile(tmp, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 18, 10, 30, 0, 0, time.UTC)
	if err := s.Commit(tmp, "docs/file.txt", want); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "docs", "file.txt")
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "payload" {
		t.Fatalf("content = %q", data)
	}
	st, _ := os.Stat(target)
	if !st.ModTime().Equal(want) {
		t.Fatalf("mtime = %v, want %v", st.ModTime(), want)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp should be gone after commit")
	}
}

func TestCommitForceSameDir(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	s.ForceSameDir = true
	tmp, err := s.TempPathFor("sub/deep.txt")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(tmp) != filepath.Join(root, "sub") {
		t.Fatalf("expected same-dir temp next to target, got %s", tmp)
	}
	if err := os.WriteFile(tmp, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(tmp, "sub/deep.txt", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "sub", "deep.txt")); err != nil {
		t.Fatal(err)
	}
}

func TestCommitReplacesExisting(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	write := func(content string) {
		tmp, _ := s.TempPathFor("f.txt")
		os.WriteFile(tmp, []byte(content), 0o644)
		s.Commit(tmp, "f.txt", time.Now())
	}
	write("first")
	write("second")
	data, _ := os.ReadFile(filepath.Join(root, "f.txt"))
	if string(data) != "second" {
		t.Fatalf("replace failed: %q", data)
	}
}

// TestCommitCopyFallback exercises the EXDEV fallback directly (copy into
// the target's directory, atomic rename, temp removed, mtime preserved).
func TestCommitCopyFallback(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	tmp := filepath.Join(root, "elsewhere-tmp") // pretend this is another device
	if err := os.WriteFile(tmp, []byte("fallback content"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "sub", "f.txt")
	mt := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := s.commitCopyFallback(tmp, target, "sub/f.txt", mt); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "fallback content" {
		t.Fatalf("target content = %q, %v", data, err)
	}
	st, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if !st.ModTime().Equal(mt) {
		t.Fatalf("mtime = %v, want %v", st.ModTime(), mt)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("original temp should be removed, stat err = %v", err)
	}
	// No .copy litter left next to the target.
	entries, _ := os.ReadDir(filepath.Join(root, "sub"))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".copy") {
			t.Fatalf("copy litter left: %s", e.Name())
		}
	}
}

func TestCleanupStale(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	tmp, _ := s.TempPathFor("f.txt")
	os.WriteFile(tmp, []byte("data"), 0o644)
	old := time.Now().Add(-48 * time.Hour)
	os.Chtimes(tmp, old, old)
	if err := s.CleanupCentral(24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("stale temp should be removed")
	}
}

// TestMoveRelocates verifies the same-filesystem relocate: atomic rename to
// the new path, mtime normalized, source gone, no litter.
func TestMoveRelocates(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	src := filepath.Join(root, "old.txt")
	if err := os.WriteFile(src, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC)
	if err := s.MoveRelocates(src, "sub/moved.txt", want); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "sub", "moved.txt")
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "payload" {
		t.Fatalf("target content = %q, %v", data, err)
	}
	st, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if !st.ModTime().Equal(want) {
		t.Fatalf("mtime = %v, want %v", st.ModTime(), want)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("source should be gone after relocate")
	}
}

// TestMoveCopyFallback exercises the EXDEV relocate fallback directly (copy
// into the target's directory, atomic rename, mtime preserved, source
// removed, no litter).
func TestMoveCopyFallback(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	src := filepath.Join(root, "elsewhere-src") // pretend this is another device
	if err := os.WriteFile(src, []byte("fallback content"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "sub", "f.txt")
	mt := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := s.moveCopyFallback(src, target, "sub/f.txt", mt); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "fallback content" {
		t.Fatalf("target content = %q, %v", data, err)
	}
	st, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if !st.ModTime().Equal(mt) {
		t.Fatalf("mtime = %v, want %v", st.ModTime(), mt)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("source should be removed after fallback relocate")
	}
	// No .move litter left next to the target.
	entries, _ := os.ReadDir(filepath.Join(root, "sub"))
	for _, e := range entries {
		if strings.Contains(e.Name(), ".move") {
			t.Fatalf("move litter left: %s", e.Name())
		}
	}
}

func TestIsReserved(t *testing.T) {
	cases := []struct {
		rel  string
		want bool
	}{
		{".sfx-tmp/abc.part", true},
		{".sfx-1234.part", true},
		{"sub/.sfx-1234.part", true},
		{"normal/file.txt", false},
		{".hidden/file.txt", false},
		{"", false},
	}
	for _, c := range cases {
		if got := IsReserved(c.rel); got != c.want {
			t.Errorf("IsReserved(%q) = %v, want %v", c.rel, got, c.want)
		}
	}
}
