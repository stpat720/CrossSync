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
		_ = os.WriteFile(tmp, []byte(content), 0o644)
		_ = s.Commit(tmp, "f.txt", time.Now())
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
	_ = os.WriteFile(tmp, []byte("data"), 0o644)
	old := time.Now().Add(-48 * time.Hour)
	_ = os.Chtimes(tmp, old, old)
	if err := s.CleanupCentral(24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("stale temp should be removed")
	}
}

// TestCleanupOrphansRemovesCentralAndSameDir verifies CleanupOrphans
// removes every reserved temp in the folder: the central temp dir, nested
// same-directory temps (the EXDEV/cross-device path), and nothing else.
func TestCleanupOrphansRemovesCentralAndSameDir(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)

	central, _ := s.TempPathFor("f.txt")
	_ = os.WriteFile(central, []byte("central"), 0o644)
	sameDir := filepath.Join(root, "sub", tempName("sub/g.txt"))
	if err := os.MkdirAll(filepath.Dir(sameDir), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(sameDir, []byte("same-dir"), 0o644)
	live := filepath.Join(root, "live.txt")
	_ = os.WriteFile(live, []byte("keep me"), 0o644)

	n, err := s.CleanupOrphans()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("removed %d temps, want 2", n)
	}
	if _, err := os.Stat(central); !os.IsNotExist(err) {
		t.Fatalf("central temp should be removed")
	}
	if _, err := os.Stat(sameDir); !os.IsNotExist(err) {
		t.Fatalf("same-dir temp should be removed")
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatalf("live file should be untouched: %v", err)
	}
	// Idempotent: a second run finds nothing.
	if n, _ := s.CleanupOrphans(); n != 0 {
		t.Fatalf("second cleanup removed %d, want 0", n)
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
		// The whole reserved namespace is decided per path COMPONENT: the
		// hold and version areas keep their contents invisible at any depth,
		// whatever the held file is named.
		{".sfx-hold/media/clip.mov", true},
		{".sfx-hold/deep/nested/file.txt", true},
		{".sfx-versions/media/clip.mov", true},
		{"sub/.sfx-hold/file.txt", true},
		{"normal/file.txt", false},
		{".hidden/file.txt", false},
		{"hold/file.txt", false},
		{"", false},
	}
	for _, c := range cases {
		if got := IsReserved(c.rel); got != c.want {
			t.Errorf("IsReserved(%q) = %v, want %v", c.rel, got, c.want)
		}
	}
}
