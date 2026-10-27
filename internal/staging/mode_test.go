package staging

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// TestCommitKeepsTheSendersMode pins the mode handling on commit. Files used to be
// created 0644 unconditionally, which is how a 0600 private file became
// world-readable after syncing and an executable lost its +x bit.
func TestCommitKeepsTheSendersMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows has no POSIX mode bits to assert")
	}
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := s.TempPathFor("bin/tool.sh")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("#!/bin/sh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(tmp, "bin/tool.sh", time.Now(), 0o750); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(root, "bin", "tool.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != 0o750 {
		t.Fatalf("mode = %04o, want the sender's 0750", got)
	}
}

// TestCommitModeOverrideWins pins the share-friendly override: a node whose files
// are edited by other accounts can force a group-writable mode regardless of what
// the sender used — otherwise every synced file is owned (and only writable) by
// the daemon's own user.
func TestCommitModeOverrideWins(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows has no POSIX mode bits to assert")
	}
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	s.FileMode = 0o664
	s.DirMode = 0o775
	tmp, err := s.TempPathFor("shared/notes.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(tmp, "shared/notes.txt", time.Now(), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(root, "shared", "notes.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != 0o664 {
		t.Fatalf("file mode = %04o, want the configured 0664", got)
	}
	dst, err := os.Stat(filepath.Join(root, "shared"))
	if err != nil {
		t.Fatal(err)
	}
	if got := dst.Mode().Perm(); got != 0o775 {
		t.Fatalf("dir mode = %04o, want the configured 0775", got)
	}
}

// TestCommitWithoutModesLeavesTheUmaskAlone: an unset policy must not force
// anything (the default path stays byte-for-byte what it was).
func TestCommitWithoutModesLeavesTheUmaskAlone(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := s.TempPathFor("plain.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(tmp, "plain.txt", time.Now(), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "plain.txt")); err != nil {
		t.Fatal(err)
	}
}
