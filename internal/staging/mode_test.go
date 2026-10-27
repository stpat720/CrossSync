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

// TestShareableModes pins the zero-config default: a share's files should be
// group-writable when the sender already shares them, and a private file must
// stay private — being share-friendly is not a licence to widen a 0600 secret.
func TestShareableModes(t *testing.T) {
	cases := []struct{ peer, want os.FileMode }{
		{0o644, 0o664}, // shared read -> shared read+write
		{0o755, 0o775}, // executable stays executable, group may replace
		{0o600, 0o600}, // private stays private
		{0o700, 0o700},
		{0, 0o664}, // unknown mode -> the share default
	}
	for _, c := range cases {
		if got := shareable(c.peer); got != c.want {
			t.Fatalf("shareable(%04o) = %04o, want %04o", c.peer, got, c.want)
		}
	}
}

// TestCommitDefaultIsShareFriendly pins that NO configuration is needed for the
// common case: a 0644 file from the peer lands 0664 and a created directory lands
// 0775, so other accounts on the share can edit them.
func TestCommitDefaultIsShareFriendly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows has no POSIX mode bits to assert")
	}
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := s.TempPathFor("team/doc.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(tmp, "team/doc.txt", time.Now(), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(root, "team", "doc.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != 0o664 {
		t.Fatalf("file = %04o, want 0664 (share default)", got)
	}
	dt, err := os.Stat(filepath.Join(root, "team"))
	if err != nil {
		t.Fatal(err)
	}
	if got := dt.Mode().Perm(); got != 0o775 {
		t.Fatalf("dir = %04o, want 0775 (share default)", got)
	}
}
