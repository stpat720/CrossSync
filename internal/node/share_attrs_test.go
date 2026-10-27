package node

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/staging"
)

// TestShareAttrsAdoptTheFolderOwner pins the zero-config default: with no
// pull_uid/pull_gid in the config, a folder adopts the owner of its own root, so
// files written into a share belong to the share's users instead of the daemon's
// account. That is what makes a fresh install correct without the operator
// discovering and typing numeric ids.
func TestShareAttrsAdoptTheFolderOwner(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no POSIX ownership on Windows")
	}
	root := t.TempDir()
	stag, err := staging.New(root)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{Cfg: &config.Config{}, Logf: func(string, ...any) {}}
	n.applyShareAttrs(stag, config.Folder{ID: "data", Path: root})

	wantUID, wantGID, ok := staging.OwnerOf(root)
	if !ok {
		t.Skip("filesystem does not expose ownership")
	}
	if stag.UID != wantUID || stag.GID != wantGID {
		t.Fatalf("adopted owner = %d:%d, want the folder root's %d:%d", stag.UID, stag.GID, wantUID, wantGID)
	}
	// Modes are left at zero so the stager applies the share default (0664/0775)
	// rather than overriding the sender's bits with a hard-coded value.
	if stag.FileMode != 0 || stag.DirMode != 0 {
		t.Fatalf("modes = %04o/%04o, want 0/0 (share defaults)", uint32(stag.FileMode), uint32(stag.DirMode))
	}
	if _, err := os.Stat(filepath.Join(root)); err != nil {
		t.Fatal(err)
	}
}

// TestShareAttrsExplicitConfigWins: a configured uid/gid must not be replaced by
// the folder's owner.
func TestShareAttrsExplicitConfigWins(t *testing.T) {
	root := t.TempDir()
	stag, err := staging.New(root)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{Cfg: &config.Config{PullUID: 4242, PullGID: 4343, PullFileMode: 0o640}, Logf: func(string, ...any) {}}
	n.applyShareAttrs(stag, config.Folder{ID: "data", Path: root})
	if stag.UID != 4242 || stag.GID != 4343 {
		t.Fatalf("owner = %d:%d, want the configured 4242:4343", stag.UID, stag.GID)
	}
	if stag.FileMode != 0o640 {
		t.Fatalf("file mode = %04o, want the configured 0640", uint32(stag.FileMode))
	}
}
