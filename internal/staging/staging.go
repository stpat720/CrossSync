// Package staging manages in-flight transfer files. Temp files live in a
// central hidden .sfx-tmp directory at the folder root by default; when the
// target lives on a different device (a share spanning disks), the temp is
// placed in the target's own directory so the final rename stays atomic.
package staging

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ReservedDir is the central temp directory name inside a share.
const ReservedDir = ".sfx-tmp"

// TempPrefix is the prefix for same-directory temp dotfiles.
const TempPrefix = ".sfx-"

// Stager manages in-flight transfer files for one folder.
type Stager struct {
	root string
	tmp  string
	// ForceSameDir, when true, always places temps next to the target
	// (used by tests to exercise the cross-device path).
	ForceSameDir bool
}

// New creates the stager and ensures the central temp dir exists.
func New(folderRoot string) (*Stager, error) {
	tmp := filepath.Join(folderRoot, ReservedDir)
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, err
	}
	return &Stager{root: folderRoot, tmp: tmp}, nil
}

// Root returns the folder root.
func (s *Stager) Root() string { return s.root }

// TmpDir returns the central temp directory.
func (s *Stager) TmpDir() string { return s.tmp }

// TempPathFor returns the absolute temp path for a relative target path.
// If the target's filesystem differs from the central temp dir (or forced),
// the temp is placed in the target's own directory as a hidden dotfile so
// the final rename is atomic on the same filesystem.
func (s *Stager) TempPathFor(rel string) (string, error) {
	targetAbs := filepath.Join(s.root, filepath.FromSlash(rel))
	dir := filepath.Dir(targetAbs)
	name := tempName(rel)
	if !s.ForceSameDir && sameDevice(dir, s.tmp) {
		return filepath.Join(s.tmp, name), nil
	}
	if err := EnsureDir(dir); err != nil {
		return "", fmt.Errorf("staging: mkdir %s: %w", dir, err)
	}
	return filepath.Join(dir, name), nil
}

// Commit finalizes a temp file: sets the desired modification time on the
// temp BEFORE the atomic rename (eliminating any timestamp race), then
// renames it into place. On EXDEV it falls back to a same-directory copy
// followed by rename.
func (s *Stager) Commit(tempAbs, rel string, modTime time.Time) error {
	targetAbs := filepath.Join(s.root, filepath.FromSlash(rel))
	parent := filepath.Dir(targetAbs)
	if err := EnsureDir(parent); err != nil {
		return fmt.Errorf("staging: commit mkdir for %s: %w", targetAbs, err)
	}
	if err := os.Chtimes(tempAbs, modTime, modTime); err != nil {
		return fmt.Errorf("staging: chtimes %s: %w", tempAbs, err)
	}
	if err := os.Rename(tempAbs, targetAbs); err == nil {
		return nil
	} else if !isEXDEV(err) {
		return fmt.Errorf("staging: rename %s -> %s: %w", tempAbs, targetAbs, err)
	}
	if err := os.Chtimes(tempAbs, modTime, modTime); err != nil {
		return fmt.Errorf("staging: chtimes %s: %w", tempAbs, err)
	}
	if err := os.Rename(tempAbs, targetAbs); err == nil {
		return nil
	} else if !isEXDEV(err) {
		return fmt.Errorf("staging: rename %s -> %s: %w", tempAbs, targetAbs, err)
	}
	return s.commitCopyFallback(tempAbs, targetAbs, rel, modTime)
}

// commitCopyFallback handles a cross-device (EXDEV) commit: copy the temp
// into the target's directory (guaranteed same filesystem), set the mtime,
// then rename atomically. The original temp is removed. Split out so the
// fallback can be tested directly without needing a real second device.
func (s *Stager) commitCopyFallback(tempAbs, targetAbs, rel string, modTime time.Time) error {
	dir := filepath.Dir(targetAbs)
	if err := EnsureDir(dir); err != nil {
		return err
	}
	stageCopy := filepath.Join(dir, tempName(rel)+".copy")
	if err := copyFile(tempAbs, stageCopy); err != nil {
		return err
	}
	if err := os.Chtimes(stageCopy, modTime, modTime); err != nil {
		os.Remove(stageCopy)
		return err
	}
	if err := os.Rename(stageCopy, targetAbs); err != nil {
		os.Remove(stageCopy)
		return err
	}
	os.Remove(tempAbs)
	return nil
}

// Remove deletes the temp for a relative target path (if any exists).
func (s *Stager) Remove(rel string) {
	abs := filepath.Join(s.root, filepath.FromSlash(rel))
	dir := filepath.Dir(abs)
	name := tempName(rel)
	os.Remove(filepath.Join(s.tmp, name))
	os.Remove(filepath.Join(dir, name))
}

// MoveRelocates relocates an existing live file from srcAbs to the relative
// target path rel: an atomic rename when both live on the same filesystem,
// and a same-directory copy + rename on cross-device (EXDEV) so the final
// placement is still atomic. The target's modification time is normalized to
// modTime so metadata matches the peer's record exactly. srcAbs is removed
// on the cross-device path. Used by move/rename detection (a local rename
// instead of delete + re-download).
func (s *Stager) MoveRelocates(srcAbs, rel string, modTime time.Time) error {
	targetAbs := filepath.Join(s.root, filepath.FromSlash(rel))
	parent := filepath.Dir(targetAbs)
	if err := EnsureDir(parent); err != nil {
		return fmt.Errorf("staging: relocate mkdir for %s: %w", targetAbs, err)
	}
	if err := os.Rename(srcAbs, targetAbs); err == nil {
		// Rename preserves the inode (and thus mtime) on most filesystems;
		// normalize it so the timestamp is exact everywhere.
		return os.Chtimes(targetAbs, modTime, modTime)
	} else if !isEXDEV(err) {
		return fmt.Errorf("staging: relocate %s -> %s: %w", srcAbs, targetAbs, err)
	}
	return s.moveCopyFallback(srcAbs, targetAbs, rel, modTime)
}

// moveCopyFallback handles a cross-device (EXDEV) relocate: copy the source
// into the target's directory, set the mtime, rename atomically, then remove
// the source. Split out so the fallback can be tested directly without a
// real second device.
func (s *Stager) moveCopyFallback(srcAbs, targetAbs, rel string, modTime time.Time) error {
	dir := filepath.Dir(targetAbs)
	if err := EnsureDir(dir); err != nil {
		return err
	}
	stageCopy := filepath.Join(dir, tempName(rel)+".move")
	if err := copyFile(srcAbs, stageCopy); err != nil {
		return err
	}
	if err := os.Chtimes(stageCopy, modTime, modTime); err != nil {
		os.Remove(stageCopy)
		return err
	}
	if err := os.Rename(stageCopy, targetAbs); err != nil {
		os.Remove(stageCopy)
		return err
	}
	return os.Remove(srcAbs)
}

// CleanupCentral removes central temp files older than maxAge.
func (s *Stager) CleanupCentral(maxAge time.Duration) error {
	entries, err := os.ReadDir(s.tmp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	cutoff := time.Now().Add(-maxAge)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		if fi.ModTime().Before(cutoff) {
			os.Remove(filepath.Join(s.tmp, e.Name()))
		}
	}
	return nil
}

// EnsureDir creates dir (recursively), tolerating transient filesystem
// errors (Windows filter drivers / antivirus) by verifying existence and
// retrying with a short backoff. It is a no-op if the directory exists.
func EnsureDir(dir string) error {
	if dir == "" || dir == "." {
		return nil
	}
	for attempt := 0; attempt < 8; attempt++ {
		err := os.MkdirAll(dir, 0o755)
		if err == nil {
			return nil
		}
		if st, serr := os.Stat(dir); serr == nil && st.IsDir() {
			// The directory exists; the error was transient.
			return nil
		}
		if attempt < 7 {
			time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
		}
	}
	return os.MkdirAll(dir, 0o755)
}

func tempName(rel string) string {
	sum := sha256.Sum256([]byte(rel))
	return TempPrefix + hex.EncodeToString(sum[:8]) + ".part"
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, cerr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	if cerr != nil {
		return cerr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// IsReserved reports whether a relative path (slash-separated) belongs to
// CrossSync's reserved namespace and must never be scanned or synced.
// This covers the central temp dir at any depth and same-dir temp dotfiles.
func IsReserved(rel string) bool {
	if rel == "" || rel == "." {
		return false
	}
	base := filepath.ToSlash(rel)
	base = base[strings.LastIndex(base, "/")+1:]
	if strings.HasPrefix(base, TempPrefix) {
		return true
	}
	for _, comp := range strings.Split(filepath.ToSlash(rel), "/") {
		if comp == ReservedDir {
			return true
		}
	}
	return false
}

var errEXDEV = errors.New("invalid cross-device link")

func isEXDEV(err error) bool {
	return err != nil && (errors.Is(err, errEXDEV) || strings.Contains(err.Error(), "invalid cross-device link") || strings.Contains(err.Error(), "not same device"))
}

// TempRel returns the relative path of the temp for rel.
func (s *Stager) TempRel(rel string) (string, error) {
	abs, err := s.TempPathFor(rel)
	if err != nil {
		return "", err
	}
	r, err := filepath.Rel(s.root, abs)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(r), nil
}
