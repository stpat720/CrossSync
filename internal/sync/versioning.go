package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Versioning archives old versions of files that are replaced or deleted by
// REMOTE changes, protecting local data from other machines' actions.
// Old local overwrites cannot be archived (the writing app already replaced
// the file) — matching Syncthing's model.
type Versioning struct {
	Type      string // none | trashcan | simple | staggered
	Keep      int    // simple / staggered: max versions kept
	MaxAge    int    // staggered: max age in days (0 = forever)
	CleanDays int    // trashcan: auto-clean after N days (0 = never)
}

// VersionsDir is the hidden versions area inside a folder.
const VersionsDir = ".sfx-versions"

// Archive moves root/rel into the versions area per the strategy.
func (v *Versioning) Archive(root, rel string, modTime time.Time) error {
	if v == nil {
		return nil
	}
	switch v.Type {
	case "", "none":
		return nil
	case "trashcan":
		return v.archiveTrashcan(root, rel)
	case "simple":
		return v.archiveSimple(root, rel, modTime)
	case "staggered":
		return v.archiveStaggered(root, rel, modTime)
	default:
		return fmt.Errorf("unknown versioning type %q", v.Type)
	}
}

func (v *Versioning) versionsRoot(root string) string {
	return filepath.Join(root, VersionsDir)
}

func (v *Versioning) archiveTrashcan(root, rel string) error {
	src := filepath.Join(root, filepath.FromSlash(rel))
	dst := filepath.Join(v.versionsRoot(root), filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	os.Remove(dst)
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	if v.CleanDays > 0 {
		v.pruneByAge(root, time.Duration(v.CleanDays)*24*time.Hour)
	}
	return nil
}

func (v *Versioning) archiveSimple(root, rel string, modTime time.Time) error {
	dir := filepath.Join(v.versionsRoot(root), filepath.FromSlash(rel))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	stamp := strconv.FormatInt(modTime.UnixNano(), 10)
	dst := filepath.Join(dir, stamp)
	if err := os.Rename(filepath.Join(root, filepath.FromSlash(rel)), dst); err != nil {
		return err
	}
	v.pruneKeep(dir)
	return nil
}

func (v *Versioning) archiveStaggered(root, rel string, modTime time.Time) error {
	day := modTime.UTC().Format("2006-01-02")
	dir := filepath.Join(v.versionsRoot(root), filepath.FromSlash(rel), day)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	stamp := strconv.FormatInt(modTime.UnixNano(), 10)
	if err := os.Rename(filepath.Join(root, filepath.FromSlash(rel)), filepath.Join(dir, stamp)); err != nil {
		return err
	}
	if v.MaxAge > 0 {
		v.pruneByAge(root, time.Duration(v.MaxAge)*24*time.Hour)
	}
	// Keep at most Keep versions per day-bucket.
	v.pruneKeep(dir)
	return nil
}

// pruneKeep removes the oldest entries in dir beyond Keep, keeping the most
// recent (entries are named by UnixNano timestamps).
func (v *Versioning) pruneKeep(dir string) {
	if v.Keep <= 0 {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	if len(entries) <= v.Keep {
		return
	}
	sort.Slice(entries, func(i, j int) bool {
		ai, _ := strconv.ParseInt(entries[i].Name(), 10, 64)
		bi, _ := strconv.ParseInt(entries[j].Name(), 10, 64)
		return ai > bi
	})
	for _, e := range entries[v.Keep:] {
		os.RemoveAll(filepath.Join(dir, e.Name()))
	}
}

// pruneByAge removes version entries older than maxAge anywhere in the
// versions area.
func (v *Versioning) pruneByAge(root string, maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)
	vroot := v.versionsRoot(root)
	filepath.WalkDir(vroot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		if info.ModTime().Before(cutoff) {
			os.Remove(path)
		}
		return nil
	})
	// Clean empty directories.
	filepath.WalkDir(vroot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if entries, _ := os.ReadDir(path); len(entries) == 0 && path != vroot {
				os.Remove(path)
			}
		}
		return nil
	})
}

// Stats returns how many archived versions exist and their total size, and
// how many conflict copies are present in the index (see StatsVersions and
// StatsConflicts in control for the folder-level breakdown).
func (v *Versioning) Stats(root string) (files int, bytes int64) {
	vroot := v.versionsRoot(root)
	filepath.WalkDir(vroot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		files++
		bytes += info.Size()
		return nil
	})
	return files, bytes
}

// Clean removes every archived version in the folder and returns how much
// was freed. The versions area is recreated on the next archive.
func (v *Versioning) Clean(root string) (files int, bytes int64, err error) {
	files, bytes = v.Stats(root)
	if err := os.RemoveAll(v.versionsRoot(root)); err != nil {
		return 0, 0, err
	}
	return files, bytes, nil
}

// VersionEntry describes one archived version, with the original file path
// it belongs to (derived from the strategy's layout).
type VersionEntry struct {
	ArchivePath string `json:"archive_path"` // relative to the versions area
	Path        string `json:"path"`         // the original file it belongs to
	Size        int64  `json:"size"`
	Modified    int64  `json:"modified"` // unix seconds
}

// List returns every archived version in the folder, sorted by original
// path then age.
func (v *Versioning) List(root string) ([]VersionEntry, error) {
	vroot := v.versionsRoot(root)
	var out []VersionEntry
	err := filepath.WalkDir(vroot, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(vroot, p)
		if rerr != nil {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		out = append(out, VersionEntry{
			ArchivePath: filepath.ToSlash(rel),
			Path:        v.originalPath(filepath.ToSlash(rel)),
			Size:        info.Size(),
			Modified:    info.ModTime().Unix(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].Modified < out[j].Modified
	})
	return out, nil
}

// originalPath derives the live file path an archived version belongs to,
// reversing the strategy's directory layout:
//
//	trashcan:  <path>                (the archive IS the path)
//	simple:    <path>/<stamp>
//	staggered: <path>/<day>/<stamp>
func (v *Versioning) originalPath(archiveRel string) string {
	p := archiveRel
	switch v.Type {
	case "simple":
		if i := strings.LastIndex(p, "/"); i >= 0 {
			p = p[:i]
		}
	case "staggered":
		if i := strings.LastIndex(p, "/"); i >= 0 {
			p = p[:i]
		}
		if i := strings.LastIndex(p, "/"); i >= 0 {
			p = p[:i]
		}
	}
	return p
}
