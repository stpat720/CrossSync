// Package hold implements the soft-deletion hold area: when a device applies
// an OVER-THRESHOLD deletion that arrived from a peer, the local copy is moved
// into the hidden .sfx-hold directory (original relative path preserved)
// instead of being unlinked. That gives the operator an undo window — the
// content is gone from the synced tree (and from every peer, exactly as
// before), but it can still be restored locally for a while.
//
// The hold area lives inside the folder so a restore is a same-volume rename,
// and it is part of the reserved .sfx-* namespace: never scanned, indexed,
// watched or sync'd.
package hold

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"crosssync/internal/staging"
)

// MaxAge is how long a held deletion is kept before maintenance purges it.
// A variable so tests can shorten it; there is deliberately no user-facing
// setting for it.
var MaxAge = 7 * 24 * time.Hour

// manifestName is the sidecar recording when each path was held. It carries no
// .sfx- prefix on purpose: the startup orphan-temp sweep removes every
// .sfx-* FILE, and the manifest must survive that.
const manifestName = ".manifest.json"

// Entry is one held deletion. Only files are listed: the directories inside the
// hold area exist to preserve the original structure, not as entries.
type Entry struct {
	Path   string    // original path relative to the folder root, slash-separated
	Size   int64     // bytes preserved
	HeldAt time.Time // when it was moved into the hold area
}

// ExpiresAt returns when the entry is purged unless it is restored or deleted
// first.
func (e Entry) ExpiresAt() time.Time { return e.HeldAt.Add(MaxAge) }

// Store is the hold area of one folder.
type Store struct {
	root string

	mu      sync.Mutex
	loaded  bool
	entries map[string]heldRecord // keyed by original relative path

	// batch counts open BeginBatch/EndBatch scopes, and dirty records that the
	// manifest changed while a batch was open. A mass deletion moves thousands of
	// copies; rewriting (and fsyncing) the whole manifest per file made that
	// O(n^2) and cost ~2s per file on the array, so a 14k backlog crawled at two
	// files a second and the counts on screen stayed high for hours.
	batch int
	dirty bool
}

type heldRecord struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	HeldAt int64  `json:"held_at"` // unix seconds
	Dir    bool   `json:"dir,omitempty"`
}

// New returns the hold area for a folder root. Nothing is created on disk
// until something is actually held.
func New(root string) *Store {
	return &Store{root: root, entries: map[string]heldRecord{}}
}

// Dir returns the absolute path of the hold area.
func (s *Store) Dir() string { return filepath.Join(s.root, staging.HoldDir) }

func (s *Store) manifestPath() string { return filepath.Join(s.Dir(), manifestName) }

// Move relocates root/rel into the hold area, recording the hold start time.
// Directory structure is preserved: .sfx-hold/<rel>.
//
// The filesystem work runs OUTSIDE the store lock. It used to run inside it, so
// every held copy in a mass deletion queued behind one mutex: the array was idle
// while the daemon applied one copy at a time (measured 2-7 files/s on a wiped
// folder). The lock now covers only the manifest, which is what it is there for.
func (s *Store) Move(rel string) error {
	rel = clean(rel)
	if rel == "" {
		return fmt.Errorf("hold: empty path")
	}
	if staging.IsReserved(rel) {
		return fmt.Errorf("hold: refusing to hold reserved path %q", rel)
	}

	src := filepath.Join(s.root, filepath.FromSlash(rel))
	st, err := os.Lstat(src)
	if err != nil {
		return err
	}
	dst := filepath.Join(s.Dir(), filepath.FromSlash(rel))
	if err := staging.EnsureDir(filepath.Dir(dst)); err != nil {
		return err
	}
	if err := movePath(src, dst); err != nil {
		return fmt.Errorf("hold: %s: %w", rel, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	s.entries[rel] = heldRecord{Path: rel, Size: st.Size(), HeldAt: time.Now().Unix(), Dir: st.IsDir()}
	return s.saveLocked()
}

// List returns every held path, sorted, with its hold start time. A path that
// is on disk but missing from the manifest (should not happen) is reported
// with the file's modification time rather than being hidden.
func (s *Store) List() ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	return s.listLocked()
}

func (s *Store) listLocked() ([]Entry, error) {
	dir := s.Dir()
	var out []Entry
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return nil // unreadable entry: skip
		}
		if p == dir {
			return nil
		}
		rel, rerr := filepath.Rel(dir, p)
		if rerr != nil {
			return nil
		}
		name := filepath.ToSlash(rel)
		if name == manifestName || d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		heldAt := info.ModTime()
		if rec, ok := s.entries[name]; ok {
			heldAt = time.Unix(rec.HeldAt, 0)
		}
		out = append(out, Entry{Path: name, Size: info.Size(), HeldAt: heldAt})
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// Restore moves a held path back to its original location. The caller scans the
// folder afterwards so the file is indexed again and re-syncs to peers.
func (s *Store) Restore(rel string) error {
	rel = clean(rel)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()

	src := filepath.Join(s.Dir(), filepath.FromSlash(rel))
	if _, err := os.Lstat(src); err != nil {
		return fmt.Errorf("hold: %q is not held: %w", rel, err)
	}
	dst := filepath.Join(s.root, filepath.FromSlash(rel))
	if _, err := os.Lstat(dst); err == nil {
		return fmt.Errorf("hold: %q already exists at its original location", rel)
	}
	if err := staging.EnsureDir(filepath.Dir(dst)); err != nil {
		return err
	}
	if err := movePath(src, dst); err != nil {
		return fmt.Errorf("hold: restore %s: %w", rel, err)
	}
	delete(s.entries, rel)
	_ = s.pruneEmptyLocked(filepath.Dir(src))
	return s.saveLocked()
}

// Delete purges one held path for real.
func (s *Store) Delete(rel string) error {
	rel = clean(rel)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	if err := s.deleteLocked(rel); err != nil {
		return err
	}
	_ = s.pruneEmptyLocked(filepath.Dir(filepath.Join(s.Dir(), filepath.FromSlash(rel))))
	return s.saveLocked()
}

// DeleteAll purges every held path for real and returns how many were removed.
func (s *Store) DeleteAll() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	entries, err := s.listLocked()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range entries {
		if s.deleteLocked(e.Path) == nil {
			n++
		}
	}
	s.entries = map[string]heldRecord{}
	if err := os.RemoveAll(s.Dir()); err != nil {
		return n, err
	}
	return n, nil
}

// Purge removes every held path older than maxAge and returns the paths that
// were purged. Called by the daemon's maintenance pass.
func (s *Store) Purge(maxAge time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadLocked()
	entries, err := s.listLocked()
	if err != nil {
		return nil, err
	}
	cutoff := time.Now().Add(-maxAge)
	var purged []string
	for _, e := range entries {
		if e.HeldAt.After(cutoff) {
			continue
		}
		if err := s.deleteLocked(e.Path); err != nil {
			continue
		}
		purged = append(purged, e.Path)
		_ = s.pruneEmptyLocked(filepath.Dir(filepath.Join(s.Dir(), filepath.FromSlash(e.Path))))
	}
	if len(purged) > 0 {
		if err := s.saveLocked(); err != nil {
			return purged, err
		}
	}
	sort.Strings(purged)
	return purged, nil
}

func (s *Store) deleteLocked(rel string) error {
	abs := filepath.Join(s.Dir(), filepath.FromSlash(rel))
	if err := os.RemoveAll(abs); err != nil {
		return err
	}
	delete(s.entries, rel)
	return nil
}

// pruneEmptyLocked removes now-empty directories between dir and the hold
// root, so a restored folder does not leave empty scaffolding behind.
func (s *Store) pruneEmptyLocked(dir string) error {
	top := s.Dir()
	for {
		if dir == top || dir == "." || dir == string(filepath.Separator) || len(dir) < len(top) {
			return nil
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) > 0 {
			return nil
		}
		if os.Remove(dir) != nil {
			return nil
		}
		dir = filepath.Dir(dir)
	}
}

func (s *Store) loadLocked() {
	if s.loaded {
		return
	}
	s.loaded = true
	if s.entries == nil {
		s.entries = map[string]heldRecord{}
	}
	raw, err := os.ReadFile(s.manifestPath())
	if err != nil {
		return
	}
	var recs []heldRecord
	if err := json.Unmarshal(raw, &recs); err != nil {
		return
	}
	for _, r := range recs {
		if r.Path == "" {
			continue
		}
		s.entries[r.Path] = r
	}
}

func (s *Store) saveLocked() error {
	if s.batch > 0 {
		// Inside a batch: the caller owns the write. One manifest write for the
		// whole batch instead of one per held file.
		s.dirty = true
		return nil
	}
	if len(s.entries) == 0 {
		// Nothing held: drop the manifest so an empty hold area leaves no
		// trace behind.
		_ = os.Remove(s.manifestPath())
		return nil
	}
	if err := staging.EnsureDir(s.Dir()); err != nil {
		return err
	}
	recs := make([]heldRecord, 0, len(s.entries))
	for _, r := range s.entries {
		recs = append(recs, r)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].Path < recs[j].Path })
	raw, err := json.Marshal(recs)
	if err != nil {
		return err
	}
	tmp := s.manifestPath() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.manifestPath())
}

// clean normalises a caller-supplied relative path and rejects anything that
// could escape the folder root.
func clean(rel string) string {
	rel = strings.TrimSpace(filepath.ToSlash(rel))
	rel = strings.TrimPrefix(rel, "./")
	if rel == "" || rel == "." {
		return ""
	}
	if strings.HasPrefix(rel, "../") || rel == ".." || strings.HasPrefix(rel, "/") {
		return ""
	}
	return rel
}

// movePath renames src to dst, falling back to a copy+remove when they live on
// different volumes (a folder root that spans mounts).
func movePath(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	st, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if st.IsDir() {
		return copyDir(src, dst)
	}
	if err := copyFile(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel(src, p)
		if rerr != nil {
			return rerr
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return staging.EnsureDir(target)
		}
		if err := staging.EnsureDir(filepath.Dir(target)); err != nil {
			return err
		}
		return copyFile(p, target)
	})
}

// BeginBatch defers manifest writes until EndBatch. A mass deletion moves
// thousands of copies into the hold area; each Move rewrote and fsynced the
// whole manifest, which is quadratic in the number of held files and measured
// ~2 s per file on the array. The files themselves are still moved immediately —
// only the manifest write is deferred, and a crash mid-batch is recoverable
// because List reports copies that are on disk but missing from the manifest.
func (s *Store) BeginBatch() {
	s.mu.Lock()
	s.batch++
	s.mu.Unlock()
}

// EndBatch closes a BeginBatch scope and writes the manifest once if anything
// changed inside it. It reports the write error, if any.
func (s *Store) EndBatch() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.batch > 0 {
		s.batch--
	}
	if s.batch > 0 {
		return nil
	}
	if !s.dirty {
		return nil
	}
	s.dirty = false
	return s.saveLocked()
}

// Flush writes the manifest now, whatever batch state the store is in. Call it
// before shutdown so a batch that never reached EndBatch is not lost.
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batch = 0
	if !s.dirty {
		return nil
	}
	s.dirty = false
	return s.saveLocked()
}
