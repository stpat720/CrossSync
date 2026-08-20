// Package scanner implements metadata-first folder scanning: it walks the
// tree, skips unchanged files without hashing them, and hashes only files
// whose (size, mtime) changed, using a parallel worker pool.
package scanner

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"crosssync/internal/hash"
	"crosssync/internal/ignore"
	"crosssync/internal/index"
	"crosssync/internal/staging"
)

// ChangeKind describes how a path changed relative to the index.
type ChangeKind int

const (
	// Unchanged means no action needed.
	Unchanged ChangeKind = iota
	// Added means the path is new.
	Added
	// Modified means the path changed.
	Modified
	// Deleted means the path vanished.
	Deleted
	// Skipped means the path is excluded by an ignore rule.
	Skipped
)

// Change is one detected filesystem change.
type Change struct {
	Kind ChangeKind
	Info *index.FileInfo // non-nil for Added/Modified/Deleted/Skipped
	Rule string          // matching ignore rule, for Skipped
}

// Progress is the live state of a scan, reported through the optional
// callback of ScanWithProgress. Phase is one of "walking", "hashing",
// "deleting" or "done".
type Progress struct {
	Phase     string // walking | hashing | deleting | done
	Walked    int    // directory entries walked so far
	HashDone  int    // files hashed so far
	HashTotal int    // total files queued to hash
}

// Scanner diffs a folder tree against its index.
type Scanner struct {
	root    string
	ix      *index.Index
	ignore  *ignore.Matcher
	workers int
}

// New creates a Scanner for a folder root and its index.
func New(root string, ix *index.Index, ig *ignore.Matcher) *Scanner {
	return &Scanner{root: root, ix: ix, ignore: ig, workers: runtime.NumCPU()}
}

type pending struct {
	rel   string
	abs   string
	kind  ChangeKind
	size  int64
	modS  int64
	modNs int32
	mode  uint32
}

// Scan walks the tree and returns changes compared to the index. Files with
// unchanged (size, mtime) are skipped without hashing. If the walk fails,
// no deletions are reported (the seen set would be incomplete).
func (s *Scanner) Scan() ([]Change, error) {
	return s.scan(nil)
}

// ScanWithProgress is Scan with a callback that receives live progress
// (how much has been walked and hashed). The callback is invoked from the
// scanning goroutine and must be fast and non-blocking.
func (s *Scanner) ScanWithProgress(fn func(Progress)) ([]Change, error) {
	return s.scan(fn)
}

func (s *Scanner) scan(fn func(Progress)) ([]Change, error) {
	emit := func(p Progress) {
		if fn != nil {
			fn(p)
		}
	}
	seen := map[string]bool{}
	var changes []Change
	var toHash []pending
	walked := 0

	walkErr := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		walked++
		if walked%64 == 0 {
			emit(Progress{Phase: "walking", Walked: walked})
		}
		rel, rerr := filepath.Rel(s.root, path)
		if rerr != nil {
			return rerr
		}
		relSlash := filepath.ToSlash(rel)
		if relSlash == "." {
			return nil
		}
		if staging.IsReserved(relSlash) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			if errors.Is(ierr, os.ErrNotExist) {
				return nil
			}
			return ierr
		}
		// Ignore rules first; ignored dirs are pruned entirely.
		if ignored, rule := s.ignore.Match(relSlash, d.IsDir()); ignored {
			seen[relSlash] = true
			if d.IsDir() {
				return filepath.SkipDir
			}
			changes = append(changes, Change{
				Kind: Skipped,
				Info: &index.FileInfo{Name: relSlash, Type: index.TypeFile},
				Rule: rule.Raw,
			})
			return nil
		}
		seen[relSlash] = true

		if d.IsDir() {
			if _, ok, _ := s.ix.Get(relSlash); !ok {
				changes = append(changes, Change{Kind: Added, Info: dirInfo(relSlash, info)})
			}
			return nil
		}

		if info.Mode()&os.ModeSymlink != 0 {
			if _, ok, _ := s.ix.Get(relSlash); !ok {
				changes = append(changes, Change{Kind: Added, Info: symlinkInfo(relSlash, info)})
			}
			return nil
		}

		sz := info.Size()
		mt := info.ModTime()
		mtS, mtNs := mt.Unix(), int32(mt.Nanosecond())

		existing, ok, _ := s.ix.Get(relSlash)
		kind := Added
		if ok {
			if existing.Size == sz && existing.ModifiedS == mtS && existing.ModifiedNs == mtNs {
				return nil // unchanged: no hashing
			}
			kind = Modified
		}
		toHash = append(toHash, pending{
			rel: relSlash, abs: path, kind: kind,
			size: sz, modS: mtS, modNs: mtNs, mode: uint32(info.Mode().Perm()),
		})
		return nil
	})
	if walkErr != nil {
		return changes, walkErr
	}

	// Hash changed/added files in parallel.
	hashTotal := len(toHash)
	emit(Progress{Phase: "hashing", Walked: walked, HashTotal: hashTotal})
	changes = append(changes, s.hashPending(toHash, func(done int) {
		emit(Progress{Phase: "hashing", Walked: walked, HashDone: done, HashTotal: hashTotal})
	})...)

	// Deleted pass (only when the walk was clean).
	emit(Progress{Phase: "deleting", Walked: walked, HashDone: hashTotal, HashTotal: hashTotal})
	_ = s.ix.List(func(fi *index.FileInfo) error {
		if !seen[fi.Name] {
			changes = append(changes, Change{Kind: Deleted, Info: fi})
		}
		return nil
	})
	emit(Progress{Phase: "done", Walked: walked, HashDone: hashTotal, HashTotal: hashTotal})
	return changes, nil
}

// hashPending hashes files using a worker pool. onDone is invoked after
// each file is processed (whether it hashed successfully or vanished).
func (s *Scanner) hashPending(pendings []pending, onDone func(int)) []Change {
	if len(pendings) == 0 {
		return nil
	}
	workers := s.workers
	if workers > len(pendings) {
		workers = len(pendings)
	}
	if workers < 1 {
		workers = 1
	}
	var (
		mu      sync.Mutex
		changes []Change
		wg      sync.WaitGroup
		hashed  int32
	)
	emit := func() {
		if onDone != nil {
			onDone(int(atomic.LoadInt32(&hashed)))
		}
	}
	jobs := make(chan pending)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				atomic.AddInt32(&hashed, 1)
				emit()
				blockSize, blocks, err := hash.FileHashes(p.abs, 0)
				if err != nil {
					// A file that vanished mid-scan is treated as a
					// deletion; the next scan's deleted pass picks it up.
					continue
				}
				ch := Change{Kind: p.kind, Info: &index.FileInfo{
					Name:       p.rel,
					Size:       p.size,
					ModifiedS:  p.modS,
					ModifiedNs: p.modNs,
					Mode:       p.mode,
					Type:       index.TypeFile,
					BlockSize:  int32(blockSize),
					Blocks:     blocks,
				}}
				mu.Lock()
				changes = append(changes, ch)
				mu.Unlock()
			}
		}()
	}
	for _, p := range pendings {
		jobs <- p
	}
	close(jobs)
	wg.Wait()
	return changes
}

func dirInfo(rel string, info fs.FileInfo) *index.FileInfo {
	return &index.FileInfo{Name: rel, Type: index.TypeDirectory, Mode: uint32(info.Mode().Perm())}
}

func symlinkInfo(rel string, info fs.FileInfo) *index.FileInfo {
	return &index.FileInfo{Name: rel, Type: index.TypeSymlink, Mode: uint32(info.Mode().Perm())}
}
