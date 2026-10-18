// Package scanner implements metadata-first folder scanning: it walks the
// tree, skips unchanged files without hashing them, and hashes only files
// whose (size, mtime) changed, using a parallel worker pool.
package scanner

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
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
	// Renamed means a path was moved to another path with identical
	// content (detected by pairing a deletion with an addition of the same
	// content). Info is the new (target) entry; From is the old name.
	Renamed
)

// Change is one detected filesystem change.
type Change struct {
	Kind ChangeKind
	Info *index.FileInfo // non-nil for Added/Modified/Deleted/Skipped/Renamed
	From string          // old name, for Renamed
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
	include *ignore.Matcher // include (whitelist) gate; nil = everything in scope
	workers int
}

// New creates a Scanner for a folder root and its index.
func New(root string, ix *index.Index, ig *ignore.Matcher) *Scanner {
	return &Scanner{root: root, ix: ix, ignore: ig, workers: runtime.NumCPU()}
}

// SetInclude enables the include (whitelist) gate: paths that do not
// match are out of scope — never indexed, transferred, or deleted.
// A nil matcher is a no-op.
func (s *Scanner) SetInclude(m *ignore.Matcher) { s.include = m }

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
		// Include (whitelist) gate: with an include list, non-matching paths
		// are out of scope — never indexed, transferred, or deleted.
		// NOTE: do NOT SkipDir here — an extension or deep-dir rule can match
		// deeper descendants of a non-matching directory.
		if s.include != nil {
			if matched, _ := s.include.Match(relSlash, d.IsDir()); !matched {
				seen[relSlash] = true
				if !d.IsDir() {
					changes = append(changes, Change{
						Kind: Skipped,
						Info: &index.FileInfo{Name: relSlash, Type: index.TypeFile},
						Rule: "include",
					})
				}
				return nil
			}
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

	// Cheap-rename pass: pair (size, mtime)-identical added files with
	// vanished index entries BEFORE hashing, so a folder rename (which
	// preserves mtimes on every major filesystem) needs no re-hash of the
	// moved data. Pairs are only trusted when the scan is rename-dominated
	// and a sampled subset verifies by hashing; anything else falls through
	// to the exact content-based detectRenames pass below.
	renames, claimed := s.cheapRenames(&toHash, seen)
	changes = append(changes, renames...)

	// Hash changed/added files in parallel.
	hashTotal := len(toHash)
	emit(Progress{Phase: "hashing", Walked: walked, HashTotal: hashTotal})
	changes = append(changes, s.hashPending(toHash, func(done int) {
		emit(Progress{Phase: "hashing", Walked: walked, HashDone: done, HashTotal: hashTotal})
	})...)

	// Deleted pass (only when the walk was clean).
	emit(Progress{Phase: "deleting", Walked: walked, HashDone: hashTotal, HashTotal: hashTotal})
	changes = s.detectRenames(changes, seen, claimed)
	emit(Progress{Phase: "done", Walked: walked, HashDone: hashTotal, HashTotal: hashTotal})
	return changes, nil
}

// detectRenames pairs deleted files with added files of identical content
// and turns each unambiguous 1:1 pair into a single Renamed change instead
// of a delete+add pair, so the index (and peers) see a move rather than a
// re-upload. Only files are paired — directories and symlinks carry no
// content and stay on the normal add/delete path. Groups that are not
// exactly 1:1 (a copy/duplicate, or content reused across several paths)
// fall back to delete+add: the pairing would be ambiguous. claimed holds
// vanished names already folded into renames by cheapRenames; they are
// skipped so they are not also emitted as deletions.
func (s *Scanner) detectRenames(changes []Change, seen map[string]bool, claimed map[string]bool) []Change {
	// Index of the added-file changes with hashed content, grouped by
	// content key, so a deleted entry can find a same-content target.
	addedByKey := map[string][]int{}
	for i, c := range changes {
		if c.Kind == Added && c.Info.Type == index.TypeFile && len(c.Info.Blocks) > 0 {
			k := contentKey(c.Info)
			addedByKey[k] = append(addedByKey[k], i)
		}
	}
	claimedAdded := map[int]bool{}
	// The deleted pass ALWAYS runs (a deletion is a change even when there
	// is nothing to pair it with); the rename pairing is an optimization
	// that only kicks in when a same-content target exists.
	_ = s.ix.List(func(fi *index.FileInfo) error {
		if seen[fi.Name] || claimed[fi.Name] {
			return nil
		}
		// Out-of-scope entries are never emitted as local deletions: the
		// include gate does not own them.
		if s.include != nil {
			if matched, _ := s.include.Match(fi.Name, fi.Type == index.TypeDirectory); !matched {
				return nil
			}
		}
		if fi.Type == index.TypeFile && len(fi.Blocks) > 0 {
			if idxs := addedByKey[contentKey(fi)]; len(idxs) == 1 && !claimedAdded[idxs[0]] {
				i := idxs[0]
				claimedAdded[i] = true
				changes = append(changes, Change{Kind: Renamed, From: fi.Name, Info: changes[i].Info})
				return nil
			}
		}
		changes = append(changes, Change{Kind: Deleted, Info: fi})
		return nil
	})
	// Drop the added changes that were folded into renames.
	if len(claimedAdded) > 0 {
		filtered := changes[:0]
		for i, c := range changes {
			if c.Kind == Added && claimedAdded[i] {
				continue
			}
			filtered = append(filtered, c)
		}
		changes = filtered
	}
	return changes
}

// mtKey identifies a file by size and modification time — the same identity
// the walk uses to skip unchanged files without hashing. A rename preserves
// both, so a moved file is a perfect (size, mtime) match for its old index
// entry.
type mtKey struct {
	size  int64
	modS  int64
	modNs int32
}

// Verification budgets for the cheap-rename sample (vars so tests can lower
// them). Files above maxVerifyFileSize are trusted on (size, mtime) alone —
// the same trust the unchanged-file fast path already grants every file on
// every scan — so renaming a folder of a few huge files never re-hashes
// them; maxVerifyTotal bounds the worst-case verification work regardless
// of how many files are moved.
var (
	maxVerifyFileSize int64 = 64 << 20  // per-file cap: 64 MiB
	maxVerifyTotal    int64 = 256 << 20 // total verification budget: 256 MiB
)

// cheapRenames pairs added files with vanished index entries of identical
// (size, mtime) so a folder rename never re-hashes the moved data: the moved
// file's content is byte-identical by construction of a rename, so its
// vanished entry's stored block hashes are reused. The pass is deliberately
// conservative:
//
//  1. Only unambiguous 1:1 (size, mtime) matches are candidates.
//  2. The majority gate requires the scan to be rename-dominated — most
//     added files pair with a vanished file, and most vanished files pair
//     back. Mixed delete/add churn (a few deletes and adds alongside) or a
//     small move amid lots of new content therefore falls through to the
//     exact content-based path, which is correct and cheap for a few files.
//  3. A sampled subset of the candidates is hashed and compared against the
//     vanished entries' stored blocks. Any mismatch — a filesystem doing
//     timestamp-preserving rewrites — downgrades the whole pass.
//
// Returns the trusted Renamed changes and the set of vanished names claimed
// (so the deleted pass does not also emit them). Unpaired added files stay
// in toHash and are hashed normally; detectRenames always runs afterwards as
// the exact safety net for copies and mtime-changed moves.
func (s *Scanner) cheapRenames(toHash *[]pending, seen map[string]bool) ([]Change, map[string]bool) {
	type cand struct {
		pi  int // index into *toHash
		old *index.FileInfo
	}
	var (
		addedByMT    = map[mtKey][]int{}
		vanishedByMT = map[mtKey][]*index.FileInfo{}
		addedCount   int
		vanished     int
	)
	for i, p := range *toHash {
		if p.kind != Added {
			continue
		}
		addedCount++
		k := mtKey{p.size, p.modS, p.modNs}
		addedByMT[k] = append(addedByMT[k], i)
	}
	// No added files -> nothing can be a rename. Skip the (otherwise
	// unconditional) full live-index read so a plain no-op/modify/delete
	// scan does not pay for rename detection.
	if addedCount == 0 {
		return nil, nil
	}
	_ = s.ix.List(func(fi *index.FileInfo) error {
		if seen[fi.Name] {
			return nil
		}
		if s.include != nil {
			if matched, _ := s.include.Match(fi.Name, fi.Type == index.TypeDirectory); !matched {
				return nil
			}
		}
		if fi.Type != index.TypeFile || len(fi.Blocks) == 0 {
			return nil
		}
		vanished++
		k := mtKey{fi.Size, fi.ModifiedS, fi.ModifiedNs}
		vanishedByMT[k] = append(vanishedByMT[k], fi)
		return nil
	})
	// 1:1 candidates only — an ambiguous (size, mtime) match is not a rename.
	var cands []cand
	for k, addedIdxs := range addedByMT {
		if len(addedIdxs) != 1 {
			continue
		}
		vs := vanishedByMT[k]
		if len(vs) != 1 {
			continue
		}
		cands = append(cands, cand{pi: addedIdxs[0], old: vs[0]})
	}
	if len(cands) == 0 {
		return nil, nil
	}
	// Majority gate: only trust mtime pairing when the scan is
	// rename-dominated. If most added files do not pair (genuine new
	// content) or most vanished files do not pair (genuine deletions), this
	// is mixed churn, not a move — the exact content path handles it.
	if len(cands)*2 < addedCount || len(cands)*2 < vanished {
		return nil, nil
	}
	// Sample verification: hash a spread of the candidates and confirm their
	// content matches the vanished entries' stored blocks. Only files under
	// the per-file cap are verified, and total verification work is bounded
	// by a byte budget — so a folder of a few huge files (the classic media
	// case) is never re-hashed: those files are trusted on (size, mtime)
	// alone, the same trust the unchanged-file fast path already grants. A
	// single verified mismatch means the environment cannot be trusted for
	// mtime pairing (e.g. a filesystem that rewrites content but preserves
	// timestamps), so the whole pass is downgraded to the exact
	// content-based path.
	sort.Slice(cands, func(i, j int) bool {
		return (*toHash)[cands[i].pi].rel < (*toHash)[cands[j].pi].rel
	})
	k := len(cands)/32 + 1
	if k < 3 {
		k = 3
	}
	if k > 64 {
		k = 64
	}
	if k > len(cands) {
		k = len(cands)
	}
	step := len(cands) / k
	if step < 1 {
		step = 1
	}
	var verifyTotal int64
	for i := 0; i < len(cands); i += step {
		c := cands[i]
		p := (*toHash)[c.pi]
		if p.size > maxVerifyFileSize || verifyTotal+p.size > maxVerifyTotal {
			continue // too large to verify cheaply: trust (size, mtime)
		}
		verifyTotal += p.size
		_, blocks, err := hash.FileHashes(p.abs, 0)
		if err != nil {
			return nil, nil // file vanished mid-scan: do not trust
		}
		if !equalBlocks(blocks, c.old.Blocks) {
			return nil, nil // content differs despite (size, mtime): downgrade
		}
	}
	// Trusted: emit a Renamed change per pair, reusing the vanished entry's
	// stored hashes, and drop the paired files from the hash pool.
	paired := map[int]bool{}
	claimed := map[string]bool{}
	var out []Change
	for _, c := range cands {
		p := (*toHash)[c.pi]
		// Shallow copy: Block slices are immutable once hashed, so sharing
		// the vanished entry's block slices is safe and avoids duplicating
		// every moved file's hashes across the whole rename.
		fi := *c.old
		fi.Name = p.rel
		// The entry moves to a NEW path, so it must get a NEW sequence: a
		// stale sequence (the vanished entry's) would fall below the peer's
		// acknowledged max and never be sent in a delta index exchange.
		fi.Sequence = 0
		out = append(out, Change{Kind: Renamed, Info: &fi, From: c.old.Name})
		paired[c.pi] = true
		claimed[c.old.Name] = true
	}
	filtered := (*toHash)[:0]
	for i, p := range *toHash {
		if paired[i] {
			continue
		}
		filtered = append(filtered, p)
	}
	*toHash = filtered
	return out, claimed
}

// equalBlocks reports whether two block-hash lists are identical.
func equalBlocks(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// contentKey is a compact identity for a file's content: size plus every
// block hash (mirrors sync.contentKey). Two entries with equal keys have
// identical content.
func contentKey(fi *index.FileInfo) string {
	var b bytes.Buffer
	b.Grow(16 + 32*len(fi.Blocks))
	b.WriteString(strconv.FormatInt(fi.Size, 10))
	b.WriteByte(0)
	for _, blk := range fi.Blocks {
		b.Write(blk)
	}
	return b.String()
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
