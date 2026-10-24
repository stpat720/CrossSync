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
	"time"

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
	// Touched means a VERIFIED scan re-hashed a file and found its content
	// byte-identical to the index while its (size, mtime) had moved. The new
	// stat is adopted and the version is kept: a touched file is not a
	// content change and must never re-transfer to peers.
	Touched
)

// Change is one detected filesystem change.
type Change struct {
	Kind ChangeKind
	Info *index.FileInfo // non-nil for Added/Modified/Deleted/Skipped/Renamed
	From string          // old name, for Renamed
	Rule string          // matching ignore rule, for Skipped
	// OutOfGate marks a Renamed change whose TARGET is outside this
	// device's include gate: a local rename moved an in-scope file to a
	// path this device no longer tracks. The scanner otherwise reports such
	// a rename as a plain deletion of the old path, which destroys peers'
	// copies instead of moving them.
	OutOfGate bool
}

// Progress is the live state of a scan, reported through the optional
// callback of ScanWithProgress. Phase is one of "walking", "hashing",
// "deleting" or "done".
type Progress struct {
	Phase     string // walking | hashing | deleting | done
	Walked    int    // directory entries walked so far
	HashDone  int    // files hashed so far
	HashTotal int    // total files queued to hash
	InFlight  int    // changed files held back: still being written
}

// InFlight is one file held back by the stability gate: it differs from its
// index entry (or has none) but has not yet been observed unchanged for
// enough consecutive scans to be trusted as a settled snapshot.
type InFlight struct {
	Name  string
	Size  int64
	Since int64 // unix seconds of the first observation (carried across scans)
	Scans int   // consecutive stable observations so far (1..K-1)
}

// Scanner diffs a folder tree against its index.
type Scanner struct {
	root    string
	ix      *index.Index
	ignore  *ignore.Matcher
	include *ignore.Matcher // include (whitelist) gate; nil = everything in scope
	workers int

	// settleScans is the stability-gate depth: a changed file is hashed and
	// offered to peers only after this many consecutive scans observed the
	// same (size, mtime). 1 disables the gate (settle on first sight).
	settleScans int
	// settleSeconds is the QUIET WINDOW half of the stability gate: a changed
	// file becomes eligible only when its (size, mtime) has been unchanged for
	// at least this long, not merely across N observations. Scan cadence is not
	// a safety property — once scans are event-driven, two of them can happen
	// milliseconds apart while a copy is still running, and a count-only gate
	// would promote a half-written file. 0 disables the window (settleScans <= 1
	// disables the whole gate).
	settleSeconds int
	// nowFn is the clock, injectable so tests can drive the quiet window.
	nowFn func() int64
	// deletionScans is the deletion gate's depth: a path that the walk did not
	// see is only reported as deleted after this many consecutive scans missed
	// it. 1 disables the gate. See SetDeletionScans.
	deletionScans int
	// bypassGate forces the gate open for one scan (rebuildFromFS only: a
	// freshly quarantined index has no prior content a partial could
	// contradict, so the filesystem is the truth for that scan).
	bypassGate bool
	// lastInFlight is the set held back by the most recent scan (published
	// to the engine by the node after the scan's changes are applied).
	lastInFlight []InFlight
	// forceHash makes the NEXT scan re-hash every file instead of trusting
	// (size, mtime). An operator-triggered verified scan: content changes that
	// preserved both size and mtime (rsync -a, restore tools, coarse SMB
	// timestamps) are invisible to the fast path. Consumed by that one scan.
	forceHash bool
}

// SetForceHash arms a one-shot verified scan: every file is re-hashed, and a
// file whose content turns out identical is reported as Touched (stat adopted,
// version kept) rather than as a modification, so nothing re-transfers.
func (s *Scanner) SetForceHash(on bool) { s.forceHash = on }

// New creates a Scanner for a folder root and its index.
func New(root string, ix *index.Index, ig *ignore.Matcher) *Scanner {
	return &Scanner{
		root: root, ix: ix, ignore: ig, workers: runtime.NumCPU(),
		settleScans: 2, settleSeconds: 10, deletionScans: 2,
		nowFn: func() int64 { return time.Now().Unix() },
	}
}

// SetInclude enables the include (whitelist) gate: paths that do not
// match are out of scope — never indexed, transferred, or deleted.
// A nil matcher is a no-op.
func (s *Scanner) SetInclude(m *ignore.Matcher) { s.include = m }

// SetSettleScans sets the stability-gate depth: how many consecutive scans
// must observe the same (size, mtime) before a changed file is hashed and
// offered to peers. n <= 0 means the default 2; 1 disables the gate
// (pre-gate behaviour: settle on first sight).
func (s *Scanner) SetSettleScans(n int) {
	if n <= 0 {
		n = 2
	}
	s.settleScans = n
}

// SetSettleSeconds sets the quiet window of the stability gate: how long a
// changed file's (size, mtime) must stay identical before it is hashed and
// offered to peers. This is what makes the gate independent of scan cadence,
// so event-driven scans cannot promote a file that is still being copied.
// n <= 0 disables the window (count-only gate: only the tests want that).
func (s *Scanner) SetSettleSeconds(n int) {
	if n < 0 {
		n = 0
	}
	s.settleSeconds = n
}

// SetClock replaces the scanner's clock (tests only).
func (s *Scanner) SetClock(fn func() int64) {
	if fn != nil {
		s.nowFn = fn
	}
}

// now is the scanner's current time in unix seconds.
func (s *Scanner) now() int64 {
	if s.nowFn != nil {
		return s.nowFn()
	}
	return time.Now().Unix()
}

// SetDeletionScans sets how many consecutive scans a path must be MISSING
// before a deletion is reported. Default 2; n <= 1 disables the gate.
//
// This is the mirror image of the settle gate, and it exists for the same
// reason: on a union/FUSE share (/mnt/user on Unraid, a network mount, a share
// whose cache is being rebuilt) a freshly written directory can be invisible to
// one walk and present on the next. Without the gate that single bad walk
// tombstones real files — the peer then deletes them, and a directory tombstone
// used to take its whole subtree with it. Believing a deletion one scan later
// costs ~one sync interval; believing it immediately can cost the data.
//
// Rename pairing still happens on the FIRST scan (a rename is detected by
// content, not by absence), so a moved file is never re-uploaded because of
// this gate.
func (s *Scanner) SetDeletionScans(n int) {
	if n <= 0 {
		n = 2
	}
	s.deletionScans = n
}

// SetGateBypass forces the stability gate open (or restores it). Only
// rebuildFromFS sets it, for the single scan that repopulates a quarantined
// index.
func (s *Scanner) SetGateBypass(b bool) { s.bypassGate = b }

// LastInFlight returns the files the most recent scan held back because they
// were still being written (empty when the gate is off or everything
// settled).
func (s *Scanner) LastInFlight() []InFlight { return s.lastInFlight }

// gateOn reports whether the stability gate applies to this scan.
func (s *Scanner) gateOn() bool { return !s.bypassGate && s.settleScans > 1 }

type pending struct {
	rel   string
	abs   string
	kind  ChangeKind
	size  int64
	modS  int64
	modNs int32
	mode  uint32
	first int64 // unix seconds the current settle run started
	// prevKey is the indexed content identity, set only for a verified
	// (forceHash) scan. When the re-hash matches it, the file is Touched
	// (stat adopted, version kept) instead of Modified.
	prevKey string
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
	// Stability gate state. A SettleList failure degrades the gate to "hold
	// everything one extra scan" — never a hard scan failure.
	settle, serr := s.ix.SettleList()
	if serr != nil {
		settle = map[string]index.SettleState{}
	}
	nextSettle := map[string]index.SettleState{}
	var inFlight []InFlight
	now := s.now()
	gateOn := s.gateOn()
	// Verified scan: re-hash everything for this one pass.
	forceHash := s.forceHash
	s.forceHash = false

	// Out-of-gate rename detection (only with an include gate): a rename that
	// moved an in-scope file to a path the gate excludes looks exactly like a
	// deletion from the walk — the old path is gone and the new one is never
	// hashed — so the content would be reported as a plain deletion and
	// destroy peers' copies instead of moving them. Candidates are collected
	// by the (size, mtime) identity a rename preserves; the deleted pass
	// verifies them by content before trusting them. The index lookup covers
	// only this device's own in-scope files, so memory stays proportional to
	// what this device actually tracks, not to the whole tree.
	var gateCands []gateCandidate
	var gateByMT map[mtKey][]string
	if s.include != nil {
		gateByMT = s.inScopeFilesByMT()
	}

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
					// A regular file the gate excludes may be the
					// destination of a rename that moved one of OUR files out
					// of scope (see the gateCands note above).
					if len(gateByMT) > 0 && info.Mode()&os.ModeSymlink == 0 {
						mt := info.ModTime()
						k := mtKey{info.Size(), mt.Unix(), int32(mt.Nanosecond())}
						if froms := gateByMT[k]; len(froms) == 1 {
							gateCands = append(gateCands, gateCandidate{
								from: froms[0], rel: relSlash, abs: path,
								size: info.Size(), modS: mt.Unix(), modNs: int32(mt.Nanosecond()),
								mode: uint32(info.Mode().Perm()),
							})
						}
					}
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
		prevKey := ""
		if ok {
			if existing.Size == sz && existing.ModifiedS == mtS && existing.ModifiedNs == mtNs && !forceHash {
				return nil // unchanged: no hashing
			}
			kind = Modified
			if forceHash {
				// Remember what the index says so an unchanged re-hash can be
				// adopted as Touched instead of re-versioned.
				prevKey = contentKey(existing)
			}
		}
		// Stability gate: a changed file becomes transfer-eligible only after
		// its (size, mtime) has been observed unchanged across settleScans
		// consecutive scans. Held-back files stay in seen (no false deletion,
		// rename machinery intact) but are never hashed, versioned or
		// advertised. With the gate off (1, bypass, receive-only) the walk is
		// byte-for-byte the pre-gate behaviour. A VERIFIED scan bypasses the
		// gate: the operator asked for the truth now, and the Touched path
		// makes an unchanged file a no-op.
		first := now
		if !gateOn || forceHash {
			toHash = append(toHash, pending{
				rel: relSlash, abs: path, kind: kind,
				size: sz, modS: mtS, modNs: mtNs, mode: uint32(info.Mode().Perm()),
				first: first, prevKey: prevKey,
			})
			return nil
		}
		count := 1
		if prev, had := settle[relSlash]; had && prev.Size == sz && prev.ModS == mtS && prev.ModNs == mtNs {
			// count resets on ANY change of size, modified_s or modified_ns:
			// no mtime-only or size-only exception.
			count, first = prev.Count+1, prev.First
		}
		// Hold while EITHER the observation count is short OR the quiet window
		// has not elapsed. The window is the part that matters now that scans can
		// be triggered by change events: without it, two scans 50 ms apart would
		// "agree" about a file that is still being written.
		windowOpen := s.settleScans > 1 && s.settleSeconds > 0 && now-first < int64(s.settleSeconds)
		if count < s.settleScans || windowOpen {
			nextSettle[relSlash] = index.SettleState{Size: sz, ModS: mtS, ModNs: mtNs, Count: count, First: first}
			inFlight = append(inFlight, InFlight{Name: relSlash, Size: sz, Since: first, Scans: count})
			return nil // indexed, NOT hashed, NOT advertised
		}
		toHash = append(toHash, pending{
			rel: relSlash, abs: path, kind: kind,
			size: sz, modS: mtS, modNs: mtNs, mode: uint32(info.Mode().Perm()),
			first: first, prevKey: prevKey,
		})
		return nil
	})
	if walkErr != nil {
		// A partial walk must never rewrite the settling table.
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
	hashed, drifted := s.hashPending(toHash, func(done int) {
		emit(Progress{Phase: "hashing", Walked: walked, HashDone: done, HashTotal: hashTotal})
	})
	changes = append(changes, hashed...)
	// A file whose stat moved while it was being read is demoted to in-flight
	// for this round: its post-hash stat starts a fresh run, so the next scan
	// re-verifies it instead of trusting a self-inconsistent entry.
	for _, d := range drifted {
		nextSettle[d.rel] = d.state
		inFlight = append(inFlight, InFlight{Name: d.rel, Size: d.state.Size, Since: d.state.First, Scans: d.state.Count})
	}

	// Deleted pass (only when the walk was clean).
	emit(Progress{Phase: "deleting", Walked: walked, HashDone: hashTotal, HashTotal: hashTotal})
	changes = s.detectRenames(changes, seen, claimed, collapseGateCands(gateCands))
	// Rename pairing already ran (above) on the same "absent from the walk" set,
	// so a move is still detected on the FIRST scan; only the deletions that
	// were not part of a rename wait for confirmation.
	changes = s.confirmDeletions(changes, now)
	// One read and one write transaction per scan, regardless of file count.
	// Rows survive only for files still unsettled: a file that settled, was
	// applied, vanished, or now matches its index is simply absent from
	// nextSettle.
	_ = s.ix.SettleReplace(nextSettle)
	s.lastInFlight = inFlight
	emit(Progress{Phase: "done", Walked: walked, HashDone: hashTotal, HashTotal: hashTotal, InFlight: len(inFlight)})
	return changes, nil
}

// confirmDeletions is the deletion gate: a path the walk did not see is only
// reported as deleted once deletionScans consecutive scans have missed it. The
// state lives in the index (not in memory) so a daemon restart or a folder
// reload cannot reset a half-confirmed deletion, and a path that comes back
// simply drops out of the table.
//
// Only unpaired deletions reach here: renames were already folded into Renamed
// changes by detectRenames, so the gate never delays a move.
func (s *Scanner) confirmDeletions(changes []Change, now int64) []Change {
	if s.deletionScans <= 1 {
		// Gate off: forget any half-confirmed state so turning it back on
		// starts from a clean table.
		_ = s.ix.MissingReplace(nil)
		return changes
	}
	prev, err := s.ix.MissingList()
	if err != nil {
		// No table (or unreadable): behave as if nothing was suspected, which
		// means deletions take one extra scan rather than none.
		prev = map[string]index.MissingState{}
	}
	next := make(map[string]index.MissingState, 8)
	out := changes[:0]
	for _, c := range changes {
		if c.Kind != Deleted || c.Info == nil {
			out = append(out, c)
			continue
		}
		st, had := prev[c.Info.Name]
		if !had {
			st = index.MissingState{First: now}
		}
		st.Count++
		if st.Count < s.deletionScans {
			// Missing for the first time: wait for the next scan to agree.
			next[c.Info.Name] = st
			continue
		}
		out = append(out, c)
	}
	_ = s.ix.MissingReplace(next)
	return out
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
//
// gateMoves holds the verified-out-of-gate rename destinations (candidate
// (size, mtime) matches, keyed by the vanished in-scope path). A vanished
// entry that pairs with one is emitted as a Renamed change marked OutOfGate
// — still no re-hash of the target beyond the verification read — instead of
// a deletion, so peers can apply a move instead of destroying their copies.
func (s *Scanner) detectRenames(changes []Change, seen map[string]bool, claimed map[string]bool, gateMoves map[string]gateCandidate) []Change {
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
			// Renamed out of this device's gate: the content lives on at an
			// out-of-scope path. Verify by content before trusting the
			// (size, mtime) match — a filesystem that rewrites content but
			// preserves timestamps must fall back to a plain deletion.
			if c, ok := gateMoves[fi.Name]; ok {
				if _, blocks, err := hash.FileHashes(c.abs, 0); err == nil && equalBlocks(blocks, fi.Blocks) {
					moved := fi.Clone()
					moved.Name = c.rel
					moved.Sequence = 0
					moved.ModifiedS, moved.ModifiedNs, moved.Mode = c.modS, c.modNs, c.mode
					changes = append(changes, Change{Kind: Renamed, From: fi.Name, Info: moved, OutOfGate: true})
					return nil
				}
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

// gateCandidate is one out-of-gate file on disk that shares its (size, mtime)
// with exactly one in-scope live index entry: the likely destination of a
// local rename that moved that entry out of this device's include gate.
type gateCandidate struct {
	from  string // the vanished in-scope index path
	rel   string // the out-of-gate path on disk (slash-separated)
	abs   string
	size  int64
	modS  int64
	modNs int32
	mode  uint32
}

// inScopeFilesByMT indexes this device's own live files by (size, mtime), so
// the walk can recognise an out-of-gate file as the destination of one of
// them without a per-file index query. Only in-scope files can be renamed
// out of the gate, so the map is bounded by the gate's own file set rather
// than by the whole tree.
func (s *Scanner) inScopeFilesByMT() map[mtKey][]string {
	out := map[mtKey][]string{}
	if s.include == nil {
		return out
	}
	_ = s.ix.List(func(fi *index.FileInfo) error {
		if fi.Type != index.TypeFile {
			return nil
		}
		if matched, _ := s.include.Match(fi.Name, false); !matched {
			return nil
		}
		k := mtKey{fi.Size, fi.ModifiedS, fi.ModifiedNs}
		out[k] = append(out[k], fi.Name)
		return nil
	})
	return out
}

// collapseGateCands keeps only unambiguous candidates: one vanished in-scope
// entry and one out-of-gate file for a given (size, mtime). An ambiguous
// match (a copy, or several files with the same size and timestamp) is not a
// rename and falls back to the normal deletion path.
func collapseGateCands(cands []gateCandidate) map[string]gateCandidate {
	if len(cands) == 0 {
		return nil
	}
	counts := make(map[string]int, len(cands))
	for _, c := range cands {
		counts[c.from]++
	}
	out := make(map[string]gateCandidate, len(cands))
	for _, c := range cands {
		if counts[c.from] == 1 {
			out[c.from] = c
		}
	}
	return out
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

// statForHash re-stats a file immediately after hashing it, so a file that
// changed under the reader is caught before its hashes are recorded against
// the walk's (now stale) size. A package-level var, like maxVerifyFileSize,
// so tests can inject drift deterministically.
var statForHash = os.Stat

// driftResult is one file that changed between the walk and the hash.
type driftResult struct {
	rel   string
	state index.SettleState
}

// hashPending hashes files using a worker pool. onDone is invoked after
// each file is processed (whether it hashed successfully or vanished).
// Files whose stat moved while they were read are returned in the drift
// list instead of as changes: the index entry would otherwise claim the
// walk's size with hashes of the newer content (blockSizeAt and the pull's
// Truncate would then disagree with the block list and the round would die
// on a hash mismatch).
func (s *Scanner) hashPending(pendings []pending, onDone func(int)) ([]Change, []driftResult) {
	if len(pendings) == 0 {
		return nil, nil
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
		drifted []driftResult
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
				st, serr := statForHash(p.abs)
				if serr != nil {
					continue // vanished: today's behaviour
				}
				if st.Size() != p.size || st.ModTime().Unix() != p.modS || int32(st.ModTime().Nanosecond()) != p.modNs {
					// Changed while we were reading it: demote to in-flight
					// for this round, starting a fresh run from the stat that
					// was actually hashed.
					mu.Lock()
					drifted = append(drifted, driftResult{rel: p.rel, state: index.SettleState{
						Size: st.Size(), ModS: st.ModTime().Unix(), ModNs: int32(st.ModTime().Nanosecond()),
						Count: 1, First: p.first,
					}})
					mu.Unlock()
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
				// A verified scan re-hashes even unchanged files: identical
				// content means the stat moved but nothing was written, so
				// adopt the stat and keep the version (no peer re-transfer).
				if p.prevKey != "" && contentKey(ch.Info) == p.prevKey {
					ch.Kind = Touched
				}
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
	return changes, drifted
}

func dirInfo(rel string, info fs.FileInfo) *index.FileInfo {
	return &index.FileInfo{Name: rel, Type: index.TypeDirectory, Mode: uint32(info.Mode().Perm())}
}

func symlinkInfo(rel string, info fs.FileInfo) *index.FileInfo {
	return &index.FileInfo{Name: rel, Type: index.TypeSymlink, Mode: uint32(info.Mode().Perm())}
}
