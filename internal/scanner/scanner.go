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
	"strings"
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
	// Committed marks a change the batch sink has ALREADY applied to the
	// index during the hash phase (see SetBatchSink). It is still in the
	// scanner's result so the rename and deletion passes can pair against it,
	// but a caller applying the returned slice must skip it: applying a
	// committed entry a second time would bump the version of content that
	// never changed.
	Committed bool
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
	// IgnoredFiles / IgnoredBytes count what the walk deliberately did not
	// index: filesystem bookkeeping names (.fuse_hidden, .smbdelete, .nfs)
	// and CrossSync's own .sfx-* areas. They are not data, but they do
	// occupy the share, and 1.8 GB of them sat unnoticed after a deletion
	// left a FUSE ghost behind.
	IgnoredFiles int
	IgnoredBytes int64
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
	// workers is the hash reader pool: how many files are read and hashed at
	// the same time. Defaults to one per CPU (see New); the node overrides it
	// from scan.hash_workers. Concurrency is the lever we own on a single
	// spinning disk: 37 interleaved readers on one platter is a seek storm (864
	// reads/s at 126 kB, 27.8 ms r_await, 100 % util, ~55-70 MB/s delivered),
	// while the same corpus hashed at ~156 MB/s on an uncontended node. Request
	// SIZE is not ours to set: hash.BlockHashes already reads 1 MiB per syscall
	// and the ~126 kB the device sees is shfs(FUSE) splitting it below us.
	workers int
	// workerSource, when set, SUPERSEDES workers and is consulted at the start
	// of every hash phase: PUT /api/settings/sync changes the pool on a running
	// daemon, and reading it once per pass is what makes the change apply to the
	// NEXT scan without disturbing one already in flight (a pass in progress
	// keeps the readers it launched).
	workerSource func() int
	// workerHook, when set, is called with the number of hash workers actually
	// launched for a pass. A test seam (like hashHook): the pool size is
	// decided inside hashPending after clamping to the number of pending files,
	// so this is how a test proves the configured value reached the goroutines.
	// Nil in production.
	workerHook func(n int)

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
	// crossScopePair lets a SCOPED pass pair a new file with an old path it can
	// PROVE is gone (targeted stat + content match). On by default; the
	// fallback (add now, delete later, peer copies from its local copy) is
	// kept reachable for tests and for hosts where the stat cannot be
	// trusted.
	crossScopePair bool
	// bypassGate forces the gate open for one scan (rebuildFromFS only: a
	// freshly quarantined index has no prior content a partial could
	// contradict, so the filesystem is the truth for that scan).
	bypassGate bool
	// Held-back sets are published per PASS KIND, not per scanner: a full walk
	// and a scoped pass of the same folder may overlap, and each must be able
	// to replace only its own set. A walk that finished after a scoped pass
	// must not un-gate the file the scoped pass is still holding (that file is
	// still being written), and a scoped pass must not drop the walk's held
	// files outside its scope. LastInFlight returns the union.
	inFlightMu     sync.Mutex
	walkInFlight   []InFlight
	scopedInFlight []InFlight
	// stateMu serialises the settling and deletion-confirmation tables'
	// read-modify-write across overlapping passes (both tables live in the same
	// index and both passes update them). It is held only for the merge+write,
	// never across a walk or a hash.
	stateMu sync.Mutex
	// cancelFn, when set, is consulted throughout a scan; returning true
	// abandons it. This is what makes a paused folder actually STOP: the folder
	// lock is held for the whole pass, so without a cooperative check a pause
	// request waits (and times out) behind a scan that can run for many minutes
	// on a large share, while the disk keeps being read.
	cancelFn func() bool
	// hashHook, when set, runs after each file has been hashed. A test seam
	// (like SetClock): a tree small enough for a test hashes in milliseconds, so
	// this is how a test lands a pause deterministically in the middle of the
	// hash phase instead of racing the clock. Nil in production.
	hashHook func(name string)
	// forceHash makes the NEXT scan re-hash every file instead of trusting
	// (size, mtime). An operator-triggered verified scan: content changes that
	// preserved both size and mtime (rsync -a, restore tools, coarse SMB
	// timestamps) are invisible to the fast path. Consumed by that one scan.
	//
	// Atomic because two passes of one folder can run at once (a full walk and
	// a watcher-driven scoped pass, see node.Folder's scan slots). A scoped
	// pass must not be able to steal the verified walk's flag: ScanWithProgress
	// consumes whatever is stored, while ScanForceWithProgress carries the
	// "re-hash everything" decision as an argument and never touches this.
	forceHash atomic.Bool
	// batchSink, when set, APPLIES hashed changes while the hash phase is
	// still running, in bounded batches, instead of leaving every entry in
	// memory until the pass ends. This is what makes a first pass over a very
	// large share resumable: the work it has already hashed is committed to
	// the index, so a restart (or a deploy) skips those files on (size, mtime)
	// and hashes only the remainder. Nil (the default) keeps the single
	// end-of-pass apply.
	batchSink func([]Change) error
	// batchWriterHook, when set, runs as the batch writer goroutine exits. A
	// test seam: it is how a test proves a pass (finished or cancelled) leaves
	// no writer behind. Nil in production.
	batchWriterHook func()
}

// Batch sizing for a pass with a batch sink. An entry count AND a byte budget:
// a folder of very large files reaches the byte budget first and a folder of
// small files the entry count, and either one bounds how much unapplied work a
// crash can lose (and how long one commit holds the engine lock). Package
// vars so tests can shrink them to a handful of files.
var (
	batchMaxEntries = 5000
	batchMaxBytes   = int64(200 << 20) // 200 MiB of hashed content
	// batchQueueEntries bounds the FIFO between the hash workers and the single
	// batch writer. It is a run-ahead buffer, not a batch size: workers block
	// only when this many finished entries are queued and the writer is still
	// committing, which at the measured pass throughput (9.3 MB/s, 0.4 files/s
	// on a 1.17M-file share) takes hours to fill even behind a multi-minute
	// commit. Memory is the block hashes of at most this many entries, not the
	// file contents.
	batchQueueEntries = 4096
)

// SetBatchLimits overrides the batch sizing and returns a restore function
// (call it from t.Cleanup). TESTS ONLY: a real pass uses 5000 entries /
// 200 MiB, small enough that a crash loses seconds of hashing and large enough
// that the transaction count stays O(files/batch). The written value is a
// count AND a byte budget; the writer goroutine is off the hash workers'
// critical path, so the entry count is larger than it was when a commit ran
// inline on a worker.
func SetBatchLimits(entries int, bytes int64) (restore func()) {
	prevEntries, prevBytes := batchMaxEntries, batchMaxBytes
	if entries > 0 {
		batchMaxEntries = entries
	}
	if bytes > 0 {
		batchMaxBytes = bytes
	}
	return func() { batchMaxEntries, batchMaxBytes = prevEntries, prevBytes }
}

// SetBatchSink installs the consumer that applies and commits hashed changes
// during the hash phase (nil restores the single end-of-pass apply).
//
// The node does NOT use this setter: a folder can have a full walk and a scoped
// pass running at once, and a sink stored on the shared scanner would be visible
// to both. Node passes its sink per call through ScanBatchWithProgress instead;
// this setter remains for direct scanner use and tests.
//
// The sink is called from ONE writer goroutine, never from a hash worker: the
// workers hand finished entries to a bounded FIFO and only the writer commits,
// so a slow index transaction cannot stop the disk reads (see hashPending).
// Calls are FIFO, one at a time, each with at most batchMaxEntries entries /
// batchMaxBytes of hashed content, and never with a partial batch once the
// pass has been cancelled or the sink has failed — so a pause can never bank a
// half-built batch. A sink error aborts the pass: the entries it had already
// committed stay (they are real index state) and the caller reports the
// failure.
func (s *Scanner) SetBatchSink(fn func([]Change) error) { s.batchSink = fn }

// ErrScanCancelled reports that a scan was abandoned because the folder was
// paused (or the daemon is shutting down). Callers must discard every
// UNCOMMITTED change: a cancelled pass has seen only part of the tree, and
// applying that would look exactly like mass deletion. Entries a batch sink
// already committed during the pass are real index state and stay — that is
// what lets the next pass resume instead of starting over.
var ErrScanCancelled = errors.New("scan cancelled")

// SetCancel installs the cancellation probe. It is called from worker
// goroutines, so it must be cheap and must not block.
func (s *Scanner) SetCancel(fn func() bool) { s.cancelFn = fn }

// SetHashHook installs a per-file hook that runs after a file has been hashed
// (tests only; nil clears it). See the hashHook field.
func (s *Scanner) SetHashHook(fn func(name string)) { s.hashHook = fn }

// SetWorkers sets the hash reader pool for this scanner, clamped to
// [1, runtime.NumCPU()]: more readers than CPUs cannot add throughput (they
// block in the read syscall) and 0 readers would never finish the pass.
//
// It is the static form of the knob; the node installs SetWorkerSource instead
// so a runtime change applies to the next pass without rebuilding the Scanner.
func (s *Scanner) SetWorkers(n int) { s.workers = clampWorkers(n) }

// SetWorkerSource installs a live pool-size provider, consulted ONCE at the
// start of every hash phase and superseding SetWorkers. This is how
// PUT /api/settings/sync changes the reader pool of a running daemon: a pass
// already in flight keeps the workers it launched (so the change cannot alter
// the meaning of a scan in progress), and the next pass uses the new value.
// A nil provider restores the static value.
func (s *Scanner) SetWorkerSource(fn func() int) { s.workerSource = fn }

// SetWorkerHook installs a test-only observer of the pool size actually
// launched for the next hash phase (nil clears it). See the workerHook field.
func (s *Scanner) SetWorkerHook(fn func(n int)) { s.workerHook = fn }

// clampWorkers bounds a reader pool to [1, NumCPU].
func clampWorkers(n int) int {
	max := runtime.NumCPU()
	if max < 1 {
		max = 1
	}
	if n < 1 {
		return 1
	}
	if n > max {
		return max
	}
	return n
}

// cancelled reports whether the scan should abandon its work.
func (s *Scanner) cancelled() bool { return s.cancelFn != nil && s.cancelFn() }

// SetForceHash arms a one-shot verified scan: every file is re-hashed, and a
// file whose content turns out identical is reported as Touched (stat adopted,
// version kept) rather than as a modification, so nothing re-transfers.
func (s *Scanner) SetForceHash(on bool) { s.forceHash.Store(on) }

// New creates a Scanner for a folder root and its index.
func New(root string, ix *index.Index, ig *ignore.Matcher) *Scanner {
	return &Scanner{
		root: root, ix: ix, ignore: ig, workers: runtime.NumCPU(),
		settleScans: 2, settleSeconds: 10, deletionScans: 2,
		crossScopePair: true,
		nowFn:          func() int64 { return time.Now().Unix() },
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
// SetCrossScopePairing controls whether a scoped pass may pair a rename whose
// old path it did not walk (it must prove the old path is gone with a
// targeted stat). Default on; see crossScopePair.
func (s *Scanner) SetCrossScopePairing(on bool) { s.crossScopePair = on }

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

// LastInFlight returns the files the most recent scans held back because they
// were still being written. A full walk and a scoped pass of the same folder may
// overlap, so this is the UNION of their sets (a path appears once); each pass
// replaces only its own kind's set, which is what stops a finishing walk from
// un-gating a file a concurrent scoped pass is still holding.
func (s *Scanner) LastInFlight() []InFlight {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	return unionInFlight(s.walkInFlight, s.scopedInFlight)
}

// publishInFlight replaces this pass kind's held-back set. Scoped passes keep
// their own previous entries outside the new scope: a narrow scope must not
// forget a file a wider scoped pass is holding.
func (s *Scanner) publishInFlight(scoped bool, list []InFlight, inScope func(string) bool) {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	if !scoped {
		s.walkInFlight = list
		return
	}
	merged := make([]InFlight, 0, len(s.scopedInFlight)+len(list))
	for _, e := range s.scopedInFlight {
		if !inScope(e.Name) {
			merged = append(merged, e)
		}
	}
	s.scopedInFlight = append(merged, list...)
}

// unionInFlight concatenates two held-back sets, keeping the first entry for a
// path (the walk's observation when both passes held the same file).
func unionInFlight(a, b []InFlight) []InFlight {
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]InFlight, 0, len(a)+len(b))
	for _, list := range [][]InFlight{a, b} {
		for _, e := range list {
			if _, dup := seen[e.Name]; dup {
				continue
			}
			seen[e.Name] = struct{}{}
			out = append(out, e)
		}
	}
	return out
}

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
	return s.scanScoped(fn, nil, false, s.batchSink)
}

// ScanBatchWithProgress is a whole-folder pass whose hashed entries are APPLIED
// IN BOUNDED BATCHES while it runs (see SetBatchSink) — the resumable first-pass
// path. The sink is passed per CALL, not read from the scanner, because a scoped
// pass of the same folder may be running: a shared field would hand the walk's
// sink the scoped pass's single file, bank it into the walk's counters and skip
// the scoped pass's own apply. The node always uses this form.
func (s *Scanner) ScanBatchWithProgress(fn func(Progress), sink func([]Change) error) ([]Change, error) {
	return s.scanScoped(fn, nil, false, sink)
}

// ScanForceWithProgress is a whole-folder VERIFIED scan: every file is re-hashed
// for this pass and an unchanged re-hash is reported as Touched. The decision is
// an argument rather than the scanner's one-shot SetForceHash flag so that a
// concurrent scoped pass of the same folder cannot consume (and silently weaken)
// an operator's verified scan. Only that one pass re-hashes.
func (s *Scanner) ScanForceWithProgress(fn func(Progress)) ([]Change, error) {
	return s.scanScoped(fn, nil, true, nil)
}

func (s *Scanner) scan(fn func(Progress)) ([]Change, error) {
	return s.scanScoped(fn, nil, false, s.batchSink)
}

// ScanScope walks only the given subtrees (slash-relative paths, files or
// directories) instead of the whole folder, and reports deletions only for paths
// INSIDE those subtrees. It exists so change events can drive cheap scans: a
// saved file no longer costs a walk of the entire share.
//
// Correctness rules that make it safe:
//   - anything not walked is not "unseen": deletions, the settling table and the
//     deletion-confirmation table are all filtered/merged by scope, so a scoped
//     scan can never tombstone or unsettle a path it did not look at;
//   - rename pairing (cheap + content based) is filtered the same way;
//   - a scope root that no longer exists is simply absent, so its index entries
//     become deletion candidates — subject to the usual confirmation gate.
func (s *Scanner) ScanScope(scope []string) ([]Change, error) {
	return s.scanScoped(nil, scope, false, nil)
}

// ScanScopeWithProgress is ScanScope with a progress callback. A scoped pass
// never banks in batches (it has no use for a resumable prefix and its changes
// are also the input to rename pairing), so it never takes a batch sink, even if
// a full walk of the same folder currently has one.
func (s *Scanner) ScanScopeWithProgress(scope []string, fn func(Progress)) ([]Change, error) {
	return s.scanScoped(fn, scope, false, nil)
}

func (s *Scanner) scanScoped(fn func(Progress), scope []string, forced bool, sink func([]Change) error) ([]Change, error) {
	emit := func(p Progress) {
		if fn != nil {
			fn(p)
		}
	}
	seen := map[string]bool{}
	var changes []Change
	var toHash []pending
	walked := 0
	ignoredFiles := 0
	var ignoredBytes int64
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
	// Verified scan: re-hash everything for this one pass. `forced` comes from
	// the caller (ScanForceWithProgress) so a concurrent scoped pass cannot
	// consume the flag; the stored one-shot is the SetForceHash test/CLI form.
	forceHash := forced || s.forceHash.Swap(false)

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

	// Scope: nil means the whole folder. A scope entry may be a file or a
	// directory (WalkDir handles both), and every relative name we report as
	// deleted must live inside one of them.
	inScope := func(name string) bool {
		if len(scope) == 0 {
			return true
		}
		for _, root := range scope {
			if name == root || strings.HasPrefix(name, root+"/") {
				return true
			}
		}
		return false
	}
	walkRoots := []string{s.root}
	if len(scope) > 0 {
		walkRoots = make([]string, 0, len(scope))
		for _, root := range scope {
			walkRoots = append(walkRoots, filepath.Join(s.root, filepath.FromSlash(root)))
		}
	}
	visit := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if s.cancelled() {
			// Paused mid-walk: stop reading the tree. The caller discards
			// everything this pass saw (ErrScanCancelled), so a partial walk can
			// never be mistaken for a mass deletion.
			return filepath.SkipAll
		}
		walked++
		if walked%64 == 0 {
			emit(Progress{Phase: "walking", Walked: walked, IgnoredFiles: ignoredFiles, IgnoredBytes: ignoredBytes})
		}
		rel, rerr := filepath.Rel(s.root, path)
		if rerr != nil {
			return rerr
		}
		relSlash := filepath.ToSlash(rel)
		if relSlash == "." {
			return nil
		}
		if staging.IsReserved(relSlash) || staging.IsTransient(relSlash) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			// Account for it before skipping: the share's footprint includes these
			// bytes even though the index does not, and an operator needs to see
			// them (a deleted-but-open file can park gigabytes here).
			if info, ierr := d.Info(); ierr == nil {
				ignoredFiles++
				ignoredBytes += info.Size()
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
	}

	var walkErr error
	for _, root := range walkRoots {
		if walkErr = filepath.WalkDir(root, visit); walkErr != nil {
			break
		}
	}
	if walkErr != nil {
		// A partial walk must never rewrite the settling table.
		return changes, walkErr
	}
	if s.cancelled() {
		// The folder was paused mid-pass: nothing this scan saw may be used.
		return nil, ErrScanCancelled
	}
	// A scoped walk leaves the rest of the folder untouched, so the settling
	// rows of everything OUTSIDE the scope must survive verbatim: they describe
	// files this pass did not look at.
	if len(scope) > 0 {
		for name, st := range settle {
			if !inScope(name) {
				nextSettle[name] = st
			}
		}
	}

	// Cheap-rename pass: pair (size, mtime)-identical added files with
	// vanished index entries BEFORE hashing, so a folder rename (which
	// preserves mtimes on every major filesystem) needs no re-hash of the
	// moved data. Pairs are only trusted when the scan is rename-dominated
	// and a sampled subset verifies by hashing; anything else falls through
	// to the exact content-based detectRenames pass below.
	renames, claimed := s.cheapRenames(&toHash, seen, inScope)
	if s.cancelled() {
		// cheapRenames verifies a sample by hashing: a pause can land here too.
		return nil, ErrScanCancelled
	}
	changes = append(changes, renames...)

	// Hash changed/added files in parallel.
	hashTotal := len(toHash)
	emit(Progress{Phase: "hashing", Walked: walked, HashTotal: hashTotal, IgnoredFiles: ignoredFiles, IgnoredBytes: ignoredBytes})
	hashed, drifted, hashCancelled, hashErr := s.hashPending(toHash, sink, func(done int) {
		emit(Progress{Phase: "hashing", Walked: walked, HashDone: done, HashTotal: hashTotal})
	})
	if hashCancelled {
		// Paused while hashing. Everything below (the deletion pass, the settle
		// write, the published in-flight set) belongs to a COMPLETE walk, and
		// this pass does not have one: without this check the caller saw "no
		// changes, no error" and reported a finished scan while the disk was
		// still being read.
		return nil, ErrScanCancelled
	}
	if hashErr != nil {
		// The batch sink could not commit (index write failure): stop here.
		// Whatever it committed before the error is real, durable index state;
		// this pass applied nothing else and must not pretend to have
		// finished, because the rest of its hashed work was never banked.
		return nil, hashErr
	}
	changes = append(changes, hashed...)
	// A file whose stat moved while it was being read is demoted to in-flight
	// for this round: its post-hash stat starts a fresh run, so the next scan
	// re-verifies it instead of trusting a self-inconsistent entry.
	for _, d := range drifted {
		nextSettle[d.rel] = d.state
		inFlight = append(inFlight, InFlight{Name: d.rel, Size: d.state.Size, Since: d.state.First, Scans: d.state.Count})
	}

	// Deleted pass (only when the walk was clean).
	emit(Progress{Phase: "deleting", Walked: walked, HashDone: hashTotal, HashTotal: hashTotal, IgnoredFiles: ignoredFiles, IgnoredBytes: ignoredBytes})
	changes = s.detectRenames(changes, seen, claimed, collapseGateCands(gateCands), inScope, len(scope) > 0)
	// A scoped pass may never TOMBSTONE a path it did not look at: everything
	// outside its scope is "not walked", not "gone". detectRenames already
	// enforces that for deletions (and only ever pairs an outside path with a
	// targeted stat + content match, never with absence), but the filter is
	// applied here as well so a future change to the rename pass cannot leak a
	// deletion out of scope.
	if len(scope) > 0 {
		changes = keepInScope(changes, inScope)
	}
	// Rename pairing already ran (above) on the same "absent from the walk" set,
	// so a move is still detected on the FIRST scan; only the deletions that
	// were not part of a rename wait for confirmation.
	changes = s.confirmDeletions(changes, now, inScope, unsettledKeys(settle, nextSettle), len(scope) > 0)
	// Last probe before any persistent state is written: a pause that landed
	// during the deletion pass must not restamp the settle/missing tables for a
	// walk that was abandoned half way.
	if s.cancelled() {
		return nil, ErrScanCancelled
	}
	// One read and one write transaction per scan, regardless of file count.
	// Rows survive only for files still unsettled: a file that settled, was
	// applied, vanished, or now matches its index is simply absent from
	// nextSettle. The write is a MERGE for overlapping passes (see
	// publishSettle): a walk that started before a scoped pass wrote a row must
	// not drop that row on its way out.
	s.publishSettle(settle, nextSettle, inScope, len(scope) > 0)
	// The published in-flight set drives "never pull or delete a file that is
	// being written". Each pass kind replaces only its own set, and LastInFlight
	// unions them (see publishInFlight): a scoped pass keeps its previous
	// entries outside the new scope, and a finishing walk cannot un-gate a file
	// the scoped pass is still holding.
	s.publishInFlight(len(scope) > 0, inFlight, inScope)
	totalInFlight := len(s.LastInFlight())
	emit(Progress{Phase: "done", Walked: walked, HashDone: hashTotal, HashTotal: hashTotal, InFlight: totalInFlight, IgnoredFiles: ignoredFiles, IgnoredBytes: ignoredBytes})
	return changes, nil
}

// keepInScope enforces the scoped pass's central rule: it speaks only for the
// paths it walked. A Deleted change for a path outside the scope is always a
// bug (there was no observation to justify it), so it is dropped rather than
// applied. Renamed changes are kept: the scanner only produces one when it has
// POSITIVE proof (the old path is gone by a targeted Lstat and exactly one new
// file added this pass carries identical content), which is an observation, not
// an inference from absence.
func keepInScope(changes []Change, inScope func(string) bool) []Change {
	out := changes[:0]
	for _, c := range changes {
		if c.Kind == Deleted && c.Info != nil && !inScope(c.Info.Name) {
			continue
		}
		out = append(out, c)
	}
	return out
}

// publishSettle writes this pass's settling table.
//
// Overlapping passes made the old "one writer, full replace" assumption false:
// a full walk snapshots the table at the start and can run for twenty minutes,
// while a watcher pass updates it several times in between. A full replace by
// the walk would silently drop the scoped rows written after its snapshot,
// delaying (or losing) the stability gate's evidence for a brand-new file — the
// file would then wait for the next walk instead of being detected in seconds.
//
// So the write merges against a FRESH read under stateMu, which is what
// serialises the read-modify-write between the two passes (the index's own
// SettleReplace transaction only serialises the SQL, not the decision):
//   - full pass: its next table wins, but rows that appeared after its snapshot
//     (written by a concurrent scoped pass) and that it has no row for are kept;
//   - scoped pass: only in-scope rows are touched; every out-of-scope row is
//     left exactly as another pass left it.
func (s *Scanner) publishSettle(prev, next map[string]index.SettleState, inScope func(string) bool, scoped bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	cur, err := s.ix.SettleList()
	if err != nil {
		// No readable table: the pass's own result is the best available and a
		// failure here has never been fatal to a scan.
		_ = s.ix.SettleReplace(next)
		return
	}
	if !scoped {
		for name, st := range cur {
			if _, had := prev[name]; had {
				continue
			}
			if _, over := next[name]; over {
				continue
			}
			next[name] = st
		}
		_ = s.ix.SettleReplace(next)
		return
	}
	for name := range prev {
		if inScope(name) {
			delete(cur, name)
		}
	}
	for name, st := range next {
		if inScope(name) {
			cur[name] = st
		}
	}
	_ = s.ix.SettleReplace(cur)
}

// publishMissing is publishSettle's counterpart for the deletion-confirmation
// table, with the same merge rule: a scoped pass may only move rows for paths it
// walked, and a walk keeps any confirmation a concurrent scoped pass recorded
// after the walk's snapshot. Deleting is still a whole-folder decision — each
// confirmation step here comes from a walk that actually covered the path.
func (s *Scanner) publishMissing(prev, next map[string]index.MissingState, inScope func(string) bool, scoped bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	cur, err := s.ix.MissingList()
	if err != nil {
		_ = s.ix.MissingReplace(next)
		return
	}
	if !scoped {
		for name, st := range cur {
			if _, had := prev[name]; had {
				continue
			}
			if _, over := next[name]; over {
				continue
			}
			next[name] = st
		}
		_ = s.ix.MissingReplace(next)
		return
	}
	for name := range prev {
		if inScope(name) {
			delete(cur, name)
		}
	}
	for name, st := range next {
		if inScope(name) {
			cur[name] = st
		}
	}
	_ = s.ix.MissingReplace(cur)
}

// confirmDeletions is the deletion gate: a path the walk did not see is only
// reported as deleted once deletionScans consecutive scans have missed it. The
// state lives in the index (not in memory) so a daemon restart or a folder
// reload cannot reset a half-confirmed deletion, and a path that comes back
// simply drops out of the table.
//
// Only unpaired deletions reach here: renames were already folded into Renamed
// changes by detectRenames, so the gate never delays a move.
//
// unsettled holds the (size, mtime) identity of every file the stability gate
// is still holding. A confirmed deletion that matches one is the OLD HALF of a
// rename whose NEW HALF the gate has not released yet (see the deferral note in
// the loop). Deferring it is what keeps the two halves of a move together on
// the wire: the gate cannot advertise the new path early, so the deletion must
// not go out early either.
func (s *Scanner) confirmDeletions(changes []Change, now int64, inScope func(string) bool, unsettled map[mtKey]struct{}, scoped bool) []Change {
	if s.deletionScans <= 1 {
		// Gate off: forget any half-confirmed state so turning it back on
		// starts from a clean table. A scoped pass must not wipe rows another
		// pass is using, so it leaves the table alone here too.
		if !scoped {
			_ = s.ix.MissingReplace(nil)
		}
		return changes
	}
	prev, err := s.ix.MissingList()
	if err != nil {
		// No table (or unreadable): behave as if nothing was suspected, which
		// means deletions take one extra scan rather than none.
		prev = map[string]index.MissingState{}
	}
	next := make(map[string]index.MissingState, 8)
	// A scoped scan only speaks for what it looked at: keep the confirmations
	// pending for every path outside the scope, or the next full pass would
	// restart their window from scratch.
	for name, st := range prev {
		if !inScope(name) {
			next[name] = st
		}
	}
	// Which deletions are the unadvertised half of a rename? A rename is
	// visible to the walk as "old path gone, new path here", but the two
	// halves sit behind DIFFERENT gates: the deletion needs only
	// deletionScans misses, while the added copy must also survive the
	// stability gate's quiet window. With change-driven scans the deletion is
	// therefore confirmed several scans before the add is released, and a
	// peer's round in that window is told "these paths are gone" with no move
	// to apply — so it destroys or parks its copies and then downloads every
	// byte again at the new paths. The pair only stays a move when both halves
	// are advertised together, so a confirmed deletion whose entry has the
	// same (size, mtime) — the identity a rename preserves — as a file the
	// gate is still holding is deferred, together with every directory above
	// it (a directory tombstone published alone would have the peer apply a
	// delete against children it still owns).
	deferred := map[string]bool{}
	for _, c := range changes {
		if c.Kind != Deleted || c.Info == nil || c.Info.Type != index.TypeFile || len(c.Info.Blocks) == 0 {
			continue
		}
		if _, waiting := unsettled[mtKey{c.Info.Size, c.Info.ModifiedS, c.Info.ModifiedNs}]; !waiting {
			continue
		}
		for p := c.Info.Name; p != ""; {
			deferred[p] = true
			i := strings.LastIndexByte(p, '/')
			if i < 0 {
				break
			}
			p = p[:i]
		}
	}
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
		// Deferral keeps the whole confirmation: the count goes back into the
		// table, so nothing is forgotten and no confirmation is lost. The next
		// scan after the gate releases either folds the pair into a Renamed
		// change (detectRenames runs first) or — if the settling file is not
		// this entry's content after all — emits the deletion as usual. A file
		// that never settles (a writer that never stops) delays this deletion
		// but can never publish a half-copied file, and a deletion is never
		// cancelled: it is only held back, so no data is lost either way.
		if deferred[c.Info.Name] {
			next[c.Info.Name] = st
			continue
		}
		out = append(out, c)
	}
	s.publishMissing(prev, next, inScope, scoped)
	return out
}

// unsettledKeys is the (size, mtime) identity of every file the stability gate
// is still holding: rows the previous scans left in the settling table plus the
// ones this scan held back. Union, not either, because a SCOPED scan only
// re-observes its own paths — a file settling outside the scope is still
// settling, and the deletion it may be the other half of must not go out
// before it does.
func unsettledKeys(prev, current map[string]index.SettleState) map[mtKey]struct{} {
	out := make(map[mtKey]struct{}, len(prev)+len(current))
	for _, st := range prev {
		out[mtKey{st.Size, st.ModS, st.ModNs}] = struct{}{}
	}
	for _, st := range current {
		out[mtKey{st.Size, st.ModS, st.ModNs}] = struct{}{}
	}
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
func (s *Scanner) detectRenames(changes []Change, seen map[string]bool, claimed map[string]bool, gateMoves map[string]gateCandidate, inScope func(string) bool, scoped bool) []Change {
	// Index of the added-file changes with hashed content, grouped by
	// content key, so a deleted entry can find a same-content target.
	addedByKey := map[string][]int{}
	// addedByMT is a cheap prefilter for the cross-scope pairing below: a
	// rename preserves size and mtime, and building a content key for every
	// index entry would make a watcher pass over a million-file folder walk
	// the block list of every file it did not even look at.
	addedByMT := map[mtKey][]int{}
	for i, c := range changes {
		if c.Kind == Added && c.Info.Type == index.TypeFile && len(c.Info.Blocks) > 0 {
			k := contentKey(c.Info)
			addedByKey[k] = append(addedByKey[k], i)
			mt := mtKey{c.Info.Size, c.Info.ModifiedS, c.Info.ModifiedNs}
			addedByMT[mt] = append(addedByMT[mt], i)
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
		// A SCOPED scan did not look here, so "not seen" says nothing about
		// this path: it is not a deletion, it is simply outside the walk.
		// It can still be the OTHER HALF of a rename, and that is the usual
		// case in practice: a watcher reports the new name the moment it
		// appears, while the old name is only noticed by a later full walk
		// and then needs the deletion gate to confirm it -- by which time
		// the new name is already indexed and ApplyLocalRename refuses a
		// target that exists, so the pair degrades into add + delete and the
		// peer re-writes every byte locally instead of moving the file
		// (measured: a folder rename cost 29.5 MiB of needless writes on the
		// peer). A TARGETED stat is enough to trust it: the old path must be
		// GONE (ENOENT) and exactly one file added this pass must have its
		// content, which is the same 1:1 rule the in-scope pass uses.
		if !inScope(fi.Name) {
			if scoped && s.crossScopePair && fi.Type == index.TypeFile && len(fi.Blocks) > 0 {
				// Cheap first: a rename preserves (size, mtime), so only a path
				// matching one added this pass is worth a stat and a content key.
				if len(addedByMT[mtKey{fi.Size, fi.ModifiedS, fi.ModifiedNs}]) > 0 {
					if idxs := addedByKey[contentKey(fi)]; len(idxs) == 1 && !claimedAdded[idxs[0]] {
						if _, err := os.Lstat(filepath.Join(s.root, filepath.FromSlash(fi.Name))); os.IsNotExist(err) {
							i := idxs[0]
							claimedAdded[i] = true
							changes = append(changes, Change{Kind: Renamed, From: fi.Name, Info: changes[i].Info})
						}
					}
				}
			}
			return nil
		}
		// Out-of-scope entries are never emitted as local deletions: the
		// include gate does not own them.
		if s.include != nil {
			if matched, _ := s.include.Match(fi.Name, fi.Type == index.TypeDirectory); !matched {
				return nil
			}
		}
		// "Not seen by this walk" is not an observation that the path is gone:
		// the walk may have read its directory BEFORE the path appeared. That is
		// the normal case now that a watcher-driven scoped pass indexes new
		// files beside a full walk — the walk started first, read the directory,
		// and only then did the file exist (and get indexed). Tombstoning it
		// would delete a file that is sitting on disk. The same window exists
		// for a peer write applied during a walk. So a deletion candidate is
		// confirmed by a TARGETED stat first, and only a definite ENOENT may
		// count as gone (a permission or I/O error proves nothing either): a
		// genuinely deleted path costs one Lstat, a clean walk with no
		// vanishings costs none, because this callback only sees
		// absent-from-the-walk rows.
		if _, err := os.Lstat(filepath.Join(s.root, filepath.FromSlash(fi.Name))); !os.IsNotExist(err) {
			return nil
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
func (s *Scanner) cheapRenames(toHash *[]pending, seen map[string]bool, inScope func(string) bool) ([]Change, map[string]bool) {
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
		if !inScope(fi.Name) {
			// Outside a scoped walk: not vanished, just not looked at.
			return nil
		}
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
		if s.cancelled() {
			return nil, nil // paused mid-verification: the caller discards
		}
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
//
// The third result says the pass was CANCELLED (the folder was paused, or the
// daemon is shutting down): the caller must discard everything it has, because
// a cancelled pass has hashed only part of the tree. Reporting it explicitly —
// rather than returning nil, nil — is what keeps a pause from looking like a
// clean empty pass: the walk half of the same bug already returned
// ErrScanCancelled, and swallowing it here let the pass carry on to the deletion
// pass and the settle write and report itself as COMPLETED.
func (s *Scanner) hashPending(pendings []pending, sink func([]Change) error, onDone func(int)) ([]Change, []driftResult, bool, error) {
	if len(pendings) == 0 {
		// Nothing to hash: still report a pause that landed before us, so the
		// caller does not apply a walk it should have abandoned.
		return nil, nil, s.cancelled(), nil
	}
	workers := s.workers
	if s.workerSource != nil {
		// Read ONCE per pass: a runtime change applies to the next scan and
		// cannot disturb this one.
		workers = s.workerSource()
	}
	workers = clampWorkers(workers)
	if workers > len(pendings) {
		workers = len(pendings)
	}
	if h := s.workerHook; h != nil {
		h(workers)
	}

	// Hashing is decoupled from committing. Every worker hands its finished
	// entry (or the one file that drifted while it was read) to a SINGLE
	// writer goroutine over a bounded FIFO, and only that writer appends to the
	// result and calls batchSink. A commit therefore cannot park the readers:
	// they stop only when batchQueueEntries entries are genuinely queued.
	//
	// Why this exists: on the live 1.17M-file / 8 TB share a first pass ran at
	// 9.3 MB/s and 0.4 files/s (27 MB average file) with all 8 hash workers
	// blocked on the old flushMu for 1-3 minutes at a time, each waiting for
	// one goroutine to write the batch to the index (ApplyLocalChanges ->
	// Ix.PutBatch under the engine lock).
	type item struct {
		ch    Change
		drift *driftResult
	}
	queue := make(chan item, batchQueueEntries)
	var (
		changes  []Change      // written only by the writer, read after it exits
		drifted  []driftResult // written only by the writer, read after it exits
		errMu    sync.Mutex
		batchErr error
	)
	setErr := func(err error) {
		errMu.Lock()
		if batchErr == nil {
			batchErr = err
		}
		errMu.Unlock()
	}
	passErr := func() error {
		errMu.Lock()
		defer errMu.Unlock()
		return batchErr
	}
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		if h := s.batchWriterHook; h != nil {
			defer h()
		}
		var (
			pend      []int // indexes into changes, oldest first
			pendBytes int64
		)
		// commit feeds the pending batch to the sink as ONE whole batch of at
		// most batchMaxEntries entries / batchMaxBytes of hashed content, and
		// marks exactly the entries it applied. It refuses to commit once the
		// pass has been cancelled or the sink has failed: an uncommitted batch
		// is DROPPED WHOLE, so a pause can never bank a half-built one. Those
		// entries stay in `changes` unmarked, and a cancelled pass discards the
		// whole result — the next pass resumes from what the index really holds.
		commit := func() {
			if len(pend) == 0 {
				return
			}
			if sink == nil || s.cancelled() || passErr() != nil {
				pend, pendBytes = nil, 0
				return
			}
			out := make([]Change, len(pend))
			for i, k := range pend {
				out[i] = changes[k]
			}
			if err := sink(out); err != nil {
				// A failing index write aborts the pass: the batches already
				// committed stay (real index state) and the rest of the queue is
				// discarded rather than applied.
				setErr(err)
				pend, pendBytes = nil, 0
				return
			}
			for _, k := range pend {
				// Committed entries must not be applied a second time at the
				// end of the pass: that would bump the version of content that
				// never changed.
				changes[k].Committed = true
			}
			pend, pendBytes = nil, 0
		}
		for it := range queue {
			if it.drift != nil {
				drifted = append(drifted, *it.drift)
				continue
			}
			changes = append(changes, it.ch)
			if sink == nil || passErr() != nil {
				continue
			}
			pend = append(pend, len(changes)-1)
			pendBytes += it.ch.Info.Size
			if len(pend) >= batchMaxEntries || pendBytes >= batchMaxBytes {
				commit()
			}
		}
		// The queue is closed only after every worker has exited, so the
		// writer DRAINS it to the end: no worker is left blocked on a full
		// queue and no hashed entry is silently dropped from the result. The
		// final short batch is committed here (the old force-flush), and only
		// when the pass is still live — commit refuses after a cancel or error.
		commit()
	}()
	var hashed int32
	emit := func() {
		if onDone != nil {
			onDone(int(atomic.LoadInt32(&hashed)))
		}
	}
	// Buffered so the producer never blocks on a worker that has stopped for
	// cancellation: on a pause the whole pass is discarded anyway, so unread
	// jobs are simply dropped.
	jobs := make(chan pending, len(pendings))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				if s.cancelled() || passErr() != nil {
					return
				}
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
					queue <- item{drift: &driftResult{rel: p.rel, state: index.SettleState{
						Size: st.Size(), ModS: st.ModTime().Unix(), ModNs: int32(st.ModTime().Nanosecond()),
						Count: 1, First: p.first,
					}}}
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
				if h := s.hashHook; h != nil {
					h(p.rel)
				}
				// Handing the entry over may block — and only here, when the
				// writer is behind by batchQueueEntries entries. It is the
				// bound on uncommitted work, not a commit on this worker.
				queue <- item{ch: ch}
			}
		}()
	}
	for _, p := range pendings {
		if s.cancelled() || passErr() != nil {
			break
		}
		jobs <- p
	}
	close(jobs)
	wg.Wait()
	close(queue)
	<-writerDone
	if s.cancelled() {
		// Partial by construction: the producer stopped queueing and the
		// workers stopped, so what is in `changes` is only the head of the
		// tree. Never return it as a result. Batches the writer had not
		// committed were dropped whole (commit refuses once cancelled) and the
		// index keeps only whole committed batches, which is what the next pass
		// resumes from.
		return nil, nil, true, nil
	}
	if err := passErr(); err != nil {
		return nil, nil, false, err
	}
	return changes, drifted, false, nil
}

func dirInfo(rel string, info fs.FileInfo) *index.FileInfo {
	return &index.FileInfo{Name: rel, Type: index.TypeDirectory, Mode: uint32(info.Mode().Perm())}
}

func symlinkInfo(rel string, info fs.FileInfo) *index.FileInfo {
	return &index.FileInfo{Name: rel, Type: index.TypeSymlink, Mode: uint32(info.Mode().Perm())}
}
