// Package sync implements the per-folder sync engine: version vectors,
// global-model convergence, conflict handling, and versioning.
package sync

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/hash"
	"crosssync/internal/ignore"
	"crosssync/internal/index"
	"crosssync/internal/scanner"
	"crosssync/internal/staging"
	"crosssync/internal/version"
)

// ConflictPolicy determines how concurrent edits are resolved.
type ConflictPolicy int

const (
	// ConflictCopy keeps the loser as name.sync-conflict-<ts>-<device>.ext
	// (and, per research, propagates it to all peers).
	ConflictCopy ConflictPolicy = iota
	// ConflictVersioning relies on the versioning area to preserve the
	// loser instead of creating an in-place conflict copy.
	ConflictVersioning
)

// ParseConflictPolicy maps a config string to a policy.
func ParseConflictPolicy(s string) ConflictPolicy {
	if s == "versioning" {
		return ConflictVersioning
	}
	return ConflictCopy
}

// ConflictSuffix is the marker appended to conflict copies.
const ConflictSuffix = ".sync-conflict"

// EngineStats holds live per-folder transfer statistics (atomic, safe for
// concurrent session goroutines).
type EngineStats struct {
	BytesUp   atomic.Int64 // data served to peers
	BytesDown atomic.Int64 // data received from peers
	FilesDown atomic.Int64 // files completed pulling
	Syncs     atomic.Int64 // completed sessions touching this folder
	LastSync  atomic.Int64 // unix nanos of the last completed session
}

// Engine is the per-folder sync brain. It holds the local index plus the
// known views of every peer and computes the global model.
type Engine struct {
	NodeID     uint64
	ID         string
	Root       string
	Policy     ConflictPolicy
	Versioning *Versioning
	Ix         *index.Index
	Stager     *staging.Stager
	Ignore     *ignore.Matcher
	// Include is the include (whitelist) gate; nil = everything in scope.
	// Set by the node from the folder config so this device only acquires
	// names it has opted into (entries already owned locally stay followed).
	Include *ignore.Matcher
	// Direction is this device's sync direction for the folder:
	// "send-receive" (default/empty), "send-only", or "receive-only". Set
	// by the node from the folder config; it is per-device, like Include.
	Direction string
	Logf      func(format string, args ...any)
	// OnEvent, when set, receives durable events for transparency (conflicts,
	// versioned archives, applied changes, skips). Never called under a lock.
	OnEvent func(*events.Event)
	// Stats holds live transfer statistics.
	Stats EngineStats

	mu    sync.Mutex
	peers map[uint64]map[string]*index.FileInfo
}

// New creates an Engine for one folder.
func New(nodeID uint64, folderID, root string, policy ConflictPolicy, v *Versioning, ix *index.Index, st *staging.Stager, ig *ignore.Matcher, logf func(format string, args ...any)) *Engine {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Engine{
		NodeID:     nodeID,
		ID:         folderID,
		Root:       root,
		Policy:     policy,
		Versioning: v,
		Ix:         ix,
		Stager:     st,
		Ignore:     ig,
		Logf:       logf,
		peers:      map[uint64]map[string]*index.FileInfo{},
	}
}

// ApplyLocalChange applies one scanner change: bumps the version vector
// with our device counter and updates the index. Deletions become
// tombstones (kept in the index so they propagate). Skipped changes are
// reported to the log and never indexed.
func (e *Engine) ApplyLocalChange(kind scanner.ChangeKind, fi *index.FileInfo) (*index.FileInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// receive-only never indexes local files. This also covers the
	// corrupt-index rebuild path (rebuildFromFS), which calls this directly
	// rather than ApplyLocalChanges: returning nil lets the rebuild finish
	// with an empty index, which is correct for receive-only.
	if e.Direction == "receive-only" {
		return nil, nil
	}

	if kind == scanner.Skipped {
		e.Logf("skip %s: ignored by rule", fi.Name)
		rule := ""
		if _, r := e.Ignore.Match(fi.Name, false); r != nil {
			rule = r.Raw
		}
		e.emit(events.CatSkipped, events.SevInfo, fi.Name, "ignored by rule", rule, 0)
		return nil, nil
	}

	name := fi.Name
	cur, ok, err := e.Ix.Get(name)
	if err != nil {
		return nil, err
	}
	var newVer version.Vector
	if ok {
		newVer = cur.Version.Bump(e.NodeID)
	} else {
		newVer = version.New().Bump(e.NodeID)
	}

	var out *index.FileInfo
	switch kind {
	case scanner.Deleted:
		t := fi.Type
		if ok {
			t = cur.Type
		}
		out = &index.FileInfo{Name: name, Type: t, Deleted: true, Version: newVer}
	case scanner.Added, scanner.Modified:
		out = fi.Clone()
		out.Version = newVer
	default:
		return nil, fmt.Errorf("unhandled change kind %d", kind)
	}
	if err := e.Ix.Put(out); err != nil {
		return nil, err
	}
	reason := "local add"
	switch kind {
	case scanner.Deleted:
		reason = "local delete"
	case scanner.Modified:
		reason = "local modify"
	}
	e.emit(events.CatApplied, events.SevInfo, name, reason, "", 0)
	return out, nil
}

// ApplyLocalRename records a filesystem rename detected by the scanner:
// the content at From (removed from disk) now lives at to.Name (added on
// disk) and is byte-identical, so the index is updated without a
// delete+add pair — From is tombstoned (peers learn the old path is gone)
// and To is adopted live with a bumped vector. The file is already at To
// on disk, so nothing is transferred or re-hashed, and peers that already
// hold the content apply it as a move rather than re-downloading it.
//
// The plan is re-verified under the lock; if the source changed or the
// target appeared meanwhile, an error is returned and the caller falls
// back to the normal delete+add path.
func (e *Engine) ApplyLocalRename(from string, to *index.FileInfo) (*index.FileInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Same receive-only gate as ApplyLocalChange: a rename is a local file
	// becoming indexed, and rebuildFromFS reaches this method directly, so
	// the batch gate alone would not stop it.
	if e.Direction == "receive-only" {
		return nil, nil
	}

	if from == to.Name {
		return nil, fmt.Errorf("rename: source and target are the same path")
	}
	old, ok, err := e.Ix.Get(from)
	if err != nil {
		return nil, err
	}
	if !ok || old.Deleted {
		return nil, fmt.Errorf("rename %s -> %s: source no longer live", from, to.Name)
	}
	if !sameContent(old, to) {
		return nil, fmt.Errorf("rename %s -> %s: content differs", from, to.Name)
	}
	if _, exists, err := e.Ix.Get(to.Name); err != nil {
		return nil, err
	} else if exists {
		return nil, fmt.Errorf("rename %s -> %s: target already indexed", from, to.Name)
	}

	// One bumped vector drives both halves, so the pair is coherent to
	// peers: From's tombstone and To's live entry carry the same version.
	ver := old.Version.Bump(e.NodeID)
	tomb := &index.FileInfo{Name: from, Type: old.Type, Deleted: true, Version: ver}
	if err := e.Ix.Put(tomb); err != nil {
		return nil, err
	}
	out := to.Clone()
	out.Version = ver
	// The renamed entry is new at its target path: it must get a fresh
	// sequence (a stale one could fall below a peer's acknowledged max and
	// be missed by a delta index exchange).
	out.Sequence = 0
	if err := e.Ix.Put(out); err != nil {
		return nil, err
	}
	e.emit(events.CatApplied, events.SevInfo, to.Name, "local rename", from, 0)
	return out, nil
}

// planChange verifies a non-rename change against the current index and
// builds the FileInfo to write, without writing. ok=false means the change
// cannot be applied as planned (I/O error, or an unhandled kind); the caller
// falls back to the per-change path, which surfaces the underlying error.
// It mirrors the verification in ApplyLocalChange and must stay in sync.
func (e *Engine) planChange(kind scanner.ChangeKind, fi *index.FileInfo) (*index.FileInfo, bool) {
	name := fi.Name
	cur, ok, err := e.Ix.Get(name)
	if err != nil {
		return nil, false
	}
	var newVer version.Vector
	if ok {
		newVer = cur.Version.Bump(e.NodeID)
	} else {
		newVer = version.New().Bump(e.NodeID)
	}
	var out *index.FileInfo
	switch kind {
	case scanner.Deleted:
		t := fi.Type
		if ok {
			t = cur.Type
		}
		out = &index.FileInfo{Name: name, Type: t, Deleted: true, Version: newVer}
	case scanner.Added, scanner.Modified:
		// Shallow copy: Block slices are immutable once hashed, so sharing
		// them avoids duplicating every file's hashes across the batch.
		cp := *fi
		cp.Version = newVer
		out = &cp
	default:
		return nil, false
	}
	return out, true
}

// planRename verifies a rename (source live and byte-identical, target
// absent) and builds the tombstone + live writes, without writing. It
// mirrors the verification in ApplyLocalRename and must stay in sync.
func (e *Engine) planRename(from string, to *index.FileInfo) ([]*index.FileInfo, bool) {
	if from == to.Name {
		return nil, false
	}
	old, ok, err := e.Ix.Get(from)
	if err != nil {
		return nil, false
	}
	if !ok || old.Deleted {
		return nil, false
	}
	if !sameContent(old, to) {
		return nil, false
	}
	if _, exists, err := e.Ix.Get(to.Name); err != nil {
		return nil, false
	} else if exists {
		return nil, false
	}
	ver := old.Version.Bump(e.NodeID)
	tomb := &index.FileInfo{Name: from, Type: old.Type, Deleted: true, Version: ver}
	// Shallow copy: Block slices are immutable once hashed, so sharing them
	// avoids duplicating every moved file's hashes across the batch.
	out := *to
	out.Version = ver
	// The renamed entry is new at its target path: it must get a fresh
	// sequence (a stale one could fall below a peer's acknowledged max and
	// be missed by a delta index exchange).
	out.Sequence = 0
	return []*index.FileInfo{tomb, &out}, true
}

// maxPerFileEvents caps per-file "local rename" events from one apply
// batch; above it a single summary event is emitted instead (mirrors
// node.emitMoves for remote moves).
const maxPerFileEvents = 50

// batchChunk bounds each index PutBatch transaction inside ApplyLocalChanges
// (a var so tests can exercise chunking cheaply).
var batchChunk = 5000

// ApplyLocalChanges applies a batch of scanner changes under one lock hold
// and one index transaction (a single WAL fsync instead of one per file —
// the difference that matters when a folder rename touches thousands of
// entries). Each change is verified exactly as the single-change methods do;
// a change that fails verification (source changed, target appeared, I/O
// error) is NOT applied and is returned in failed so the caller can fall
// back to the per-change path. Applied changes emit the same durable events
// as their single-change counterparts. Returns the number of changes applied
// (skipped included) and the rejected changes.
func (e *Engine) ApplyLocalChanges(changes []scanner.Change) (applied int, failed []scanner.Change, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	// receive-only never records local changes (no index entries, no
	// tombstones), so it never propagates them either. The caller treats
	// failed==nil as "nothing to report"; the files stay on disk untouched.
	if e.Direction == "receive-only" {
		return 0, nil, nil
	}
	if len(changes) == 0 {
		return 0, nil, nil
	}
	type planned struct {
		writes []*index.FileInfo
		change scanner.Change
		reason string
	}
	var (
		plans []planned
		all   []*index.FileInfo
	)
	for _, c := range changes {
		if c.Kind == scanner.Skipped {
			e.Logf("skip %s: ignored by rule", c.Info.Name)
			// Prefer the rule the scanner recorded (the include gate reports
			// "include", which the ignore matcher alone cannot derive).
			rule := c.Rule
			if rule == "" {
				if _, r := e.Ignore.Match(c.Info.Name, false); r != nil {
					rule = r.Raw
				}
			}
			e.emit(events.CatSkipped, events.SevInfo, c.Info.Name, "ignored by rule", rule, 0)
			applied++
			continue
		}
		p := planned{change: c}
		if c.Kind == scanner.Renamed {
			writes, ok := e.planRename(c.From, c.Info)
			if !ok {
				failed = append(failed, c)
				continue
			}
			p.writes = writes
			p.reason = "local rename"
		} else {
			out, ok := e.planChange(c.Kind, c.Info)
			if !ok {
				failed = append(failed, c)
				continue
			}
			p.writes = []*index.FileInfo{out}
			switch c.Kind {
			case scanner.Deleted:
				p.reason = "local delete"
			case scanner.Modified:
				p.reason = "local modify"
			default:
				p.reason = "local add"
			}
		}
		plans = append(plans, p)
		all = append(all, p.writes...)
		applied++
	}
	// Commit the planned writes in bounded transactions. A chunk keeps WAL
	// growth and per-transaction memory in check for very large renames; a
	// hard DB error mid-way leaves a partial apply that the next scan
	// reconciles (verification failures are handled before any commit).
	for i := 0; i < len(all); i += batchChunk {
		end := i + batchChunk
		if end > len(all) {
			end = len(all)
		}
		if err := e.Ix.PutBatch(all[i:end]); err != nil {
			return 0, nil, err
		}
	}
	// Emit durable events. A large local rename (a folder move) must not
	// flood the event store with one event per file: collapse to a single
	// summary, mirroring emitMoves on the remote side.
	renamed := 0
	for _, p := range plans {
		if p.change.Kind == scanner.Renamed {
			renamed++
		}
	}
	if renamed > maxPerFileEvents {
		e.emit(events.CatApplied, events.SevInfo, "",
			fmt.Sprintf("moved %d file(s) locally (same-content rename, no transfer)", renamed), "", 0)
	} else {
		for _, p := range plans {
			if p.change.Kind == scanner.Renamed {
				e.emit(events.CatApplied, events.SevInfo, p.change.Info.Name, p.reason, p.change.From, 0)
				continue
			}
			e.emit(events.CatApplied, events.SevInfo, p.change.Info.Name, p.reason, "", 0)
		}
	}
	return applied, failed, nil
}

// SetPeerIndex records a peer's view of the folder. If update is false the
// view is replaced wholesale; otherwise only the named entries change.
// PeerCount returns how many entries the engine currently holds in its view
// of peerID for this folder. Exposed for tests verifying index exchange.
func (e *Engine) PeerCount(peerID uint64) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.peers[peerID])
}

// SetPeerIndex replaces (update=false) or merges (update=true) the engine's
// in-memory view of a peer's index for one folder.
func (e *Engine) SetPeerIndex(peerID uint64, files []*index.FileInfo, update bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	view, ok := e.peers[peerID]
	if !ok {
		view = map[string]*index.FileInfo{}
		e.peers[peerID] = view
	}
	if !update {
		seen := make(map[string]bool, len(files))
		for _, f := range files {
			seen[f.Name] = true
		}
		for name := range view {
			if !seen[name] {
				delete(view, name)
			}
		}
	}
	for _, f := range files {
		view[f.Name] = f.Clone()
	}
}

// CleanupPeerIndex drops the engine's in-memory view of a peer's index for
// this folder. Called when a peer is removed from the configuration so the
// view cannot leak (one full-folder view per removed peer, forever).
func (e *Engine) CleanupPeerIndex(peerID uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.peers, peerID)
}

// SetPeerIndexAppend merges files into the peer's view without deleting
// entries absent from files: used by chunked index exchanges, where only
// the final frame of a full send sweeps stale entries (a full send's final
// frame carries the folder's complete entry set, and an append-only delta
// never deletes).
func (e *Engine) SetPeerIndexAppend(peerID uint64, files []*index.FileInfo) {
	e.mu.Lock()
	defer e.mu.Unlock()
	view, ok := e.peers[peerID]
	if !ok {
		view = map[string]*index.FileInfo{}
		e.peers[peerID] = view
	}
	for _, f := range files {
		view[f.Name] = f.Clone()
	}
}

// GlobalFor returns the winning version of name across the local index and
// all peer views, plus whether the winner is our local entry.
func (e *Engine) GlobalFor(name string) (*index.FileInfo, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.globalForLocked(name)
}

func (e *Engine) globalForLocked(name string) (*index.FileInfo, bool, error) {
	local, ok, err := e.Ix.Get(name)
	if err != nil {
		return nil, false, err
	}
	best, bestLocal := e.globalFrom(local, ok, name)
	return best, bestLocal, nil
}

// globalFrom combines a (possibly nil) local entry with the peer views for
// name and returns the winning version. It is the same selection logic as
// globalForLocked but accepts a preloaded local entry, so bulk paths can
// load the whole local index once instead of issuing one query per name.
func (e *Engine) globalFrom(local *index.FileInfo, ok bool, name string) (*index.FileInfo, bool) {
	var best *index.FileInfo
	bestLocal := false
	if ok {
		best, bestLocal = local, true
	}
	for _, view := range e.peers {
		p, pok := view[name]
		if !pok {
			continue
		}
		if best == nil {
			best, bestLocal = p, false
			continue
		}
		switch p.Version.Compare(best.Version) {
		case 1:
			best, bestLocal = p, false
		case 2:
			// Concurrent versions (a real conflict): the file with the newer
			// modification time is the default winner — the most recent edit
			// wins, and the older one is preserved as a conflict copy / an
			// archived version on the losing side. Ties fall back to the
			// deterministic tie-breaker so every peer still converges to the
			// same choice.
			if newerOrGreater(p, best) {
				best, bestLocal = p, false
			}
		}
	}
	if best == nil {
		return nil, false
	}
	return best, bestLocal
}

// NeedPulls returns the names this device must fetch or apply to converge
// to the global model (files, deletions, and conflicts).
func (e *Engine) NeedPulls() ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// send-only never acquires anything from peers: no pulls, no remote
	// deletions, no conflict copies. Peer views are still merged (see
	// SetPeerIndex) because the engine needs the peer's state to serve its
	// own files and to compute the global model.
	if e.Direction == "send-only" {
		return nil, nil
	}

	names := map[string]bool{}
	// Load the local index into memory ONCE: the per-name path below would
	// otherwise issue one SQLite query per file, which dominates on
	// million-file folders.
	localMap := map[string]*index.FileInfo{}
	if err := e.Ix.ListAll(func(fi *index.FileInfo) error {
		names[fi.Name] = true
		localMap[fi.Name] = fi
		return nil
	}); err != nil {
		return nil, err
	}
	for _, view := range e.peers {
		for name := range view {
			names[name] = true
		}
	}

	var out []string
	for name := range names {
		local := localMap[name]
		ok := local != nil
		global, _ := e.globalFrom(local, ok, name)
		if global == nil {
			continue
		}
		switch {
		case !ok && !global.Deleted:
			// Only the acquire path is gated: a name we do not own and the
			// include list does not cover is out of scope, so it is never
			// pulled. Owned entries keep following peer updates/deletions.
			if e.Include != nil {
				isDir := false
				for _, view := range e.peers {
					if g, eok := view[name]; eok {
						isDir = g.Type == index.TypeDirectory
						break
					}
				}
				if matched, _ := e.Include.Match(name, isDir); !matched {
					continue
				}
			}
			out = append(out, name)
		case ok && !local.Deleted && global.Deleted:
			out = append(out, name)
		case ok && !local.Deleted && !global.Deleted:
			cmp := global.Version.Compare(local.Version)
			switch cmp {
			case 1:
				out = append(out, name)
			case 2:
				// Concurrent edit: need the winner unless content is
				// already identical (the equality check that kills
				// false conflicts).
				if !sameContent(local, global) {
					out = append(out, name)
				}
			}
		case ok && local.Deleted && !global.Deleted:
			// Local tombstone but a peer resurrected the file.
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// PendingDeletions returns the names this device must delete to converge:
// files that are live locally but globally tombstoned (a peer deleted them
// and the deletion is authoritative). Used by the deletion guard so the
// session can count/block them before applying anything destructive.
//
// NOTE: the index pool is capped at one connection, so no nested query may
// run inside a List/ListAll callback — names are collected first and the
// global model is consulted afterwards.
func (e *Engine) PendingDeletions() ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	// send-only never applies remote deletions, so it never has any pending:
	// this guards both the session delete pass and the operator's "apply
	// pending deletions" override.
	if e.Direction == "send-only" {
		return nil, nil
	}
	var names []string
	localMap := map[string]*index.FileInfo{}
	if err := e.Ix.List(func(fi *index.FileInfo) error {
		if !fi.Deleted {
			names = append(names, fi.Name)
			localMap[fi.Name] = fi
		}
		return nil
	}); err != nil {
		return nil, err
	}
	var out []string
	for _, name := range names {
		global, _ := e.globalFrom(localMap[name], true, name)
		if global != nil && global.Deleted {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// Move is a same-content relocation detected between a path a peer deleted
// and a path a peer added in the same session: the receiver satisfies the
// new path with a local rename instead of re-downloading the content.
type Move struct {
	From   string
	To     string
	Global *index.FileInfo // the peer's entry for To (adopted on apply)
}

// PlanMoves detects same-content moves in peerID's index view: paths the
// peer deleted (live locally, tombstoned in that view) that pair one-to-one
// with paths the peer added (absent locally, live in that view) of identical
// content (same type, size, and block hashes). Detected moves are applied as
// local renames on the receiver: no transfer, and they are excluded from the
// deletion guard and mass-change signal (a move is not a deletion).
//
// Only files are paired — directories and symlinks carry no content and are
// cheap to create/adopt anyway. Content groups that are not exactly 1:1 (one
// source matching several targets, a target matching several sources, or
// leftovers) are skipped and fall back to the normal pull+delete path.
func (e *Engine) PlanMoves(peerID uint64) ([]Move, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// send-only never relocates local files on a peer's behalf: a move is
	// an applied remote change (ApplyMove renames the local copy), so it is
	// gated exactly like NeedPulls. pullAll plans moves BEFORE it asks for
	// pulls, so the NeedPulls early-out alone would not stop them.
	if e.Direction == "send-only" {
		return nil, nil
	}
	view, ok := e.peers[peerID]
	if !ok || len(view) == 0 {
		return nil, nil
	}

	// Load the live local index once (no nested queries inside callbacks).
	localMap := map[string]*index.FileInfo{}
	if err := e.Ix.ListAll(func(fi *index.FileInfo) error {
		if !fi.Deleted && fi.Type == index.TypeFile {
			localMap[fi.Name] = fi
		}
		return nil
	}); err != nil {
		return nil, err
	}

	type group struct {
		sources []string // local live files this peer tombstoned
		targets []string // peer-added files absent locally
	}
	groups := map[string]*group{}
	for name, local := range localMap {
		g, ok := view[name]
		if !ok || !g.Deleted {
			continue // this peer did not delete the path
		}
		k := contentKey(local)
		grp := groups[k]
		if grp == nil {
			grp = &group{}
			groups[k] = grp
		}
		grp.sources = append(grp.sources, name)
	}
	for name, g := range view {
		if g.Deleted || g.Type != index.TypeFile {
			continue
		}
		if _, exists := localMap[name]; exists {
			continue // already present locally (concurrent local change)
		}
		k := contentKey(g)
		grp := groups[k]
		if grp == nil {
			grp = &group{}
			groups[k] = grp
		}
		grp.targets = append(grp.targets, name)
	}

	var moves []Move
	for _, grp := range groups {
		if len(grp.sources) != 1 || len(grp.targets) != 1 {
			continue // ambiguous (a copy/duplicate) — fall back to normal path
		}
		g, ok := view[grp.targets[0]]
		if !ok || g.Deleted {
			continue
		}
		moves = append(moves, Move{From: grp.sources[0], To: grp.targets[0], Global: g})
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].From < moves[j].From })
	return moves, nil
}

// contentKey is a compact identity for a file's content: size plus every
// block hash. Two files with equal keys have identical content (and, for
// indexed files, vice versa). Used to group move sources and targets.
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

// ApplyMove performs a detected move as a local rename: content already
// present at From is relocated to To, with the peer's mtime applied (like a
// pull commit) so metadata matches exactly. On a cross-device (EXDEV) rename
// it falls back to a same-directory copy + rename via the stager — still
// local I/O, never a network transfer. The index is updated to mirror the
// peer's view: From is tombstoned (no versioning archive — the data lives on
// at To) and To is adopted as live with the peer's version.
//
// The source and target are re-verified under the lock, so a stale plan
// (source changed, or target appeared locally) is rejected and the caller
// falls back to the normal pull+delete path.
func (e *Engine) ApplyMove(m Move) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if m.From == m.To {
		return fmt.Errorf("move: source and target are the same path")
	}
	local, ok, err := e.Ix.Get(m.From)
	if err != nil {
		return err
	}
	if !ok || local.Deleted {
		return fmt.Errorf("move %s -> %s: source no longer live", m.From, m.To)
	}
	if !sameContent(local, m.Global) {
		return fmt.Errorf("move %s -> %s: source content changed since planning", m.From, m.To)
	}
	if _, exists, err := e.Ix.Get(m.To); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("move %s -> %s: target already exists locally", m.From, m.To)
	}

	mt := time.Unix(m.Global.ModifiedS, int64(m.Global.ModifiedNs))
	if err := e.Stager.MoveRelocates(e.abs(m.From), m.To, mt); err != nil {
		return fmt.Errorf("move %s -> %s: %w", m.From, m.To, err)
	}

	// Index mirrors the peer's view: From tombstoned with our bumped vector
	// (same as ApplyDeletion, so the tombstone dominates), To adopted live
	// with the peer's version (a subsequent local edit supersedes it).
	tomb := &index.FileInfo{Name: m.From, Type: local.Type, Deleted: true,
		Version: local.Version.Bump(e.NodeID)}
	if err := e.Ix.Put(tomb); err != nil {
		return err
	}
	out := m.Global.Clone()
	if err := e.Ix.Put(out); err != nil {
		return err
	}
	return nil
}

// DiffForPeer returns the local entries that differ from a peer's view
// (used for index updates pushed to that peer).
func (e *Engine) DiffForPeer(peerID uint64) ([]*index.FileInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	view := e.peers[peerID]
	var out []*index.FileInfo
	err := e.Ix.ListAll(func(fi *index.FileInfo) error {
		if pf, ok := view[fi.Name]; ok && pf.Version.Equal(fi.Version) {
			return nil
		}
		out = append(out, fi.Clone())
		return nil
	})
	return out, err
}

// PendingUploads returns the files that at least one peer does not yet have
// (or has at an older version) and would therefore pull from us — i.e. the
// files waiting to be uploaded. Deletions are excluded.
func (e *Engine) PendingUploads() ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.peers) == 0 {
		return nil, nil
	}
	names := map[string]bool{}
	err := e.Ix.ListAll(func(fi *index.FileInfo) error {
		if fi.Deleted {
			return nil
		}
		for _, view := range e.peers {
			pf, ok := view[fi.Name]
			if !ok {
				names[fi.Name] = true
				break
			}
			if pf.Deleted || fi.Version.Compare(pf.Version) == 1 {
				names[fi.Name] = true
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// PlanOverwrite determines what to do with the local copy of name before a
// remote change is applied: archive it (versioning), and/or preserve it as
// a conflict copy. It returns a plan to execute and the global target.
func (e *Engine) PlanOverwrite(name string) (*OverwritePlan, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	global, _, err := e.globalForLocked(name)
	if err != nil {
		return nil, err
	}
	if global == nil {
		return nil, fmt.Errorf("no global for %q", name)
	}
	local, ok, err := e.Ix.Get(name)
	if err != nil {
		return nil, err
	}

	plan := &OverwritePlan{Name: name, Global: global.Clone(), Delete: global.Deleted}
	if !ok || local.Deleted {
		return plan, nil
	}

	conflict := global.Version.Compare(local.Version) == 2 && !sameContent(local, global)
	versioningEnabled := e.Versioning != nil && e.Versioning.Type != "" && e.Versioning.Type != "none"
	if versioningEnabled {
		plan.ArchiveLocal = true
	}
	// A conflict copy is only created when the versioning area is NOT the
	// preservation mechanism (they both would try to move the same file).
	if conflict && e.Policy == ConflictCopy && !versioningEnabled {
		plan.MakeConflictCopy = true
		plan.ConflictName = conflictName(local.Name, local.ModifiedS, local.ModifiedNs, e.NodeID)
	}
	return plan, nil
}

// OverwritePlan describes how to preserve the local copy before applying a
// remote change. The session executes it, then pulls the global version.
type OverwritePlan struct {
	Name             string
	Global           *index.FileInfo
	Delete           bool // applying a deletion
	ArchiveLocal     bool
	MakeConflictCopy bool
	ConflictName     string
	PeerID           uint64 // device that caused this change (0 = this device)
}

// Execute runs the plan's local-preservation steps (versioning archive and
// conflict copy) and returns the new local FileInfo that should be indexed
// for the conflict copy (nil if none).
func (e *Engine) Execute(plan *OverwritePlan) (*index.FileInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	abs := e.abs(plan.Name)
	var conflictFI *index.FileInfo

	if plan.ArchiveLocal {
		st, err := os.Stat(abs)
		if err == nil && !st.IsDir() {
			if err := e.Versioning.Archive(e.Root, plan.Name, st.ModTime()); err != nil {
				e.Logf("versioning: archive %s: %v", plan.Name, err)
			}
			e.emit(events.CatVersioned, events.SevInfo, plan.Name, "versioning: archived before remote replace", e.Versioning.Type, plan.PeerID)
		}
	}

	if plan.MakeConflictCopy {
		conflictAbs := e.abs(plan.ConflictName)
		if err := os.Rename(abs, conflictAbs); err != nil {
			return nil, fmt.Errorf("creating conflict copy for %s: %w", plan.Name, err)
		}
		e.emit(events.CatConflict, events.SevWarn, plan.Name, "concurrent edit: conflict copy created", plan.ConflictName, plan.PeerID) // Index the conflict copy as a new local change so it propagates.
		cur, ok, _ := e.Ix.Get(plan.ConflictName)
		var nv version.Vector
		if ok {
			nv = cur.Version.Bump(e.NodeID)
		} else {
			nv = version.New().Bump(e.NodeID)
		}
		st, _ := os.Stat(conflictAbs)
		conflictFI = &index.FileInfo{
			Name: plan.ConflictName, Size: st.Size(),
			ModifiedS: st.ModTime().Unix(), ModifiedNs: int32(st.ModTime().Nanosecond()),
			Mode: uint32(st.Mode().Perm()), Type: index.TypeFile,
			Version: nv, BlockSize: 0, Blocks: nil,
		}
		if err := e.Ix.Put(conflictFI); err != nil {
			return nil, err
		}
	}
	return conflictFI, nil
}

// Pull represents an in-progress download of one file.
type Pull struct {
	engine  *Engine
	name    string
	info    *index.FileInfo
	tmp     string
	f       *os.File
	written []bool
	peerID  uint64
}

// BlockNeed describes one block still required to complete a file.
type BlockNeed struct {
	Index  int
	Offset int64
	Size   int
	Hash   []byte
}

// StartPull begins downloading name (the global winner) into a staged temp.
// peerID is the device providing the data (0 for local-only uses).
//
// If a staged temp left by a previous (aborted) pull of this same file is
// already on disk, it is resumed: the temp is re-opened and every block is
// re-verified against its recorded hash, so the pull continues from the
// first block that is not yet valid. Re-verification makes resume safe
// against corruption and version drift — a temp of the wrong size is
// discarded and started fresh, and a temp of the right size but different
// content simply fails the hash check on the first stale block and is
// re-fetched from there. A pull therefore costs at most the blocks it has
// not already written, instead of always the whole file.
func (e *Engine) StartPull(name string, peerID uint64) (*Pull, error) {
	global, _, err := e.GlobalFor(name)
	if err != nil {
		return nil, err
	}
	if global == nil {
		return nil, fmt.Errorf("no global for %q", name)
	}
	if global.Deleted {
		return nil, fmt.Errorf("%s is a deletion; use ApplyDeletion", name)
	}
	tmp, err := e.Stager.TempPathFor(name)
	if err != nil {
		return nil, err
	}
	bs := int64(global.BlockSize)
	if bs <= 0 {
		bs = int64(hash.MinBlockSize)
	}
	written := make([]bool, len(global.Blocks))
	// Resume path: reuse a valid partial temp if one exists. Only a temp of
	// the exact expected size is trusted; the per-block re-verify below
	// establishes which prefix of blocks is actually valid.
	if fi, serr := os.Stat(tmp); serr == nil && fi.Size() == global.Size {
		if f, oerr := os.OpenFile(tmp, os.O_RDWR, 0); oerr == nil {
			for i := range global.Blocks {
				sz := blockSizeAt(global, i)
				buf := make([]byte, sz)
				if _, rerr := f.ReadAt(buf, int64(i)*bs); rerr != nil || !bytes.Equal(hash.HashBytes(buf), global.Blocks[i]) {
					break // this block (and everything after) is not yet valid
				}
				written[i] = true
			}
			return &Pull{engine: e, name: name, info: global.Clone(), tmp: tmp, f: f, written: written, peerID: peerID}, nil
		}
		// Could not re-open the existing temp: fall through to a fresh open.
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(global.Size); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Pull{engine: e, name: name, info: global.Clone(), tmp: tmp, f: f, written: written, peerID: peerID}, nil
}

// NeedBlocks returns the blocks still missing.
func (p *Pull) NeedBlocks() []BlockNeed {
	var out []BlockNeed
	for i := range p.info.Blocks {
		if p.written[i] {
			continue
		}
		out = append(out, BlockNeed{
			Index:  i,
			Offset: int64(i) * int64(p.info.BlockSize),
			Size:   blockSizeAt(p.info, i),
			Hash:   p.info.Blocks[i],
		})
	}
	return out
}

// ReceiveBlock verifies and writes one block. Hash mismatches are rejected.
func (p *Pull) ReceiveBlock(idx int, data []byte) error {
	if idx < 0 || idx >= len(p.info.Blocks) {
		return fmt.Errorf("block index %d out of range", idx)
	}
	if !bytes.Equal(hash.HashBytes(data), p.info.Blocks[idx]) {
		return fmt.Errorf("block %d hash mismatch (bad transfer)", idx)
	}
	if _, err := p.f.WriteAt(data, int64(idx)*int64(p.info.BlockSize)); err != nil {
		return err
	}
	p.written[idx] = true
	p.engine.Stats.BytesDown.Add(int64(len(data)))
	return nil
}

// Finish validates completeness, fsyncs, commits (mtime set on the temp
// BEFORE the atomic rename), and records the adopted version in the index.
func (p *Pull) Finish() (*index.FileInfo, error) {
	for i, w := range p.written {
		if !w {
			return nil, fmt.Errorf("missing block %d", i)
		}
	}
	if err := p.f.Sync(); err != nil {
		_ = p.f.Close()
		return nil, err
	}
	if err := p.f.Close(); err != nil {
		return nil, err
	}
	mt := time.Unix(p.info.ModifiedS, int64(p.info.ModifiedNs))
	if err := p.engine.Stager.Commit(p.tmp, p.name, mt); err != nil {
		return nil, err
	}
	// Adopt the global version (bump on our next local edit supersedes it).
	out := p.info.Clone()
	if err := p.engine.Ix.Put(out); err != nil {
		return nil, err
	}
	p.engine.Stats.FilesDown.Add(1)
	p.engine.emit(events.CatApplied, events.SevInfo, p.name, "pulled from peer", "", p.peerID)
	return out, nil
}

// Abort ends the pull WITHOUT removing the staged temp: a later StartPull of
// the same file reuses and re-verifies it, so an interrupted download resumes
// from where it stopped instead of restarting. The temp is left on disk only
// while the daemon runs; the startup orphan sweep (Stager.CleanupOrphans)
// clears it on restart, so aborted pulls never accumulate across runs.
func (p *Pull) Abort() {
	_ = p.f.Close()
}

// ApplyDeletion applies a global tombstone: archives the local file if
// versioning is enabled, then records the deletion in the index. peerID is
// the device that deleted the file (0 = local-only).
func (e *Engine) ApplyDeletion(name string, peerID uint64) error {
	plan, err := e.PlanOverwrite(name)
	if err != nil {
		return err
	}
	plan.PeerID = peerID
	if _, err := e.Execute(plan); err != nil {
		return err
	}
	abs := e.abs(name)
	// A directory deletion removes the whole subtree: the peer tombstoning
	// the directory implies its contents are gone from the global model, and
	// the session processes names lexicographically (parents before children),
	// so a plain os.Remove of a non-empty directory would silently fail and
	// leave the empty parent dir behind (e.g. after a same-content move).
	if plan.Global.Type == index.TypeDirectory {
		if err := os.RemoveAll(abs); err != nil {
			e.Logf("deletion %s: %v", name, err)
		}
	} else {
		_ = os.Remove(abs)
	}
	cur, ok, _ := e.Ix.Get(name)
	var nv version.Vector
	if ok {
		nv = cur.Version.Bump(e.NodeID)
	} else {
		nv = version.New().Bump(e.NodeID)
	}
	fi := &index.FileInfo{Name: name, Deleted: true, Version: nv}
	if err := e.Ix.Put(fi); err != nil {
		return err
	}
	e.emit(events.CatApplied, events.SevInfo, name, "deleted (remote)", "", peerID)
	return nil
}

// emit delivers an event to the optional handler. It may be called with
// e.mu held; the handler must be fast and must not call back into the
// engine (the event store has its own independent lock, so lock ordering
// is always engine → events, never the reverse). peerID attributes the
// event to the device that caused it (0 = this device).
func (e *Engine) emit(cat events.Category, sev events.Severity, path, reason, linked string, peerID uint64) {
	if e.OnEvent == nil {
		return
	}
	e.OnEvent(&events.Event{
		TS: time.Now(), Folder: e.ID, Path: path,
		Category: cat, Severity: sev, Reason: reason, Linked: linked, PeerID: peerID,
	})
}

// EnsureDir creates a directory that the global model expects and adopts
// its version into the index. Directories have no content; they are created
// with MkdirAll rather than committed as files.
func (e *Engine) EnsureDir(fi *index.FileInfo) error {
	if err := staging.EnsureDir(e.abs(fi.Name)); err != nil {
		return err
	}
	out := fi.Clone()
	return e.Ix.Put(out)
}

// AdoptMetadata records a non-file entry (currently symlinks) in the index
// without transferring content. Symlink targets are not followed in v1;
// the entry is indexed so it does not keep being reported as needed.
func (e *Engine) AdoptMetadata(fi *index.FileInfo) error {
	out := fi.Clone()
	return e.Ix.Put(out)
}

func (e *Engine) abs(name string) string {
	return path.Join(e.Root, filepathToSlash(name))
}

func filepathToSlash(name string) string {
	return strings.ReplaceAll(name, "\\", "/")
}

// sameContent compares two file entries by block hashes.
func sameContent(a, b *index.FileInfo) bool {
	if a.Type != b.Type || a.Size != b.Size {
		return false
	}
	if len(a.Blocks) != len(b.Blocks) {
		return false
	}
	for i := range a.Blocks {
		if !bytes.Equal(a.Blocks[i], b.Blocks[i]) {
			return false
		}
	}
	return true
}

// totalGreater is a deterministic, symmetric tie-breaker for concurrent
// versions: the file with the lexicographically larger canonical version
// representation wins, so every peer converges on the same choice.
func totalGreater(a, b *index.FileInfo) bool {
	return canonicalVersion(a.Version) > canonicalVersion(b.Version)
}

// newerOrGreater is the conflict winner rule: prefer the file with the newer
// modification time (the most recent edit becomes the default winner), and
// fall back to the deterministic totalGreater tie-breaker when the times are
// equal so all peers still converge on the same winner. It is only consulted
// for CONCURRENT versions (a real conflict), never for causally-newer ones.
func newerOrGreater(a, b *index.FileInfo) bool {
	at := a.ModifiedS*1_000_000_000 + int64(a.ModifiedNs)
	bt := b.ModifiedS*1_000_000_000 + int64(b.ModifiedNs)
	if at != bt {
		return at > bt
	}
	return totalGreater(a, b)
}

func canonicalVersion(v version.Vector) string {
	keys := make([]uint64, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(strconv.FormatUint(k, 16))
		sb.WriteByte(':')
		sb.WriteString(strconv.FormatUint(v[k], 16))
		sb.WriteByte('|')
	}
	return sb.String()
}

func blockSizeAt(fi *index.FileInfo, idx int) int {
	bs := int64(fi.BlockSize)
	if bs <= 0 {
		bs = int64(hash.MinBlockSize)
	}
	off := int64(idx) * bs
	remain := fi.Size - off
	if remain <= 0 {
		return 0
	}
	if remain < bs {
		return int(remain)
	}
	return int(bs)
}

// conflictName builds name.sync-conflict-<ts>-<device>.ext.
func conflictName(name string, modS int64, modNs int32, device uint64) string {
	ext := path.Ext(name)
	base := strings.TrimSuffix(name, ext)
	ts := time.Unix(modS, int64(modNs)).UTC().Format("20060102-150405")
	return base + ConflictSuffix + "-" + ts + "-" + strconv.FormatUint(device, 10) + ext
}
