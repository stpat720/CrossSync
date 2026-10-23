// Package node implements the CrossSync daemon: folders, scanning, and
// peer sessions. It wires together the index, scanner, staging, and engine.
package node

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	std "sync"
	"sync/atomic"
	"time"

	"crosssync/internal/alert"
	"crosssync/internal/certs"
	"crosssync/internal/config"
	"crosssync/internal/events"
	"crosssync/internal/hold"
	"crosssync/internal/ignore"
	"crosssync/internal/index"
	"crosssync/internal/protocol"
	"crosssync/internal/scanner"
	"crosssync/internal/staging"
	"crosssync/internal/sync"
	"crosssync/internal/transfer"
)

// Folder is a live synced folder within a node.
type Folder struct {
	Cfg     config.Folder
	Root    string
	Ix      *index.Index
	Stager  *staging.Stager
	Engine  *sync.Engine
	Scanner *scanner.Scanner
	// Hold is the soft-deletion hold area (.sfx-hold): copies removed by an
	// over-threshold deletion are parked here for an undo window instead of
	// being unlinked.
	Hold *hold.Store

	// mu serializes scanning with structural folder mutation
	// (update/remove/rebuild/remove-conflict) for THIS folder, so a scan
	// never runs against a folder whose scanner/engine/index are being
	// swapped or closed. Scans of different folders are independent. Lock
	// order is always f.mu before foldersMu (never the reverse).
	mu std.Mutex

	// paused mirrors Cfg.Paused as an atomic so the scan and session paths
	// can consult it without taking f.mu (they run hot, and a paused folder
	// must not add lock traffic to a peer round).
	paused atomic.Bool
}

// Paused reports whether this folder is paused on this device.
func (f *Folder) Paused() bool { return f.paused.Load() }

// Node is the daemon core: a device identity plus its folders.
type Node struct {
	Cfg     *config.Config
	ID      uint64
	Name    string
	Folders map[string]*Folder // keyed by folder id
	Logf    func(format string, args ...any)

	// ConnMgr schedules outbound peer syncs with exponential backoff.
	ConnMgr *ConnManager

	// Events is the durable, append-only event store (transparency layer).
	Events *events.Store

	// SentEntries counts index entries transmitted during sessions; used by
	// tests to assert delta exchange behavior.
	SentEntries atomic.Int64

	// SessionID is a random per-process identifier (hex), advertised in the
	// Hello handshake. Peers store it in their peer-index markers, so when
	// we restart they re-send full indexes instead of deltas against our
	// (now wiped) in-memory view of their files.
	SessionID string

	// Event-stream subscribers (control plane SSE).
	subMu std.Mutex
	subs  map[*eventSub]struct{}

	// peerFolders records which folder ids each peer has advertised in its
	// cluster config, learned during sync sessions. The control plane uses
	// it to offer only peers that actually have a folder in its peer list.
	peerFoldersMu std.Mutex
	peerFolders   map[uint64]map[string]bool

	// peerUnsynced holds, per peer and folder, the open unsynced set that
	// peer advertised at the end of its last successful round (MsgEvent,
	// protocol rev >= 2). It is deliberately transient: it describes
	// another process's live state, so it is replaced wholesale each
	// round, wiped for a peer when it is removed or fails a session, and
	// never persisted across restarts.
	peerUnsyncedMu std.Mutex
	peerUnsynced   map[uint64]map[string][]protocol.UnsyncedEntry

	// problems tracks the last reported reason per problem file, so a file
	// stuck for days (locked, permission, disk full) does not spam the
	// event store — only the first failure and reason changes emit records.
	probMu    std.Mutex
	problems  map[string]string // "folder\x00name" -> last reason
	folderErr map[string]string // "folder" -> last scan-error reason

	// delGuard tracks folders whose deletion guard has tripped. Since the
	// guard became soft it never blocks: an over-threshold pass goes through
	// and parks this device's copies in the hold area instead. The map (and
	// the "Apply pending deletions" override) remains for legacy state and
	// for the operator-facing flag.
	delGuard map[string]bool // folder id -> tripped

	// caseColl tracks the last reported case-collision warning per colliding
	// path, so identical warnings are not re-emitted on every scan and are
	// resolved once the collision clears.
	caseColl map[string]string // "folder\x00path" -> last reason

	// massChange tracks folders currently flagged for a massive single-sync
	// change (e.g. >50% of live files), so the warning is emitted once and
	// resolved when the change rate drops back to normal.
	massChange map[string]bool // folder id -> flagged

	// hold tracks the stability gate's "held back" episode per folder: the
	// last non-zero in-flight count and whether the one-shot summary event
	// for the episode is still owed (emitted on the >0 → 0 transition, so a
	// 100k-file paste does not write one event per file).
	holdMu   std.Mutex
	holdings map[string]*holding

	// Alert posts page-worthy notifications (nil = disabled).
	Alert *alert.Notifier
	// alertThrottle prevents the same condition from paging repeatedly
	// (e.g. a peer that flaps) — one alert per condition per window.
	alertMu       std.Mutex
	alertThrottle map[string]time.Time // "folder|path|category" -> last fire
	alertCooldown time.Duration        // default 15m; settable for tests

	// foldersMu guards the Folders map (rare writes on add/remove/rebuild;
	// frequent reads from scans and lookups). Scans and structural folder
	// mutations serialize per folder via Folder.mu, so a long scan of one
	// folder never stalls scans of the others. Lock order is always f.mu
	// before foldersMu (never the reverse), and foldersMu is never held
	// while waiting for an f.mu.
	foldersMu std.RWMutex

	// act tracks live scan and sync activity so the control plane can
	// report progress (how much has been scanned, when it started) and
	// reject overlapping manual rescans/syncs.
	act activity

	// TLS identity: populated when config.TLS is set.
	TLS       bool
	Certs     *certs.Manager
	ClientTLS *tls.Config
	ServerTLS *tls.Config

	// peerAllowed holds the pinned peer fingerprints, shared by reference
	// with the tls.Config VerifyPeerCertificate closures, so ApplyPeers can
	// update the allowlist live without rebuilding TLS state.
	peerAllowed map[string]bool

	// sessionSems holds one semaphore per peer id so only one sync round runs
	// with a given peer at a time, inbound or outbound. Both sides staging the
	// same paths to the same deterministic .part temp is the confirmed cause of
	// the "chtimes <root>/.sfx-tmp/.sfx-*.part: no such file or directory"
	// churn: the winner renames the temp into place, the loser's commit finds
	// it gone. ConnManager's inFlight guard only covers OUTBOUND entry points
	// (scheduler tick, manual sync, ping); inbound sessions arrive through
	// Run->Serve ungated, so this slot is the one that covers both directions.
	// Entries are never deleted: one small channel per peer id ever seen is
	// bounded by the configured peer set.
	sessionMu   std.Mutex
	sessionSems map[uint64]chan struct{}
}

// New creates a Node from configuration.
func New(cfg *config.Config) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.MetaDir, 0o755); err != nil {
		return nil, fmt.Errorf("meta_dir: %w", err)
	}
	n := &Node{
		Cfg:     cfg,
		ID:      cfg.Device.ID,
		Name:    cfg.Device.Name,
		Folders: map[string]*Folder{},
		Logf:    log.Printf,
		act: activity{
			scans: map[string]*ScanStatus{},
		},
		peerFolders:   map[uint64]map[string]bool{},
		peerUnsynced:  map[uint64]map[string][]protocol.UnsyncedEntry{},
		problems:      map[string]string{},
		folderErr:     map[string]string{},
		delGuard:      map[string]bool{},
		caseColl:      map[string]string{},
		massChange:    map[string]bool{},
		alertThrottle: map[string]time.Time{},
		holdings:      map[string]*holding{},
	}
	// A fresh random session id per process: it invalidates every
	// peer-index marker from a previous run, so the first session after a
	// restart re-sends full indexes (see sessionValid).
	var sid [16]byte
	if _, err := rand.Read(sid[:]); err != nil {
		return nil, fmt.Errorf("session id: %w", err)
	}
	n.SessionID = hex.EncodeToString(sid[:])
	ev, err := n.openEventsStore()
	if err != nil {
		return nil, fmt.Errorf("events: %w", err)
	}
	n.Events = ev
	n.ConnMgr = NewConnManager(cfg.Peers, n.SyncWithPeer, n.Logf, n.recordEvent, n.peerRecovered)
	if cfg.AlertURL != "" {
		n.Alert = alert.New(cfg.AlertURL)
	}
	n.alertCooldown = alertCooldownDefault
	if cfg.TLS {
		cm, err := certs.LoadOrCreate(filepath.Join(cfg.MetaDir, "key.pem"),
			filepath.Join(cfg.MetaDir, "cert.pem"), cfg.Device.Name)
		if err != nil {
			return nil, fmt.Errorf("tls identity: %w", err)
		}
		allowed := map[string]bool{}
		for _, p := range cfg.Peers {
			allowed[strings.ToLower(strings.TrimSpace(p.Fingerprint))] = true
		}
		n.TLS = true
		n.Certs = cm
		n.peerAllowed = allowed
		n.ClientTLS = certs.ClientConfig(cm, allowed)
		n.ServerTLS = certs.ServerConfig(cm, allowed)
		if cm.DeviceID() != n.ID {
			n.Logf("warning: configured device id %d does not match certificate-derived id %d (fingerprint %s)",
				n.ID, cm.DeviceID(), cm.Fingerprint())
		}
	}
	for _, fc := range cfg.Folders {
		if _, err := n.AddFolder(fc); err != nil {
			_ = ev.Close()
			return nil, err
		}
	}
	return n, nil
}

// openEventsStore opens the durable event store, transparently recovering
// from corruption the same way per-folder indexes do: if the database is
// unreadable or fails SQLite's fast structural check, it is quarantined
// (renamed with a .corrupt-<ts> suffix, kept for forensics) and a fresh
// store is created. A corrupt event log must never keep the daemon from
// booting.
func (n *Node) openEventsStore() (*events.Store, error) {
	path := filepath.Join(n.Cfg.MetaDir, "events.db")
	st, err := events.Open(path)
	if err == nil {
		qc := st.QuickCheck()
		if qc == nil {
			return st, nil
		}
		_ = st.Close()
		err = qc
	}
	// Recover: quarantine the corrupt database (and any WAL/SHM sidecars)
	// so it can be inspected, then start fresh.
	q := path + fmt.Sprintf(".corrupt-%d", time.Now().Unix())
	if rerr := os.Rename(path, q); rerr != nil {
		return nil, fmt.Errorf("events corrupt (%v); quarantine failed: %w", err, rerr)
	}
	for _, ext := range []string{"-wal", "-shm"} {
		_ = os.Rename(path+ext, q+ext)
	}
	n.Logf("events: store was corrupt (%v); quarantined to %s, starting fresh", err, q)
	st, oerr := events.Open(path)
	if oerr != nil {
		return nil, fmt.Errorf("fresh events store after quarantine: %w", oerr)
	}
	// ConnMgr does not exist yet while Node.New is bootstrapping, so record
	// the recovery directly on the new store rather than via recordEvent
	// (which broadcasts through it). The integrity check reports multi-line
	// detail, so collapse it to one line for the durable record.
	detail := strings.Join(strings.Fields(err.Error()), " ")
	if r := []rune(detail); len(r) > 300 {
		detail = string(r[:300]) + "..."
	}
	if _, rerr := st.Record(&events.Event{
		TS: time.Now(), Category: events.CatSystem, Severity: events.SevWarn,
		Reason: fmt.Sprintf("event store was corrupt (%s); quarantined to %s and started fresh", detail, q),
	}); rerr != nil {
		n.Logf("events: recording recovery event: %v", rerr)
	}
	return st, nil
}

// Listen returns a listener bound to the configured listen address,
// wrapped in TLS 1.3 when enabled.
func (n *Node) Listen() (net.Listener, error) {
	if n.TLS {
		return transfer.ListenTLS(n.Cfg.Listen, n.ServerTLS)
	}
	return transfer.Listen(n.Cfg.Listen)
}

// Dial opens a connection to addr, over TLS 1.3 when enabled.
func (n *Node) Dial(addr string) (*transfer.Conn, error) {
	if n.TLS {
		return transfer.DialTLS(addr, n.ClientTLS)
	}
	return transfer.Dial(addr)
}

// Fingerprint returns this node's certificate fingerprint, or "" when TLS
// is disabled. This is the value to configure on peers to pin this node.
func (n *Node) Fingerprint() string {
	if n.Certs == nil {
		return ""
	}
	return n.Certs.Fingerprint()
}

// ApplyPeers replaces the live peer set so add/update/delete take effect
// without a restart. It updates, in order:
//  1. the outbound scheduler's peer list (new peers start dialing on the
//     next tick; deleted peers stop immediately and their backoff state is
//     pruned),
//  2. the TLS allowlist (pinned fingerprints) — the map is mutated in
//     place, so the tls.Config verification closures see it live without
//     any state rebuild,
//  3. the in-memory config, which the dial path and control plane read.
//
// The caller is responsible for persisting the same peer list to the
// config file; this method never touches disk.
func (n *Node) ApplyPeers(peers []config.Peer) {
	if n.ConnMgr != nil {
		n.ConnMgr.SetPeers(peers)
	}
	if n.peerAllowed != nil {
		// Mutate the shared map in place (never reassign): verifyFunc
		// closed over the original map at boot, so reassignment would be
		// invisible to the running TLS state.
		for fp := range n.peerAllowed {
			delete(n.peerAllowed, fp)
		}
		for _, p := range peers {
			fp := strings.ToLower(strings.TrimSpace(p.Fingerprint))
			if fp != "" {
				n.peerAllowed[fp] = true
			}
		}
	}
	n.Cfg.Peers = append([]config.Peer(nil), peers...)
}

// RecordPeerFolders remembers which folder ids a peer advertised in its
// cluster config during a sync session. Sessions call this on every
// exchange; it is exported so the control plane (and tests) can also record
// peer folder knowledge.
func (n *Node) RecordPeerFolders(peerID uint64, folders map[string]bool) {
	n.peerFoldersMu.Lock()
	defer n.peerFoldersMu.Unlock()
	n.peerFolders[peerID] = folders
}

// PeerFolders returns the sorted folder ids a peer has advertised (the
// second value is false if the peer has never been seen in a session).
func (n *Node) PeerFolders(peerID uint64) ([]string, bool) {
	n.peerFoldersMu.Lock()
	defer n.peerFoldersMu.Unlock()
	fs, ok := n.peerFolders[peerID]
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(fs))
	for id := range fs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, true
}

// StorePeerUnsynced replaces the stored open-unsynced view for one
// (peer, folder) with the set the peer just advertised. Replacing wholesale
// is the point: an empty set means the peer is fully synced now, and a
// merge would leave stale entries behind forever. Only folders we actually
// sync are recorded, and the entry count is capped to keep a confused peer
// from growing our memory without bound.
func (n *Node) StorePeerUnsynced(peerID uint64, folder string, entries []protocol.UnsyncedEntry) {
	if peerID == 0 || folder == "" {
		return
	}
	if _, ok := n.lookupFolder(folder); !ok {
		return
	}
	if len(entries) > maxUnsyncedPerFolder {
		entries = entries[:maxUnsyncedPerFolder]
	}
	cp := make([]protocol.UnsyncedEntry, len(entries))
	copy(cp, entries)
	n.peerUnsyncedMu.Lock()
	defer n.peerUnsyncedMu.Unlock()
	byFolder := n.peerUnsynced[peerID]
	if byFolder == nil {
		byFolder = map[string][]protocol.UnsyncedEntry{}
		n.peerUnsynced[peerID] = byFolder
	}
	byFolder[folder] = cp
}

// PeerUnsynced returns the open unsynced set a peer last advertised for a
// folder (nil = nothing known: never received, or the peer is gone). The
// result is a copy, safe to hand to the control plane.
func (n *Node) PeerUnsynced(peerID uint64, folder string) []protocol.UnsyncedEntry {
	n.peerUnsyncedMu.Lock()
	defer n.peerUnsyncedMu.Unlock()
	byFolder := n.peerUnsynced[peerID]
	if byFolder == nil {
		return nil
	}
	entries, ok := byFolder[folder]
	if !ok {
		return nil
	}
	out := make([]protocol.UnsyncedEntry, len(entries))
	copy(out, entries)
	return out
}

// clearPeerUnsynced drops every stored unsynced view for a peer. Called
// when the peer is removed and when a session with it fails: the view is
// only ever a snapshot of that peer's live state, so a stale snapshot would
// keep telling the operator about files the peer has since synced. The next
// successful round repopulates it.
func (n *Node) clearPeerUnsynced(peerID uint64) {
	n.peerUnsyncedMu.Lock()
	delete(n.peerUnsynced, peerID)
	n.peerUnsyncedMu.Unlock()
}

// AddFolder registers a new folder, opening its index and creating its
// stager. Safe to call after construction. If the folder's index is
// corrupt, it is transparently quarantined and rebuilt from the filesystem
// (the FS is the source of truth).
func (n *Node) AddFolder(fc config.Folder) (*Folder, error) {
	st, err := os.Stat(fc.Path)
	if err != nil || !st.IsDir() {
		return nil, fmt.Errorf("folder %q: %w", fc.ID, err)
	}
	// Two folders over the same tree (or one nested inside the other) would
	// index the same files twice, both watchers would see the same events and
	// their tombstones would fight, so the second folder is refused outright.
	// Bounded by the folder count, and checked here so config load and the API
	// share one rule.
	cleanRoot := filepath.Clean(fc.Path)
	n.foldersMu.RLock()
	for _, ex := range n.Folders {
		exRoot := filepath.Clean(ex.Cfg.Path)
		if exRoot == cleanRoot || isSubPath(exRoot, cleanRoot) || isSubPath(cleanRoot, exRoot) {
			n.foldersMu.RUnlock()
			return nil, fmt.Errorf("folder %q: path %s overlaps folder %q (%s) — one tree cannot be indexed by two folders",
				fc.ID, fc.Path, ex.Cfg.ID, ex.Cfg.Path)
		}
	}
	n.foldersMu.RUnlock()
	ix, rebuilt, err := n.openFolderIndex(fc)
	if err != nil {
		return nil, err
	}
	stag, err := staging.New(fc.Path)
	if err != nil {
		_ = ix.Close()
		return nil, err
	}
	ig, err := ignore.Parse(fc.Ignore)
	if err != nil {
		_ = ix.Close()
		return nil, err
	}
	// An empty include list means "everything in scope": never hand a
	// non-nil empty matcher to the scanner or engine.
	var inc *ignore.Matcher
	if len(fc.Include) > 0 {
		inc, err = ignore.Parse(fc.Include)
		if err != nil {
			_ = ix.Close()
			return nil, err
		}
	}
	v := &sync.Versioning{
		Type:      fc.Versioning.Type,
		Keep:      fc.Versioning.Keep,
		MaxAge:    fc.Versioning.MaxAge,
		CleanDays: fc.Versioning.CleanDays,
	}
	eng := sync.New(n.ID, fc.ID, fc.Path, sync.ParseConflictPolicy(fc.ConflictPolicy),
		v, ix, stag, ig, n.Logf)
	eng.OnEvent = n.recordEvent
	eng.Include = inc
	eng.Direction = fc.SyncDirection()
	sc := scanner.New(fc.Path, ix, ig)
	if inc != nil {
		sc.SetInclude(inc)
	}
	sc.SetSettleScans(fc.SettleThreshold())
	if fc.SyncDirection() == "receive-only" {
		// A receive-only node never indexes local content, so every file
		// would look permanently changed and stay held back forever — the
		// folder would stop receiving. It also never advertises local
		// content, so it cannot leak a partial: the gate is pointless and
		// harmful here.
		sc.SetSettleScans(1)
	}
	f := &Folder{Cfg: fc, Root: fc.Path, Ix: ix, Stager: stag, Engine: eng, Scanner: sc,
		Hold: hold.New(fc.Path)}
	f.paused.Store(fc.Paused)
	eng.HoldLocal = f.Hold.Move
	n.foldersMu.Lock()
	n.Folders[fc.ID] = f
	n.foldersMu.Unlock()
	if rebuilt {
		// Rebuild under the new folder's lock so a concurrent scan of it
		// (e.g. a control-plane rescan) waits for the fresh index.
		f.mu.Lock()
		_, err := n.rebuildFromFS(f, "index was corrupt — rebuilt from filesystem")
		f.mu.Unlock()
		if err != nil {
			return f, fmt.Errorf("folder %q: rebuild from filesystem: %w", fc.ID, err)
		}
	}
	return f, nil
}

// openFolderIndex opens a folder's per-folder index, transparently
// recovering from corruption: if the database is unreadable or fails
// SQLite's fast structural check, it is quarantined (renamed with a
// .corrupt-<ts> suffix, kept for forensics) and a fresh index is created.
// The caller rebuilds the fresh index from the filesystem.
func (n *Node) openFolderIndex(fc config.Folder) (ix *index.Index, rebuilt bool, err error) {
	path := filepath.Join(n.Cfg.MetaDir, fc.ID+".db")
	ix, err = index.Open(path, fc.ID)
	if err == nil {
		if qc := ix.QuickCheck(); qc == nil {
			return ix, false, nil
		} else {
			_ = ix.Close()
			err = qc
		}
	}
	// Recover: quarantine the corrupt database (and any WAL/SHM sidecars)
	// so it can be inspected, then start fresh.
	q := path + fmt.Sprintf(".corrupt-%d", time.Now().Unix())
	if rerr := os.Rename(path, q); rerr != nil {
		return nil, false, fmt.Errorf("index corrupt (%v); quarantine failed: %w", err, rerr)
	}
	for _, ext := range []string{"-wal", "-shm"} {
		_ = os.Rename(path+ext, q+ext)
	}
	n.Logf("folder %s: index was corrupt (%v); quarantined to %s, rebuilding from filesystem",
		fc.ID, err, q)
	ix, err = index.Open(path, fc.ID)
	if err != nil {
		return nil, false, fmt.Errorf("fresh index after quarantine: %w", err)
	}
	return ix, true, nil
}

// rebuildFromFS scans a folder into a freshly-created index, restoring it
// from the filesystem (the source of truth) and recording a durable system
// event so the operator can see that a recovery happened. Returns the
// number of changes applied.
func (n *Node) rebuildFromFS(f *Folder, reason string) (int, error) {
	// Bypass the stability gate for this one scan: a rebuild quarantines the
	// DB, so both the index and the settling table are empty. With the gate
	// on, the first rebuild scan would return ZERO changes (every file at
	// count 1) and leave the index empty for a full scan interval — during
	// which NeedPulls would see no local entries and pull the entire folder
	// back from a peer. With an empty index there is no prior content a
	// partial could contradict: the filesystem IS the truth (this function's
	// existing contract). f.mu is held, so the flag cannot race a scan.
	f.Scanner.SetGateBypass(true)
	changes, err := f.Scanner.Scan()
	f.Scanner.SetGateBypass(false)
	if err != nil {
		return 0, err
	}
	applied := 0
	for _, c := range changes {
		if c.Kind == scanner.Unchanged {
			continue
		}
		if c.Kind == scanner.Renamed {
			if _, err := f.Engine.ApplyLocalRename(c.From, c.Info); err != nil {
				return applied, err
			}
			applied++
			continue
		}
		if _, err := f.Engine.ApplyLocalChange(c.Kind, c.Info); err != nil {
			return applied, err
		}
		applied++
	}
	if n.Events != nil {
		n.recordEvent(&events.Event{
			TS: time.Now(), Folder: f.Cfg.ID, Category: events.CatSystem,
			Severity: events.SevWarn, Reason: reason,
		})
	}
	n.Logf("folder %s: %s (%d change(s))", f.Cfg.ID, reason, applied)
	return applied, nil
}

// RebuildFolder forces a full rebuild of one folder's index from the
// filesystem, quarantining the current index. Used by the CLI when an
// operator wants to regenerate an index (e.g. after a manual edit or a
// suspected consistency issue).
func (n *Node) RebuildFolder(id string) (int, error) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return 0, fmt.Errorf("unknown folder %q", id)
	}
	// Exclude any in-flight scan of this folder before closing its index.
	f.mu.Lock()
	fc := f.Cfg
	path := f.Ix.Path()
	_ = f.Ix.Close()
	n.foldersMu.Lock()
	if n.Folders[id] == f {
		delete(n.Folders, id)
	}
	n.foldersMu.Unlock()
	f.mu.Unlock()
	q := path + fmt.Sprintf(".corrupt-%d", time.Now().Unix())
	if err := os.Rename(path, q); err != nil {
		return 0, fmt.Errorf("quarantine index: %w", err)
	}
	for _, ext := range []string{"-wal", "-shm"} {
		_ = os.Rename(path+ext, q+ext)
	}
	n.Logf("folder %s: manual rebuild — old index quarantined to %s", id, q)
	nf, err := n.AddFolder(fc)
	if err != nil {
		return 0, err
	}
	// Rebuild under the new folder's lock so a concurrent scan of it waits.
	nf.mu.Lock()
	defer nf.mu.Unlock()
	return n.rebuildFromFS(nf, "index rebuilt manually from filesystem")
}

// Close closes all folder indexes and the event store.
func (n *Node) Close() error {
	for _, f := range n.Folders {
		_ = f.Ix.Close()
	}
	if n.Events != nil {
		_ = n.Events.Close()
		n.Events = nil
	}
	return nil
}

// recordEvent writes one event to the durable store. Safe to call from any
// engine or session goroutine.
func (n *Node) recordEvent(e *events.Event) {
	if n.Events == nil {
		return
	}
	if _, err := n.Events.Record(e); err != nil {
		n.Logf("events: record: %v", err)
	}
	n.maybeAlert(e)
	n.broadcast(e)
}

// alertCooldownDefault is how long the same condition stays silent before
// it can page again (a peer flapping offline/online must not spam the
// endpoint).
const alertCooldownDefault = 15 * time.Minute

// maybeAlert fires a page-worthy notification for a curated set of events:
// folder-level errors (disk full, share unavailable, read-only, deletion
// guard) and peer-offline warnings. Routine info events never page.
func (n *Node) maybeAlert(e *events.Event) {
	if n.Alert == nil {
		return
	}
	pageWorthy := false
	switch {
	case e.Category == events.CatError && e.Severity >= events.SevWarn:
		pageWorthy = true
	case e.Category == events.CatPeer && e.Severity >= events.SevWarn &&
		strings.Contains(e.Reason, "peer offline"):
		pageWorthy = true
	}
	if !pageWorthy {
		return
	}
	key := e.Folder + "|" + e.Path + "|" + string(e.Category)
	n.alertMu.Lock()
	if last, ok := n.alertThrottle[key]; ok && time.Since(last) < n.alertCooldown {
		n.alertMu.Unlock()
		return
	}
	n.alertThrottle[key] = time.Now()
	n.alertMu.Unlock()
	title := "CrossSync " + n.Name
	msg := fmt.Sprintf("[%s] %s: %s", e.Category, e.Folder+"/"+e.Path, e.Reason)
	if err := n.Alert.Fire(title, msg); err != nil {
		n.Logf("alert: %v", err)
	}
}

// staleSyncAfter is how long a previously-working folder may go without a
// completed sync round before the daemon PAGES about it. A variable so tests can
// shorten it. The pull-side equivalent is /healthz (which goes 503).
var staleSyncAfter = 2 * time.Hour

// CheckStaleSync pages when a folder that used to sync has stopped completing
// rounds — the silent failure mode (peer up, daemon up, nothing moving) that
// otherwise surfaces only when someone notices stale files. Called from the
// daemon tick; the alert throttle keeps it to one page per folder per cooldown.
// Folders that never synced, folders with no peers and paused folders are
// skipped: none of them can be "stale" in a way an operator must act on.
func (n *Node) CheckStaleSync() {
	if n.Alert == nil || len(n.Cfg.Peers) == 0 {
		return
	}
	now := time.Now()
	n.foldersMu.RLock()
	folders := make([]*Folder, 0, len(n.Folders))
	for _, f := range n.Folders {
		folders = append(folders, f)
	}
	n.foldersMu.RUnlock()

	for _, f := range folders {
		if f.Paused() {
			continue
		}
		// A scan in flight is progress, not a stall: a verified scan of a large
		// share can hold a folder for a long time legitimately.
		if n.ScanRunning(f.Cfg.ID) {
			continue
		}
		ls := f.Engine.Stats.LastSync.Load()
		if ls == 0 {
			continue // never synced: the peer-offline path already covers this
		}
		last := time.Unix(0, ls)
		age := now.Sub(last)
		if age < staleSyncAfter {
			continue
		}
		key := f.Cfg.ID + "|stale-sync|"
		cooldown := n.alertCooldown
		if cooldown <= 0 {
			cooldown = alertCooldownDefault
		}
		n.alertMu.Lock()
		if n.alertThrottle == nil {
			n.alertThrottle = map[string]time.Time{}
		}
		lastFired, fired := n.alertThrottle[key]
		if fired && now.Sub(lastFired) < cooldown {
			n.alertMu.Unlock()
			continue
		}
		n.alertThrottle[key] = now
		n.alertMu.Unlock()

		msg := fmt.Sprintf("folder %s (%s): no completed sync round in %s (last: %s). The daemon is up, so check the peer connection and the share.",
			f.Cfg.ID, f.Root, age.Round(time.Minute), last.UTC().Format(time.RFC3339))
		n.Logf("warning: %s", msg)
		if err := n.Alert.Fire("CrossSync "+n.Name+": sync stalled", msg); err != nil {
			n.Logf("alert: %v", err)
		}
	}
}

// SendTestAlert fires a test notification so an operator can verify the
// endpoint before relying on it. Returns the error if the endpoint fails.
func (n *Node) SendTestAlert() error {
	if n.Alert == nil {
		return fmt.Errorf("no alert endpoint configured (set alert_url)")
	}
	return n.Alert.Fire("CrossSync "+n.Name, "Test alert — notifications are working")
}

// peerRecovered dismisses every outstanding "peer offline" attention event
// for a peer that just came back online. The records stay in the history
// (marked auto-resolved — the condition is no longer present) but they
// leave the attention badge automatically. If the peer drops again later,
// the next offline event opens fresh attention.
func (n *Node) peerRecovered(name string) {
	if n.Events == nil {
		return
	}
	if err := n.Events.ResolveAutoCondition("peers", name, events.CatPeer, "system"); err != nil {
		n.Logf("events: resolve peer offline: %v", err)
	}
}

// RecordSystem writes a daemon-level event (start/stop).
func (n *Node) RecordSystem(sev events.Severity, reason string) {
	n.recordEvent(&events.Event{
		TS: time.Now(), Category: events.CatSystem, Severity: sev, Reason: reason,
	})
}

// Record writes an event to the durable store and broadcasts it to SSE
// subscribers. Used by the control plane for daemon-originated actions
// (e.g. restoring a file from an archived version).
func (n *Node) Record(e *events.Event) {
	if e.TS.IsZero() {
		e.TS = time.Now()
	}
	n.recordEvent(e)
}

// reportProblem records a durable "unsynced" event for a file that could
// not be applied (locked, permission denied, disk full, bad transfer). It
// is throttled: only the first failure and changes to the reason emit new
// records, so a file stuck for days does not grow the event store. The
// event re-opens per the store's never-dismissed semantics, and is resolved
// by clearProblem once the file applies.
func (n *Node) reportProblem(folder, name string, err error) {
	if n.Events == nil || err == nil {
		return
	}
	reason := classifyProblem(err)
	key := folder + "\x00" + name
	n.probMu.Lock()
	if n.problems[key] == reason {
		n.probMu.Unlock()
		return // unchanged; do not spam
	}
	n.problems[key] = reason
	n.probMu.Unlock()
	n.recordEvent(&events.Event{
		TS: time.Now(), Folder: folder, Path: name,
		Category: events.CatUnsynced, Severity: events.SevWarn, Reason: reason,
	})
}

// clearProblem resolves a previously-reported unsynced condition once the
// file applies successfully, clearing it from the badge/attention.
func (n *Node) clearProblem(folder, name string) {
	if n.Events == nil {
		return
	}
	key := folder + "\x00" + name
	n.probMu.Lock()
	_, was := n.problems[key]
	delete(n.problems, key)
	n.probMu.Unlock()
	if was {
		_ = n.Events.ResolveCondition(folder, name, events.CatUnsynced, "system")
	}
}

// reportFolderError records a durable error event for a folder-level
// failure (share unavailable, scan error), throttled by reason so a down
// array does not spam the store.
func (n *Node) reportFolderError(folder string, err error) {
	if n.Events == nil || err == nil {
		return
	}
	reason := classifyProblem(err)
	n.probMu.Lock()
	if n.folderErr[folder] == reason {
		n.probMu.Unlock()
		return
	}
	n.folderErr[folder] = reason
	n.probMu.Unlock()
	n.recordEvent(&events.Event{
		TS: time.Now(), Folder: folder, Category: events.CatError,
		Severity: events.SevWarn, Reason: "folder error: " + reason,
	})
}

// clearFolderError resolves a previously-reported folder error event.
func (n *Node) clearFolderError(folder string) {
	if n.Events == nil {
		return
	}
	n.probMu.Lock()
	_, was := n.folderErr[folder]
	delete(n.folderErr, folder)
	n.probMu.Unlock()
	if was {
		_ = n.Events.ResolveCondition(folder, "", events.CatError, "system")
	}
}

// emitMoves records move activity for a folder: a folder-level summary so a
// large reorganization is visible, plus per-file applied events when the
// count is small (a 10k-file folder move must not spam the event store).
func (n *Node) emitMoves(folder string, moves []sync.Move, peerID uint64) {
	if n.Events == nil || len(moves) == 0 {
		return
	}
	if len(moves) <= 50 {
		for _, m := range moves {
			n.recordEvent(&events.Event{
				TS: time.Now(), Folder: folder, Path: m.To,
				Category: events.CatApplied, Severity: events.SevInfo,
				Reason: "moved (local rename, no transfer)", Linked: m.From, PeerID: peerID,
			})
		}
	}
	n.recordEvent(&events.Event{
		TS: time.Now(), Folder: folder,
		Category: events.CatApplied, Severity: events.SevInfo,
		Reason: fmt.Sprintf("moved %d file(s) locally (same-content rename, no transfer)", len(moves)),
		PeerID: peerID,
	})
}

// emitReleases records one folder-level event for the peer moves this device
// released because their destination is outside its include gate. There are
// deliberately no per-file events (a folder-wide reorganization can release
// thousands of paths); the content stays safe on the peer that owns it.
func (n *Node) emitReleases(folder string, moves []sync.Move) {
	if n.Events == nil || len(moves) == 0 {
		return
	}
	n.recordEvent(&events.Event{
		TS: time.Now(), Folder: folder,
		Category: events.CatApplied, Severity: events.SevInfo,
		Reason: fmt.Sprintf("released %d path(s) moved outside this device's include gate (the peer that holds them keeps them)", len(moves)),
	})
}

// classifyProblem maps an I/O error to a short, human-actionable reason.
func classifyProblem(err error) string {
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "used by another process"),
		strings.Contains(s, "being used by another"),
		strings.Contains(s, "text file busy"),
		strings.Contains(s, "device or resource busy"):
		return "file is locked/in use — retrying"
	case strings.Contains(s, "no space left"), strings.Contains(s, "disk full"), strings.Contains(s, "enospc"):
		return "disk full — space needed on the share"
	case strings.Contains(s, "permission denied"):
		return "permission denied — check PUID/PGID ownership"
	case strings.Contains(s, "read-only file system"), strings.Contains(s, "erofs"):
		return "share is read-only — check the SMB export / array mode"
	case strings.Contains(s, "hash mismatch"), strings.Contains(s, "bad transfer"):
		return "transfer failed hash verification — will re-request"
	case strings.Contains(s, "block request failed"):
		// The peer answered a block request with a generic refusal: its own
		// disk could not read the file (a directory where a file is
		// indexed, a driver error, a locked handle). Per-file, retried
		// next round — say so instead of echoing the wire code.
		return "the peer could not read this file from its disk — retrying next round (check the file and its permissions on that peer)"
	default:
		if len(s) > 140 {
			s = s[:140]
		}
		return "cannot apply: " + s
	}
}

// SyncHealth reports the newest and oldest last-completed-sync timestamps
// (unix nanos, 0 = never) across all folders, plus the folder count. It reads
// only per-folder atomics — no SQL, no index walk — so a container healthcheck
// can call it as often as it likes. Folders with a scan in flight are EXCLUDED:
// a busy folder is not a stalled one (a verified scan of a large share holds
// its folder for a long time by design), and counting it would flip /healthz
// to degraded and page an operator for nothing.
func (n *Node) SyncHealth() (newest, oldest int64, folders int) {
	n.foldersMu.RLock()
	folders = len(n.Folders)
	for id, f := range n.Folders {
		if f.Engine == nil {
			continue
		}
		if n.ScanRunning(id) {
			folders--
			continue
		}
		ls := f.Engine.Stats.LastSync.Load()
		if ls > newest {
			newest = ls
		}
		if oldest == 0 || ls < oldest {
			oldest = ls
		}
	}
	n.foldersMu.RUnlock()
	return newest, oldest, folders
}

// isSubPath reports whether child sits strictly inside parent (both already
// cleaned). It is used to refuse folders whose roots overlap.
func isSubPath(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil || rel == "." || rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// DeleteGuardTripped reports whether the deletion guard has blocked a
// folder: a sync proposed deleting more files than the configured threshold
// and the operator has not yet applied them.
func (n *Node) DeleteGuardTripped(id string) bool {
	n.probMu.Lock()
	defer n.probMu.Unlock()
	return n.delGuard[id]
}

// PendingDeletionsCount returns how many live files are globally deleted
// and waiting to be removed (blocked or not).
func (n *Node) PendingDeletionsCount(id string) (int, error) {
	f, ok := n.Folders[id]
	if !ok {
		return 0, fmt.Errorf("unknown folder %q", id)
	}
	dels, err := f.Engine.PendingDeletions()
	return len(dels), err
}

// reportDeletionGuard marks a folder as guard-tripped and emits a durable
// folder error event once (the guard stays tripped until the operator
// applies the deletions, so it must not spam).
func (n *Node) reportDeletionGuard(id, msg string) {
	n.probMu.Lock()
	if n.delGuard[id] {
		n.probMu.Unlock()
		return
	}
	n.delGuard[id] = true
	n.probMu.Unlock()
	if n.Events == nil {
		return
	}
	if _, err := n.Events.Record(&events.Event{
		TS: time.Now(), Folder: id, Category: events.CatError,
		Severity: events.SevWarn, Reason: "deletion guard: " + msg,
	}); err != nil {
		n.Logf("events: deletion guard: %v", err)
	}
}

// clearDeletionGuard resolves a tripped guard's error event. Called after
// the operator applies the pending deletions (or when the peer's deletions
// are no longer pending).
func (n *Node) clearDeletionGuard(id string) {
	n.probMu.Lock()
	_, was := n.delGuard[id]
	delete(n.delGuard, id)
	n.probMu.Unlock()
	if was && n.Events != nil {
		_ = n.Events.ResolveCondition(id, "", events.CatError, "system")
	}
}

// ApplyPendingDeletions is the operator override for the deletion guard: it
// applies every currently-pending deletion for the folder locally and
// clears the guard. Returns how many files were removed.
func (n *Node) ApplyPendingDeletions(id string) (int, error) {
	f, ok := n.Folders[id]
	if !ok {
		return 0, fmt.Errorf("unknown folder %q", id)
	}
	dels, err := f.Engine.PendingDeletions()
	if err != nil {
		return 0, err
	}
	applied := 0
	for _, name := range dels {
		if err := f.Engine.ApplyDeletion(name, 0); err != nil {
			return applied, err
		}
		applied++
	}
	if applied > 0 {
		n.clearDeletionGuard(id)
	}
	return applied, nil
}

// RunAutoArchive prunes ROUTINE event history older than the configured
// auto-archive age. Open attention/conflict events (warn+ and not
// dismissed) are never touched — this is the includeOpen=false archive, so
// nothing needing attention is ever silently dropped. It is a no-op when
// the feature is disabled, and is safe to call on any cadence (the daemon
// calls it at startup and then daily).
func (n *Node) RunAutoArchive() (int, error) {
	if n.Events == nil {
		return 0, nil
	}
	enabled, olderThan := n.Cfg.AutoArchive()
	if !enabled {
		return 0, nil
	}
	cutoff := time.Now().Add(-time.Duration(olderThan) * time.Second)
	deleted, _, _, err := n.Events.Archive(cutoff, false)
	if err != nil {
		return 0, err
	}
	if deleted > 0 {
		n.Logf("events: auto-archive removed %d routine event(s) older than %s (attention/conflict kept)",
			deleted, (time.Duration(olderThan) * time.Second).String())
	}
	return deleted, nil
}

// CleanupPeerState drops all per-peer state for a removed peer: the
// persisted delta marker in each shared folder's index (so re-adding the
// peer cannot use a stale marker) and the in-memory peer view in each
// folder's engine (so it cannot leak).
func (n *Node) CleanupPeerState(peerID uint64) {
	// Drop the peer's advertised unsynced view first: it describes a
	// process we no longer trust to be there at all.
	n.clearPeerUnsynced(peerID)
	n.foldersMu.RLock()
	ids := make([]string, 0, len(n.Folders))
	for id := range n.Folders {
		ids = append(ids, id)
	}
	n.foldersMu.RUnlock()

	for _, id := range ids {
		f, ok := n.lookupFolder(id)
		if !ok {
			continue
		}
		f.mu.Lock()
		_ = f.Ix.RemovePeerIndex(peerID)
		f.Engine.CleanupPeerIndex(peerID)
		f.mu.Unlock()
	}
}

// CleanupStaleTemps removes orphaned staging temp files — leftovers of
// transfers that died with a previous process (crash, kill, restart mid-
// transfer). Only safe to call when no transfer is in flight (daemon
// startup): a live transfer needs its own temp for the duration of the
// write. Returns the total number of temp files removed.
func (n *Node) CleanupStaleTemps() (int, error) {
	n.foldersMu.RLock()
	ids := make([]string, 0, len(n.Folders))
	for id := range n.Folders {
		ids = append(ids, id)
	}
	n.foldersMu.RUnlock()

	total := 0
	for _, id := range ids {
		f, ok := n.lookupFolder(id)
		if !ok || f.Stager == nil {
			continue
		}
		f.mu.Lock()
		n, err := f.Stager.CleanupOrphans()
		f.mu.Unlock()
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// HeldFiles lists the folder's soft-deleted paths: copies that an
// over-threshold deletion parked in the hold area instead of unlinking.
func (n *Node) HeldFiles(id string) ([]hold.Entry, error) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return nil, fmt.Errorf("unknown folder %q", id)
	}
	if f.Hold == nil {
		return nil, nil
	}
	return f.Hold.List()
}

// RestoreHeld moves one held path back to its original location and rescans the
// folder, so the file becomes a normal local change again and re-syncs to every
// peer — including the device that originally deleted it.
func (n *Node) RestoreHeld(id, rel string) error {
	f, ok := n.lookupFolder(id)
	if !ok {
		return fmt.Errorf("unknown folder %q", id)
	}
	if f.Hold == nil {
		return fmt.Errorf("folder %q has no hold area", id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := n.lookupFolder(id); !ok || cur != f {
		return fmt.Errorf("unknown folder %q", id)
	}
	if err := f.Hold.Restore(rel); err != nil {
		return err
	}
	if n.Events != nil {
		n.recordEvent(&events.Event{
			TS: time.Now(), Folder: id, Path: rel, Category: events.CatApplied, Severity: events.SevInfo,
			Reason: "restored from held deletions (undo)",
		})
	}
	// Index it again so it propagates as a normal local change.
	if _, err := n.scanFolder(f); err != nil {
		return err
	}
	return nil
}

// DeleteHeld purges one held path for real (rel == "" purges every held path)
// and returns how many copies were removed. The deletions are not propagated:
// the paths are already deleted globally, this only gives up the undo window.
func (n *Node) DeleteHeld(id, rel string) (int, error) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return 0, fmt.Errorf("unknown folder %q", id)
	}
	if f.Hold == nil {
		return 0, fmt.Errorf("folder %q has no hold area", id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := n.lookupFolder(id); !ok || cur != f {
		return 0, fmt.Errorf("unknown folder %q", id)
	}
	nRemoved := 0
	var err error
	if rel == "" {
		nRemoved, err = f.Hold.DeleteAll()
	} else if err = f.Hold.Delete(rel); err == nil {
		nRemoved = 1
	}
	if err != nil {
		return nRemoved, err
	}
	if nRemoved > 0 && n.Events != nil {
		n.recordEvent(&events.Event{
			TS: time.Now(), Folder: id, Category: events.CatSystem, Severity: events.SevInfo,
			Reason: fmt.Sprintf("purged %d held deletion(s) (delete now — the undo window is closed)", nRemoved),
		})
	}
	return nRemoved, nil
}

// RunMaintenance prunes index state that would otherwise grow without bound
// over a long-running deployment: tombstones whose sequence every peer
// sharing the folder has already acknowledged are hard-deleted, and the
// database is VACUUMed so the file shrinks. The floor is derived from
// persisted peer markers, so a tombstone is only deleted once no peer can
// ever receive it through a delta — safe to call on any cadence (the daemon
// runs it at startup and then daily). Returns the total tombstones pruned.
func (n *Node) RunMaintenance() (int, error) {
	n.foldersMu.RLock()
	ids := make([]string, 0, len(n.Folders))
	for id := range n.Folders {
		ids = append(ids, id)
	}
	n.foldersMu.RUnlock()

	total := 0
	for _, id := range ids {
		f, ok := n.lookupFolder(id)
		if !ok {
			continue
		}
		f.mu.Lock()
		pruned, err := f.Ix.PruneTombstones(n.tombstoneFloor(f))
		f.mu.Unlock()
		if err != nil {
			return total, err
		}
		total += pruned
		if pruned > 0 {
			n.Logf("folder %s: pruned %d tombstone(s) acknowledged by all peers (db vacuumed)", id, pruned)
		}
		// Soft deletions expire: a copy held for the undo window is purged for
		// real once it is older than hold.MaxAge.
		if purged, herr := n.purgeHeld(id); herr != nil {
			n.Logf("folder %s: purge held deletions: %v", id, herr)
		} else if len(purged) > 0 {
			n.Logf("folder %s: purged %d held deletion(s) older than %s", id, len(purged), hold.MaxAge)
		}
	}
	return total, nil
}

// purgeHeld removes held deletions older than hold.MaxAge for one folder and
// records a durable event for the operator.
func (n *Node) purgeHeld(id string) ([]string, error) {
	f, ok := n.lookupFolder(id)
	if !ok || f.Hold == nil {
		return nil, nil
	}
	purged, err := f.Hold.Purge(hold.MaxAge)
	if err != nil {
		return purged, err
	}
	if len(purged) > 0 && n.Events != nil {
		n.recordEvent(&events.Event{
			TS: time.Now(), Folder: id, Category: events.CatSystem, Severity: events.SevInfo,
			Reason: fmt.Sprintf("purged %d held deletion(s) older than %s", len(purged), hold.MaxAge),
		})
	}
	return purged, nil
}

// tombstoneFloor returns the lowest delta-marker sequence among the
// configured peers sharing folder f, or 0 when nothing may be pruned: the
// folder shares no peers, or some sharing peer has no marker yet (its next
// session would still receive the full history).
func (n *Node) tombstoneFloor(f *Folder) int64 {
	var (
		floor int64
		seen  bool
	)
	for _, p := range n.Cfg.Peers {
		if !f.Cfg.AllowsPeer(p.ID) {
			continue
		}
		_, maxSeq, _, ok, err := f.Ix.GetPeerIndex(p.ID)
		if err != nil {
			continue
		}
		if !ok {
			return 0
		}
		if !seen || maxSeq < floor {
			floor = maxSeq
			seen = true
		}
	}
	return floor
}

// checkMassChange flags a folder when a single sync would change more than
// half of its live files after at least one prior completed session with
// the peer (the first sync is legitimately 100%). It is informational — a
// heads-up for rename/format shifts and migrations — and self-resolves once
// the change rate drops back below the threshold. Every branch releases
// probMu (a missed unlock here deadlocks every later clearProblem).
func (n *Node) checkMassChange(id string, changed, live int, peerID uint64) {
	above := live > 0 && changed > 0 && changed*100 >= live*50
	if above {
		// Only flag after a prior completed session with this peer: a fresh
		// folder's first sync always changes everything and is not suspicious.
		if f, ok := n.Folders[id]; ok {
			if _, _, _, hadPrior, err := f.Ix.GetPeerIndex(peerID); err == nil && hadPrior {
				n.probMu.Lock()
				if !n.massChange[id] {
					n.massChange[id] = true
					n.probMu.Unlock()
					n.recordEvent(&events.Event{
						TS: time.Now(), Folder: id, Category: events.CatWarning,
						Severity: events.SevWarn,
						Reason: fmt.Sprintf(
							"mass change: %d of %d files changed in one sync (>50%% after a prior sync) — could be a rename/format shift or a migration; verify the change is intended",
							changed, live),
					})
				} else {
					n.probMu.Unlock()
				}
				return
			}
		}
		return
	}
	// Below threshold: clear the flag if it was set.
	n.probMu.Lock()
	was := n.massChange[id]
	delete(n.massChange, id)
	n.probMu.Unlock()
	if was && n.Events != nil {
		_ = n.Events.ResolveCondition(id, "", events.CatWarning, "system")
	}
}

// checkCaseCollisions scans the live index for files that differ only by
// case within the same directory — SMB clients are case-insensitive and
// cannot hold both. It emits one durable warning event per colliding set
// (throttled: only when the set changes) and resolves the warning when the
// collision clears. It walks the whole live index, so it is only invoked
// after a scan that actually changed files.
func (n *Node) checkCaseCollisions(id string) {
	f, ok := n.lookupFolder(id)
	if !ok || n.Events == nil {
		return
	}
	// Group live file paths by their lowercased directory + basename.
	byKey := map[string][]string{}
	_ = f.Ix.List(func(fi *index.FileInfo) error {
		if fi.Deleted || strings.HasSuffix(fi.Name, sync.ConflictSuffix) {
			return nil
		}
		dir, base := path.Split(fi.Name)
		key := strings.ToLower(dir) + "\x00" + strings.ToLower(base)
		byKey[key] = append(byKey[key], fi.Name)
		return nil
	})
	seen := map[string]bool{}
	for key, names := range byKey {
		if len(names) < 2 {
			continue
		}
		sort.Strings(names)
		// Use the directory + colliding names as the condition path so each
		// collision is one distinct attention item.
		dir := path.Dir(key[:strings.IndexByte(key, '\x00')])
		if dir == "" {
			dir = "."
		}
		cond := dir
		reason := "case collision: " + strings.Join(names, " / ") + " differ only by case — SMB clients cannot hold both"
		ck := id + "\x00" + cond
		seen[ck] = true
		n.probMu.Lock()
		last := n.caseColl[ck]
		if last != reason {
			n.caseColl[ck] = reason
			n.probMu.Unlock()
			n.recordEvent(&events.Event{
				TS: time.Now(), Folder: id, Path: cond, Category: events.CatWarning,
				Severity: events.SevWarn, Reason: reason,
			})
		} else {
			n.probMu.Unlock()
		}
	}
	// Resolve warnings whose collision has cleared.
	n.probMu.Lock()
	for ck := range n.caseColl {
		if !strings.HasPrefix(ck, id+"\x00") {
			continue
		}
		if !seen[ck] {
			delete(n.caseColl, ck)
			cond := ck[len(id)+1:]
			n.probMu.Unlock()
			_ = n.Events.ResolveCondition(id, cond, events.CatWarning, "system")
			n.probMu.Lock()
		}
	}
	n.probMu.Unlock()
}

// ScanFolder scans one folder and applies the detected changes to the
// engine, returning the number of applied changes. Scans of different
// folders run concurrently (each folder is locked individually); scans of
// the same folder, and structural folder mutations (update/remove/rebuild/
// remove-conflict), are serialized per folder so a change is never applied
// twice and a scan never runs against a folder being swapped or closed.
// lookupFolder returns the live folder for id, or false. It is a brief read
// of the folder map.
func (n *Node) lookupFolder(id string) (*Folder, bool) {
	n.foldersMu.RLock()
	defer n.foldersMu.RUnlock()
	f, ok := n.Folders[id]
	return f, ok
}

func (n *Node) ScanFolder(id string) (int, error) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return 0, fmt.Errorf("unknown folder %q", id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// The folder may have been removed or rebuilt between the lookup and
	// acquiring its lock; re-verify so we never scan a closed index.
	if cur, ok := n.lookupFolder(id); !ok || cur != f {
		return 0, fmt.Errorf("unknown folder %q", id)
	}
	// An explicit rescan on a paused folder answers instead of silently doing
	// nothing, so the operator learns why (the periodic and watcher paths skip
	// paused folders without an event or a log line).
	if f.Paused() {
		return 0, ErrFolderPaused
	}
	return n.scanFolder(f)
}

// ErrFolderPaused is returned by explicit operations (rescan, sync-now) on a
// folder this device has paused. Periodic and watcher-driven paths skip paused
// folders silently instead — they must not log an error every tick.
var ErrFolderPaused = errors.New("folder is paused — resume it to scan or sync")

// SetFolderPaused pauses or resumes one folder on this device. Deliberately
// cheap: it never stats the path, re-opens the index or rebuilds the scanner,
// so pausing works when the share is offline or unmounted — which is the usual
// reason to pause in the first place.
func (n *Node) SetFolderPaused(id string, paused bool) error {
	f, ok := n.lookupFolder(id)
	if !ok {
		return fmt.Errorf("unknown folder %q", id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := n.lookupFolder(id); !ok || cur != f {
		return fmt.Errorf("unknown folder %q", id)
	}
	f.Cfg.Paused = paused
	f.paused.Store(paused)
	if paused {
		n.Logf("folder %s: paused (no scans, no sync with any peer)", id)
	} else {
		n.Logf("folder %s: resumed", id)
	}
	return nil
}

// Paused reports whether a folder is paused on this device.
func (n *Node) Paused(id string) bool {
	f, ok := n.lookupFolder(id)
	return ok && f.Paused()
}

// ScanFolderDeep runs a VERIFIED scan for one folder: every file is re-hashed
// instead of trusting (size, mtime). That catches content changes the fast path
// cannot see — rsync -a / restore tools that preserve timestamps, and
// filesystems with coarse timestamp granularity (SMB, exFAT). Files whose
// content is unchanged are adopted with their new stat and NO version bump, so
// a deep scan never re-transfers data to peers.
func (n *Node) ScanFolderDeep(id string) (int, error) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return 0, fmt.Errorf("unknown folder %q", id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := n.lookupFolder(id); !ok || cur != f {
		return 0, fmt.Errorf("unknown folder %q", id)
	}
	if f.Paused() {
		return 0, ErrFolderPaused
	}
	// A verified scan is a maintenance action: leave a trace of when it ran and
	// what it found, because it can hold the folder for a long time.
	n.Logf("folder %s: verified scan started (re-hashing every file; sync for this folder waits)", id)
	start := time.Now()
	f.Scanner.SetForceHash(true)
	applied, err := n.scanFolder(f)
	if err != nil {
		n.Logf("folder %s: verified scan failed after %s: %v", id, time.Since(start).Round(time.Second), err)
		return applied, err
	}
	n.Logf("folder %s: verified scan finished in %s (%d content change(s))", id, time.Since(start).Round(time.Second), applied)
	return applied, nil
}

func (n *Node) scanFolder(f *Folder) (applied int, err error) {
	id := f.Cfg.ID
	// A paused folder does not scan: no walking, no hashing, no index writes
	// and no events. Resuming picks up wherever the tree actually is.
	if f.Paused() {
		return 0, nil
	}
	n.startScan(id)
	defer func() {
		n.endScan(id, applied, err)
		if err != nil {
			n.reportFolderError(id, err)
		} else {
			n.clearFolderError(id)
		}
	}()
	changes, err := f.Scanner.ScanWithProgress(func(p scanner.Progress) {
		n.setScan(id, func(st *ScanStatus) {
			st.Phase = p.Phase
			st.Walked = p.Walked
			st.HashDone = p.HashDone
			st.HashTotal = p.HashTotal
			st.InFlight = p.InFlight
		})
	})
	if err != nil {
		return 0, err
	}
	applied = 0
	foundNewOrGone := false
	for _, c := range changes {
		if c.Kind != scanner.Unchanged {
			applied++
			if c.Kind == scanner.Added || c.Kind == scanner.Deleted || c.Kind == scanner.Renamed {
				foundNewOrGone = true
			}
		}
	}
	n.setScan(id, func(st *ScanStatus) {
		st.Phase = "applying"
		st.Changed = applied
	})
	// Apply all changes in one engine lock hold and one index transaction
	// (a single WAL fsync instead of one per file). A change that fails
	// verification is returned and re-applied per-change below: a stale
	// rename falls back to a normal delete+add so nothing is lost.
	if len(changes) > 0 {
		if _, failed, err := f.Engine.ApplyLocalChanges(changes); err != nil {
			return applied, err
		} else {
			for _, c := range failed {
				if c.Kind == scanner.Renamed {
					// Local rename: same content, new path. Apply it as a move;
					// if the plan is stale (source changed, target appeared),
					// fall back to a normal delete+add so nothing is lost.
					if _, err := f.Engine.ApplyLocalRename(c.From, c.Info); err == nil {
						// A rename that left this device's include gate needs
						// its pending release recorded here too (the batch
						// path inside ApplyLocalChanges does it itself).
						if c.OutOfGate {
							if err := f.Engine.RecordOutOfGateRename(c.From, c.Info.Name); err != nil {
								n.Logf("folder %s: record out-of-gate rename %s -> %s: %v", id, c.From, c.Info.Name, err)
							}
						}
						continue
					}
					if _, err := f.Engine.ApplyLocalChange(scanner.Deleted, &index.FileInfo{Name: c.From, Type: c.Info.Type}); err != nil {
						return applied, err
					}
					if _, err := f.Engine.ApplyLocalChange(scanner.Added, c.Info); err != nil {
						return applied, err
					}
					continue
				}
				if _, err := f.Engine.ApplyLocalChange(c.Kind, c.Info); err != nil {
					return applied, err
				}
			}
		}
	}
	// Case collisions can only newly appear (or clear) when files were
	// added or deleted, so only re-check then.
	if foundNewOrGone {
		n.checkCaseCollisions(id)
	}
	// Publish the stability gate's in-flight set. This must happen on EVERY
	// successful scan, including with an empty list, so a file that just
	// settled stops being gated (pulled/deleted/moved/served) immediately.
	inFlight := f.Scanner.LastInFlight()
	f.Engine.SetInFlight(inFlight)
	n.setScan(id, func(st *ScanStatus) { st.InFlight = len(inFlight) })
	n.noteHeldBack(id, inFlight)
	// A scan may have just recorded renames that moved content out of this
	// folder's include gate; release them if no peer still owns the old path
	// (with peers waiting for confirmation this is a no-op until a round
	// completes — see confirmGateReleases).
	n.confirmGateReleases(id)
	return applied, nil
}

// confirmGateReleases releases the local renames that moved content OUT of a
// folder's include gate once every peer that owned the old path has confirmed
// the move. Until then the paths stay tracked: nothing is deleted and no
// tombstone is broadcast, so a peer that is offline or mid-round can never be
// told to drop a file it has not received the replacement for. The release
// itself only drops this device's tracking rows — its files on disk stay
// exactly where the local rename put them.
func (n *Node) confirmGateReleases(id string) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return
	}
	if f.Engine.PendingReleases() == 0 {
		return
	}
	released, err := f.Engine.ConfirmReleases()
	if err != nil {
		n.Logf("folder %s: confirm out-of-gate releases: %v", id, err)
		return
	}
	if len(released) > 0 {
		n.Logf("folder %s: released %d path(s) renamed out of gate (content confirmed on peers)", id, len(released))
	}
}

// holding is one folder's stability-gate episode: the last non-zero in-flight
// count, and whether the episode's single summary event is still owed.
type holding struct {
	files    int
	announce bool
}

// noteHeldBack records ONE durable summary event per held-back episode: when a
// folder's in-flight count transitions from >0 to 0, the operator learns that
// N files were waiting for their copy to finish and are now syncing. There are
// deliberately no per-file events — a 100k-file paste would drown the event
// store, and every event write is an fsync.
func (n *Node) noteHeldBack(id string, inFlight []scanner.InFlight) {
	n.holdMu.Lock()
	h := n.holdings[id]
	if h == nil {
		h = &holding{}
		n.holdings[id] = h
	}
	var owed int
	switch {
	case len(inFlight) > 0:
		h.files = len(inFlight)
		h.announce = true
	case h.announce:
		// Episode over: announce what was held, then self-clear.
		owed = h.files
		h.files = 0
		h.announce = false
	}
	n.holdMu.Unlock()
	if owed == 0 || n.Events == nil {
		return
	}
	n.recordEvent(&events.Event{
		TS: time.Now(), Folder: id, Category: events.CatApplied, Severity: events.SevInfo,
		Reason: fmt.Sprintf("held back %d file(s) until they stopped changing; syncing now", owed),
	})
}

// UpdateFolder changes a folder's name and/or path in place. The folder id
// (the sync key shared with peers) never changes and the index is
// preserved; the scanner, stager, and engine are re-pointed at the new
// root. Callers should trigger a rescan afterwards so the index reflects
// the new root's contents (deletions of files absent from the new path
// still go through the deletion guard).
func (n *Node) UpdateFolder(id string, fc config.Folder) (*Folder, error) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return nil, fmt.Errorf("unknown folder %q", id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := n.lookupFolder(id); !ok || cur != f {
		return nil, fmt.Errorf("unknown folder %q", id)
	}
	st, err := os.Stat(fc.Path)
	if err != nil || !st.IsDir() {
		return nil, fmt.Errorf("folder %q: %w", fc.ID, err)
	}
	ig, err := ignore.Parse(fc.Ignore)
	if err != nil {
		return nil, err
	}
	var inc *ignore.Matcher
	if len(fc.Include) > 0 {
		inc, err = ignore.Parse(fc.Include)
		if err != nil {
			return nil, err
		}
	}
	stag, err := staging.New(fc.Path)
	if err != nil {
		return nil, err
	}
	f.Cfg = fc
	f.Root = fc.Path
	f.Stager = stag
	f.Hold = hold.New(fc.Path)
	f.Engine.Root = fc.Path
	f.Engine.Stager = stag
	f.Engine.Ignore = ig
	f.Engine.Include = inc
	f.Engine.Direction = fc.SyncDirection()
	f.Engine.HoldLocal = f.Hold.Move
	sc := scanner.New(fc.Path, f.Ix, ig)
	if inc != nil {
		sc.SetInclude(inc)
	}
	// UpdateFolder re-creates the scanner, so the gate wiring must be
	// repeated here or the folder would silently reset to the default.
	sc.SetSettleScans(fc.SettleThreshold())
	if fc.SyncDirection() == "receive-only" {
		sc.SetSettleScans(1) // gate off: see AddFolder
	}
	f.Scanner = sc
	return f, nil
}

// RemoveFolder closes and drops a folder from the node. The configuration
// (config file) is updated by the caller (control plane).
func (n *Node) RemoveFolder(id string) error {
	f, ok := n.lookupFolder(id)
	if !ok {
		return fmt.Errorf("unknown folder %q", id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// Remove from the map only if it is still the same folder object (a
	// concurrent rebuild may have replaced it).
	n.foldersMu.Lock()
	if n.Folders[id] == f {
		delete(n.Folders, id)
	}
	n.foldersMu.Unlock()
	_ = f.Ix.Close()
	return nil
}

// RemoveConflict deletes a conflict copy from disk and tombstones it in the
// index so the deletion propagates to all peers. Only files whose name
// contains the conflict suffix can be removed through this path, and the
// path is confined to the folder root (no traversal).
func (n *Node) RemoveConflict(folder, name string) error {
	f, ok := n.lookupFolder(folder)
	if !ok {
		return fmt.Errorf("unknown folder %q", folder)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := n.lookupFolder(folder); !ok || cur != f {
		return fmt.Errorf("unknown folder %q", folder)
	}
	if !strings.Contains(name, sync.ConflictSuffix) {
		return fmt.Errorf("not a conflict copy: %q", name)
	}
	abs := filepath.Join(f.Root, filepath.FromSlash(name))
	rel, err := filepath.Rel(f.Root, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path escapes folder root")
	}
	if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
		return err
	}
	if _, err := f.Engine.ApplyLocalChange(scanner.Deleted, &index.FileInfo{Name: name, Type: index.TypeFile}); err != nil {
		return err
	}
	// The conflict is settled (the winner is kept): clear open conflict
	// events for the copy and its original file so the badge/attention
	// stops flagging it.
	if n.Events != nil {
		orig := name[:strings.LastIndex(name, sync.ConflictSuffix)]
		_ = n.Events.ResolveCondition(folder, orig, events.CatConflict, "system")
		_ = n.Events.ResolveCondition(folder, name, events.CatConflict, "system")
	}
	return nil
}

// ScanAll scans every folder. Different folders scan concurrently (each is
// locked individually via ScanFolder); each folder is still scanned at most
// once per pass.
func (n *Node) ScanAll() error {
	n.foldersMu.RLock()
	ids := make([]string, 0, len(n.Folders))
	for id := range n.Folders {
		ids = append(ids, id)
	}
	n.foldersMu.RUnlock()
	for _, id := range ids {
		// Paused folders are skipped silently: their scans are stopped on
		// purpose, so they must not produce an error (or a log line) per tick.
		if n.Paused(id) {
			continue
		}
		if _, err := n.ScanFolder(id); err != nil {
			n.Logf("scan of folder %s failed: %v", id, err)
			continue
		}
	}
	return nil
}
