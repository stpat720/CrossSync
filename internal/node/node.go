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
	"crosssync/internal/lockorder"
	"crosssync/internal/protocol"
	"crosssync/internal/scanner"
	"crosssync/internal/staging"
	"crosssync/internal/sync"
	"crosssync/internal/testgen"
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

	// deletionsPending counts the follow-up walks already queued to confirm
	// deletions this folder's last pass only suspected. A mass delete (an
	// operator wiping a tree) needs a SECOND independent pass before the index
	// drops the entries, and the periodic safety-net walk can be up to ten
	// minutes away — so the counts on screen kept reporting files that were
	// already gone. Reset when the missing table empties.
	deletionsPending atomic.Int32
	// lastDeletionsPending is the size of the missing table when a confirmation
	// pass was last queued: a shrinking table is progress, and progress lets the
	// next pass be queued even after the per-burst cap.
	lastDeletionsPending atomic.Int32
	// driftMissing/driftUnknown are the last drift check's counts, published so
	// the status and metrics can show them without walking the tree again.
	driftMissing atomic.Int64
	driftUnknown atomic.Int64
	// freeBytes/versionFiles/versionBytes are the two SLOW disk figures the
	// status page needs: a statfs of the share and a walk of the reserved
	// .sfx-versions area. Both used to run per folder per /api/status request;
	// measured live on Tower that made the call take 5.8-8.3 s (NAS-02, with
	// the same folders on a faster array, answered the identical call in
	// 0.28 s, which is how the filesystem calls were identified as the cause).
	// The 5 s background ticker refreshes them (see RefreshDiskStats) and the
	// request path reads these atomics only: /api/status must never touch the
	// filesystem. Deliberately NOT under f.mu, which a scan can hold for
	// minutes.
	freeBytes    atomic.Int64
	versionFiles atomic.Int64
	versionBytes atomic.Int64
	// ixFiles/ixDirectories/ixTombstones/ixSize/ixConflictFiles/ixConflictBytes
	// are the folder's cached index aggregate (one SQL pass over the whole
	// index), and openEvents is its cached open warn+ event count. Both used to
	// be queried per folder per /api/status request, which — with the UI polling
	// every 2 s — was the remaining cost of that call once the disk figures were
	// cached (measured: GET /api/events?open=1 took 519 ms on Tower against
	// 164 ms on NAS-02, and the index aggregate ~83 ms). Refreshed by the same
	// 5 s background ticker (see RefreshStatusCounts) and after a scan; the
	// request path only loads them.
	ixFiles         atomic.Int64
	ixDirectories   atomic.Int64
	ixTombstones    atomic.Int64
	ixSize          atomic.Int64
	ixConflictFiles atomic.Int64
	ixConflictBytes atomic.Int64
	openEvents      atomic.Int64
	// followUpScheduled stops a pathological folder from queueing walk after
	// walk when something keeps re-suspending the same paths.
	followUpScheduled atomic.Bool

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
	// firstPassPending is true from daemon start until THIS folder's first full
	// scan has finished. Until then the folder's index is a PREFIX of the
	// share — empty if it was re-added under a new id, or filled only up to
	// where the walk got to — so it must not be published: advertisedFolders
	// and sharedFolders skip it (the peer sees the clean "no shared folders"
	// no-op rather than "I have nothing"), a manual rescan for it is a
	// duplicate walk, and /api/status says startup_scanning. Deliberately PER
	// FOLDER: a 57.8 GB folder being hashed must not delay a 700-file folder's
	// first pass, its advertisement, or its sync rounds. Armed by
	// Node.SetStartupScanning (the daemon's first walk), cleared by the folder's
	// own pass ending — never armed by Node.New, so a node that never walks
	// (CLI paths, tests) advertises as it always did. Atomic: read on every
	// session and status path.
	firstPassPending atomic.Bool
	// transferPaused stops transfers only: scanning continues, so the index
	// stays current and a resume needs no re-walk.
	transferPaused atomic.Bool
	// pullFsync mirrors Cfg.PullFsync (default ON) as an atomic: pulls read it
	// at COMMIT time, from the session goroutine, without taking f.mu — so a
	// PUT /api/folders/{id} takes effect on the next file pulled, including on a
	// folder that is being pulled while the setting changes.
	pullFsync atomic.Bool

	// pendMu guards pend: scan requests an operator made while the folder was
	// paused. They are NOT dropped (a verified rescan is expensive to ask for
	// and its whole point is the answer) and they are not run either: resume
	// takes them once.
	pendMu std.Mutex
	pend   pendingScan
}

// pendingScan is the set of scan requests waiting for a paused folder to be
// resumed. The kinds merge upward: a rebuild has to re-walk from scratch with
// the gate bypassed, which subsumes a verified pass, which subsumes a plain
// walk.
type pendingScan struct {
	full    bool // a plain full walk was asked for
	verify  bool // a verified (re-hash everything) pass was asked for
	rebuild bool // the index was rebuilt while paused: walk it with the gate off
}

// empty reports whether nothing is queued.
func (p pendingScan) empty() bool { return !p.full && !p.verify && !p.rebuild }

// merge folds one request into the pending set, keeping the strongest kind.
func (p *pendingScan) merge(verify, rebuild bool) {
	if rebuild {
		p.rebuild = true
		return
	}
	if verify {
		p.verify = true
		return
	}
	p.full = true
}

// addPending queues a scan request while the folder is paused.
func (f *Folder) addPending(verify, rebuild bool) {
	f.pendMu.Lock()
	f.pend.merge(verify, rebuild)
	f.pendMu.Unlock()
}

// takePending removes and returns the queued requests, so exactly one pass runs
// per queued request even when a resume and an incoming request race.
func (f *Folder) takePending() (pendingScan, bool) {
	f.pendMu.Lock()
	defer f.pendMu.Unlock()
	if f.pend.empty() {
		return pendingScan{}, false
	}
	p := f.pend
	f.pend = pendingScan{}
	return p, true
}

// Paused reports whether this folder is paused on this device.
func (f *Folder) Paused() bool { return f.paused.Load() }

// TransfersPaused reports whether this folder may move data right now.
func (f *Folder) TransfersPaused() bool {
	return f.transferPaused.Load() || f.paused.Load()
}

// PullFsyncOn reports whether this folder's pulls fsync a file before renaming
// it into place (`pull_fsync`, default ON). Read per commit: the engine calls it
// through this method value, so a change lands on the next pulled file.
func (f *Folder) PullFsyncOn() bool { return f.pullFsync.Load() }

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

	// openEvents is the CACHED global count of open warn+ events (the
	// top-level open_events field). The live count is a SQL aggregate and
	// /api/status is polled every couple of seconds; the 5 s background
	// ticker refreshes this (RefreshStatusCounts) and the request path only
	// loads it.
	openEvents atomic.Int64

	// pullK is the effective pull concurrency (K) the next pull batch will
	// use: files pulled at once over one peer connection. It is an atomic so
	// PUT /api/settings/sync can change it on a running daemon without a
	// restart — the boxes have no shell — and each batch samples it ONCE when
	// it is built, so a change never disturbs a batch already in flight. 0
	// means "not set": fall back to config.PullFiles() (see PullConcurrency).
	pullK atomic.Int64

	// hashW is the effective hash reader pool the NEXT scan will use: how many
	// files the index hasher reads at once. Same contract as pullK (an atomic so
	// PUT /api/settings/sync can change it on a running daemon; each pass samples
	// it once, when its worker pool is built, so a change never disturbs a pass
	// in flight). 0 means "not set": fall back to
	// Cfg.Scan.HashWorkersOrDefault() (see HashWorkers).
	hashW atomic.Int64

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
	// serveCache keeps recently served source files open between block
	// requests: serveBlock used to open, read and close per block, which on
	// shfs (one FUSE round trip per open) capped a pull at ~2 MB/s. See
	// serve_cache.go.
	serveCache *serveHandleCache

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

	// scanAbort is the shutdown stop for scans in progress. A cancelled pass
	// applies NOTHING (see cancelledPass), which is what keeps the index
	// consistent when the process goes away mid-walk; a startup walk of a
	// large share is hours of hashing and must not be waited out or raced by
	// Close. Set once, at shutdown: it is deliberately never cleared.
	scanAbort atomic.Bool

	// TLS identity: populated when config.TLS is set.
	TLS       bool
	Certs     *certs.Manager
	ClientTLS *tls.Config
	ServerTLS *tls.Config

	// peerAllowed holds the pinned peer fingerprints, shared by reference
	// with the tls.Config VerifyPeerCertificate closures, so ApplyPeers can
	// update the allowlist live without rebuilding TLS state.
	peerAllowed map[string]bool

	// sessionSems holds one 1-slot channel per (peer, folder) so only ONE sync
	// round runs on a given folder with a given peer at a time, inbound or
	// outbound. Both sides staging the same paths to the same deterministic
	// .part temp is the confirmed cause of the "chtimes
	// <root>/.sfx-tmp/.sfx-*.part: no such file or directory" churn: the winner
	// renames the temp into place, the loser's commit finds it gone.
	//
	// The key is per FOLDER, not per peer: a session carries exactly one folder,
	// so two folders of one peer are two independent sessions with their own
	// connection and pipeline, and neither waits for the other. ConnManager's
	// inFlight guard only covers OUTBOUND entry points (scheduler tick, manual
	// sync, ping); inbound sessions arrive through Run->Serve ungated, so this
	// is the slot that covers both directions.
	//
	// Entries are never deleted: one small channel per (peer, folder) ever seen
	// is bounded by the configured peer set times the configured folder set.
	sessionMu   std.Mutex
	sessionSems map[sessionSlot]chan struct{}

	// sessionLive counts the sessions running with each peer, so the
	// max_folder_sessions_per_peer cap (folderK) can be enforced across
	// folders. freed is closed and replaced on every release, which is how an
	// inbound session waiting for capacity wakes up without polling.
	sessionLive map[uint64]*peerSessions

	// folderK is the cap on concurrent (peer, folder) sessions per peer the next
	// scheduling decision will use. Same contract as pullK: an atomic so
	// PUT /api/settings/sync can change it on a running daemon, and each
	// decision samples it once, so a change never disturbs a live session. 0
	// means "not set": fall back to config.FolderSessionsPerPeer() (see
	// FolderSessions).
	folderK atomic.Int64

	// Folder watchers (see watchers.go). watchArmed is false until
	// StartFolderWatchers runs, so a node that is only being CONSTRUCTED (config
	// load, CLI commands, tests) never opens an inotify instance; a folder added
	// while armed starts its own watcher through AddFolder.
	//
	// Lock order: f.mu -> watchMu -> foldersMu. watchers.go takes watchMu and,
	// inside it, foldersMu (to check the folder still exists); nothing here ever
	// takes watchMu while holding foldersMu.
	watchMu    std.Mutex
	watchArmed bool
	watchers   map[string]*folderWatch
	watchKick  func()
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
		serveCache:    newServeHandleCache(),
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
	n.applyShareAttrs(stag, fc)
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
	eng.OnEvents = n.recordEvents
	n.wireLockOrder(eng)
	eng.Include = inc
	eng.Direction = fc.SyncDirection()
	sc := scanner.New(fc.Path, ix, ig)
	if inc != nil {
		sc.SetInclude(inc)
	}
	// The hash reader pool is a live value (scan.hash_workers, changeable at
	// runtime): the scanner samples it once per pass.
	sc.SetWorkerSource(n.HashWorkers)
	sc.SetSettleScans(fc.SettleThreshold())
	sc.SetSettleSeconds(fc.SettleQuietSeconds())
	sc.SetDeletionScans(fc.DeleteConfirmThreshold())
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
	f.transferPaused.Store(fc.TransferPaused)
	f.pullFsync.Store(fc.PullFsyncOn())
	// The engine asks the LIVE folder at every pull commit (see
	// Engine.PullFsync): a knob changed later applies without rebuilding the
	// folder, and the default (absent key) is ON, never off.
	eng.PullFsync = f.PullFsyncOn
	// A pause must stop a scan that is already running: the folder lock is held
	// for the whole pass, so without this probe a pause request would wait (and
	// time out) behind minutes of hashing while the disk kept being read. The
	// same probe carries the shutdown abort, so a stop during the startup walk
	// abandons the pass instead of being raced by Close.
	sc.SetCancel(n.scanCancelProbe(f))
	eng.HoldLocal = f.Hold.Move
	n.foldersMu.Lock()
	n.Folders[fc.ID] = f
	n.foldersMu.Unlock()
	// A folder added at RUNTIME gets a watcher here as well as at boot (see
	// watchers.go): without it the folder's changes are only caught by the
	// safety walk, and a change on a share other machines write to over SMB is
	// exactly what the watcher is for. Watchers are an optimisation, so a
	// watcher that cannot start is logged here and the folder still works.
	// Outside foldersMu: starting one walks the tree. ASYNCHRONOUSLY, and that
	// is load-bearing rather than tidiness: registering watches over every
	// directory of a large share takes minutes, and this function runs inside
	// the control plane's add-folder request. Blocking here left the folder
	// live in the index and already scanning while the request never returned,
	// so the caller never reached its append to Cfg.Folders or the config
	// persist: the folder worked until the next restart and then vanished with
	// its index orphaned (measured on a 400k+ file share, where the add request
	// hung past a 60 s client timeout and the config never gained the entry).
	go func(id string) {
		start := n.StartFolderWatcher
		if folderWatcherStart != nil { // test seam: see watchers.go
			start = func(string) error { return folderWatcherStart(n, id) }
		}
		if err := start(id); err != nil {
			n.Logf("watcher: %s: %v", id, err)
		}
	}(fc.ID)
	// Prime the cached disk figures now: /api/status reads them and must never
	// run the statfs / versions walk itself, so a folder that was just added
	// would otherwise report 0 free bytes / 0 versions until the next tick.
	f.refreshDiskStats()
	// Same for the cached index aggregate and open-event counts (see
	// RefreshStatusCounts): the status page reads atomics only.
	n.refreshFolderStatusCounts(f)
	if rebuilt {
		// A paused folder must not read the tree either — the index stays
		// empty (and unadvertised) until the resume, which then runs the
		// rebuild. Falling through here would re-walk every byte of a share the
		// operator has just parked.
		if f.Paused() {
			f.addPending(false, true)
			n.Logf("folder %s: index was corrupt — rebuild deferred until the folder is resumed", fc.ID)
		} else {
			// Rebuild under the new folder's lock so a concurrent scan of it
			// (e.g. a control-plane rescan) waits for the fresh index.
			f.mu.Lock()
			_, err := n.rebuildFromFS(f, "index was corrupt — rebuilt from filesystem")
			f.mu.Unlock()
			if err != nil {
				return f, fmt.Errorf("folder %q: rebuild from filesystem: %w", fc.ID, err)
			}
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
	// A rebuild writes a FRESH index from an empty one, which is exactly the
	// pass that can run for tens of minutes over a large share: commit its
	// hashed entries in bounded batches too, so a crash or a restart during a
	// rebuild resumes instead of re-hashing the whole tree. The index is empty
	// and the settling table is empty, so there is nothing to pair or gate
	// against — the sink's usual "building an index" rule holds by
	// construction.
	f.Scanner.SetBatchSink(func(batch []scanner.Change) error {
		return n.commitScanBatch(f, batch)
	})
	changes, err := f.Scanner.Scan()
	f.Scanner.SetBatchSink(nil)
	f.Scanner.SetGateBypass(false)
	if err != nil {
		return 0, err
	}
	applied := 0
	for _, c := range changes {
		if c.Kind == scanner.Unchanged {
			continue
		}
		// Entries the batch sink already committed are in the index: applying
		// them again would bump the version of content that never changed.
		if c.Committed {
			applied++
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
	if nf.Paused() {
		// A paused folder reads no disk here either: the (now empty) index is
		// not advertised while the folder is paused, so nothing can see the
		// gap — the rebuild runs on the resume.
		nf.addPending(false, true)
		n.Logf("folder %s: rebuild requested while paused — deferred until the folder is resumed", id)
		return 0, ErrFolderPaused
	}
	return n.rebuildFromFS(nf, "index rebuilt manually from filesystem")
}

// Close closes all folder indexes and the event store.
func (n *Node) Close() error {
	// Watchers first: their consumer goroutines scan, and a scan of an index
	// closed below is an error rather than a shutdown.
	n.CloseWatchers()
	for _, f := range n.Folders {
		_ = f.Ix.Close()
		if f.Hold != nil {
			// A shutdown in the middle of a held-deletion batch: the copies are
			// already in the hold area, but the manifest write was deferred.
			_ = f.Hold.Flush()
		}
	}
	n.closeServeHandles()
	if n.Events != nil {
		_ = n.Events.Close()
		n.Events = nil
	}
	return nil
}

// wireLockOrder declares the one lock-order rule this daemon depends on: no
// goroutine may hold a folder's ENGINE lock and the durable EVENT STORE's lock
// at the same time, in either order.
//
// Both sides of that rule are now structural — the engine queues the events it
// wants to record and delivers them after releasing its lock (Engine.emit,
// Engine.flushEventQueue), and the event store has no engine dependency at all
// (control/connmgr read events OUTSIDE any engine lock) — but the rule is
// invisible at the call sites, so it is declared here and checked at runtime.
// A violation is counted and logged once per pair, naming the goroutine and
// both locks: the live wedge it comes from was diagnosed from a goroutine dump
// (engine lock held inside the event callback, 8 hash workers parked on the
// scan commit, a query holding the event store's lock) only after it had frozen
// a first index pass for 180 s and cost a restart.
func (n *Node) wireLockOrder(eng *sync.Engine) {
	if eng == nil || n.Events == nil {
		return
	}
	eg, ev := eng.LockGuard(), n.Events.LockGuard()
	lockorder.Pair(eg, ev)
	report := func(msg string) { n.Logf("warning: %s", msg) }
	eg.OnViolation(report)
	ev.OnViolation(report)
}

// recordEvent writes one event to the durable store. Safe to call from any
// engine or session goroutine — and it is called from the engine's after-unlock
// event flush, never from under the engine lock (see Engine.emit).
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

// recordEvents writes a whole flush of events in ONE durable transaction
// (events.Store.RecordBatch) and then runs the per-event side effects (alert
// routing, SSE broadcast) that are not part of the store.
//
// This is the engine's OnEvents handler: the scan commit path emits one event
// per applied file, and writing each in its own SQLite transaction made the
// commit write rate the throughput ceiling of the whole hash pass (live: 0.5
// files/s, 9.0 MB/s, all 8 hash workers parked on the scan commit while the
// event store was written one row at a time). The same rows, one transaction.
//
// Like recordEvent it must never be called under the engine lock.
func (n *Node) recordEvents(evs []*events.Event) {
	if n.Events == nil || len(evs) == 0 {
		return
	}
	if err := n.Events.RecordBatch(evs); err != nil {
		n.Logf("events: record %d: %v", len(evs), err)
	}
	for _, e := range evs {
		n.maybeAlert(e)
		n.broadcast(e)
	}
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
	// Cheap guard: a healthy pair completes a round every few seconds, and
	// without this every round would issue an UPDATE that matches nothing.
	open, err := n.Events.HasOpenCondition("peers", name, events.CatPeer)
	if err != nil || !open {
		if err != nil {
			n.Logf("events: peer offline check: %v", err)
		}
		return
	}
	if err := n.Events.ResolveAutoCondition("peers", name, events.CatPeer, "system"); err != nil {
		n.Logf("events: resolve peer offline: %v", err)
	}
}

// FreeBytes reports the free space of the volume that holds the folder root.
//
// It reads the CACHED figure (see Folder.freeBytes and RefreshDiskStats): a
// statfs is a blocking call on a spinning array and /api/status polls every
// couple of seconds, so the request path must not run one per folder per
// request. The value is at most one refresh interval (~5 s) old.
func (n *Node) FreeBytes(id string) int64 {
	if f, ok := n.lookupFolder(id); ok {
		return f.freeBytes.Load()
	}
	return 0
}

// VersionStats reports the cached number/size of archived versions in the
// folder's .sfx-versions area. Cached for the same reason as FreeBytes: the
// live answer is a filepath.WalkDir per folder per status request.
func (n *Node) VersionStats(id string) (files int, bytes int64) {
	if f, ok := n.lookupFolder(id); ok {
		return int(f.versionFiles.Load()), f.versionBytes.Load()
	}
	return 0, 0
}

// GenerateTestData runs the convergence test-data generator against one folder.
// It holds the folder lock for the whole write so a scan cannot index a
// half-built corpus, and records what it did (the endpoint is a testing tool and
// must be visible in the history).
func (n *Node) GenerateTestData(id string, spec testgen.Spec) (testgen.Result, error) {
	var res testgen.Result
	f, ok := n.lookupFolder(id)
	if !ok {
		return res, fmt.Errorf("unknown folder %q", id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := n.lookupFolder(id); !ok || cur != f {
		return res, fmt.Errorf("unknown folder %q", id)
	}
	res, err := testgen.Run(f.Root, spec)
	if err != nil {
		return res, err
	}
	n.Record(&events.Event{
		Category: events.CatWarning,
		Severity: events.SevInfo,
		Folder:   id,
		Path:     spec.Dir,
		Reason: fmt.Sprintf("test data: %s (%d files, %d bytes) — generated by the convergence test endpoint",
			res.Kind, res.Files, res.Bytes),
	})
	return res, nil
}

// settleMassChange clears a mass-change heads-up once its sync has finished.
// The event exists to say "a lot changed, verify it was intended"; leaving the
// warning open after the backlog drained kept a warn badge (and the open-event
// count) on a healthy, fully converged folder.
func (n *Node) settleMassChange(id string) {
	if n.Events == nil {
		return
	}
	n.probMu.Lock()
	was := n.massChange[id]
	delete(n.massChange, id)
	n.probMu.Unlock()
	if !was {
		return
	}
	if _, err := n.Events.ResolveAutoReasonPrefix(id, events.CatWarning, "mass change:", "system"); err != nil {
		n.Logf("events: resolve mass change: %v", err)
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
	case strings.Contains(s, "peer sent nothing"):
		// The round's progress watchdog already worded this for an operator
		// (how long, and the likely cause: the peer stalled reading the file).
		// Return it as written instead of burying it under "cannot apply:".
		return err.Error()
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
		// A folder still taking its FIRST pass since start has not had the
		// chance to complete a round: it is being indexed, not stalled. Judging
		// it would report this node degraded for as long as the walk takes.
		if f.firstPassPending.Load() {
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
//
// humanBytes renders a byte count for an operator-facing message.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// holdWarnBytes is when the soft-deletion hold area is worth an alarm. It is
// not a limit and nothing is deleted for it: the hold area is the undo window,
// and purging it to satisfy a threshold would destroy the very protection it
// exists for. 5 GiB, not 20: the case that motivated this was 7 GiB of held
// copies, which a 20 GiB default would have stayed silent about. A folder can
// override it with hold_warn_bytes.
const holdWarnBytes int64 = 5 << 30

// warnIfHoldIsLarge raises one warning when a folder is holding more bytes
// than the operator expects. Measured here: 22,739 copies / 7.0 GB sat in the
// hold area with nothing anywhere saying so, after a folder wipe.
func (n *Node) warnIfHoldIsLarge(f *Folder) {
	if f.Hold == nil || n.Events == nil {
		return
	}
	limit := holdWarnBytes
	if f.Cfg.HoldWarnBytes > 0 {
		limit = f.Cfg.HoldWarnBytes
	}
	if limit <= 0 {
		return
	}
	entries, err := f.Hold.List()
	if err != nil {
		return
	}
	var total int64
	for _, e := range entries {
		total += e.Size
	}
	if total < limit {
		return
	}
	fraction := float64(total) / float64(limit)
	n.recordEvent(&events.Event{
		TS: time.Now(), Folder: f.Cfg.ID, Category: events.CatSystem, Severity: events.SevWarn,
		Reason: fmt.Sprintf("hold area is holding %d copies (%s), %.1fx the %s warning threshold: they expire after %s, or clear them from the folder menu",
			len(entries), humanBytes(total), fraction, humanBytes(limit), hold.MaxAge),
	})
}

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
		n.warnIfHoldIsLarge(f)
		// The scan reconciles the index and the disk, so they should never
		// disagree; this is the check that says so out loud, and it is the
		// only place that notices a disagreement that a scan cannot fix.
		if rep, derr := n.CheckDrift(id); derr != nil {
			n.Logf("folder %s: drift check: %v", id, derr)
		} else {
			n.recordDrift(rep)
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

// ScanFolderPaths scans only the given relative paths of one folder: the cheap
// path the filesystem watcher and the held-back re-check use. Same locking,
// paused handling and change application as ScanFolder, but the walk covers just
// those paths, and the scanner guarantees it cannot delete or unsettle anything
// outside them. Returns the number of applied changes.
func (n *Node) ScanFolderPaths(id string, rels []string) (int, error) {
	if len(rels) == 0 {
		return 0, nil
	}
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
	return n.scanFolderScoped(f, rels)
}

// FolderScanInfo reports the entry count and duration of the folder's last
// COMPLETED scan (0, 0 when it has not finished one yet). Used by /metrics so
// walk cost is visible per folder — walk cost, not file count, is what decides
// the safety-net schedule.
func (n *Node) FolderScanInfo(id string) (entries int, seconds int) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, ok := n.act.scans[id]
	if !ok || st == nil || st.Finished <= st.Started {
		return 0, 0
	}
	return st.Walked, int(st.Finished - st.Started)
}

// FolderIgnored reports how much of the share the last full scan deliberately
// did not index (FUSE/SMB bookkeeping names and CrossSync's own .sfx-* areas).
// Those bytes occupy the disk without being data; they are the silent half of
// the footprint, and 1.8 GB of them once sat there unnoticed.
func (n *Node) FolderIgnored(id string) (files int, bytes int64, ok bool) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, found := n.act.scans[id]
	if !found || st == nil || st.Finished == 0 {
		return 0, 0, false
	}
	return st.IgnoredFiles, st.IgnoredBytes, true
}

// FolderScanAge reports how many seconds ago the folder's last completed scan
// finished, and whether there has been one. The file/folder/byte counts are a
// statement about that observation — not about "now" — so their freshness has to
// be observable. On a share that other machines write to over SMB the local
// watcher sees nothing, so this age is the only honest bound on how far the
// numbers can lag the disk: it is the walk cadence (scan.full_interval /
// scan.full_max, 3m/15m by default), not a display refresh.
func (n *Node) FolderScanAge(id string) (seconds int, ok bool) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, found := n.act.scans[id]
	if !found || st == nil || st.Finished == 0 {
		return 0, false
	}
	age := time.Now().Unix() - st.Finished
	if age < 0 {
		age = 0
	}
	return int(age), true
}

// ScanHeldBack re-observes the files the stability gate is holding (the copies
// that were still being written on the last scan). It is what makes a finished
// copy sync promptly: the quiet window is time-based, so the file becomes
// eligible on the first pass after the window elapses — without waiting for a
// full walk. Cheap: the held set is a handful of paths.
func (n *Node) ScanHeldBack() int {
	applied := 0
	n.foldersMu.RLock()
	ids := make([]string, 0, len(n.Folders))
	folders := make([]*Folder, 0, len(n.Folders))
	for id, f := range n.Folders {
		ids = append(ids, id)
		folders = append(folders, f)
	}
	n.foldersMu.RUnlock()
	for i, id := range ids {
		f := folders[i]
		if f.Paused() || f.Scanner == nil {
			continue
		}
		held := f.Scanner.LastInFlight()
		if len(held) == 0 {
			continue
		}
		paths := make([]string, 0, len(held))
		for _, h := range held {
			paths = append(paths, h.Name)
		}
		got, err := n.ScanFolderPaths(id, paths)
		if err != nil && !errors.Is(err, ErrFolderPaused) && !errors.Is(err, ErrScanCancelled) {
			n.Logf("held-back re-check %s: %v", id, err)
		}
		applied += got
	}
	return applied
}

// ErrFolderPaused is returned by explicit operations (rescan, sync-now) on a
// folder this device has paused. Periodic and watcher-driven paths skip paused
// folders silently instead — they must not log an error every tick.
var ErrFolderPaused = errors.New("folder is paused — resume it to scan or sync")

// ErrScanCancelled reports that a scan pass was abandoned because the folder was
// paused while it ran. NOTHING was applied: the index, the settling table and
// the deletion table are exactly as they were, because a partial walk applied to
// the index looks like mass deletion.
//
// The distinction matters to callers: a cancelled pass is not "this folder has
// no changes", so a queued rescan or a verify request must not be treated as
// answered by it.
var ErrScanCancelled = scanner.ErrScanCancelled

// ErrScanBusy means a scan for the folder cannot start right now because one is
// already running — including the daemon's startup index walk, which covers
// every folder. It is not a failure: the pass in flight is doing the work. The
// control plane maps it to 409 (or a 202 "already running" answer) instead of an
// error the UI would show as a crash.
var ErrScanBusy = errors.New("scan already in progress")

// scanCancelProbe is the scanner's cancellation probe for one folder: a pause
// the operator asked for, or the daemon shutting down mid-pass. Both abandon the
// pass without applying anything (see cancelledPass).
func (n *Node) scanCancelProbe(f *Folder) func() bool {
	return func() bool { return f.Paused() || n.scanAbort.Load() }
}

// SetStartupScanning arms (true) or clears (false) the per-folder "first index
// pass since start is still outstanding" mark. cmdServe arms it immediately
// before the startup walk and does not need to clear it: each folder clears its
// own mark when its own pass ends (see scanFolderScoped), which is what lets a
// finished folder be advertised and synced while a slower one is still hashing.
func (n *Node) SetStartupScanning(on bool) {
	n.foldersMu.RLock()
	folders := make([]*Folder, 0, len(n.Folders))
	for _, f := range n.Folders {
		folders = append(folders, f)
	}
	n.foldersMu.RUnlock()
	for _, f := range folders {
		f.firstPassPending.Store(on)
	}
}

// StartupScanning reports whether ANY folder's first index pass is still
// outstanding — the state /api/status publishes as startup_scanning and
// /healthz reports as "indexing". Paused folders do not count: they are not
// scanned, not advertised and not expected to converge. Per-folder atomics only,
// no I/O and no folder lock, so the request paths stay cheap.
func (n *Node) StartupScanning() bool {
	n.foldersMu.RLock()
	defer n.foldersMu.RUnlock()
	for _, f := range n.Folders {
		if f.firstPassPending.Load() && !f.Paused() {
			return true
		}
	}
	return false
}

// FolderIndexing reports whether this folder's first pass since the daemon
// started is still queued or running: its index is incomplete, so it is not
// advertised to peers and a manual scan for it would be a duplicate walk over
// the same tree. Lock-free, apart from the brief folder-map read.
func (n *Node) FolderIndexing(id string) bool {
	f, ok := n.lookupFolder(id)
	return ok && f.firstPassPending.Load()
}

// AbortScans cancels scans in progress, for shutdown only: it is never cleared
// because the process is going away. A cancelled pass applies nothing, so the
// index is left exactly as the last completed pass left it.
func (n *Node) AbortScans() { n.scanAbort.Store(true) }

// cancelledPass ends an abandoned pass: the live status says "cancelled" (a
// distinct outcome from finished and from failed), nothing UNCOMMITTED is
// applied, and the caller gets ErrScanCancelled so it can tell the operator the
// pass was interrupted rather than empty.
//
// Entries the pass had already banked in batches are real, durable index state
// and stay (that is what makes the next pass resume instead of re-hashing); the
// status reports how many under `committed`, and the log line says so.
func (n *Node) cancelledPass(f *Folder) (int, error) {
	n.setScan(f.Cfg.ID, func(st *ScanStatus) {
		st.Phase = "cancelled"
		st.Error = ""
	})
	why := "folder paused"
	if n.scanAbort.Load() {
		why = "daemon stopping"
	}
	if kept := n.scanSnapshot(f.Cfg.ID).Committed; kept > 0 {
		n.Logf("folder %s: scan cancelled (%s); %d entr(y/ies) already committed in batches are kept, nothing else applied — the next pass resumes from the index",
			f.Cfg.ID, why, kept)
	} else {
		n.Logf("folder %s: scan cancelled (%s); nothing applied", f.Cfg.ID, why)
	}
	return 0, ErrScanCancelled
}

// commitScanBatch applies ONE bounded batch of hashed changes to the index while
// the pass that produced them is still running, and it is the whole point of a
// resumable first pass: rather than holding a million hashed entries in memory
// and committing once at the end (so a restart threw all of it away and the next
// pass re-walked and re-hashed the share from zero), each batch is applied
// through the same engine path the end-of-pass apply uses — a local add of a new
// file, verified and versioned once — and committed by Ix.PutBatch as one
// transaction. A batch is therefore all-or-nothing: the process can die between
// batches and the index holds a whole number of them.
//
// The scanner never calls this after the pass was cancelled, so a pause cannot
// bank a half-built batch; and the entries already committed survive a cancel or
// a crash by design.
func (n *Node) commitScanBatch(f *Folder, batch []scanner.Change) error {
	if len(batch) == 0 {
		return nil
	}
	applied, failed, err := f.Engine.ApplyLocalChanges(batch)
	if err != nil {
		// A failing index write aborts the pass (the scanner stops hashing);
		// the batches already committed stay, so the next pass resumes from
		// them rather than from zero.
		return err
	}
	if len(failed) > 0 {
		// Per-file failure isolation: a change that fails verification is not
		// applied and not counted. It stays unindexed, so the next pass sees it
		// as new and re-hashes it — nothing is lost and nothing is half-applied.
		n.Logf("folder %s: batch commit skipped %d change(s) that failed verification (left unindexed for the next pass)",
			f.Cfg.ID, len(failed))
	}
	n.setScan(f.Cfg.ID, func(st *ScanStatus) {
		st.Committed += int64(applied)
		st.Batches++
	})
	// Publish the committed entries to the cached status figures without a table
	// scan: the batch already knows what it wrote, and the 5 s aggregate is
	// refused while a pass runs (refreshFolderStatusCounts). This is what keeps a
	// folder row's count and size climbing during a first pass instead of
	// freezing until the pass ends.
	var written int64
	bad := make(map[string]struct{}, len(failed))
	for _, c := range failed {
		if c.Info != nil {
			bad[c.Info.Name] = struct{}{}
		}
	}
	for _, c := range batch {
		if c.Info == nil {
			continue
		}
		if _, skip := bad[c.Info.Name]; skip {
			continue
		}
		written += c.Info.Size
	}
	f.ixFiles.Add(int64(applied))
	f.ixSize.Add(written)
	return nil
}

// RequestRescan is the operator-facing rescan: run one now, or — when the folder
// is paused — remember it and run it exactly once on resume. It is the only
// queueing entry point on purpose. The internal scan paths (ticker, watcher,
// held-back re-check) must never queue, or every periodic tick that lands during
// a pause would arm an extra walk for later.
func (n *Node) RequestRescan(id string, verify bool) (queued bool, applied int, err error) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return false, 0, fmt.Errorf("unknown folder %q", id)
	}
	if f.Paused() {
		// Remembered, not dropped, and checked BEFORE "busy": the pass that is
		// still unwinding was cancelled by this same pause, so an operator
		// asking again means "on the next resume", not "try later".
		f.addPending(verify, false)
		n.Logf("folder %s: rescan queued (folder paused) — it will run once when the folder is resumed", id)
		return true, 0, nil
	}
	// A concurrent pass is a busy answer, not a queued request: nothing is
	// remembered, so a caller that retries does not accumulate walks.
	//
	// The startup walk counts as a concurrent pass FOR THIS FOLDER only: every
	// folder's first pass is queued at start, so a manual pass for one still
	// waiting would hash the same tree twice on the same disk. Other folders are
	// unaffected — an already-indexed folder can be rescanned and synced while a
	// different folder is still being hashed.
	if f.firstPassPending.Load() {
		return false, 0, fmt.Errorf("%w: this folder's first index pass since start is still running or queued", ErrScanBusy)
	}
	if n.ScanRunning(id) {
		return false, 0, fmt.Errorf("%w: scan already in progress for folder %q", ErrScanBusy, id)
	}
	// Absorb whatever a resume queued a moment ago, so this call serves it and
	// the folder does not run two passes back to back for one request.
	rebuild := false
	if p, had := f.takePending(); had {
		verify = verify || p.verify
		rebuild = p.rebuild
	}
	if rebuild {
		err = n.runRebuild(f)
	} else if verify {
		applied, err = n.ScanFolderDeep(id)
	} else {
		applied, err = n.ScanFolder(id)
	}
	if errors.Is(err, ErrScanCancelled) || errors.Is(err, ErrFolderPaused) {
		// Paused while this pass ran: the request is still owed, so it goes to
		// the resume. Answering "cancelled" here would look like "0 changes",
		// which is the whole failure this queue exists to prevent.
		f.addPending(verify, rebuild)
		return true, 0, nil
	}
	return false, applied, err
}

// runPendingScan runs the pass a resume inherited from a paused folder.
func (n *Node) runPendingScan(id string, p pendingScan) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return
	}
	if p.rebuild {
		if err := n.runRebuild(f); err != nil && !errors.Is(err, ErrFolderPaused) {
			n.Logf("folder %s: rebuild after resume: %v", id, err)
		}
		return
	}
	var err error
	if p.verify {
		_, err = n.ScanFolderDeep(id)
	} else {
		_, err = n.ScanFolder(id)
	}
	if err == nil {
		return
	}
	if errors.Is(err, ErrFolderPaused) || errors.Is(err, ErrScanCancelled) {
		// Paused again before it could finish: the request is still owed, so
		// put it back rather than losing the operator's rescan.
		f.addPending(p.verify, false)
		n.Logf("folder %s: queued rescan deferred again (folder paused)", id)
		return
	}
	n.Logf("folder %s: queued rescan: %v", id, err)
}

// runRebuild re-walks a folder whose index was quarantined, with the stability
// gate bypassed (an empty index has no prior content a partial could
// contradict). Paused folders postpone it: it reads the whole tree.
func (n *Node) runRebuild(f *Folder) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := n.lookupFolder(f.Cfg.ID); !ok || cur != f {
		return fmt.Errorf("unknown folder %q", f.Cfg.ID)
	}
	if f.Paused() {
		f.addPending(false, true)
		return ErrFolderPaused
	}
	_, err := n.rebuildFromFS(f, "index was corrupt — rebuilt from filesystem")
	return err
}

// SetFolderPaused pauses or resumes one folder on this device. Deliberately
// cheap: it never stats the path, re-opens the index or rebuilds the scanner,
// so pausing works when the share is offline or unmounted — which is the usual
// reason to pause in the first place.
func (n *Node) SetFolderPaused(id string, paused bool) error {
	f, ok := n.lookupFolder(id)
	if !ok {
		return fmt.Errorf("unknown folder %q", id)
	}
	// No folder lock here, deliberately. A scan holds f.mu for its whole pass,
	// and this handler must be able to INTERRUPT that scan: storing the atomic
	// flag is the interruption (the scanner's cancel probe reads it), so taking
	// the lock as well would make pause queue behind the very work it is meant
	// to stop — measured as a 30s+ hang while the disk kept being read. The
	// config field is written by the caller that persists it.
	f.paused.Store(paused)
	if paused {
		n.Logf("folder %s: paused (no scans, no sync with any peer)", id)
		return nil
	}
	n.Logf("folder %s: resumed", id)
	// A rescan the operator asked for while the folder was paused runs now, in
	// the background: exactly once (takePending clears it), and never inside
	// this call — resume must return immediately too.
	if p, queued := f.takePending(); queued {
		go n.runPendingScan(id, p)
	}
	return nil
}

// SetFolderPullFsync turns fsync-before-rename on or off for one folder's pulls
// (see config.Folder.PullFsync). The live value is an atomic that the pull path
// reads per commit, so the change applies to the next file pulled, even
// mid-round; the folder's config row is updated under f.mu so a later
// persistFolders or folder edit cannot resurrect the old value.
//
// f.mu can be held for minutes by a scan, so this is the same wait a folder edit
// (PUT /api/folders/{id}) already takes — unlike the pause setters, which must
// never wait, a durability knob is not an emergency stop.
func (n *Node) SetFolderPullFsync(id string, on bool) error {
	f, ok := n.lookupFolder(id)
	if !ok {
		return fmt.Errorf("unknown folder %q", id)
	}
	f.pullFsync.Store(on)
	f.mu.Lock()
	f.Cfg.PullFsync = pullFsyncRow(on)
	f.mu.Unlock()
	if on {
		n.Logf("folder %s: pulled files are fsynced before the rename (durable)", id)
	} else {
		n.Logf("folder %s: WARNING: pulled files are no longer fsynced before the rename — a power cut can leave a file shorter than its index entry", id)
	}
	return nil
}

// pullFsyncRow is the config-row form of the knob: the default (on) is written
// as an ABSENT key, so a config never claims more than it means and only an
// explicit `pull_fsync: false` ever disables durability.
func pullFsyncRow(on bool) *bool {
	if on {
		return nil
	}
	off := false
	return &off
}

// SetFolderTransfersPaused stops or allows data movement for one folder without
// touching scanning. Atomic, lock-free, and deliberately independent of the
// folder lock: a round holds that lock while transferring, so anything that had
// to wait for it could not stop a transfer in progress.
func (n *Node) SetFolderTransfersPaused(id string, paused bool) error {
	f, ok := n.lookupFolder(id)
	if !ok {
		return fmt.Errorf("unknown folder %q", id)
	}
	// Lock-free for the same reason as SetFolderPaused: a transfer must be
	// interruptible while the folder lock is held by a scan or a round.
	f.transferPaused.Store(paused)
	if paused {
		n.Logf("folder %s: transfers paused (scanning continues)", id)
	} else {
		n.Logf("folder %s: transfers resumed", id)
	}
	return nil
}

// allTransfersPaused reports whether every folder has transfers paused, so a
// whole round can be skipped instead of opened and then abandoned.
func (n *Node) allTransfersPaused() bool {
	n.foldersMu.Lock()
	defer n.foldersMu.Unlock()
	for _, f := range n.Folders {
		if !f.TransfersPaused() {
			return false
		}
	}
	return len(n.Folders) > 0
}

// applyShareAttrs gives a folder's stager the share's ownership/mode policy, so
// files this node writes are editable by the people who actually use the share,
// and so the sender's mode (exec bits, private files) survives the trip.
//
// With nothing configured, the policy adopts the folder root's own owner: a
// folder owned by nobody:users gets files owned by nobody:users, without the
// operator having to discover and type two numeric ids. That is what makes a
// fresh install behave correctly instead of writing root-owned files into a
// share that other accounts edit over SMB.
func (n *Node) applyShareAttrs(stag *staging.Stager, fc config.Folder) {
	cfg := n.Cfg
	stag.FileMode = cfg.FileModeForPull()
	stag.DirMode = cfg.DirModeForPull()
	uid, gid := cfg.PullOwner()
	if uid < 0 && gid < 0 {
		if fu, fg, ok := staging.OwnerOf(fc.Path); ok {
			uid, gid = fu, fg
		}
	}
	stag.UID, stag.GID = uid, gid
	if (stag.FileMode != 0 || stag.DirMode != 0 || uid >= 0 || gid >= 0) && n.Logf != nil {
		n.Logf("folder %s: pulled files mode=%04o dirs=%04o owner=%d:%d",
			fc.ID, uint32(stag.FileMode), uint32(stag.DirMode), uid, gid)
	}
}

// confirmDeletionsSoon queues one more walk when the pass that just finished only
// SUSPECTED deletions. The deletion gate needs a second independent observation
// before the index drops the entries; the periodic safety-net walk is up to ten
// minutes away, so until it lands /api/status and the UI still report files that
// are already gone from disk. That gap is what made "3,276 files / 6.5 GB" show
// for an empty folder. A full pass here is the same walk the ticker would do, just
// now instead of later; nothing is deleted that a second pass would not have
// deleted anyway.
//
// Bounded on purpose: at most three follow-ups per detected burst, never two at
// once, and the counter resets as soon as nothing is pending.
func (n *Node) confirmDeletionsSoon(f *Folder) {
	pending, err := f.Ix.MissingList()
	if err != nil || len(pending) == 0 {
		f.deletionsPending.Store(0)
		f.lastDeletionsPending.Store(0)
		return
	}
	// A shrinking table is progress: a long drain confirms a fraction per pass
	// (a scoped walk only speaks for what it walked), and stopping after three
	// passes left thousands of entries to the fifteen-minute safety net.
	if prev := f.lastDeletionsPending.Swap(int32(len(pending))); prev > int32(len(pending)) {
		f.deletionsPending.Store(0)
	}
	if f.deletionsPending.Load() >= 3 {
		n.Logf("folder %s: %d path(s) still awaiting deletion confirmation; leaving it to the scheduled walk",
			f.Cfg.ID, len(pending))
		return
	}
	if !f.followUpScheduled.CompareAndSwap(false, true) {
		return
	}
	f.deletionsPending.Add(1)
	n.Logf("folder %s: %d path(s) missing from disk; confirming with an immediate second pass",
		f.Cfg.ID, len(pending))
	go func() {
		// Small delay so this pass's index transaction is closed and the walk
		// sees the same filesystem state a fresh observer would.
		time.Sleep(2 * time.Second)
		f.followUpScheduled.Store(false)
		if _, err := n.ScanFolder(f.Cfg.ID); err != nil {
			n.Logf("folder %s: confirmation pass: %v", f.Cfg.ID, err)
		}
	}()
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
	if errors.Is(err, ErrScanCancelled) {
		// Paused mid-pass: not a failure, and nothing was applied. A verified
		// scan that is interrupted must NOT report "0 content changes" — that
		// is exactly the answer the operator was waiting for.
		n.Logf("folder %s: verified scan CANCELLED after %s (folder paused; nothing applied, no content was compared)",
			id, time.Since(start).Round(time.Second))
		return 0, err
	}
	if err != nil {
		n.Logf("folder %s: verified scan failed after %s: %v", id, time.Since(start).Round(time.Second), err)
		return applied, err
	}
	n.Logf("folder %s: verified scan finished in %s (%d content change(s))", id, time.Since(start).Round(time.Second), applied)
	return applied, nil
}

func (n *Node) scanFolder(f *Folder) (int, error) {
	return n.scanFolderScoped(f, nil)
}

// scanFolderScoped is scanFolder with an optional scope (nil = whole folder).
func (n *Node) scanFolderScoped(f *Folder, scope []string) (applied int, err error) {
	id := f.Cfg.ID
	// A paused folder does not scan: no walking, no hashing, no index writes
	// and no events. Resuming picks up wherever the tree actually is.
	if f.Paused() {
		return 0, ErrFolderPaused
	}
	// This pass may be BUILDING the folder's index rather than updating one:
	// the first pass since the daemon started, or any pass over an index that
	// still holds no files. Those are the passes that can run for tens of
	// minutes over a million files, and until now they saved every hashed
	// entry in memory and committed once at the very end — so a restart threw
	// all of it away and the next pass re-walked and re-hashed from zero.
	// A building pass therefore commits its hashed entries in bounded batches
	// (see commitScanBatch), which is what makes it resumable.
	//
	// A pass over an ALREADY populated index keeps the single end-of-pass
	// apply: there the in-memory change set is also the input to rename
	// pairing (an add committed early can no longer be paired with the
	// deletion of its old path, and the pair would degrade into a delete +
	// full re-upload on the peer). A building pass has no vanished entries to
	// pair - the index it is filling is empty or a prefix of the same tree -
	// so batching costs it nothing.
	if f.firstPassPending.Load() {
		// One aggregate, once per boot, so the resumed pass can report the
		// entries the interrupted pass already banked instead of "committed 0"
		// while half the share sits in the index.
		n.refreshFolderStatusCounts(f)
	}
	banked := f.ixFiles.Load()
	batchPass := f.firstPassPending.Load() || banked == 0
	n.startScan(id, banked)
	if batchPass {
		f.Scanner.SetBatchSink(func(batch []scanner.Change) error {
			return n.commitScanBatch(f, batch)
		})
		defer f.Scanner.SetBatchSink(nil)
	}
	defer func() {
		n.endScan(id, applied, err)
		// This folder's first pass since start is over (successfully or with an
		// error). Clear the "index incomplete" mark HERE rather than when the
		// whole startup walk returns: from this point the folder's index
		// describes the share as well as it ever will, so it may be advertised
		// and synced while slower folders are still hashing.
		//
		// A CANCELLED pass proves nothing (a pause, or shutdown mid-hash): it
		// applied nothing, so the folder stays marked and unadvertised until a
		// pass really finishes — a paused folder is not advertised anyway, and
		// its resume queues the scan that clears this.
		if !errors.Is(err, ErrScanCancelled) {
			f.firstPassPending.Store(false)
		}
		// A scan is what moves the index counts and can open/resolve events,
		// so republish the cached figures the status page reads right here
		// instead of making it wait for the next 5 s tick — the request path
		// itself stays SQL-free (see RefreshStatusCounts).
		if f != nil {
			n.refreshFolderStatusCounts(f)
			if n.Events != nil {
				if open, oerr := openEventsProbe(n.Events); oerr == nil {
					n.openEvents.Store(open)
				}
			}
		}
		switch {
		case errors.Is(err, ErrScanCancelled):
			// A pause is not a fault: the pass proves nothing about the share,
			// so the folder's error state is left exactly as it was.
		case err != nil:
			n.reportFolderError(id, err)
		default:
			n.clearFolderError(id)
		}
	}()
	progress := func(p scanner.Progress) {
		n.setScan(id, func(st *ScanStatus) {
			st.Phase = p.Phase
			st.Walked = p.Walked
			st.HashDone = p.HashDone
			st.HashTotal = p.HashTotal
			// A scoped pass only sees its own paths, so publishing its count
			// would under-report the files held back folder-wide.
			if len(scope) == 0 {
				st.InFlight = p.InFlight
				// Whole-folder accounting only: a scoped pass counted just its
				// own subtree and would replace the folder total with it.
				st.IgnoredFiles = p.IgnoredFiles
				st.IgnoredBytes = p.IgnoredBytes
			}
		})
	}
	var changes []scanner.Change
	if len(scope) == 0 {
		changes, err = f.Scanner.ScanWithProgress(progress)
	} else {
		changes, err = f.Scanner.ScanScopeWithProgress(scope, progress)
	}
	if errors.Is(err, scanner.ErrScanCancelled) {
		return n.cancelledPass(f)
	}
	// The scanner can finish a pass a moment before the pause lands (or the
	// pause can land between its last probe and this point). Applying then
	// would write the index for a folder the operator has just parked, so the
	// same rule applies: abandon, apply nothing.
	if f.Paused() {
		return n.cancelledPass(f)
	}
	if err != nil {
		return 0, err
	}
	applied = 0
	touched := 0
	foundNewOrGone := false
	for _, c := range changes {
		switch c.Kind {
		case scanner.Unchanged:
		case scanner.Touched:
			// Verified scan, content identical: the stat was adopted but
			// nothing changed. Counting it would report "N changes" for a scan
			// that changed nothing (and the deep-scan log would lie).
			touched++
		default:
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
	if touched > 0 {
		n.Logf("folder %s: %d file(s) re-hashed and unchanged (stat adopted, no new version)", id, touched)
	}
	// A pass that only SUSPECTED deletions leaves the index holding entries
	// whose files are already gone (the counts on screen still include them).
	// Confirm them with an immediate second pass instead of waiting for the
	// safety-net walk, which can be fifteen minutes away.
	//
	// A scoped (watcher) pass asks for the same thing: it cannot confirm a
	// deletion because it only speaks for the paths it walked, so without this
	// a tree wiped over SMB dripped a few entries per watcher event and took
	// hours to reach the index (measured: 2 files/s). The follow-up is always a
	// FULL pass, which can speak for the whole folder.
	n.confirmDeletionsSoon(f)
	// Changes the batch sink already committed during the hash phase must not
	// be applied a second time: re-applying an entry that is already correct
	// would bump its version for content that never changed. They stay in the
	// count above (the pass really did apply them) and are skipped here.
	remaining := make([]scanner.Change, 0, len(changes))
	for _, c := range changes {
		if c.Committed {
			continue
		}
		remaining = append(remaining, c)
	}
	// Apply the rest in one engine lock hold and one index transaction
	// (a single WAL fsync instead of one per file). A change that fails
	// verification is returned and re-applied per-change below: a stale
	// rename falls back to a normal delete+add so nothing is lost.
	if len(remaining) > 0 {
		if _, failed, err := f.Engine.ApplyLocalChanges(remaining); err != nil {
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
	n.applyShareAttrs(stag, fc)
	f.Cfg = fc
	f.Root = fc.Path
	f.Stager = stag
	f.Hold = hold.New(fc.Path)
	f.pullFsync.Store(fc.PullFsyncOn())
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
	// Same live pool source as AddFolder: UpdateFolder re-creates the scanner,
	// so the wiring has to be repeated or a folder edit would silently pin the
	// reader pool back to one per CPU.
	sc.SetWorkerSource(n.HashWorkers)
	// UpdateFolder re-creates the scanner, so the gate wiring must be
	// repeated here or the folder would silently reset to the default.
	sc.SetSettleScans(fc.SettleThreshold())
	sc.SetSettleSeconds(fc.SettleQuietSeconds())
	sc.SetDeletionScans(fc.DeleteConfirmThreshold())
	if fc.SyncDirection() == "receive-only" {
		sc.SetSettleScans(1) // gate off: see AddFolder
	}
	sc.SetCancel(n.scanCancelProbe(f))
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
	// Close the watcher BEFORE the index: its consumer goroutine scans, and a
	// scan of a closed index is an error rather than a shutdown.
	n.StopFolderWatcher(id)
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
// once per pass. A folder whose FIRST pass since start is still outstanding is
// skipped: the startup walk owns it, and blocking this caller behind a
// 57.8 GB hash would freeze a manual sync or the safety ticker for an hour.
func (n *Node) ScanAll() error {
	return n.scanAll(false)
}

// ScanAllFirstPass is the daemon's startup walk. Every folder's first pass is
// started CONCURRENTLY (bounded by scanAllConcurrency; the per-folder scan lock
// still keeps one folder to one pass at a time) so a big share being hashed does
// not delay a small folder's first observation — and therefore its
// advertisement and its first sync round, both of which unblock folder by
// folder as each pass finishes. It returns when every folder's pass has
// finished; the caller schedules the next full walk from that duration.
func (n *Node) ScanAllFirstPass() error {
	return n.scanAll(true)
}

// scanAllConcurrency bounds how many folders the startup walk hashes at once.
// Two by default: enough that a small folder is indexed in seconds while a large
// one hashes, without putting four array reads through the same disk at the
// same time. A variable so tests can pin it.
var scanAllConcurrency = 2

func (n *Node) scanAll(firstPass bool) error {
	n.foldersMu.RLock()
	ids := make([]string, 0, len(n.Folders))
	for id := range n.Folders {
		ids = append(ids, id)
	}
	n.foldersMu.RUnlock()

	limit := scanAllConcurrency
	if limit < 1 {
		limit = 1
	}
	sem := make(chan struct{}, limit)
	var wg std.WaitGroup
	for _, id := range ids {
		// Paused folders are skipped silently: their scans are stopped on
		// purpose, so they must not produce an error (or a log line) per tick.
		if n.Paused(id) {
			continue
		}
		// The startup walk owns a folder until its first pass ends, so a later
		// ScanAll caller (a manual sync, the safety ticker) SKIPS it instead of
		// queueing behind it: one slow share must not hold this caller — or the
		// folders it was asked to walk — hostage for an hour.
		if !firstPass && n.FolderIndexing(id) {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(id string) {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := n.ScanFolder(id); err != nil {
				// A pause that landed mid-pass is the same deliberate stop: the
				// pass applied nothing and the first pass after the resume
				// catches up, so it is not a scan failure.
				if errors.Is(err, ErrScanCancelled) || errors.Is(err, ErrFolderPaused) {
					return
				}
				n.Logf("scan of folder %s failed: %v", id, err)
			}
		}(id)
	}
	wg.Wait()
	return nil
}
