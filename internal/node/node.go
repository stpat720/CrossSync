// Package node implements the CrossSync daemon: folders, scanning, and
// peer sessions. It wires together the index, scanner, staging, and engine.
package node

import (
	"crypto/tls"
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
	"crosssync/internal/ignore"
	"crosssync/internal/index"
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
}

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

	// Event-stream subscribers (control plane SSE).
	subMu std.Mutex
	subs  map[*eventSub]struct{}

	// peerFolders records which folder ids each peer has advertised in its
	// cluster config, learned during sync sessions. The control plane uses
	// it to offer only peers that actually have a folder in its peer list.
	peerFoldersMu std.Mutex
	peerFolders   map[uint64]map[string]bool

	// problems tracks the last reported reason per problem file, so a file
	// stuck for days (locked, permission, disk full) does not spam the
	// event store — only the first failure and reason changes emit records.
	probMu    std.Mutex
	problems  map[string]string // "folder\x00name" -> last reason
	folderErr map[string]string // "folder" -> last scan-error reason

	// delGuard tracks folders whose deletion guard has tripped: a peer's
	// index proposed deleting more files than the configured threshold in
	// one session, so deletions stay blocked until the operator applies
	// them explicitly (never let one pass wipe a folder silently).
	delGuard map[string]bool // folder id -> tripped

	// caseColl tracks the last reported case-collision warning per colliding
	// path, so identical warnings are not re-emitted on every scan and are
	// resolved once the collision clears.
	caseColl map[string]string // "folder\x00path" -> last reason

	// massChange tracks folders currently flagged for a massive single-sync
	// change (e.g. >50% of live files), so the warning is emitted once and
	// resolved when the change rate drops back to normal.
	massChange map[string]bool // folder id -> flagged

	// Alert posts page-worthy notifications (nil = disabled).
	Alert *alert.Notifier
	// alertThrottle prevents the same condition from paging repeatedly
	// (e.g. a peer that flaps) — one alert per condition per window.
	alertMu        std.Mutex
	alertThrottle  map[string]time.Time // "folder|path|category" -> last fire
	alertCooldown  time.Duration        // default 15m; settable for tests

	// scanMu serializes scans (daemon ticker, watcher triggers, control
	// plane rescans) so two scans can never apply the same change twice.
	scanMu std.Mutex

	// act tracks live scan and sync activity so the control plane can
	// report progress (how much has been scanned, when it started) and
	// reject overlapping manual rescans/syncs.
	act activity

	// TLS identity: populated when config.TLS is set.
	TLS       bool
	Certs     *certs.Manager
	ClientTLS *tls.Config
	ServerTLS *tls.Config
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
		problems:      map[string]string{},
		folderErr:     map[string]string{},
		delGuard:      map[string]bool{},
		caseColl:      map[string]string{},
		massChange:    map[string]bool{},
		alertThrottle: map[string]time.Time{},
	}
	ev, err := events.Open(filepath.Join(cfg.MetaDir, "events.db"))
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
		n.ClientTLS = certs.ClientConfig(cm, allowed)
		n.ServerTLS = certs.ServerConfig(cm, allowed)
		if cm.DeviceID() != n.ID {
			n.Logf("warning: configured device id %d does not match certificate-derived id %d (fingerprint %s)",
				n.ID, cm.DeviceID(), cm.Fingerprint())
		}
	}
	for _, fc := range cfg.Folders {
		if _, err := n.AddFolder(fc); err != nil {
			ev.Close()
			return nil, err
		}
	}
	return n, nil
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

// AddFolder registers a new folder, opening its index and creating its
// stager. Safe to call after construction. If the folder's index is
// corrupt, it is transparently quarantined and rebuilt from the filesystem
// (the FS is the source of truth).
func (n *Node) AddFolder(fc config.Folder) (*Folder, error) {
	st, err := os.Stat(fc.Path)
	if err != nil || !st.IsDir() {
		return nil, fmt.Errorf("folder %q: %w", fc.ID, err)
	}
	ix, rebuilt, err := n.openFolderIndex(fc)
	if err != nil {
		return nil, err
	}
	stag, err := staging.New(fc.Path)
	if err != nil {
		ix.Close()
		return nil, err
	}
	ig, err := ignore.Parse(fc.Ignore)
	if err != nil {
		ix.Close()
		return nil, err
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
	sc := scanner.New(fc.Path, ix, ig)
	f := &Folder{Cfg: fc, Root: fc.Path, Ix: ix, Stager: stag, Engine: eng, Scanner: sc}
	n.Folders[fc.ID] = f
	if rebuilt {
		if _, err := n.rebuildFromFS(f, "index was corrupt — rebuilt from filesystem"); err != nil {
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
			ix.Close()
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
	changes, err := f.Scanner.Scan()
	if err != nil {
		return 0, err
	}
	applied := 0
	for _, c := range changes {
		if c.Kind == scanner.Unchanged {
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
	n.scanMu.Lock()
	defer n.scanMu.Unlock()
	f, ok := n.Folders[id]
	if !ok {
		return 0, fmt.Errorf("unknown folder %q", id)
	}
	fc := f.Cfg
	path := f.Ix.Path()
	f.Ix.Close()
	delete(n.Folders, id)
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
	return n.rebuildFromFS(nf, "index rebuilt manually from filesystem")
}

// Close closes all folder indexes and the event store.
func (n *Node) Close() error {
	for _, f := range n.Folders {
		f.Ix.Close()
	}
	if n.Events != nil {
		n.Events.Close()
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
	default:
		if len(s) > 140 {
			s = s[:140]
		}
		return "cannot apply: " + s
	}
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
			if _, _, hadPrior, err := f.Ix.GetPeerIndex(peerID); err == nil && hadPrior {
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
	f, ok := n.Folders[id]
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
// engine, returning the number of applied changes. Scans are serialized so
// concurrent callers (ticker, watcher, control plane) cannot double-apply.
func (n *Node) ScanFolder(id string) (int, error) {
	n.scanMu.Lock()
	defer n.scanMu.Unlock()
	return n.scanFolder(id)
}

func (n *Node) scanFolder(id string) (applied int, err error) {
	f, ok := n.Folders[id]
	if !ok {
		return 0, fmt.Errorf("unknown folder %q", id)
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
		})
	})
	if err != nil {
		return 0, err
	}
	applied = 0
	foundNewOrGone := false
	for _, c := range changes {
		if _, err := f.Engine.ApplyLocalChange(c.Kind, c.Info); err != nil {
			return applied, err
		}
		if c.Kind != scanner.Unchanged {
			applied++
			if c.Kind == scanner.Added || c.Kind == scanner.Deleted {
				foundNewOrGone = true
			}
			n.setScan(id, func(st *ScanStatus) {
				st.Phase = "applying"
				st.Changed = applied
			})
		}
	}
	// Case collisions can only newly appear (or clear) when files were
	// added or deleted, so only re-check then.
	if foundNewOrGone {
		n.checkCaseCollisions(id)
	}
	return applied, nil
}

// RemoveFolder closes and drops a folder from the node. The configuration
// (config file) is updated by the caller (control plane).
func (n *Node) RemoveFolder(id string) error {
	n.scanMu.Lock()
	defer n.scanMu.Unlock()
	f, ok := n.Folders[id]
	if !ok {
		return fmt.Errorf("unknown folder %q", id)
	}
	f.Ix.Close()
	delete(n.Folders, id)
	return nil
}

// RemoveConflict deletes a conflict copy from disk and tombstones it in the
// index so the deletion propagates to all peers. Only files whose name
// contains the conflict suffix can be removed through this path, and the
// path is confined to the folder root (no traversal).
func (n *Node) RemoveConflict(folder, name string) error {
	n.scanMu.Lock()
	defer n.scanMu.Unlock()
	f, ok := n.Folders[folder]
	if !ok {
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

// ScanAll scans every folder (serialized like ScanFolder).
func (n *Node) ScanAll() error {
	n.scanMu.Lock()
	defer n.scanMu.Unlock()
	for id := range n.Folders {
		if _, err := n.scanFolder(id); err != nil {
		}
	}
	return nil
}
