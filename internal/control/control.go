// Package control implements the control plane: a single service exposing
// node status, folders, the durable event store, rescan/sync actions, and
// a live event stream. It is consumed by the REST API and the MCP server.
package control

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	std "sync"
	"time"

	"gopkg.in/yaml.v3"

	"crosssync/internal/alert"
	"crosssync/internal/config"
	"crosssync/internal/events"
	"crosssync/internal/hash"
	"crosssync/internal/index"
	"crosssync/internal/node"
	"crosssync/internal/scanner"
	"crosssync/internal/sync"
	"crosssync/internal/ui"
	"crosssync/internal/version"
)

// Service exposes a Node to the control plane.
type Service struct {
	n          *node.Node
	Version    string
	ConfigPath string // path to the config file, for folder add/remove persistence
	ControlToken string // when set, every control-plane request must present it

	// Cached Prometheus exposition. Building it inline blocked for over a
	// minute while a scan held the locks it reads, so scrapes now serve the
	// cached body and a background goroutine refreshes it (see handleMetrics).
	metricsMu       std.Mutex
	metricsBody     string
	metricsAt       time.Time
	metricsBuilding bool

	// Web UI self-update state (see ui package).
	UIDir       string // override directory; "" = embedded UI only
	UIUpdateURL string // URL to fetch a newer index.html from

	// Daemon self-update state.
	UpdateURL  string // JSON update manifest (version + per-platform binary)
	UpdateDir  string // where the new binary is installed ("" = config dir)
	UpdateAuto bool   // apply updates automatically when detected

	updateMu     std.Mutex
	updateInfo   DaemonUpdateInfo
	updateLogged string // last logged check outcome (change-only logging)
}

// New wraps a node. version is the daemon version reported in Status.
func New(n *node.Node, version string) *Service {
	return &Service{n: n, Version: version}
}

// SetConfigPath records the config file path so folder add/remove can be
// persisted to disk.
func (s *Service) SetConfigPath(p string) { s.ConfigPath = p }

// SetUI records the web-UI override directory and self-update URL.
func (s *Service) SetUI(dir, updateURL string) {
	s.UIDir = dir
	s.UIUpdateURL = updateURL
}

// SetUpdate records the daemon self-update settings (manifest URL, install
// directory, and whether updates apply automatically).
func (s *Service) SetUpdate(url, dir string, auto bool) {
	s.UpdateURL = url
	s.UpdateDir = dir
	s.UpdateAuto = auto
}

// PeerStatus describes one configured peer.
type PeerStatus struct {
	ID           uint64   `json:"id"`
	Name         string   `json:"name"`
	Addresses    []string `json:"addresses"`
	Pinned       bool     `json:"pinned"`                  // TLS fingerprint configured
	Connected    bool     `json:"connected"`               // reachable within the last ~2 minutes
	LastOnline   *int64   `json:"last_online"`             // unix seconds peer was last reachable
	LastSync     *int64   `json:"last_sync"`               // unix seconds data last synced with peer
	Known        bool     `json:"known"`                   // we have seen this peer's folder list in a session
	KnownFolders []string `json:"known_folders,omitempty"` // folder ids the peer advertises
}

// FileStatus describes one indexed file.
type FileStatus struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Modified int64  `json:"modified"` // unix seconds
	Type     string `json:"type"`     // file | dir | symlink
	Deleted  bool   `json:"deleted"`
}

// PendingFile is one file still to be pulled to converge.
type PendingFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// FolderStatus summarizes one folder.
type FolderStatus struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Path             string   `json:"path"`
	Files            int      `json:"files"`
	Directories      int      `json:"directories"`
	Tombstones       int      `json:"tombstones"`
	Size             int64    `json:"size"`
	PendingFiles     int      `json:"pending_files"` // files to download from peers
	PendingBytes     int64    `json:"pending_bytes"` // bytes to download
	PendingUp        int      `json:"pending_up"`    // files peers need from us (uploads)
	FreeBytes        int64    `json:"free_bytes"`
	BytesUp          int64    `json:"bytes_up"`
	BytesDown        int64    `json:"bytes_down"`
	FilesDown        int64    `json:"files_down"`
	FilesLocalCopy   int64    `json:"files_local_copy"` // pulls committed from a byte-identical local copy (no transfer)
	BytesLocalCopy   int64    `json:"bytes_local_copy"` // bytes those copies saved on the wire
	Syncs            int64    `json:"syncs"`
	LastSync         *int64   `json:"last_sync,omitempty"`  // unix seconds
	ConflictFiles    int      `json:"conflict_files"`       // .sync-conflict copies on disk
	ConflictBytes    int64    `json:"conflict_bytes"`       // bytes used by conflict copies
	VersionFiles     int      `json:"version_files"`        // archived versions in .sfx-versions
	VersionBytes     int64    `json:"version_bytes"`        // bytes used by archived versions
	OpenEvents       int64    `json:"open_events"`          // distinct open warn+ conditions for this folder
	Peers            []uint64 `json:"peers"`                // peer ids this folder syncs with (empty = all peers)
	PeersAll         bool     `json:"peers_all"`            // true when Peers is empty (syncs with every peer)
	PeerNames        []string `json:"peer_names,omitempty"` // configured names of Peers
	SharedPeers      int      `json:"shared_peers"`         // peers known to have this folder and allowed by scoping
	NotShared        bool     `json:"not_shared"`           // peers exist but none (known) will sync this folder
	GuardTripped     bool     `json:"guard_tripped"`        // legacy: the guard no longer blocks (see Hold)
	PendingDeletions int      `json:"pending_deletions"`    // live files globally deleted, waiting to be removed
	// PendingVerified is false when the pending counters cannot be trusted: the
	// peer has not exchanged recently, an exchange is in flight, or this index
	// has changes the peer has not been sent. Numbers are only shown as facts
	// when this is true; PendingReason says why when it is not.
	PendingVerified bool   `json:"pending_verified"`
	PendingReason   string `json:"pending_reason,omitempty"`
	// IgnoredFiles/Bytes are on the share but not in the index: FUSE/SMB
	// bookkeeping names and .sfx-* areas. Not data, but real disk.
	IgnoredFiles int   `json:"ignored_files"`
	IgnoredBytes int64 `json:"ignored_bytes"`
	Direction        string   `json:"direction"`            // send-receive | send-only | receive-only (effective)
	Paused           bool     `json:"paused"`               // scanning and syncing stopped for this folder on this device
	TransferPaused   bool     `json:"transfer_paused"`      // data movement stopped; scanning continues
	InFlightFiles    int      `json:"inflight_files"`       // changed files held back: still being written
	InFlightBytes    int64    `json:"inflight_bytes"`       // bytes of those held-back files
	Include          []string `json:"include,omitempty"`    // include (whitelist) patterns; empty = sync everything
	Ignore           []string `json:"ignore,omitempty"`     // exclude patterns
	// PeerUnsynced is what the OTHER peers say they have not synced in this
	// folder (one block per peer that advertised a non-empty set at the end
	// of its last successful round). Empty when everyone is caught up or no
	// peer has told us yet.
	PeerUnsynced []PeerUnsynced `json:"peer_unsynced,omitempty"`
	// Hold is the soft-deletion hold area: copies that an over-threshold
	// delete pass parked in .sfx-hold instead of unlinking, with the undo
	// window each one has left. Nil when nothing is held.
	Hold *FolderHold `json:"hold,omitempty"`
}

// FolderPauseStatus is the answer to a pause/resume request: the flags this
// device just set, and nothing else.
//
// Deliberately NOT a full FolderStatus. Building one runs per-folder SQL
// aggregates, a statfs on the share and directory walks — measured at ~3.3s
// live on a 34k-file Unraid folder — and that is the worst possible cost for the
// one request whose whole purpose is to stop disk I/O immediately. Everything
// else a caller needs is on the next /api/status poll, which is already cheap
// and cached.
type FolderPauseStatus struct {
	ID             string `json:"id"`
	Paused         bool   `json:"paused"`          // no scanning, no transfer, not advertised
	TransferPaused bool   `json:"transfer_paused"` // data movement stopped, scanning continues
}

// FolderHold summarizes the soft-deletion hold area of one folder. Entries is
// served only by GET /api/folders/{id}/hold; the status summary omits it (the
// list is unbounded and status is polled every couple of seconds).
type FolderHold struct {
	Count   int        `json:"count"`
	Bytes   int64      `json:"bytes"`
	Entries []HeldFile `json:"entries,omitempty"`
}

// HeldFile is one held (soft-deleted) path, with its undo window.
type HeldFile struct {
	Path      string `json:"path"` // original path relative to the folder root
	Size      int64  `json:"size"` // bytes preserved
	HeldAt    int64  `json:"held_at"`
	ExpiresAt int64  `json:"expires_at"`
}

// PeerUnsynced is one peer's open unsynced set for a shared folder: what
// that peer still has to do, from its own point of view. It is transient by
// nature — it is what the peer advertised at the end of its last successful
// round with us, and it is dropped when that peer goes away.
type PeerUnsynced struct {
	PeerID    uint64         `json:"peer_id"`
	PeerName  string         `json:"peer_name"`
	Connected bool           `json:"connected"` // reachable within the last ~2 minutes
	Entries   []UnsyncedFile `json:"entries"`
}

// UnsyncedFile is one path a peer reports as not yet synced.
type UnsyncedFile struct {
	Path   string `json:"path"`
	Reason string `json:"reason,omitempty"`
	Dir    string `json:"dir,omitempty"` // down | up | both | "" (same as AttentionFile.dir)
}

// Status is the top-level health/identity snapshot.
type Status struct {
	Device               string            `json:"device"`
	DeviceID             uint64            `json:"device_id"`
	Version              string            `json:"version"`
	TLS                  bool              `json:"tls"`
	Fingerprint          string            `json:"fingerprint,omitempty"`
	OpenEvents           int64             `json:"open_events"`
	BytesUp              int64             `json:"bytes_up"`
	BytesDown            int64             `json:"bytes_down"`
	LastSync             *int64            `json:"last_sync,omitempty"` // max over folders
	Folders              []FolderStatus    `json:"folders"`
	Peers                []PeerStatus      `json:"peers"`
	Scans                []node.ScanStatus `json:"scans"`                   // live scan progress per folder
	Sync                 node.SyncActivity `json:"sync"`                    // live sync activity
	UIVersion            string            `json:"ui_version"`              // UI currently served
	UIOverride           bool              `json:"ui_override"`             // served from ui_dir (not embedded)
	UIUpdateURL          string            `json:"ui_update_url,omitempty"` // configured self-update URL
	UpdateURL            string            `json:"update_url,omitempty"`    // daemon self-update manifest URL
	UpdateAuto           bool              `json:"update_auto"`             // auto-apply daemon updates
	ConflictFiles        int               `json:"conflict_files"`          // global conflict-copy count
	ConflictBytes        int64             `json:"conflict_bytes"`          // global conflict-copy bytes
	VersionFiles         int               `json:"version_files"`           // global archived-version count
	VersionBytes         int64             `json:"version_bytes"`           // global archived-version bytes
	AutoArchiveEnabled   bool              `json:"auto_archive_enabled"`    // routine event history auto-pruned
	AutoArchiveOlderThan int64             `json:"auto_archive_older_than"` // seconds; effective age when enabled
	AlertURL             string            `json:"alert_url,omitempty"`     // ntfy/webhook endpoint for page-worthy alerts
}

// Status returns the current snapshot.
func (s *Service) Status() Status {
	st := Status{
		Device:   s.n.Name,
		DeviceID: s.n.ID,
		Version:  s.Version,
		TLS:      s.n.TLS,
	}
	if fp := s.n.Fingerprint(); fp != "" {
		st.Fingerprint = fp
	}
	if s.n.Events != nil {
		if open, err := s.n.Events.CountOpen(); err == nil {
			st.OpenEvents = open
		}
	}
	st.Folders = s.Folders()
	st.Scans = s.n.ScanStatuses()
	st.Sync = s.n.SyncActivity()
	st.AutoArchiveEnabled, st.AutoArchiveOlderThan = s.n.Cfg.AutoArchive()
	st.AlertURL = s.n.Cfg.AlertURL
	// Read the currently-served UI version live: after a self-update the
	// override file takes effect immediately and this must reflect it.
	st.UIVersion = ui.CurrentVersion(s.UIDir)
	st.UIUpdateURL = s.UIUpdateURL
	st.UpdateURL = s.UpdateURL
	st.UpdateAuto = s.UpdateAuto
	if _, ok, err := ui.ReadOverride(s.UIDir); err == nil && ok {
		st.UIOverride = true
	}
	var maxSync int64
	for _, f := range st.Folders {
		st.BytesUp += f.BytesUp
		st.BytesDown += f.BytesDown
		st.ConflictFiles += f.ConflictFiles
		st.ConflictBytes += f.ConflictBytes
		st.VersionFiles += f.VersionFiles
		st.VersionBytes += f.VersionBytes
		if f.LastSync != nil && *f.LastSync > maxSync {
			maxSync = *f.LastSync
		}
	}
	if maxSync > 0 {
		st.LastSync = &maxSync
	}
	for _, p := range s.n.Cfg.Peers {
		ps := PeerStatus{
			ID: p.ID, Name: p.Name, Addresses: p.Addresses,
			Pinned: strings.TrimSpace(p.Fingerprint) != "",
		}
		if s.n.ConnMgr != nil {
			if lo := s.n.ConnMgr.LastOnline(p.ID); !lo.IsZero() {
				t := lo.Unix()
				ps.LastOnline = &t
				// "Connected" means the peer was reachable recently, not
				// merely "ever". The manager dials every interval, so a
				// healthy peer's last-online is seconds old.
				ps.Connected = time.Since(lo) < 2*time.Minute
			}
			if ls := s.n.ConnMgr.LastSync(p.ID); !ls.IsZero() {
				t := ls.Unix()
				ps.LastSync = &t
			}
		}
		if fs, seen := s.n.PeerFolders(p.ID); seen {
			ps.Known = true
			ps.KnownFolders = fs
		}
		st.Peers = append(st.Peers, ps)
	}
	return st
}

// Folders summarizes every folder.
func (s *Service) Folders() []FolderStatus {
	var out []FolderStatus
	for _, f := range s.n.Folders {
		fs := FolderStatus{ID: f.Cfg.ID, Name: f.Cfg.Name, Path: f.Root}
		if fs.Name == "" {
			fs.Name = fs.ID // older configs have no name; show the id
		}
		// One aggregate SQL pass instead of several full index walks — for
		// million-file folders this is the difference between milliseconds
		// and seconds on every status poll.
		if st, err := f.Ix.Stats(sync.ConflictSuffix); err == nil {
			fs.Files = st.Files
			fs.Directories = st.Directories
			fs.Size = st.Size
			fs.Tombstones = st.Tombstones
			fs.ConflictFiles = st.ConflictFiles
			fs.ConflictBytes = st.ConflictBytes
		}
		if ls := f.Engine.Stats.LastSync.Load(); ls > 0 {
			t := time.Unix(0, ls).Unix()
			fs.LastSync = &t
		}
		// Free space on the volume holding the folder root: the number that says
		// whether a corpus (or a restore) will fit before it is written.
		fs.FreeBytes = s.n.FreeBytes(f.Cfg.ID)
		fs.BytesUp = f.Engine.Stats.BytesUp.Load()
		fs.BytesDown = f.Engine.Stats.BytesDown.Load()
		fs.FilesDown = f.Engine.Stats.FilesDown.Load()
		fs.FilesLocalCopy = f.Engine.Stats.FilesLocalCopy.Load()
		fs.BytesLocalCopy = f.Engine.Stats.BytesLocalCopy.Load()
		fs.Syncs = f.Engine.Stats.Syncs.Load()
		fs.PendingFiles, fs.PendingBytes = s.pending(f.Cfg.ID)
		fs.PendingUp = int(f.Engine.Stats.PendingUpCount.Load())
		// Cached, like the other counters: this used to be a live
		// PendingDeletionsCount call, i.e. a full index walk per status
		// request (the UI polls every couple of seconds). It is refreshed by
		// the same 5s ticker as pending_files/pending_up.
		fs.PendingDeletions = int(f.Engine.Stats.PendingDeleteCount.Load())
		fs.PendingVerified = true
		if nf, nb, ok := s.n.FolderIgnored(f.Cfg.ID); ok {
			fs.IgnoredFiles, fs.IgnoredBytes = nf, nb
		}
		if reason := f.Engine.PendingUnverifiedReason(); reason != "" {
			fs.PendingVerified = false
			fs.PendingReason = reason
		}
		// Space used by archived versions (the reserved .sfx-versions area,
		// not in the index).
		fs.VersionFiles, fs.VersionBytes = (&sync.Versioning{}).Stats(f.Root)
		if s.n.Events != nil {
			if n, err := s.n.Events.CountOpenFolder(f.Cfg.ID); err == nil {
				fs.OpenEvents = n
			}
		}
		// Per-folder peer scoping: which peers this folder syncs with.
		// "All peers" is SMART: a folder counts as all-peers when it has no
		// explicit list OR its list covers every currently configured peer.
		// The explicit list is still reported (PeerNames) so the UI can
		// preselect it, and so that adding a NEW peer later automatically
		// flips the folder back to showing its actual scoped peers instead
		// of "all peers".
		fs.Peers = f.Cfg.Peers
		fs.PeersAll = len(f.Cfg.Peers) == 0 || coversAllPeers(f.Cfg.Peers, s.n.Cfg.Peers)
		for _, pid := range f.Cfg.Peers {
			if nm := s.peerName(pid); nm != "" {
				fs.PeerNames = append(fs.PeerNames, nm)
			} else {
				fs.PeerNames = append(fs.PeerNames, fmt.Sprintf("device %d", pid))
			}
		}
		// Not-shared indicator: if there are configured peers, all of them
		// have been seen in a session, and none of the ones allowed by
		// scoping advertises this folder, it is effectively orphaned (e.g.
		// its id changed on the other servers, or it was scoped to a peer
		// that never added it). Unknown (never-seen) peers suppress the
		// warning rather than guessing.
		known := 0
		shared := 0
		for _, p := range s.n.Cfg.Peers {
			if !f.Cfg.AllowsPeer(p.ID) {
				continue
			}
			pfs, seen := s.n.PeerFolders(p.ID)
			if !seen {
				continue
			}
			known++
			for _, id := range pfs {
				if id == f.Cfg.ID {
					shared++
					break
				}
			}
		}
		fs.SharedPeers = shared
		fs.NotShared = len(s.n.Cfg.Peers) > 0 && known > 0 && shared == 0
		fs.GuardTripped = s.n.DeleteGuardTripped(f.Cfg.ID)
		fs.Direction = f.Cfg.SyncDirection()
		fs.Paused = f.Paused()
		fs.TransferPaused = f.TransfersPaused() && !f.Paused()
		// Stability gate: pure in-memory read (no SQL, no index walk),
		// consistent with the one-aggregate-pass rule above.
		fs.InFlightFiles, fs.InFlightBytes = f.Engine.InFlightStats()
		fs.Include = f.Cfg.Include
		fs.Ignore = f.Cfg.Ignore
		fs.PeerUnsynced = s.peerUnsynced(f)
		// Status is polled every couple of seconds by the UI, so it carries
		// only the hold SUMMARY (count + bytes). The entries themselves are
		// unbounded — a mirror-copy delete wave can park tens of thousands of
		// copies — and shipping them here made /api/status hundreds of KB per
		// poll. The list lives behind GET /api/folders/{id}/hold, fetched when
		// a folder's detail panel is opened.
		if entries, err := s.n.HeldFiles(f.Cfg.ID); err == nil && len(entries) > 0 {
			h := &FolderHold{Count: len(entries)}
			for _, e := range entries {
				h.Bytes += e.Size
			}
			fs.Hold = h
		}
		out = append(out, fs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// peerUnsynced builds the per-peer unsynced views for a folder: one block
// per configured peer that is allowed to sync the folder and has advertised
// a non-empty open unsynced set at the end of its last successful round.
// A peer that is caught up advertises an empty set, which is good news and
// needs no row in the UI.
func (s *Service) peerUnsynced(f *node.Folder) []PeerUnsynced {
	var out []PeerUnsynced
	for _, p := range s.n.Cfg.Peers {
		if !f.Cfg.AllowsPeer(p.ID) {
			continue
		}
		entries := s.n.PeerUnsynced(p.ID, f.Cfg.ID)
		if len(entries) == 0 {
			continue
		}
		pu := PeerUnsynced{PeerID: p.ID, PeerName: p.Name, Entries: make([]UnsyncedFile, 0, len(entries))}
		if pu.PeerName == "" {
			pu.PeerName = fmt.Sprintf("device %d", p.ID)
		}
		// Same freshness rule the peer list uses: "connected" means
		// reachable recently, not merely "ever seen".
		if s.n.ConnMgr != nil {
			if lo := s.n.ConnMgr.LastOnline(p.ID); !lo.IsZero() {
				pu.Connected = time.Since(lo) < 2*time.Minute
			}
		}
		for _, e := range entries {
			pu.Entries = append(pu.Entries, UnsyncedFile{Path: e.Path, Reason: e.Reason, Dir: e.Dir})
		}
		out = append(out, pu)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerName < out[j].PeerName })
	return out
}

// coversAllPeers reports whether the folder's peer list includes every
// currently configured peer, i.e. the folder is effectively "all peers"
// today (but will stop being so if a new peer is configured later).
func coversAllPeers(folderPeers []uint64, configured []config.Peer) bool {
	if len(configured) == 0 {
		return true
	}
	for _, p := range configured {
		found := false
		for _, fp := range folderPeers {
			if fp == p.ID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Files lists the indexed (non-deleted) files in a folder.
func (s *Service) Files(folder string) ([]FileStatus, error) {
	f, ok := s.n.Folders[folder]
	if !ok {
		return nil, fmt.Errorf("unknown folder %q", folder)
	}
	var out []FileStatus
	if err := f.Ix.List(func(fi *index.FileInfo) error {
		out = append(out, FileStatus{
			Name: fi.Name, Size: fi.Size, Modified: fi.ModifiedS,
			Type:    []string{"file", "dir", "symlink"}[fi.Type],
			Deleted: fi.Deleted,
		})
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// pending returns how many files are still needed to converge and their
// total size, for the given folder.
func (s *Service) pending(folder string) (int, int64) {
	f, ok := s.n.Folders[folder]
	if !ok {
		return 0, 0
	}
	// Cached: the counters are refreshed by each round and by the node's
	// background refresher. Computing them here cost two full index scans per
	// HTTP request, which both hammered the engine lock (the UI polls every
	// couple of seconds) and could block for minutes while a round held it.
	return int(f.Engine.Stats.PendingPullCount.Load()), f.Engine.Stats.PendingPullBytes.Load()
}

// Pending lists the files still to be pulled, with their sizes.
func (s *Service) Pending(folder string) ([]PendingFile, error) {
	f, ok := s.n.Folders[folder]
	if !ok {
		return nil, fmt.Errorf("unknown folder %q", folder)
	}
	names, err := f.Engine.NeedPulls()
	if err != nil {
		return nil, err
	}
	var out []PendingFile
	for _, name := range names {
		g, _, err := f.Engine.GlobalFor(name)
		if err != nil || g == nil || g.Deleted {
			continue
		}
		out = append(out, PendingFile{Name: name, Size: g.Size})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Conflicts lists the conflict copies currently present in a folder.
func (s *Service) Conflicts(folder string) ([]FileStatus, error) {
	files, err := s.Files(folder)
	if err != nil {
		return nil, err
	}
	var out []FileStatus
	for _, f := range files {
		if strings.Contains(f.Name, ".sync-conflict") {
			out = append(out, f)
		}
	}
	return out, nil
}

// AttentionFile is one file the user may need to act on. It is a pull
// that is pending, a conflict copy, or a file with an open warn+ event.
type AttentionFile struct {
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	Modified   int64  `json:"modified"`              // unix seconds
	Type       string `json:"type"`                  // file | dir | symlink
	Pending    bool   `json:"pending"`               // still to be downloaded from a peer
	PendingUp  bool   `json:"pending_up"`            // a peer needs this file from us (upload)
	Conflict   bool   `json:"conflict"`              // .sync-conflict copy
	ConflictOf string `json:"conflict_of,omitempty"` // live file the copy belongs to
	Event      bool   `json:"event"`                 // has an open warn+ event
	EventCat   string `json:"event_cat,omitempty"`
	Reason     string `json:"reason,omitempty"` // first open event's reason
	// Dir is the sync direction this row is waiting on, derived from the
	// same NeedPulls/PendingUploads data that sets Pending/PendingUp:
	// "down" (we need the file from a peer), "up" (a peer needs it from us),
	// "both", or "" for an event-only row with nothing pending either way.
	Dir string `json:"dir,omitempty"`
}

// Attention lists the files in a folder that need attention: pending
// pulls, conflict copies, and files with open warn+ events. It is the
// default view of the folder detail panel (instead of every indexed file).
func (s *Service) Attention(folder string) ([]AttentionFile, error) {
	f, ok := s.n.Folders[folder]
	if !ok {
		return nil, fmt.Errorf("unknown folder %q", folder)
	}
	byName := map[string]*AttentionFile{}
	ensure := func(name string) *AttentionFile {
		a, ok := byName[name]
		if !ok {
			a = &AttentionFile{Name: name}
			byName[name] = a
		}
		return a
	}
	// Map of live file -> its conflict copy, so an open conflict event on
	// the live file is folded into the conflict row instead of showing the
	// same conflict twice (once as the copy, once as the live file).
	conflictOf := map[string]string{}
	// Pending pulls (may not exist locally yet — take size from global). The
	// name list comes from the refresher's cache: computing it walks the whole
	// index, and doing that on a page open is what made this endpoint take tens
	// of seconds while a copy was running.
	names, cached := f.Engine.CachedPendingPulls()
	if !cached {
		var err error
		names, err = f.Engine.NeedPulls()
		if err != nil {
			return nil, err
		}
	}
	for _, name := range names {
		g, _, err := f.Engine.GlobalFor(name)
		if err != nil || g == nil || g.Deleted {
			continue
		}
		a := ensure(name)
		a.Pending = true
		if a.Size == 0 {
			a.Size = g.Size
		}
		a.Type = "file"
	}
	// Pending uploads: files at least one peer needs from us. Cached list, same
	// reason as the pulls above.
	ups, haveUps := f.Engine.CachedPendingUploads()
	if !haveUps {
		if computed, err := f.Engine.PendingUploads(); err == nil {
			ups = computed
		}
	}
	for _, name := range ups {
		a := ensure(name)
		a.PendingUp = true
		if a.Size == 0 {
			if g, _, err := f.Engine.GlobalFor(name); err == nil && g != nil {
				a.Size = g.Size
			}
		}
		if a.Type == "" {
			a.Type = "file"
		}
	}
	// Conflict copies. Discovered with a SQL-side name filter: walking every
	// entry to find the handful of copies cost ~48 s on an 84k-entry folder under
	// load (measured live), which the UI reads as an empty list because it gives
	// up first. This scales with the conflicts that exist, not with the folder.
	if err := f.Ix.ListLike(sync.ConflictSuffix, func(fi *index.FileInfo) error {
		if fi.Deleted {
			return nil
		}
		a := ensure(fi.Name)
		a.Size = fi.Size
		a.Modified = fi.ModifiedS
		a.Type = []string{"file", "dir", "symlink"}[fi.Type]
		a.Conflict = true
		orig, ok := sync.ConflictOrigin(fi.Name)
		if !ok {
			return nil // a name that cannot be traced back is not actionable
		}
		a.ConflictOf = orig
		conflictOf[orig] = fi.Name
		return nil
	}); err != nil {
		return nil, err
	}
	// Local metadata for the rows already in the list: one batched query, not one
	// decode per file in the folder and not one query per row.
	if len(byName) > 0 {
		names := make([]string, 0, len(byName))
		for name := range byName {
			names = append(names, name)
		}
		locals, err := f.Ix.GetMany(names)
		if err != nil {
			return nil, err
		}
		for name, fi := range locals {
			a := byName[name]
			if a == nil {
				continue
			}
			a.Size = fi.Size
			a.Modified = fi.ModifiedS
			a.Type = []string{"file", "dir", "symlink"}[fi.Type]
		}
	}
	// Reconcile stale conflict events: if a folder uses conflict-copy policy
	// and an open conflict event's conflict copy no longer exists (removed
	// outside the resolve flow), clear the event so the badge/attention do
	// not keep showing a conflict that can no longer be acted on.
	if s.n.Events != nil && f.Cfg.ConflictPolicy == "conflict-copy" {
		if cevs, err := s.n.Events.Query(events.Filter{Folder: folder, Category: events.CatConflict, OpenOnly: true}); err == nil {
			for _, ce := range cevs {
				if _, ok := conflictOf[ce.Path]; !ok {
					_ = s.n.Events.ResolveCondition(folder, ce.Path, events.CatConflict, "system")
				}
			}
		}
	}
	// Open warn+ events per file. Events on a live file that has a conflict
	// copy are attached to the copy's row (one row per conflict).
	if s.n.Events != nil {
		evs, err := s.n.Events.Query(events.Filter{Folder: folder, OpenOnly: true})
		if err == nil {
			for _, e := range evs {
				if e.Path == "" || e.Severity < events.SevWarn {
					continue
				}
				a := ensure(e.Path)
				if copyName, ok := conflictOf[e.Path]; ok {
					a = ensure(copyName)
				}
				a.Event = true
				if a.EventCat == "" {
					a.EventCat = string(e.Category)
					a.Reason = e.Reason
				}
			}
		}
	}
	out := make([]AttentionFile, 0, len(byName))
	for _, a := range byName {
		// Direction tag from the flags the loops above already set — no
		// second index/engine pass.
		a.Dir = dirTag(a.Pending, a.PendingUp)
		// Uploads (files a peer needs from us) are informational, not
		// "needs your attention", but they must be available so the ↑
		// upload filter can list them.
		if a.Pending || a.PendingUp || a.Conflict || a.Event {
			out = append(out, *a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// dirTag renders the sync direction tag shared with the wire format
// (protocol.UnsyncedEntry.Dir) and the UI: "down" = we still need the path
// from a peer, "up" = a peer still needs it from us, "both", or "" when
// nothing is pending in either direction (e.g. an event-only row).
func dirTag(down, up bool) string {
	switch {
	case down && up:
		return "both"
	case down:
		return "down"
	case up:
		return "up"
	default:
		return ""
	}
}

// UIUpdateInfo describes the availability of a newer web UI.
type UIUpdateInfo struct {
	Configured      bool   `json:"configured"` // ui_update_url is set
	Current         string `json:"current"`
	Latest          string `json:"latest"`
	UpdateAvailable bool   `json:"update_available"`
	Error           string `json:"error,omitempty"`
}

// fetchUIPayload downloads the UI from the configured URL. http/https and
// file:// URLs are supported (file:// is convenient for local testing and
// peer appdata shares).
func (s *Service) fetchUIPayload() (string, error) {
	if s.UIUpdateURL == "" {
		return "", fmt.Errorf("ui_update_url is not configured")
	}
	if strings.HasPrefix(s.UIUpdateURL, "file://") {
		b, err := os.ReadFile(strings.TrimPrefix(s.UIUpdateURL, "file://"))
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	if !strings.HasPrefix(s.UIUpdateURL, "http://") && !strings.HasPrefix(s.UIUpdateURL, "https://") {
		return "", fmt.Errorf("unsupported ui_update_url scheme (use http://, https:// or file://)")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(s.UIUpdateURL)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("update URL returned %s", resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// CheckUIUpdate fetches the configured update URL and compares versions.
// A fetch failure is reported in Error rather than returned, so the UI can
// still show "check failed" without breaking.
func (s *Service) CheckUIUpdate() UIUpdateInfo {
	info := UIUpdateInfo{
		Configured: s.UIUpdateURL != "",
		Current:    ui.CurrentVersion(s.UIDir),
	}
	if !info.Configured {
		return info
	}
	payload, err := s.fetchUIPayload()
	if err != nil {
		info.Error = err.Error()
		return info
	}
	info.Latest = ui.VersionFrom([]byte(payload))
	if info.Latest == "" {
		info.Error = "fetched payload is not a CrossSync UI"
		return info
	}
	cm, cn, cp := ui.ParseVersion(info.Current)
	lm, ln, lp := ui.ParseVersion(info.Latest)
	info.UpdateAvailable = lm > cm || (lm == cm && (ln > cn || (ln == cn && lp > cp)))
	return info
}

// UpdateUI fetches a newer UI and atomically writes it to ui_dir so the
// override file takes effect immediately (per-request read). Returns the
// new UI version.
func (s *Service) UpdateUI() (string, error) {
	if !s.CheckUIUpdate().Configured {
		return "", fmt.Errorf("ui_update_url is not configured")
	}
	payload, err := s.fetchUIPayload()
	if err != nil {
		return "", err
	}
	if s.UIDir == "" {
		return "", fmt.Errorf("ui_dir is not configured; cannot persist a UI override")
	}
	return ui.WriteOverride(s.UIDir, payload)
}

// CleanVersions removes every archived version in a folder's reserved
// .sfx-versions area (the "cache handling" action for versioned files) and
// reports how much space was freed. Conflict copies are NOT touched.
func (s *Service) CleanVersions(folder string) (map[string]any, error) {
	f, ok := s.n.Folders[folder]
	if !ok {
		return nil, fmt.Errorf("unknown folder %q", folder)
	}
	files, bytes, err := (&sync.Versioning{}).Clean(f.Root)
	if err != nil {
		return nil, err
	}
	return map[string]any{"folder": folder, "removed_files": files, "removed_bytes": bytes}, nil
}

// Versions lists every archived version in a folder's .sfx-versions area,
// with the original file path each version belongs to.
func (s *Service) Versions(folder string) ([]sync.VersionEntry, error) {
	f, ok := s.n.Folders[folder]
	if !ok {
		return nil, fmt.Errorf("unknown folder %q", folder)
	}
	v := f.Engine.Versioning
	if v == nil {
		v = &sync.Versioning{}
	}
	return v.List(f.Root)
}

// RestoreVersion copies an archived version back over the live file and
// records it as a new local change, so the restored content propagates to
// every peer. The archive is kept (you can restore it again later).
func (s *Service) RestoreVersion(folder, path, archivePath string) error {
	f, ok := s.n.Folders[folder]
	if !ok {
		return fmt.Errorf("unknown folder %q", folder)
	}
	if strings.TrimSpace(path) == "" || strings.TrimSpace(archivePath) == "" {
		return fmt.Errorf("path and archive_path are required")
	}
	vroot := filepath.Join(f.Root, sync.VersionsDir)
	src := filepath.Join(vroot, filepath.FromSlash(archivePath))
	rel, err := filepath.Rel(vroot, src)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("archive path escapes the versions area")
	}
	if st, err := os.Stat(src); err != nil || st.IsDir() {
		return fmt.Errorf("archived version not found")
	}
	if err := s.restoreFile(f, path, func() error {
		return copyFileAtomic(src, f.Root, path)
	}, "restored from archived version "+filepath.ToSlash(rel)); err != nil {
		return err
	}
	return nil
}

// RestoreConflict makes a conflict copy the winner: its content is copied
// back over the live file (recorded as a local change that propagates) and
// the conflict copy is removed + tombstoned so the cleanup syncs too.
func (s *Service) RestoreConflict(folder, conflictName string) error {
	f, ok := s.n.Folders[folder]
	if !ok {
		return fmt.Errorf("unknown folder %q", folder)
	}
	if !strings.Contains(conflictName, sync.ConflictSuffix) {
		return fmt.Errorf("not a conflict copy: %q", conflictName)
	}
	orig, ok := sync.ConflictOrigin(conflictName)
	if !ok {
		return fmt.Errorf("cannot derive the original path from %q", conflictName)
	}
	conflictAbs := filepath.Join(f.Root, filepath.FromSlash(conflictName))
	rel, err := filepath.Rel(f.Root, conflictAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path escapes folder root")
	}
	if st, err := os.Stat(conflictAbs); err != nil || st.IsDir() {
		return fmt.Errorf("conflict copy not found")
	}
	if err := s.restoreFile(f, orig, func() error {
		return copyFileAtomic(conflictAbs, f.Root, orig)
	}, "restored from conflict copy "+conflictName); err != nil {
		return err
	}
	if err := os.Remove(conflictAbs); err != nil && !os.IsNotExist(err) {
		return err
	}
	if _, err := f.Engine.ApplyLocalChange(scanner.Deleted, &index.FileInfo{Name: conflictName, Type: index.TypeFile}); err != nil {
		return err
	}
	s.resolveConflictEvents(folder, orig, conflictName)
	return nil
}

// FileCompare describes one side of a conflict for the compare dialog.
type FileCompare struct {
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	Created    int64  `json:"created"`               // unix seconds (file birth time)
	Modified   int64  `json:"modified"`              // unix seconds (last save)
	ProducedBy string `json:"produced_by,omitempty"` // device that last wrote it
	History    string `json:"history,omitempty"`     // version-vector edit history
	SHA256     string `json:"sha256"`
	Preview    string `json:"preview"` // first bytes (text) or hex marker
	Exists     bool   `json:"exists"`
}

// ConflictCompare is the data for the compare popup.
type ConflictCompare struct {
	Current   *FileCompare `json:"current"`  // the live winner on this server
	Conflict  *FileCompare `json:"conflict"` // the alternative version (the copy)
	Identical bool         `json:"identical"`
}

// CompareConflict returns both sides of a conflict (live file vs copy) so
// the UI can show which is which, their stats, hashes and content previews.
func (s *Service) CompareConflict(folder, name string) (*ConflictCompare, error) {
	f, ok := s.n.Folders[folder]
	if !ok {
		return nil, fmt.Errorf("unknown folder %q", folder)
	}
	if !strings.Contains(name, sync.ConflictSuffix) {
		return nil, fmt.Errorf("not a conflict copy: %q", name)
	}
	orig, ok := sync.ConflictOrigin(name)
	if !ok {
		return nil, fmt.Errorf("cannot derive the original path from %q", name)
	}
	cc := &ConflictCompare{
		Current:  compareFile(orig, filepath.Join(f.Root, filepath.FromSlash(orig))),
		Conflict: compareFile(name, filepath.Join(f.Root, filepath.FromSlash(name))),
	}
	// Who produced each side? From the version vector in the index.
	if fi, ok, err := f.Ix.Get(orig); err == nil && ok && !fi.Deleted {
		cc.Current.ProducedBy, cc.Current.History = s.vectorSummary(fi.Version)
	}
	if fi, ok, err := f.Ix.Get(name); err == nil && ok && !fi.Deleted {
		cc.Conflict.ProducedBy, cc.Conflict.History = s.vectorSummary(fi.Version)
	}
	if cc.Current != nil && cc.Conflict != nil && cc.Current.Exists && cc.Conflict.Exists {
		cc.Identical = cc.Current.SHA256 == cc.Conflict.SHA256 && cc.Current.SHA256 != ""
	}
	return cc, nil
}

// vectorSummary turns a version vector into "last writer" and an edit
// history string, resolving device ids to names.
func (s *Service) vectorSummary(v version.Vector) (producedBy, history string) {
	if len(v) == 0 {
		return "unknown", ""
	}
	devices := make([]uint64, 0, len(v))
	for id := range v {
		devices = append(devices, id)
	}
	sort.Slice(devices, func(i, j int) bool { return v[devices[i]] > v[devices[j]] })
	producedBy = s.deviceName(devices[0])
	parts := make([]string, 0, len(devices))
	for _, id := range devices {
		parts = append(parts, fmt.Sprintf("%s (×%d)", s.deviceName(id), v[id]))
	}
	return producedBy, strings.Join(parts, ", ")
}

// deviceName resolves a device id to a friendly name (self or a peer).
func (s *Service) deviceName(id uint64) string {
	if id == s.n.ID {
		return s.n.Name + " (this server)"
	}
	for _, p := range s.n.Cfg.Peers {
		if p.ID == id {
			return p.Name
		}
	}
	return fmt.Sprintf("device %d", id)
}

// peerName returns the configured name of a peer id ("" if not a peer).
func (s *Service) peerName(id uint64) string {
	for _, p := range s.n.Cfg.Peers {
		if p.ID == id {
			return p.Name
		}
	}
	return ""
}

// resolvePeer accepts either a numeric device id or a configured peer name
// and returns the peer id (0 if unknown). Name matching lets the web UI
// filter by peer without round-tripping uint64 ids through JSON/JS.
func (s *Service) resolvePeer(v string) uint64 {
	if id, err := strconv.ParseUint(v, 10, 64); err == nil && id != 0 {
		for _, p := range s.n.Cfg.Peers {
			if p.ID == id {
				return id
			}
		}
		return 0
	}
	for _, p := range s.n.Cfg.Peers {
		if strings.EqualFold(p.Name, v) {
			return p.ID
		}
	}
	return 0
}

// isSelf reports whether v refers to THIS device (by configured name or id).
// The web UI filters local events by the device name (it cannot round-trip
// uint64 ids through JSON/JS exactly).
func (s *Service) isSelf(v string) bool {
	d := s.n.Cfg.Device
	if strings.EqualFold(d.Name, v) {
		return true
	}
	if id, err := strconv.ParseUint(v, 10, 64); err == nil && id != 0 {
		return id == d.ID
	}
	return false
}

// resolveConflictEvents settles open conflict conditions once a conflict is
// resolved (copy deleted or restored), so the badge/attention clears.
func (s *Service) resolveConflictEvents(folder, orig, copyName string) {
	if s.n.Events == nil {
		return
	}
	_ = s.n.Events.ResolveCondition(folder, orig, events.CatConflict, "system")
	_ = s.n.Events.ResolveCondition(folder, copyName, events.CatConflict, "system")
}

// compareFile stats + hashes a file for the compare dialog.
func compareFile(name, abs string) *FileCompare {
	fc := &FileCompare{Name: name}
	st, err := os.Stat(abs)
	if err != nil {
		return fc
	}
	fc.Exists = true
	fc.Size = st.Size()
	fc.Modified = st.ModTime().Unix()
	fc.Created = birthTime(st).Unix()
	if sum, err := sha256File(abs); err == nil {
		fc.SHA256 = sum
	}
	fc.Preview = previewText(abs)
	return fc
}

// sha256File returns the hex SHA-256 of a file's content.
func sha256File(abs string) (string, error) {
	f, err := os.Open(abs)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// previewText returns the first bytes of a file as text (or a hex marker
// for binary content).
func previewText(abs string) string {
	f, err := os.Open(abs)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, 256)
	n, _ := io.ReadFull(f, buf)
	buf = buf[:n]
	printable := true
	for _, b := range buf {
		if b < 0x20 && b != '\n' && b != '\r' && b != '\t' {
			printable = false
			break
		}
	}
	if printable {
		return string(buf)
	}
	if len(buf) > 32 {
		buf = buf[:32]
	}
	return "[binary] " + hex.EncodeToString(buf)
}

// restoreFile copies the source into root/path (atomic same-dir rename),
// indexes it as a fresh local change (so it syncs), hashes it, and records
// an applied event with the given reason.
func (s *Service) restoreFile(f *node.Folder, path string, write func() error, reason string) error {
	target := filepath.Join(f.Root, filepath.FromSlash(path))
	rel, err := filepath.Rel(f.Root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path escapes folder root")
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if err := write(); err != nil {
		return err
	}
	st, err := os.Stat(target)
	if err != nil {
		return err
	}
	mt := st.ModTime()
	blockSize, blocks, err := hash.FileHashes(target, 0)
	if err != nil {
		return fmt.Errorf("hashing restored file: %w", err)
	}
	if _, err := f.Engine.ApplyLocalChange(scanner.Modified, &index.FileInfo{
		Name:       filepath.ToSlash(rel),
		Size:       st.Size(),
		ModifiedS:  mt.Unix(),
		ModifiedNs: int32(mt.Nanosecond()),
		Mode:       uint32(st.Mode().Perm()),
		Type:       index.TypeFile,
		BlockSize:  int32(blockSize),
		Blocks:     blocks,
	}); err != nil {
		return err
	}
	s.n.Record(&events.Event{
		TS: time.Now(), Folder: f.Cfg.ID, Category: events.CatApplied, Severity: events.SevInfo,
		Path: filepath.ToSlash(rel), Reason: reason,
	})
	return nil
}

// copyFileAtomic copies src into root/rel via a same-directory temp file
// (`.sfx-restore-*`, suppressed by the watcher) and an atomic rename.
func copyFileAtomic(src, root, rel string) error {
	dst := filepath.Join(root, filepath.FromSlash(rel))
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".sfx-restore-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, dst)
}

// DeleteConflict removes a conflict copy (propagating the deletion to all
// peers). The name is validated to be a conflict copy inside the folder.
func (s *Service) DeleteConflict(folder, name string) error {
	return s.n.RemoveConflict(folder, name)
}

// ResolveAllConflicts deletes every conflict copy in a folder (the current
// files stay the winners; each deletion propagates) and reports how many
// were resolved.
func (s *Service) ResolveAllConflicts(folder string) (int, error) {
	f, ok := s.n.Folders[folder]
	if !ok {
		return 0, fmt.Errorf("unknown folder %q", folder)
	}
	var names []string
	if err := f.Ix.List(func(fi *index.FileInfo) error {
		if !fi.Deleted && strings.Contains(fi.Name, sync.ConflictSuffix) {
			names = append(names, fi.Name)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	for _, name := range names {
		if err := s.n.RemoveConflict(folder, name); err != nil {
			return 0, err
		}
	}
	return len(names), nil
}

// RestoreHeld moves one held (soft-deleted) path back to its original location.
// The file becomes a normal local change again and re-syncs to every peer,
// including the device that originally deleted it.
func (s *Service) RestoreHeld(folder, path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("path is required")
	}
	return s.n.RestoreHeld(folder, path)
}

// DeleteHeld purges a held (soft-deleted) path for real, giving up its undo
// window. An empty path purges every held entry for the folder.
func (s *Service) DeleteHeld(folder, path string) (int, error) {
	return s.n.DeleteHeld(folder, path)
}

// Hold returns the folder's held deletions (nil when nothing is held).
func (s *Service) Hold(folder string) (*FolderHold, error) {
	entries, err := s.n.HeldFiles(folder)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	h := &FolderHold{Count: len(entries), Entries: make([]HeldFile, 0, len(entries))}
	for _, e := range entries {
		h.Bytes += e.Size
		h.Entries = append(h.Entries, HeldFile{
			Path: e.Path, Size: e.Size,
			HeldAt: e.HeldAt.Unix(), ExpiresAt: e.ExpiresAt().Unix(),
		})
	}
	return h, nil
}

// ApplyDeletions is the legacy operator override for the deletion guard: it
// applies every currently-pending deletion for the folder locally. The guard no
// longer blocks over-threshold passes (they hold their copies instead), so
// nothing is normally pending; the endpoint stays for leftover state.
func (s *Service) ApplyDeletions(folder string) (int, error) {
	if _, ok := s.n.Folders[folder]; !ok {
		return 0, fmt.Errorf("unknown folder %q", folder)
	}
	return s.n.ApplyPendingDeletions(folder)
}

// SetAutoArchive turns routine event-history auto-pruning on/off and sets
// the age (seconds) after which routine logs are removed. Open
// attention/conflict events are never auto-pruned regardless of age. The
// change applies immediately and is persisted to the config file.
func (s *Service) SetAutoArchive(enabled bool, olderThan int64) error {
	if olderThan < 0 {
		return fmt.Errorf("older_than must be >= 0")
	}
	s.n.Cfg.AutoArchiveEvents = enabled
	s.n.Cfg.AutoArchiveOlderThan = olderThan
	if s.ConfigPath == "" {
		return nil
	}
	return s.persistSettings()
}

// SetAlertURL configures (or clears, with "") the page-worthy notification
// endpoint. It applies immediately and is persisted to the config file.
func (s *Service) SetAlertURL(url string) error {
	s.n.Cfg.AlertURL = url
	if url == "" {
		s.n.Alert = nil
	} else {
		s.n.Alert = alert.New(url)
	}
	if s.ConfigPath == "" {
		return nil
	}
	return s.persistSettings()
}

// SendTestAlert fires a test notification through the configured endpoint.
func (s *Service) SendTestAlert() error {
	return s.n.SendTestAlert()
}

// persistSettings rewrites the two auto-archive keys in the config file,
// preserving comments and every other section.
func (s *Service) persistSettings() error {
	if s.ConfigPath == "" {
		return nil
	}
	data, err := os.ReadFile(s.ConfigPath)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}
	root := &doc
	if len(doc.Content) > 0 {
		root = doc.Content[0]
	}
	setBool := func(key string, v bool) {
		setKey(root, key, &yaml.Node{Kind: yaml.ScalarNode, Value: strconv.FormatBool(v)})
	}
	setInt := func(key string, v int64) {
		setKey(root, key, &yaml.Node{Kind: yaml.ScalarNode, Value: strconv.FormatInt(v, 10)})
	}
	setBool("auto_archive_events", s.n.Cfg.AutoArchiveEvents)
	setInt("auto_archive_older_than", s.n.Cfg.AutoArchiveOlderThan)
	setKey(root, "alert_url", &yaml.Node{Kind: yaml.ScalarNode, Value: s.n.Cfg.AlertURL})
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return err
	}
	return os.WriteFile(s.ConfigPath, out, 0o600)
}

// Events queries the durable event store.
func (s *Service) Events(f events.Filter) ([]events.Event, error) {
	if s.n.Events == nil {
		return nil, fmt.Errorf("event store not available")
	}
	return s.n.Events.Query(f)
}

// Ack acknowledges an event.
func (s *Service) Ack(id int64, by string) error {
	if s.n.Events == nil {
		return fmt.Errorf("event store not available")
	}
	if by == "" {
		by = s.n.Name
	}
	return s.n.Events.Acknowledge(id, by)
}

// AckCondition acknowledges every open occurrence of a condition at once.
func (s *Service) AckCondition(folder, path string, cat events.Category, by string) error {
	if s.n.Events == nil {
		return fmt.Errorf("event store not available")
	}
	if by == "" {
		by = s.n.Name
	}
	return s.n.Events.AcknowledgeCondition(folder, path, cat, by)
}

// ArchiveResult describes an event-history archive operation.
type ArchiveResult struct {
	Deleted     int    `json:"deleted"`      // events permanently removed
	DeletedOpen int    `json:"deleted_open"` // open attention events among the deleted (only >0 when includeOpen)
	KeptOpen    int    `json:"kept_open"`    // unviewed attention events preserved
	Cutoff      int64  `json:"cutoff"`       // unix seconds cutoff actually used
	CutoffText  string `json:"cutoff_text"`  // human-readable cutoff
}

// archiveCutoff resolves the cutoff for an archive request. olderThan==0
// means "older than the oldest open attention event" (the smart mode that
// never archives anything still needing attention); otherwise it is
// now-olderThan.
func (s *Service) archiveCutoff(olderThan int64) (time.Time, string) {
	if olderThan > 0 {
		return time.Now().Add(-time.Duration(olderThan) * time.Second),
			"older than " + (time.Duration(olderThan) * time.Second).String()
	}
	if t, ok := s.n.Events.OldestOpenWarn(); ok {
		return t, "older than the oldest open attention event"
	}
	return time.Now(), "all events (no open attention)"
}

// ArchivePreview reports what an archive would delete without doing it, so
// the UI can warn before confirming (especially about unviewed attention).
func (s *Service) ArchivePreview(olderThan int64, includeOpen bool) (*ArchiveResult, error) {
	if s.n.Events == nil {
		return nil, fmt.Errorf("event store not available")
	}
	cutoff, label := s.archiveCutoff(olderThan)
	older, openOlder, err := s.n.Events.CountArchive(cutoff)
	if err != nil {
		return nil, err
	}
	kept, del, delOpen := 0, older, 0
	if !includeOpen {
		kept = openOlder
		del = older - openOlder
	} else {
		delOpen = openOlder
	}
	return &ArchiveResult{Deleted: del, DeletedOpen: delOpen, KeptOpen: kept,
		Cutoff: cutoff.Unix(), CutoffText: label}, nil
}

// ArchiveEvents permanently removes events older than the cutoff. With
// includeOpen=false (default) open attention events are preserved; the
// caller is responsible for warning the user when includeOpen=true, and
// DeletedOpen reports exactly how many dismissed-but-unviewed notes would be
// lost so the warning can carry a real count.
func (s *Service) ArchiveEvents(olderThan int64, includeOpen bool) (*ArchiveResult, error) {
	if s.n.Events == nil {
		return nil, fmt.Errorf("event store not available")
	}
	cutoff, label := s.archiveCutoff(olderThan)
	deleted, deletedOpen, keptOpen, err := s.n.Events.Archive(cutoff, includeOpen)
	if err != nil {
		return nil, err
	}
	return &ArchiveResult{Deleted: deleted, DeletedOpen: deletedOpen, KeptOpen: keptOpen,
		Cutoff: cutoff.Unix(), CutoffText: label}, nil
}

// Rescan scans the given folder (or all folders when folder is empty) and
// returns the number of changes applied per folder. If a scan is already
// running for a folder (ticker, watcher, or a previous manual rescan), the
// overlapping request is rejected instead of queueing behind it.
func (s *Service) Rescan(folder string) (map[string]int, error) {
	return s.RescanVerify(folder, false)
}

// RescanVerify is Rescan with the verified flag: verify=true re-hashes every
// file in the folder instead of trusting (size, mtime), catching content
// changes that preserved both (rsync -a, restore tools, coarse SMB timestamps).
// Unchanged files are adopted with their new stat and no version bump, so a
// verified scan never re-transfers data to peers.
//
// A folder that is paused right now is not scanned and not refused either: the
// request is QUEUED and runs once when the folder is resumed. Silently eating it
// is the failure this exists to prevent — a verified pass reports "0 content
// changes", which is indistinguishable from "everything is fine" when in fact
// nothing was read.
func (s *Service) RescanVerify(folder string, verify bool) (map[string]int, error) {
	out := map[string]int{}
	ids := make([]string, 0, len(s.n.Folders))
	if folder != "" {
		if _, ok := s.n.Folders[folder]; !ok {
			return nil, fmt.Errorf("unknown folder %q", folder)
		}
		ids = append(ids, folder)
	} else {
		for id := range s.n.Folders {
			ids = append(ids, id)
		}
		sort.Strings(ids)
	}
	for _, id := range ids {
		queued, applied, err := s.n.RequestRescan(id, verify)
		if err != nil {
			return out, err
		}
		if queued {
			// Remembered for the resume; it is not a result for this request.
			out[id] = -1
			continue
		}
		out[id] = applied
	}
	return out, nil
}

// ErrBusy means the request is valid but cannot run right now: a scan or sync
// is already in flight, or the folder is paused. It maps to 409 Conflict, not
// 500 -- "try again in a moment" is not a server fault, and answering 500 made
// a routine overlap look like a crash.
var ErrBusy = errors.New("busy")

// ErrUnavailable means the work could not start because a peer is unreachable.
// It maps to 503 with the underlying dial error, so the operator sees
// "peer unreachable: dial tcp 100.x:55557: connect: connection refused"
// instead of a bare status code.
var ErrUnavailable = errors.New("peer unavailable")

// SyncNow runs a one-shot sync with the given peer (0 = all peers). It is
// the same path as the CLI's sync subcommand: rescan first, then sync.
func (s *Service) SyncNow(peerID uint64) error {
	if s.n.SyncRunning() {
		return fmt.Errorf("%w: a sync is already running; retry when it finishes", ErrBusy)
	}
	if err := s.n.ScanAll(); err != nil {
		return err
	}
	if peerID != 0 {
		if _, err := s.n.SyncWithPeer(peerID); err != nil {
			return fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		return nil
	}
	if err := s.n.SyncAllPeers(); err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return nil
}

// FolderSync rescans one folder then syncs with every peer (the session
// exchanges all shared folders; the rescan ensures this folder's changes
// are indexed first). Overlapping manual syncs and in-progress scans are
// rejected with a friendly error instead of queuing.
func (s *Service) FolderSync(id string) error {
	if _, ok := s.n.Folders[id]; !ok {
		return fmt.Errorf("unknown folder %q", id)
	}
	if s.n.SyncRunning() {
		return fmt.Errorf("%w: a sync is already running; retry when it finishes", ErrBusy)
	}
	if s.n.ScanRunning(id) {
		return fmt.Errorf("%w: scan already in progress for folder %q", ErrBusy, id)
	}
	if _, err := s.n.ScanFolder(id); err != nil {
		if errors.Is(err, node.ErrFolderPaused) {
			return fmt.Errorf("%w: folder %q is paused - resume it to scan or sync", ErrBusy, id)
		}
		return err
	}
	if err := s.n.SyncAllPeers(); err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return nil
}

// validateFolderDirection accepts the known per-device sync directions. An
// empty direction means send-receive: the zero value keeps old configs and
// API callers on today's bidirectional behavior.
func validateFolderDirection(d string) error {
	switch d {
	case "", "send-receive", "send-only", "receive-only":
		return nil
	}
	return fmt.Errorf("unknown direction %q", d)
}

// AddFolder registers a new folder (id + share path + conflict policy) and
// persists it to the config file.
func (s *Service) AddFolder(id, name, path, policy string, peers []uint64, direction string, include, ignore []string) (FolderStatus, error) {
	if strings.TrimSpace(id) == "" && strings.TrimSpace(name) == "" {
		return FolderStatus{}, fmt.Errorf("id or name is required")
	}
	if strings.TrimSpace(path) == "" {
		return FolderStatus{}, fmt.Errorf("path is required")
	}
	for _, p := range s.n.Cfg.Folders {
		if p.ID == id {
			return FolderStatus{}, fmt.Errorf("folder %q already exists", id)
		}
	}
	if !filepath.IsAbs(path) && !strings.HasPrefix(path, "/") {
		return FolderStatus{}, fmt.Errorf("path must be absolute")
	}
	if err := config.ValidateSharePath(path); err != nil {
		return FolderStatus{}, err
	}
	if policy == "" {
		policy = "conflict-copy"
	}
	switch policy {
	case "conflict-copy", "versioning":
	default:
		return FolderStatus{}, fmt.Errorf("unknown conflict policy %q", policy)
	}
	if err := validateFolderDirection(direction); err != nil {
		return FolderStatus{}, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		// No id supplied: generate a unique one. The folder is keyed by id
		// across servers, so it must not collide with an existing folder.
		id = s.generateFolderID()
	}
	fc := config.Folder{ID: id, Name: strings.TrimSpace(name), Path: path, ConflictPolicy: policy,
		Peers: peers, Direction: direction, Include: include, Ignore: ignore}
	if _, err := s.n.AddFolder(fc); err != nil {
		return FolderStatus{}, err
	}
	s.n.Cfg.Folders = append(s.n.Cfg.Folders, fc)
	if err := s.persistFolders(); err != nil {
		return FolderStatus{}, fmt.Errorf("folder added but config not persisted: %w", err)
	}
	return s.folderStatus(id), nil
}

// SetFolderPaused pauses or resumes one folder on this device: no scans and no
// sync with any peer while paused (the folder is not advertised, so peers stop
// expecting it), with the index preserved so resuming reconciles normally. The
// setting is per-device and persisted, so a restart keeps the folder paused.
//
// The answer is the minimal FolderPauseStatus: this request must be instant (the
// operator is trying to stop disk I/O), so it does no SQL, no statfs, no folder
// walk and takes no folder lock.
func (s *Service) SetFolderPaused(id string, paused bool) (FolderPauseStatus, error) {
	if _, ok := s.n.Folders[id]; !ok {
		return FolderPauseStatus{}, fmt.Errorf("folder %q not found", id)
	}
	if err := s.n.SetFolderPaused(id, paused); err != nil {
		return FolderPauseStatus{}, err
	}
	// Resuming the folder means "run normally", which includes a transfer pause
	// set earlier: an operator who paused the link, then paused everything, then
	// resumed everything expects the share to sync again.
	if !paused {
		if err := s.n.SetFolderTransfersPaused(id, false); err != nil {
			return FolderPauseStatus{}, err
		}
	}
	for i := range s.n.Cfg.Folders {
		if s.n.Cfg.Folders[i].ID == id {
			s.n.Cfg.Folders[i].Paused = paused
			if !paused {
				s.n.Cfg.Folders[i].TransferPaused = false
			}
		}
	}
	if err := s.persistFolders(); err != nil {
		return FolderPauseStatus{}, fmt.Errorf("folder %s paused state not saved: %w", id, err)
	}
	if !paused {
		// Changes made while the folder was paused were never observed: scanning
		// is stopped, and on a share other machines write to over SMB the watcher
		// sees nothing either. Resuming therefore has to LOOK, not assume — queue
		// a full pass now instead of waiting for the next scheduled walk (up to
		// scan.full_max, fifteen minutes). Without this, a resume reports a folder
		// as settled from an index that predates everything done during the pause,
		// which is exactly the "it says in sync but it is not" failure.
		// Asynchronously: a 30 GB share takes minutes, and the pause endpoint is
		// the request whose whole purpose is to answer immediately.
		go func(id string) {
			if _, _, err := s.n.RequestRescan(id, false); err != nil {
				// A scan already running for this folder is not a problem: it will
				// observe the same tree. Anything else is worth a log line.
				s.n.Logf("folder %s: rescan on resume: %v", id, err)
			}
		}(id)
	}
	return s.pauseStatus(id), nil
}

// SetFolderTransfersPaused stops or allows data movement for one folder while
// leaving scanning on: the index stays current, the folder stays advertised, and
// resuming needs no re-walk. Persisted, so a restart keeps the link parked.
func (s *Service) SetFolderTransfersPaused(id string, paused bool) (FolderPauseStatus, error) {
	if _, ok := s.n.Folders[id]; !ok {
		return FolderPauseStatus{}, fmt.Errorf("folder %q not found", id)
	}
	if err := s.n.SetFolderTransfersPaused(id, paused); err != nil {
		return FolderPauseStatus{}, err
	}
	for i := range s.n.Cfg.Folders {
		if s.n.Cfg.Folders[i].ID == id {
			s.n.Cfg.Folders[i].TransferPaused = paused
		}
	}
	if err := s.persistFolders(); err != nil {
		return FolderPauseStatus{}, fmt.Errorf("folder %s transfer pause not saved: %w", id, err)
	}
	return s.pauseStatus(id), nil
}

// pauseStatus builds the minimal pause answer: atomics only — no SQL, no statfs,
// no directory walk, no folder lock.
func (s *Service) pauseStatus(id string) FolderPauseStatus {
	f, ok := s.n.Folders[id]
	if !ok {
		return FolderPauseStatus{}
	}
	return FolderPauseStatus{
		ID:             id,
		Paused:         f.Paused(),
		TransferPaused: f.TransfersPaused() && !f.Paused(),
	}
}

// UpdateFolder changes a folder's name, path, sync direction and
// include/ignore patterns. The folder id (the sync key shared with peers)
// never changes, so pairing is untouched. The index is preserved and the
// folder is re-scanned immediately so it reflects the new root.
func (s *Service) UpdateFolder(id, name, path string, direction string, include, ignore []string) (FolderStatus, error) {
	f, ok := s.n.Folders[id]
	if !ok {
		return FolderStatus{}, fmt.Errorf("folder %q not found", id)
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return FolderStatus{}, fmt.Errorf("path is required")
	}
	if !filepath.IsAbs(path) && !strings.HasPrefix(path, "/") {
		return FolderStatus{}, fmt.Errorf("path must be absolute")
	}
	if err := config.ValidateSharePath(path); err != nil {
		return FolderStatus{}, err
	}
	for _, o := range s.n.Cfg.Folders {
		if o.ID != id && filepath.Clean(o.Path) == filepath.Clean(path) {
			return FolderStatus{}, fmt.Errorf("another folder already uses path %q", path)
		}
	}
	if err := validateFolderDirection(direction); err != nil {
		return FolderStatus{}, err
	}
	// Direction and the pattern lists are per-device settings: they go into
	// the live folder so the next scan/session honors them, and into the
	// config row that persistFolders writes back.
	fc := f.Cfg
	fc.Name = strings.TrimSpace(name)
	fc.Path = path
	fc.Direction = direction
	fc.Include = include
	fc.Ignore = ignore
	if _, err := s.n.UpdateFolder(id, fc); err != nil {
		return FolderStatus{}, err
	}
	for i := range s.n.Cfg.Folders {
		if s.n.Cfg.Folders[i].ID == id {
			s.n.Cfg.Folders[i] = fc
		}
	}
	if err := s.persistFolders(); err != nil {
		return FolderStatus{}, fmt.Errorf("folder updated but config not persisted: %w", err)
	}
	// Re-scan immediately so the UI shows the new root's contents (and a
	// sync that runs right after honors the moved path). A scan already in
	// progress is fine — the periodic ticker catches up.
	if !s.n.ScanRunning(id) {
		if _, err := s.n.ScanFolder(id); err != nil {
			// The update itself succeeded; log the scan failure rather than
			// failing the whole request.
			s.n.Logf("update folder %s: rescan: %v", id, err)
		}
	}
	return s.folderStatus(id), nil
}

// generateFolderID returns a new unique folder id: a compact random string
// that does not collide with any existing folder id on this node.
func (s *Service) generateFolderID() string {
	exists := func(id string) bool {
		for _, f := range s.n.Cfg.Folders {
			if f.ID == id {
				return true
			}
		}
		return false
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	const n = 16
	buf := make([]byte, n)
	rb := make([]byte, n)
	for {
		if _, err := rand.Read(rb); err != nil {
			// crypto/rand failure is effectively fatal; fall back to a
			// time+pid based id rather than looping forever.
			return fmt.Sprintf("f%d", time.Now().UnixNano())
		}
		for i := range buf {
			buf[i] = alphabet[int(rb[i])%len(alphabet)]
		}
		id := string(buf)
		if !exists(id) {
			return id
		}
	}
}

// SetFolderPeers updates which peers a folder syncs with (empty = all peers)
// and persists the change. The live node folder is updated so the next sync
// session honors the new scoping immediately.
func (s *Service) SetFolderPeers(id string, peers []uint64) error {
	f, ok := s.n.Folders[id]
	if !ok {
		return fmt.Errorf("folder %q not found", id)
	}
	f.Cfg.Peers = peers
	for i := range s.n.Cfg.Folders {
		if s.n.Cfg.Folders[i].ID == id {
			s.n.Cfg.Folders[i].Peers = peers
		}
	}
	return s.persistFolders()
}

// resolvePeers resolves a list of peer references (configured names or
// numeric device ids) to device ids, erroring on any unknown reference or
// on this device itself (a folder never syncs with itself).
func (s *Service) resolvePeers(refs []string) ([]uint64, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	var out []uint64
	for _, ref := range refs {
		if s.isSelf(ref) {
			return nil, fmt.Errorf("peer %q is this device", ref)
		}
		id := s.resolvePeer(ref)
		if id == 0 {
			return nil, fmt.Errorf("unknown peer %q", ref)
		}
		out = append(out, id)
	}
	return out, nil
}

// RemoveFolder removes a folder from the node and the config file.
func (s *Service) RemoveFolder(id string) error {
	if err := s.n.RemoveFolder(id); err != nil {
		return err
	}
	kept := s.n.Cfg.Folders[:0]
	for _, f := range s.n.Cfg.Folders {
		if f.ID != id {
			kept = append(kept, f)
		}
	}
	s.n.Cfg.Folders = kept
	return s.persistFolders()
}

// AddPeer adds a peer to the live node (scheduler, TLS allowlist, config)
// and persists it to the config file — no restart. id 0 generates a unique
// id that does not collide with the device or existing peers.
func (s *Service) AddPeer(id uint64, name string, addresses []string, fingerprint string) (config.Peer, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return config.Peer{}, fmt.Errorf("peer name is required")
	}
	if id == 0 {
		id = s.generatePeerID()
	}
	if err := s.validatePeer(0, config.Peer{ID: id, Name: name, Addresses: addresses, Fingerprint: fingerprint}); err != nil {
		return config.Peer{}, err
	}
	peers := append([]config.Peer{}, s.n.Cfg.Peers...)
	peers = append(peers, config.Peer{ID: id, Name: name, Addresses: addresses, Fingerprint: strings.ToLower(strings.TrimSpace(fingerprint))})
	s.n.ApplyPeers(peers)
	if err := s.persistPeers(); err != nil {
		return config.Peer{}, fmt.Errorf("peer added but config not persisted: %w", err)
	}
	return peers[len(peers)-1], nil
}

// UpdatePeer changes a peer's name/addresses/fingerprint live (address
// changes take effect on the next dial; fingerprint changes update the TLS
// allowlist) and persists the change.
func (s *Service) UpdatePeer(id uint64, name string, addresses []string, fingerprint string) (config.Peer, error) {
	peers := append([]config.Peer{}, s.n.Cfg.Peers...)
	idx := -1
	for i := range peers {
		if peers[i].ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return config.Peer{}, fmt.Errorf("peer %d not found", id)
	}
	if name == "" {
		name = peers[idx].Name
	}
	if addresses == nil {
		addresses = peers[idx].Addresses
	}
	if err := s.validatePeer(id, config.Peer{ID: id, Name: name, Addresses: addresses, Fingerprint: fingerprint}); err != nil {
		return config.Peer{}, err
	}
	peers[idx] = config.Peer{ID: id, Name: name, Addresses: addresses, Fingerprint: strings.ToLower(strings.TrimSpace(fingerprint))}
	s.n.ApplyPeers(peers)
	if err := s.persistPeers(); err != nil {
		return config.Peer{}, fmt.Errorf("peer updated but config not persisted: %w", err)
	}
	return peers[idx], nil
}

// RemovePeer deletes a peer live: the scheduler stops dialing it (backoff
// state pruned) and its fingerprint is removed from the TLS allowlist.
func (s *Service) RemovePeer(id uint64) error {
	peers := append([]config.Peer{}, s.n.Cfg.Peers...)
	kept := peers[:0]
	found := false
	for _, p := range peers {
		if p.ID == id {
			found = true
			continue
		}
		kept = append(kept, p)
	}
	if !found {
		return fmt.Errorf("peer %d not found", id)
	}
	s.n.ApplyPeers(kept)
	if err := s.persistPeers(); err != nil {
		return err
	}
	// Drop the peer's persisted delta markers and in-memory views so
	// removing a peer leaves no state behind (a re-add re-exchanges from
	// scratch instead of reusing a stale marker).
	s.n.CleanupPeerState(id)
	return nil
}

// validatePeer checks one peer entry the same way config validation does,
// plus two peer-API rules: the id must be unique and not this device's id.
// excludeID is the peer id being edited (its own entry is skipped by the
// duplicate check); pass 0 for pure adds.
func (s *Service) validatePeer(excludeID uint64, p config.Peer) error {
	if p.ID == 0 {
		return fmt.Errorf("peer.id is required")
	}
	if p.ID == s.n.Cfg.Device.ID {
		return fmt.Errorf("peer id %d is this device's id", p.ID)
	}
	for _, o := range s.n.Cfg.Peers {
		if o.ID != excludeID && o.ID == p.ID {
			return fmt.Errorf("peer id %d already exists (peer %q)", p.ID, o.Name)
		}
	}
	if strings.TrimSpace(p.Name) == "" {
		return fmt.Errorf("peer name is required")
	}
	if len(p.Addresses) == 0 {
		return fmt.Errorf("at least one address required")
	}
	for _, a := range p.Addresses {
		if strings.TrimSpace(a) == "" {
			return fmt.Errorf("empty address")
		}
		if _, _, err := net.SplitHostPort(a); err != nil {
			return fmt.Errorf("address %q must be host:port (missing port)", a)
		}
	}
	fp := strings.ToLower(strings.TrimSpace(p.Fingerprint))
	if fp != "" && !validFingerprintLocal(fp) {
		return fmt.Errorf("fingerprint must be 64 lowercase hex characters")
	}
	return nil
}

// validFingerprintLocal mirrors config's fingerprint check: 64 lowercase
// hex characters (SHA-256 of the DER certificate).
func validFingerprintLocal(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// generatePeerID returns an unused device id: a random 48-bit value in the
// same range the device ids use (keeps ids JSON-safe in the web UI).
func (s *Service) generatePeerID() uint64 {
	exists := func(id uint64) bool {
		if id == s.n.Cfg.Device.ID {
			return true
		}
		for _, p := range s.n.Cfg.Peers {
			if p.ID == id {
				return true
			}
		}
		return false
	}
	var id uint64
	for {
		b := make([]byte, 8)
		if _, err := rand.Read(b); err != nil {
			// crypto/rand failure is effectively fatal; fall back to a
			// time-based id rather than looping forever.
			return uint64(time.Now().UnixNano() % (1 << 48))
		}
		// Mask to 48 bits: stays in the same range as device ids and
		// remains JSON-safe in the web UI.
		id = binary.BigEndian.Uint64(b) >> 16
		if !exists(id) {
			return id
		}
	}
}

// persistPeers rewrites only the `peers:` section of the config file
// (preserving comments and all other sections) with the current peer list.
// It is a no-op if no config path was set.
func (s *Service) persistPeers() error {
	if s.ConfigPath == "" {
		return nil
	}
	data, err := os.ReadFile(s.ConfigPath)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}
	var list yaml.Node
	raw, err := yaml.Marshal(s.n.Cfg.Peers)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(raw, &list); err != nil {
		return err
	}
	if list.Kind == yaml.DocumentNode && len(list.Content) > 0 {
		list = *list.Content[0]
	}
	root := &doc
	if len(doc.Content) > 0 {
		root = doc.Content[0]
	}
	setKey(root, "peers", &list)
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return err
	}
	return os.WriteFile(s.ConfigPath, out, 0o600)
}

func (s *Service) folderStatus(id string) FolderStatus {
	for _, f := range s.Folders() {
		if f.ID == id {
			return f
		}
	}
	return FolderStatus{}
}

// persistFolders rewrites only the `folders:` section of the config file
// (preserving comments and all other sections) with the current folder
// list. It is a no-op if no config path was set.
func (s *Service) persistFolders() error {
	if s.ConfigPath == "" {
		return nil
	}
	data, err := os.ReadFile(s.ConfigPath)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}
	// Build a node for the folders list from the current config.
	var list yaml.Node
	raw, err := yaml.Marshal(s.n.Cfg.Folders)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(raw, &list); err != nil {
		return err
	}
	if list.Kind == yaml.DocumentNode && len(list.Content) > 0 {
		list = *list.Content[0]
	}
	root := &doc
	if len(doc.Content) > 0 {
		root = doc.Content[0]
	}
	setKey(root, "folders", &list)
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return err
	}
	return os.WriteFile(s.ConfigPath, out, 0o600)
}

// setKey sets or replaces a key in a mapping node.
func setKey(m *yaml.Node, key string, value *yaml.Node) {
	if m.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			m.Content[i+1] = value
			return
		}
	}
	m.Content = append(m.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: key},
		value)
}

// Config exposes the raw configuration (for tools that need it).
func (s *Service) Config() *config.Config { return s.n.Cfg }

// BrowseEntry is one filesystem entry in a browse listing.
type BrowseEntry struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	IsDir bool   `json:"is_dir"`
}

// BrowseResult is a directory listing for the folder-picker popup.
type BrowseResult struct {
	Path    string        `json:"path"`   // the directory being listed
	Parent  string        `json:"parent"` // its parent ("" at the root)
	Entries []BrowseEntry `json:"entries"`
}

// defaultBrowseRoot returns where the folder picker starts: /mnt/user on
// Linux (Unraid shares live there), the system drive root on Windows.
func defaultBrowseRoot() string {
	if filepath.Separator == '\\' {
		if d := os.Getenv("SystemDrive"); d != "" {
			return d + string(filepath.Separator)
		}
		return "C:\\"
	}
	return "/mnt/user"
}

// Browse lists a directory for the folder-picker popup. An empty path
// starts at the default root. Only directories are guaranteed readable;
// files are listed for context but cannot be selected.
func (s *Service) Browse(path string) (*BrowseResult, error) {
	if strings.TrimSpace(path) == "" {
		path = defaultBrowseRoot()
	}
	dir := filepath.Clean(path)
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("path must be absolute")
	}
	st, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	res := &BrowseResult{Path: dir}
	if filepath.Dir(dir) != dir {
		res.Parent = filepath.Dir(dir)
	}
	for _, e := range ents {
		res.Entries = append(res.Entries, BrowseEntry{
			Name:  e.Name(),
			Path:  filepath.Join(dir, e.Name()),
			IsDir: e.IsDir(),
		})
	}
	// Directories first, then alphabetical within each group.
	sort.SliceStable(res.Entries, func(i, j int) bool {
		if res.Entries[i].IsDir != res.Entries[j].IsDir {
			return res.Entries[i].IsDir
		}
		return strings.ToLower(res.Entries[i].Name) < strings.ToLower(res.Entries[j].Name)
	})
	return res, nil
}

// Subscribe returns a live stream of new events plus a cancel function.
func (s *Service) Subscribe() (<-chan *events.Event, func()) {
	return s.n.Subscribe()
}
