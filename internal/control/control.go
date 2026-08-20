// Package control implements the control plane: a single service exposing
// node status, folders, the durable event store, rescan/sync actions, and
// a live event stream. It is consumed by the REST API and the MCP server.
package control

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

	// Web UI self-update state (see ui package).
	UIDir       string // override directory; "" = embedded UI only
	UIUpdateURL string // URL to fetch a newer index.html from
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
	Tombstones       int      `json:"tombstones"`
	Size             int64    `json:"size"`
	PendingFiles     int      `json:"pending_files"` // files to download from peers
	PendingBytes     int64    `json:"pending_bytes"` // bytes to download
	PendingUp        int      `json:"pending_up"`    // files peers need from us (uploads)
	BytesUp          int64    `json:"bytes_up"`
	BytesDown        int64    `json:"bytes_down"`
	FilesDown        int64    `json:"files_down"`
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
	GuardTripped     bool     `json:"guard_tripped"`        // deletion guard blocked a large delete pass
	PendingDeletions int      `json:"pending_deletions"`    // live files globally deleted, waiting to be removed
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
			fs.Size = st.Size
			fs.Tombstones = st.Tombstones
			fs.ConflictFiles = st.ConflictFiles
			fs.ConflictBytes = st.ConflictBytes
		}
		if ls := f.Engine.Stats.LastSync.Load(); ls > 0 {
			t := time.Unix(0, ls).Unix()
			fs.LastSync = &t
		}
		fs.BytesUp = f.Engine.Stats.BytesUp.Load()
		fs.BytesDown = f.Engine.Stats.BytesDown.Load()
		fs.FilesDown = f.Engine.Stats.FilesDown.Load()
		fs.Syncs = f.Engine.Stats.Syncs.Load()
		fs.PendingFiles, fs.PendingBytes = s.pending(f.Cfg.ID)
		if up, err := f.Engine.PendingUploads(); err == nil {
			fs.PendingUp = len(up)
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
		if pd, err := s.n.PendingDeletionsCount(f.Cfg.ID); err == nil {
			fs.PendingDeletions = pd
		}
		out = append(out, fs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
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
	names, err := f.Engine.NeedPulls()
	if err != nil {
		return 0, 0
	}
	n, total := 0, int64(0)
	for _, name := range names {
		g, _, err := f.Engine.GlobalFor(name)
		if err != nil || g == nil || g.Deleted {
			continue
		}
		n++
		total += g.Size
	}
	return n, total
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
	// Pending pulls (may not exist locally yet — take size from global).
	names, err := f.Engine.NeedPulls()
	if err != nil {
		return nil, err
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
	// Pending uploads: files at least one peer needs from us.
	if up, err := f.Engine.PendingUploads(); err == nil {
		for _, name := range up {
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
	}
	// Local files: conflict flags + size/type.
	if err := f.Ix.List(func(fi *index.FileInfo) error {
		if fi.Deleted {
			return nil
		}
		a := ensure(fi.Name)
		a.Size = fi.Size
		a.Modified = fi.ModifiedS
		a.Type = []string{"file", "dir", "symlink"}[fi.Type]
		if strings.Contains(fi.Name, sync.ConflictSuffix) {
			a.Conflict = true
			orig := fi.Name[:strings.LastIndex(fi.Name, sync.ConflictSuffix)]
			a.ConflictOf = orig
			conflictOf[orig] = fi.Name
		}
		return nil
	}); err != nil {
		return nil, err
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
	defer resp.Body.Close()
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
	idx := strings.LastIndex(conflictName, sync.ConflictSuffix)
	orig := conflictName[:idx]
	if orig == "" {
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
	orig := name[:strings.LastIndex(name, sync.ConflictSuffix)]
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
	defer f.Close()
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
	defer f.Close()
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
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".sfx-restore-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
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

// ApplyDeletions is the operator override for the deletion guard: it
// applies every currently-pending deletion for the folder locally. Use only
// after verifying the pending deletions are intentional.
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
		if s.n.ScanRunning(id) {
			return out, fmt.Errorf("scan already in progress for folder %q", id)
		}
		applied, err := s.n.ScanFolder(id)
		if err != nil {
			return out, err
		}
		out[id] = applied
	}
	return out, nil
}

// SyncNow runs a one-shot sync with the given peer (0 = all peers). It is
// the same path as the CLI's sync subcommand: rescan first, then sync.
func (s *Service) SyncNow(peerID uint64) error {
	if s.n.SyncRunning() {
		return fmt.Errorf("a sync is already running; retry when it finishes")
	}
	if err := s.n.ScanAll(); err != nil {
		return err
	}
	if peerID != 0 {
		_, err := s.n.SyncWithPeer(peerID)
		return err
	}
	return s.n.SyncAllPeers()
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
		return fmt.Errorf("a sync is already running; retry when it finishes")
	}
	if s.n.ScanRunning(id) {
		return fmt.Errorf("scan already in progress for folder %q", id)
	}
	if _, err := s.n.ScanFolder(id); err != nil {
		return err
	}
	return s.n.SyncAllPeers()
}

// AddFolder registers a new folder (id + share path + conflict policy) and
// persists it to the config file.
func (s *Service) AddFolder(id, name, path, policy string, peers []uint64) (FolderStatus, error) {
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
	id = strings.TrimSpace(id)
	if id == "" {
		// No id supplied: generate a unique one. The folder is keyed by id
		// across servers, so it must not collide with an existing folder.
		id = s.generateFolderID()
	}
	fc := config.Folder{ID: id, Name: strings.TrimSpace(name), Path: path, ConflictPolicy: policy, Peers: peers}
	if _, err := s.n.AddFolder(fc); err != nil {
		return FolderStatus{}, err
	}
	s.n.Cfg.Folders = append(s.n.Cfg.Folders, fc)
	if err := s.persistFolders(); err != nil {
		return FolderStatus{}, fmt.Errorf("folder added but config not persisted: %w", err)
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
