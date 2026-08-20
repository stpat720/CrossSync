// Package config defines the daemon configuration file format.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"gopkg.in/yaml.v3"
)

// Device identifies this node.
type Device struct {
	ID   uint64 `yaml:"id"`
	Name string `yaml:"name"`
}

// Peer describes a remote node we sync with.
type Peer struct {
	ID          uint64   `yaml:"id"`
	Name        string   `yaml:"name"`
	Addresses   []string `yaml:"addresses"`   // tailnet IPs / host:port
	Fingerprint string   `yaml:"fingerprint"` // TLS identity: hex SHA-256 of the peer's certificate
}

// VersioningPolicy controls how old versions of files are preserved when a
// file is replaced or deleted by a REMOTE change.
type VersioningPolicy struct {
	Type      string `yaml:"type"`       // none | trashcan | simple | staggered
	Keep      int    `yaml:"keep"`       // simple: number of versions to keep
	MaxAge    int    `yaml:"max_age"`    // staggered: max age in days (0 = forever)
	CleanDays int    `yaml:"clean_days"` // trashcan: auto-clean after N days (0 = never)
}

// Folder is a synced folder.
type Folder struct {
	// Name is the human-friendly label shown in the UI (e.g. "Archive").
	// When empty, the UI falls back to showing ID.
	Name string `yaml:"name,omitempty"`
	// ID is the unique, stable sync key shared across servers. It is a
	// generated random string (or a pasted-in value); it must be identical
	// on every server for the folder to sync.
	ID             string           `yaml:"id"`
	Path           string           `yaml:"path"`
	Ignore         []string         `yaml:"ignore"`
	ConflictPolicy string           `yaml:"conflict"` // conflict-copy | versioning
	Versioning     VersioningPolicy `yaml:"versioning"`
	// Peers optionally restricts which peers this folder syncs with (device
	// ids). Empty means ALL configured peers that also have the folder.
	Peers []uint64 `yaml:"peers,omitempty"`
	// MaxDeletePct is the deletion guard: a single sync is not allowed to
	// delete more than this percentage of the folder's live files (0 =
	// default 25). MaxDeleteFiles is an optional absolute cap (0 = off).
	// When the guard trips, deletions are blocked until the operator
	// explicitly applies them, so a wiped/misconfigured peer cannot
	// silently empty a folder.
	MaxDeletePct   int `yaml:"max_delete_pct,omitempty"`
	MaxDeleteFiles int `yaml:"max_delete_files,omitempty"`
}

// DeleteGuard returns the effective deletion-guard thresholds for the
// folder (percentage cap and absolute file cap).
func (f *Folder) DeleteGuard() (pct, files int) {
	pct = f.MaxDeletePct
	if pct <= 0 {
		pct = 25
	}
	return pct, f.MaxDeleteFiles
}

// AllowsPeer reports whether this folder should sync with the given peer:
// an empty Peers list means every peer; otherwise the peer id must be listed.
func (f Folder) AllowsPeer(peerID uint64) bool {
	if len(f.Peers) == 0 {
		return true
	}
	for _, id := range f.Peers {
		if id == peerID {
			return true
		}
	}
	return false
}

// Config is the top-level daemon configuration.
type Config struct {
	Device  Device   `yaml:"device"`
	Folders []Folder `yaml:"folders"`
	Peers   []Peer   `yaml:"peers"`
	MetaDir string   `yaml:"meta_dir"` // where per-folder databases live
	Listen  string   `yaml:"listen"`   // address:port for peer connections
	TLS     bool     `yaml:"tls"`      // enable TLS 1.3 + certificate-fingerprint pinning

	// ControlListen is the address:port for the REST API + SSE control
	// plane (empty disables it). Bind to localhost or a tailnet IP.
	ControlListen string `yaml:"control_listen"`

	// UIDir optionally points at a directory containing an index.html that
	// overrides the embedded web UI (e.g. a bind-mounted appdata share in
	// Docker). When the file is absent the embedded UI is served.
	UIDir string `yaml:"ui_dir"`

	// UIUpdateURL optionally points at a newer index.html (http/https or
	// file:// URL) that the control plane can fetch to self-update the UI.
	UIUpdateURL string `yaml:"ui_update_url"`

	// AutoArchiveEvents prunes ROUTINE event history older than
	// AutoArchiveOlderThan seconds (0 = off; use the manual Archive… button
	// instead). Open attention/conflict events are NEVER pruned — only
	// dismissed/resolved routine logs older than the cutoff are removed.
	AutoArchiveEvents    bool  `yaml:"auto_archive_events,omitempty"`
	AutoArchiveOlderThan int64 `yaml:"auto_archive_older_than,omitempty"` // seconds; 0 = default 90 days
	// AlertURL is an ntfy topic or generic webhook that receives
	// page-worthy notifications (disk full, share unavailable, peer
	// offline, deletion guard tripped, corrupt-index rebuild). Empty
	// disables alerts.
	AlertURL string `yaml:"alert_url,omitempty"`}

// AutoArchive returns whether routine event history should be auto-pruned
// and after how long (seconds), applying the 90-day default when the field
// is unset but the feature is enabled.
func (c *Config) AutoArchive() (enabled bool, olderThan int64) {
	if !c.AutoArchiveEvents {
		return false, 0
	}
	olderThan = c.AutoArchiveOlderThan
	if olderThan <= 0 {
		olderThan = 90 * 24 * 60 * 60 // 90 days
	}
	return true, olderThan
}

// Load reads and parses a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// LoadLenient reads and parses a YAML config file for bootstrap commands
// that only need the device identity and meta_dir (e.g. `fingerprint`).
// Unlike Load it does NOT require a fully valid folder/peer set, so it can
// be run before TLS fingerprints have been exchanged between nodes.
func LoadLenient(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if cfg.Device.ID == 0 {
		return nil, fmt.Errorf("device.id must be set to this node's short device ID")
	}
	if cfg.Device.Name == "" {
		return nil, fmt.Errorf("device.name must be set")
	}
	if cfg.MetaDir == "" {
		return nil, fmt.Errorf("meta_dir must be set (where per-folder databases live)")
	}
	return &cfg, nil
}

// Validate checks the config for structural errors.
func (c *Config) Validate() error {
	if c.Device.ID == 0 {
		return fmt.Errorf("device.id must be set to this node's short device ID")
	}
	if c.Device.Name == "" {
		return fmt.Errorf("device.name must be set")
	}
	if c.MetaDir == "" {
		return fmt.Errorf("meta_dir must be set (where per-folder databases live)")
	}
	seenFolders := map[string]bool{}
	for _, f := range c.Folders {
		if f.ID == "" {
			return fmt.Errorf("folder.id must be set")
		}
		if seenFolders[f.ID] {
			return fmt.Errorf("duplicate folder id %q", f.ID)
		}
		seenFolders[f.ID] = true
		if f.Path == "" {
			return fmt.Errorf("folder %q: path must be set", f.ID)
		}
		// Paths may be Linux-style (/mnt/user/...) even when the config is
		// authored/validated on another platform.
		if !filepath.IsAbs(f.Path) && !strings.HasPrefix(f.Path, "/") {
			return fmt.Errorf("folder %q: path must be absolute", f.ID)
		}
		if err := ValidateSharePath(f.Path); err != nil {
			return fmt.Errorf("folder %q: %w", f.ID, err)
		}
		switch f.ConflictPolicy {
		case "", "conflict-copy", "versioning":
		default:
			return fmt.Errorf("folder %q: unknown conflict policy %q", f.ID, f.ConflictPolicy)
		}
		switch f.Versioning.Type {
		case "", "none", "trashcan", "simple", "staggered":
		default:
			return fmt.Errorf("folder %q: unknown versioning type %q", f.ID, f.Versioning.Type)
		}
	}
	seenPeers := map[uint64]bool{}
	for _, p := range c.Peers {
		if p.ID == 0 {
			return fmt.Errorf("peer.id must be set")
		}
		if seenPeers[p.ID] {
			return fmt.Errorf("duplicate peer id %d", p.ID)
		}
		seenPeers[p.ID] = true
		if len(p.Addresses) == 0 {
			return fmt.Errorf("peer %q: at least one address required", p.Name)
		}
		for _, a := range p.Addresses {
			if strings.TrimSpace(a) == "" {
				return fmt.Errorf("peer %q: empty address", p.Name)
			}
		}
		if p.Fingerprint != "" && !validFingerprint(p.Fingerprint) {
			return fmt.Errorf("peer %q: fingerprint must be 64 lowercase hex characters", p.Name)
		}
	}
	if c.TLS {
		for _, p := range c.Peers {
			if strings.TrimSpace(p.Fingerprint) == "" {
				return fmt.Errorf("peer %q: fingerprint required when tls is enabled (set peers[].fingerprint)", p.Name)
			}
		}
	}
	return nil
}

// validFingerprint reports whether s looks like a certificate fingerprint:
// 64 lowercase hex characters (SHA-256 of the DER certificate).
func validFingerprint(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// ValidateSharePath enforces the share-level constraint on Linux/Unraid:
// synced folders must live under /mnt/user/<share> so all I/O goes through
// the Unraid share abstraction (parity-safe). Raw disk paths (/mnt/disk*,
// /mnt/cache, /mnt/user0) are rejected. On other platforms (development on
// Windows/macOS) any absolute path is allowed, since there is no Unraid
// share layer there.
func ValidateSharePath(p string) error {
	return validateSharePath(runtime.GOOS == "linux", p)
}

// validateSharePath is the platform-parameterized form for testing.
func validateSharePath(isLinux bool, p string) error {
	if !isLinux {
		return nil
	}
	if p == "/mnt/user" || strings.HasPrefix(p, "/mnt/user/") {
		return nil
	}
	return fmt.Errorf("path must be under /mnt/user/<share> on Unraid (share-level only; raw disk paths are not allowed)")
}
