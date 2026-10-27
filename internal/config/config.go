// Package config defines the daemon configuration file format.
package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

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
	ID     string   `yaml:"id"`
	Path   string   `yaml:"path"`
	Ignore []string `yaml:"ignore"`
	// Include, when non-empty, is an include (whitelist) list: only files
	// and directories matching one of these patterns are in scope for
	// sync — everything else is out of scope (never indexed, transferred,
	// or touched). Same pattern syntax as Ignore. Empty = sync everything
	// (today's behavior). It is per-device: each node carries its own
	// include list in its own config, so a laptop can sync proxies-only
	// while the desktop keeps the full-res media.
	Include []string `yaml:"include,omitempty"`
	// Direction is this device's sync direction for the folder: "" or
	// "send-receive" (default: today's bidirectional behavior), "send-only"
	// (index and serve local changes, never apply remote ones) or
	// "receive-only" (pull remote changes, never index or propagate local
	// ones). Like Include it is per-device: each node carries its own
	// direction in its own config.
	Direction string `yaml:"direction,omitempty"`
	// Paused stops this device from scanning the folder and from syncing it
	// with ANY peer: no scans, no pulls, no pushes, and the folder is not
	// advertised while paused (peers see it as not shared, so they stop
	// expecting it). The index is kept, so resuming reconciles with a normal
	// exchange — pause is never a delete. Per-device like Direction/Include:
	// pausing on one server does not pause the others.
	Paused bool `yaml:"paused,omitempty"`
	// TransferPaused stops data movement for this folder WITHOUT stopping
	// scanning: the index keeps tracking local changes and the folder is still
	// advertised, but no blocks are pulled, pushed or served. Use it to stop a
	// saturated link (or to hold a slow WAN link during the day) while keeping
	// the folder monitored, so resuming converges from a current index instead
	// of re-walking the tree. Distinct from Paused, which stops everything
	// including scans.
	TransferPaused bool `yaml:"transfer_paused,omitempty"`
	// SettleScans is the stability gate: how many consecutive scans must
	// observe the same (size, mtime) before a changed file is hashed and
	// offered to peers. It stops a file that is still being written (a
	// large copy-in-progress) from being transferred as a partial version.
	// 0/absent = default 2; 1 = gate disabled (settle on first sight, the
	// pre-gate behaviour); values outside 0..64 are rejected by Validate.
	SettleScans    int              `yaml:"settle_scans,omitempty"`
	// DeleteConfirmScans is the deletion gate: how many consecutive scans must
	// miss a path before the deletion is believed and propagated. It protects
	// against a single bad walk (union/FUSE shares, cache rebuilds, a network
	// mount hiccup) tombstoning real files. 0/absent = default 2; 1 disables
	// it; values outside 0..64 are rejected by Validate.
	DeleteConfirmScans int `yaml:"delete_confirm_scans,omitempty"`
	// SettleSeconds is the QUIET WINDOW of the stability gate: a changed file is
	// offered to peers only once its (size, mtime) has been unchanged for this
	// many seconds. It is what keeps event-driven scanning safe — two scans can
	// happen milliseconds apart, so "seen unchanged twice" alone would promote a
	// file that is still being copied. 0/absent = default 10; ignored when
	// settle_scans is 1 (gate off). Validated 0..3600.
	SettleSeconds int `yaml:"settle_seconds,omitempty"` // -1 = window disabled (advanced: count-only)
	ConflictPolicy string           `yaml:"conflict"` // conflict-copy | versioning
	Versioning     VersioningPolicy `yaml:"versioning"`
	// Peers optionally restricts which peers this folder syncs with (device
	// ids). Empty means ALL configured peers that also have the folder.
	Peers []uint64 `yaml:"peers,omitempty"`
	// MaxDeletePct is the deletion guard: a single sync should not delete
	// more than this percentage of the folder's live files (0 = default 25).
	// MaxDeleteFiles is an optional absolute cap (0 = off). When the guard
	// trips, the deletions still go through (peers delete exactly as
	// before) but this device MOVES its own copies into the reserved
	// .sfx-hold area instead of unlinking them, so an over-threshold pass
	// has a 7-day undo window rather than a manual apply step.
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

// SyncDirection reports the folder's effective sync direction, mapping
// empty to the default send-receive.
func (f Folder) SyncDirection() string {
	if f.Direction == "" {
		return "send-receive"
	}
	return f.Direction
}

// SettleThreshold returns the effective stability-gate depth (default 2;
// 1 disables the gate).
func (f Folder) SettleThreshold() int {
	if f.SettleScans <= 0 {
		return 2
	}
	return f.SettleScans
}

// SettleQuietSeconds returns the stability gate's quiet window in seconds
// (default 10). The window is measured from the moment a file's current
// (size, mtime) was first observed.
func (f Folder) SettleQuietSeconds() int {
	switch {
	case f.SettleSeconds < 0:
		return 0 // explicitly disabled: count-only gate
	case f.SettleSeconds == 0:
		return 10
	default:
		return f.SettleSeconds
	}
}

// DeleteConfirmThreshold returns the effective deletion-gate depth: the explicit
// delete_confirm_scans when set, otherwise the same depth as the stability gate.
// settle_scans: 1 is documented as "trust the filesystem immediately, no gates",
// so it turns this one off too; the default is 2 in both cases.
func (f Folder) DeleteConfirmThreshold() int {
	if f.DeleteConfirmScans > 0 {
		return f.DeleteConfirmScans
	}
	return f.SettleThreshold()
}

// Pull concurrency bounds. K files are pulled at the same time over one peer
// connection, each with its own block window (node.pullWindowSize = 8), so at
// most K x 8 block requests are outstanding per peer and the puller holds at
// most K x 8 block buffers: ~8 MiB at the usual 256 KiB block size with the
// default K, ~16 MiB at the hard cap. The peer serves blocks ONE at a time
// (serveUntil), so a larger K only deepens its socket queue — it cannot make
// that side allocate more than one block per request, but it can starve the
// peer's own read loop, which is why the cap is 8 and not unbounded.
const (
	// DefaultPullConcurrency is how many files a round pulls at once when
	// the config does not say (K).
	DefaultPullConcurrency = 4
	// MaxPullConcurrency is the hard cap on K.
	MaxPullConcurrency = 8
)

// PullFiles returns the effective number of files pulled concurrently over one
// peer connection. 0 = default; anything else is clamped to [1, MaxPullConcurrency]
// so a hand-edited config can never turn a round into an unbounded fan-out.
func (c *Config) PullFiles() int {
	switch {
	case c == nil || c.PullConcurrency <= 0:
		return DefaultPullConcurrency
	case c.PullConcurrency > MaxPullConcurrency:
		return MaxPullConcurrency
	default:
		return c.PullConcurrency
	}
}

// Pull tuning bounds for the experimental knobs.
const (
	// DefaultPullRunBytes is the largest block run one request asks for.
	DefaultPullRunBytes = 4 << 20
	// DefaultPullInFlightBytes bounds response bytes outstanding per peer.
	DefaultPullInFlightBytes = 16 << 20
	// MaxPullRunBytes is the protocol's per-request block ceiling.
	MaxPullRunBytes = 16 << 20
)

// ReuseLocalBlocks reports whether a pull may satisfy a block from another
// local file instead of the wire. On by default; only an explicit
// pull_disable_block_reuse turns it off.
func (c *Config) ReuseLocalBlocks() bool {
	return c == nil || !c.PullDisableBlockReuse
}

// FileModeForPull returns the mode to force on pulled files (0 = keep the
// sender's mode).
func (c *Config) FileModeForPull() os.FileMode {
	if c == nil || c.PullFileMode <= 0 || c.PullFileMode > 0o777 {
		return 0
	}
	return os.FileMode(c.PullFileMode)
}

// DirModeForPull returns the mode to force on directories a pull creates
// (0 = the built-in 0o755).
func (c *Config) DirModeForPull() os.FileMode {
	if c == nil || c.PullDirMode <= 0 || c.PullDirMode > 0o777 {
		return 0
	}
	return os.FileMode(c.PullDirMode)
}

// PullOwner returns the uid/gid pulled files should get (-1 = the daemon's own).
//
// 0 counts as UNSET, not "root": an absent key must never quietly chown a
// share's files to 0:0, and a share owned by root is not a configuration
// anyone wants. (Observed live: with no keys set the daemon logged
// "owner=0:0" and would have re-owned every pulled file as root.)
func (c *Config) PullOwner() (int, int) {
	if c == nil {
		return -1, -1
	}
	uid, gid := c.PullUID, c.PullGID
	if uid <= 0 {
		uid = -1
	}
	if gid <= 0 {
		gid = -1
	}
	return uid, gid
}

// PullOrderOrDefault validates the pull order and returns the effective one:
// smallest (default), largest or index.
func (c *Config) PullOrderOrDefault() string {
	if c == nil {
		return "smallest"
	}
	switch strings.ToLower(strings.TrimSpace(c.PullOrder)) {
	case "", "smallest", "small":
		return "smallest"
	case "largest", "large":
		return "largest"
	case "index", "name", "none":
		return "index"
	default:
		return "smallest"
	}
}

// PullRunBytesOrDefault returns the largest block run to request.
func (c *Config) PullRunBytesOrDefault() int {
	if c == nil || c.PullRunKB <= 0 {
		return DefaultPullRunBytes
	}
	n := c.PullRunKB << 10
	if n > MaxPullRunBytes {
		return MaxPullRunBytes
	}
	return n
}

// PullInFlightBytesOrDefault returns the per-peer response byte budget.
func (c *Config) PullInFlightBytesOrDefault() int {
	if c == nil || c.PullInFlightMB <= 0 {
		return DefaultPullInFlightBytes
	}
	n := c.PullInFlightMB << 20
	if n < 256<<10 {
		n = 256 << 10 // below one run the batch cannot make progress
	}
	return n
}

// ScanTuning controls the periodic full walk. The interval adapts to how long
// the walk takes (a slow share is not walked back-to-back); these are the
// bounds.
type ScanTuning struct {
	// FullInterval is the minimum gap between full walks ("3m" default).
	FullInterval string `yaml:"full_interval,omitempty"`
	// FullMax is the maximum gap, i.e. how stale the safety net may get
	// ("15m" default).
	FullMax string `yaml:"full_max,omitempty"`
}

// FullIntervalOrDefault returns the minimum gap between full walks.
func (s ScanTuning) FullIntervalOrDefault() time.Duration {
	if d, err := time.ParseDuration(s.FullInterval); err == nil && d > 0 {
		return d
	}
	return 3 * time.Minute
}

// FullMaxOrDefault returns the maximum gap between full walks.
func (s ScanTuning) FullMaxOrDefault() time.Duration {
	if d, err := time.ParseDuration(s.FullMax); err == nil && d > 0 {
		return d
	}
	return 15 * time.Minute
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

	// Scan tunes the periodic FULL walk — the safety net that catches changes
	// the filesystem watcher missed (queue overflow, a share that was unmounted,
	// a subtree added while unwatched). Scans are otherwise driven by change
	// events, so this is the only routine full walk.
	Scan ScanTuning `yaml:"scan"`

	// PullConcurrency is how many FILES a sync round pulls at the same time
	// over one peer connection (K). Each file keeps its own block window in
	// flight, so this is what makes a tree of many small files cost K round
	// trips per file-batch instead of one round trip per file. 0 = default
	// (4), hard-capped at 8 (see PullFiles).
	PullConcurrency int `yaml:"pull_concurrency,omitempty"`

	// --- experimental pull tuning (all ON by default: the zero value is the
	// recommended setting, so an existing config.yaml keeps working) ---

	// PullDisableBlockReuse turns OFF per-block reuse from local files: by
	// default a needed block whose bytes already exist in ANOTHER file on this
	// device is copied locally instead of pulled over the wire (the same
	// mechanism Syncthing calls local block reuse). Only the blocks that exist
	// nowhere locally are fetched.
	PullDisableBlockReuse bool `yaml:"pull_disable_block_reuse,omitempty"`

	// PullOrder decides which pending file is pulled next: "smallest" (default)
	// gets small files finished first — a big backlog feels responsive and a
	// failed round loses less work — "largest" streams the big ones early, and
	// "index" keeps the previous name-ordered behaviour.
	PullOrder string `yaml:"pull_order,omitempty"`

	// PullRunKB is the largest block run requested in one wire request
	// (default 4096 = 4 MiB, capped by the protocol's 16 MiB block limit).
	// Bigger runs mean fewer round trips and fewer disk I/Os per byte.
	PullRunKB int `yaml:"pull_run_kb,omitempty"`

	// PullInFlightMB bounds the response bytes one peer connection may have
	// outstanding (default 16). This, not the request count, is what keeps
	// memory bounded now that a single request can be megabytes.
	PullInFlightMB int `yaml:"pull_inflight_mb,omitempty"`

	// PullFileMode / PullDirMode set the permissions of the files and
	// directories this node writes when it pulls. 0 keeps the SENDER's mode (so
	// an executable stays executable and a private file stays private); a value
	// like 0o664 / 0o775 makes everything group-writable, which is what a share
	// edited by several accounts needs — the daemon writes as its own user, so
	// its files are owned by that user alone.
	PullFileMode int `yaml:"pull_file_mode,omitempty"`
	PullDirMode  int `yaml:"pull_dir_mode,omitempty"`
	// PullUID / PullGID give pulled files and created directories to this owner
	// when the daemon runs as root (-1 = leave them owned by the daemon's user).
	PullUID int `yaml:"pull_uid,omitempty"`
	PullGID int `yaml:"pull_gid,omitempty"`

	// UpdateURL optionally points at a JSON update manifest (version +
	// per-platform binary url + sha256) used to self-update the DAEMON:
	// the daemon polls it, downloads a newer binary, verifies the
	// checksum, swaps it into update_dir, and restarts itself. Supports
	// http://, https:// and file:// URLs. Empty disables daemon updates.
	UpdateURL string `yaml:"update_url"`

	// UpdateDir is where the downloaded daemon binary is staged/installed
	// (must be writable by the daemon and survive restarts, e.g. the
	// appdata bind mount). Empty defaults to the directory of the config
	// file. The running config.yaml and meta_dir are NEVER touched.
	UpdateDir string `yaml:"update_dir"`

	// UpdateAuto applies a newer daemon automatically as soon as it is
	// detected (default off: the daemon still detects and offers the
	// update in the web UI, but waits for you to click Apply). When on,
	// the daemon restarts itself once a newer release is published.
	UpdateAuto bool `yaml:"update_auto,omitempty"`

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
	AlertURL string `yaml:"alert_url,omitempty"`
}

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
	if c.PullConcurrency < 0 || c.PullConcurrency > MaxPullConcurrency {
		return fmt.Errorf("pull_concurrency must be between 0 (default %d) and %d (got %d)",
			DefaultPullConcurrency, MaxPullConcurrency, c.PullConcurrency)
	}
	if c.Scan.FullInterval != "" {
		if d, err := time.ParseDuration(c.Scan.FullInterval); err != nil || d < 10*time.Second {
			return fmt.Errorf("scan.full_interval must be a duration of at least 10s (got %q)", c.Scan.FullInterval)
		}
	}
	if c.Scan.FullMax != "" {
		if d, err := time.ParseDuration(c.Scan.FullMax); err != nil || d < 30*time.Second {
			return fmt.Errorf("scan.full_max must be a duration of at least 30s (got %q)", c.Scan.FullMax)
		}
	}
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
		// The direction must be one of the three known modes: an unknown
		// value would silently fall back to some default, and a typo'd
		// "send-only" that degrades to bidirectional is exactly the
		// misconfiguration this setting must never accept.
		switch f.Direction {
		case "", "send-receive", "send-only", "receive-only":
		default:
			return fmt.Errorf("folder %q: unknown direction %q", f.ID, f.Direction)
		}
		// The stability gate depth must stay in a sane range: a typo must
		// not silently change sync behaviour (a huge value would hold every
		// changed file for hours), and 0 keeps its "use the default"
		// meaning.
		if f.SettleScans < 0 || f.SettleScans > 64 {
			return fmt.Errorf("folder %q: settle_scans must be between 0 and 64 (0 = default 2, 1 = disabled)", f.ID)
		}
		if f.SettleSeconds < -1 || f.SettleSeconds > 3600 {
			return fmt.Errorf("folder %q: settle_seconds must be between -1 and 3600 (0 = default 10, -1 = quiet window disabled)", f.ID)
		}
		if f.DeleteConfirmScans < 0 || f.DeleteConfirmScans > 64 {
			return fmt.Errorf("folder %q: delete_confirm_scans must be between 0 and 64 (0 = follow settle_scans, 1 = disabled)", f.ID)
		}
	}
	seenPeers := map[uint64]bool{}
	seenAddrs := map[string]string{}
	for _, p := range c.Peers {
		if p.ID == 0 {
			return fmt.Errorf("peer.id must be set")
		}
		if seenPeers[p.ID] {
			return fmt.Errorf("duplicate peer id %d", p.ID)
		}
		seenPeers[p.ID] = true
		if p.ID == c.Device.ID {
			// A peer with this node's own device id would make the daemon try
			// to sync with itself: the TLS allowlist would hold our own
			// fingerprint, rounds would open sessions against ourselves and
			// version vectors would collide.
			return fmt.Errorf("peer %q: id %d is this device's own id", p.Name, p.ID)
		}
		if len(p.Addresses) == 0 {
			return fmt.Errorf("peer %q: at least one address required", p.Name)
		}
		for _, a := range p.Addresses {
			if strings.TrimSpace(a) == "" {
				return fmt.Errorf("peer %q: empty address", p.Name)
			}
			if _, _, err := net.SplitHostPort(a); err != nil {
				return fmt.Errorf("peer %q: address %q must be host:port (missing port)", p.Name, a)
			}
			// The same endpoint under two peer entries is always a copy-paste
			// mistake, and it splits one real peer's state (delta markers,
			// backoff, unsynced view) across two device ids.
			if other, dup := seenAddrs[a]; dup {
				return fmt.Errorf("peer %q: address %q is already used by peer %q", p.Name, a, other)
			}
			seenAddrs[a] = p.Name
		}
		if p.Fingerprint != "" && !validFingerprint(p.Fingerprint) {
			return fmt.Errorf("peer %q: fingerprint must be 64 lowercase hex characters", p.Name)
		}
		// An empty fingerprint is allowed even with TLS: the peer is simply
		// rejected at the TLS handshake until its fingerprint is pinned. This
		// lets a node boot (and reach the web UI) before any peers exist or
		// are pinned.
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
		// De Morgan: reject anything that is NOT a lowercase hex digit.
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
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
