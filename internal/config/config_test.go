package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadValid(t *testing.T) {
	p := writeTemp(t, `
device:
  id: 1
  name: nas-01
meta_dir: /mnt/user/appdata/crosssync
listen: ":55557"
folders:
  - id: media
    path: /mnt/user/media
    conflict: conflict-copy
    ignore:
      - "*.tmp"
    versioning:
      type: staggered
      max_age: 30
peers:
  - id: 2
    name: nas-02
    addresses:
      - "100.64.0.2:55557"
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Device.ID != 1 || cfg.Device.Name != "nas-01" {
		t.Fatalf("device parsed wrong: %+v", cfg.Device)
	}
	if len(cfg.Folders) != 1 || cfg.Folders[0].ID != "media" {
		t.Fatalf("folders parsed wrong: %+v", cfg.Folders)
	}
	if cfg.Folders[0].Versioning.MaxAge != 30 {
		t.Fatalf("versioning parsed wrong: %+v", cfg.Folders[0].Versioning)
	}
	if len(cfg.Peers) != 1 || cfg.Peers[0].Addresses[0] != "100.64.0.2:55557" {
		t.Fatalf("peers parsed wrong: %+v", cfg.Peers)
	}
}

func TestValidateRejects(t *testing.T) {
	cases := []struct {
		name string
		cfg  *Config
	}{
		{"missing device id", &Config{Device: Device{Name: "x"}, MetaDir: "/tmp/m"}},
		{"missing meta dir", &Config{Device: Device{ID: 1, Name: "x"}}},
		{"relative folder path", &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
			Folders: []Folder{{ID: "f", Path: "relative/path"}}}},
		{"bad conflict policy", &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
			Folders: []Folder{{ID: "f", Path: "/abs", ConflictPolicy: "bogus"}}}},
		{"duplicate folder", &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
			Folders: []Folder{{ID: "f", Path: "/abs1"}, {ID: "f", Path: "/abs2"}}}},
		{"peer no address", &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
			Peers: []Peer{{ID: 2, Name: "y"}}}},
		{"peer bad fingerprint", &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
			Peers: []Peer{{ID: 2, Name: "y", Addresses: []string{"h:1"}, Fingerprint: "not-hex"}}}},
		{"peer with this device's own id", &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
			Peers: []Peer{{ID: 1, Name: "self", Addresses: []string{"h:1"}}}}},
		{"duplicate peer address", &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
			Peers: []Peer{
				{ID: 2, Name: "a", Addresses: []string{"h:1"}},
				{ID: 3, Name: "b", Addresses: []string{"h:1"}},
			}}},
	}
	for _, c := range cases {
		if err := c.cfg.Validate(); err == nil {
			t.Errorf("%s: expected validation error", c.name)
		}
	}
}

func TestPullConcurrencyDefaults(t *testing.T) {
	base := func(k int) *Config {
		return &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m", PullConcurrency: k}
	}
	if got := base(0).PullFiles(); got != DefaultPullConcurrency {
		t.Fatalf("unset pull_concurrency = %d, want the default %d", got, DefaultPullConcurrency)
	}
	if got := base(2).PullFiles(); got != 2 {
		t.Fatalf("pull_concurrency 2 = %d", got)
	}
	// Programmatic use (tests, a future API) is clamped rather than trusted.
	if got := base(MaxPullConcurrency + 5).PullFiles(); got != MaxPullConcurrency {
		t.Fatalf("over-cap pull_concurrency = %d, want the hard cap %d", got, MaxPullConcurrency)
	}
	var nilCfg *Config
	if got := nilCfg.PullFiles(); got != DefaultPullConcurrency {
		t.Fatalf("nil config = %d, want the default %d", got, DefaultPullConcurrency)
	}
	for _, k := range []int{0, 1, MaxPullConcurrency} {
		if err := base(k).Validate(); err != nil {
			t.Fatalf("pull_concurrency %d rejected: %v", k, err)
		}
	}
	for _, k := range []int{-1, MaxPullConcurrency + 1} {
		if err := base(k).Validate(); err == nil {
			t.Fatalf("pull_concurrency %d must be refused (0 = default, hard cap %d)", k, MaxPullConcurrency)
		}
	}
}

func TestValidateTLSAcceptsPinnedPeer(t *testing.T) {
	fp := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	cfg := &Config{
		Device:  Device{ID: 1, Name: "x"},
		MetaDir: "/tmp/m",
		TLS:     true,
		Peers:   []Peer{{ID: 2, Name: "y", Addresses: []string{"h:1"}, Fingerprint: fp}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid TLS config rejected: %v", err)
	}
	// Uppercase fingerprint is also rejected (we store lowercase).
	cfg.Peers[0].Fingerprint = "0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF"
	if err := cfg.Validate(); err == nil {
		t.Fatal("uppercase fingerprint should be rejected")
	}
}

func TestValidateTLSAllowsUnpinnedPeer(t *testing.T) {
	// An empty fingerprint is allowed at config time even with TLS: the
	// peer is rejected at the TLS handshake until pinned, so a node can
	// boot (and serve the web UI) before any peers are configured.
	cfg := &Config{
		Device:  Device{ID: 1, Name: "x"},
		MetaDir: "/tmp/m",
		TLS:     true,
		Peers:   []Peer{{ID: 2, Name: "y", Addresses: []string{"h:1"}}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unpinned peer should be accepted at config time: %v", err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

// TestAllowsPeer verifies per-folder peer scoping: an empty Peers list means
// every peer, otherwise only listed device ids are allowed.
func TestAllowsPeer(t *testing.T) {
	open := Folder{ID: "a", Path: "/x"} // no peers -> all
	if !open.AllowsPeer(0) || !open.AllowsPeer(2) || !open.AllowsPeer(99) {
		t.Fatal("empty Peers must allow every peer")
	}
	scoped := Folder{ID: "a", Path: "/x", Peers: []uint64{2, 5}}
	if !scoped.AllowsPeer(2) || !scoped.AllowsPeer(5) {
		t.Fatal("listed peer ids must be allowed")
	}
	if scoped.AllowsPeer(1) || scoped.AllowsPeer(3) || scoped.AllowsPeer(0) {
		t.Fatal("unlisted peer ids must be rejected")
	}
}

func TestDeleteGuardDefaults(t *testing.T) {
	def := Folder{ID: "a", Path: "/x"}
	pct, files := def.DeleteGuard()
	if pct != 25 || files != 0 {
		t.Fatalf("defaults = (%d, %d), want (25, 0)", pct, files)
	}
	explicit := Folder{ID: "a", Path: "/x", MaxDeletePct: 10, MaxDeleteFiles: 500}
	pct, files = explicit.DeleteGuard()
	if pct != 10 || files != 500 {
		t.Fatalf("explicit = (%d, %d), want (10, 500)", pct, files)
	}
}

func TestAutoArchiveDefaults(t *testing.T) {
	off := Config{}
	if on, _ := off.AutoArchive(); on {
		t.Fatal("auto-archive must be off by default")
	}
	on := Config{AutoArchiveEvents: true}
	enabled, age := on.AutoArchive()
	if !enabled {
		t.Fatal("auto-archive should be enabled")
	}
	if age != 90*24*60*60 {
		t.Fatalf("default age = %d, want 90 days", age)
	}
	explicit := Config{AutoArchiveEvents: true, AutoArchiveOlderThan: 31536000}
	enabled, age = explicit.AutoArchive()
	if !enabled || age != 31536000 {
		t.Fatalf("explicit = (%v, %d), want (true, 31536000)", enabled, age)
	}
}

// TestLoadFolderPeers verifies the peers list round-trips through YAML and
// that omitted peers stay nil (meaning "all peers").
// TestValidateSharePath verifies the Unraid share-level constraint: on
// Linux, only /mnt/user/<share> paths are allowed; raw disk paths are
// rejected. Non-Linux platforms accept any absolute path.
func TestValidateSharePath(t *testing.T) {
	// Linux (Unraid): share-level only.
	if err := validateSharePath(true, "/mnt/user/media"); err != nil {
		t.Fatalf("/mnt/user/media should be allowed: %v", err)
	}
	if err := validateSharePath(true, "/mnt/user"); err != nil {
		t.Fatalf("/mnt/user should be allowed: %v", err)
	}
	for _, bad := range []string{"/mnt/disk1/media", "/mnt/cache/media", "/mnt/user0/media", "/var/lib/foo", "relative"} {
		if err := validateSharePath(true, bad); err == nil {
			t.Fatalf("%q should be rejected on Unraid", bad)
		}
	}
	// Non-Linux (dev): any absolute path is fine.
	if err := validateSharePath(false, `C:\Users\me\media`); err != nil {
		t.Fatalf("non-linux path should be allowed: %v", err)
	}
}

// TestFolderDirection verifies the per-device sync direction: the three known
// values validate (empty included, meaning send-receive), an unknown one is
// rejected, and SyncDirection maps empty to the default.
func TestFolderDirection(t *testing.T) {
	for _, dir := range []string{"", "send-receive", "send-only", "receive-only"} {
		cfg := &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
			Folders: []Folder{{ID: "f", Path: "/abs", Direction: dir}}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("direction %q should validate: %v", dir, err)
		}
		want := dir
		if want == "" {
			want = "send-receive"
		}
		if got := cfg.Folders[0].SyncDirection(); got != want {
			t.Fatalf("SyncDirection() with %q = %q, want %q", dir, got, want)
		}
	}
	bad := &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
		Folders: []Folder{{ID: "f", Path: "/abs", Direction: "both"}}}
	if err := bad.Validate(); err == nil {
		t.Fatal("unknown direction must be rejected")
	}
}

// TestLoadFolderDirection verifies the direction round-trips through YAML and
// that an omitted direction stays empty (send-receive: existing configs are
// untouched).
func TestLoadFolderDirection(t *testing.T) {
	p := writeTemp(t, `
device:
  id: 1
  name: nas-01
meta_dir: /tmp/m
folders:
  - id: proxy
    path: /s
    direction: send-only
  - id: inbox
    path: /o
    direction: receive-only
  - id: both
    path: /b
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Folders[0].SyncDirection(); got != "send-only" {
		t.Fatalf("proxy direction = %q, want send-only", got)
	}
	if got := cfg.Folders[1].SyncDirection(); got != "receive-only" {
		t.Fatalf("inbox direction = %q, want receive-only", got)
	}
	if cfg.Folders[2].Direction != "" {
		t.Fatalf("omitted direction should stay empty, got %q", cfg.Folders[2].Direction)
	}
	if got := cfg.Folders[2].SyncDirection(); got != "send-receive" {
		t.Fatalf("default direction = %q, want send-receive", got)
	}
}

// TestLoadFolderPeers verifies the peers list round-trips through YAML and
// that omitted peers stay nil (meaning "all peers").
func TestLoadFolderPeers(t *testing.T) {
	p := writeTemp(t, `
device:
  id: 1
  name: nas-01
meta_dir: /tmp/m
folders:
  - id: scoped
    path: /s
    peers: [2, 5]
  - id: open
    path: /o
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Folders) != 2 {
		t.Fatalf("folders parsed wrong: %+v", cfg.Folders)
	}
	if got := cfg.Folders[0].Peers; len(got) != 2 || got[0] != 2 || got[1] != 5 {
		t.Fatalf("scoped folder peers parsed wrong: %v", got)
	}
	if cfg.Folders[1].Peers != nil {
		t.Fatalf("open folder peers should be nil, got %v", cfg.Folders[1].Peers)
	}
}

// TestSettleThreshold verifies the stability-gate depth accessor: absent/0
// means the default 2, an explicit value is used verbatim, and 1 disables
// the gate (pre-gate behaviour).
func TestSettleThreshold(t *testing.T) {
	absent := Folder{ID: "a", Path: "/x"}
	if got := absent.SettleThreshold(); got != 2 {
		t.Fatalf("absent settle_scans threshold = %d, want 2", got)
	}
	zero := Folder{ID: "a", Path: "/x", SettleScans: 0}
	if got := zero.SettleThreshold(); got != 2 {
		t.Fatalf("settle_scans 0 threshold = %d, want 2 (default)", got)
	}
	off := Folder{ID: "a", Path: "/x", SettleScans: 1}
	if got := off.SettleThreshold(); got != 1 {
		t.Fatalf("settle_scans 1 threshold = %d, want 1 (gate off)", got)
	}
	deep := Folder{ID: "a", Path: "/x", SettleScans: 5}
	if got := deep.SettleThreshold(); got != 5 {
		t.Fatalf("settle_scans 5 threshold = %d, want 5", got)
	}
}

// TestValidateSettleScans verifies the accepted range: 0..64 validates (0 =
// default, 1 = disabled) and anything outside it is rejected instead of
// silently changing sync behaviour.
func TestValidateSettleScans(t *testing.T) {
	for _, n := range []int{0, 1, 2, 64} {
		cfg := &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
			Folders: []Folder{{ID: "f", Path: "/abs", SettleScans: n}}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("settle_scans %d should validate: %v", n, err)
		}
	}
	for _, n := range []int{-1, 65} {
		cfg := &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m",
			Folders: []Folder{{ID: "f", Path: "/abs", SettleScans: n}}}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("settle_scans %d must be rejected", n)
		}
	}
}

// TestLoadFolderSettleScans verifies settle_scans round-trips through YAML
// and that an omitted value keeps its default (2) rather than 0-as-disabled.
func TestLoadFolderSettleScans(t *testing.T) {
	p := writeTemp(t, `
device:
  id: 1
  name: nas-01
meta_dir: /tmp/m
folders:
  - id: fast
    path: /f
    settle_scans: 4
  - id: plain
    path: /p
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Folders[0].SettleScans; got != 4 {
		t.Fatalf("fast settle_scans = %d, want 4", got)
	}
	if got := cfg.Folders[0].SettleThreshold(); got != 4 {
		t.Fatalf("fast threshold = %d, want 4", got)
	}
	if got := cfg.Folders[1].SettleScans; got != 0 {
		t.Fatalf("omitted settle_scans should stay 0, got %d", got)
	}
	if got := cfg.Folders[1].SettleThreshold(); got != 2 {
		t.Fatalf("omitted threshold = %d, want default 2", got)
	}
}
