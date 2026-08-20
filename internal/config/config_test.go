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
		{"tls peer missing fingerprint", &Config{Device: Device{ID: 1, Name: "x"}, MetaDir: "/tmp/m", TLS: true,
			Peers: []Peer{{ID: 2, Name: "y", Addresses: []string{"h:1"}}}}},
	}
	for _, c := range cases {
		if err := c.cfg.Validate(); err == nil {
			t.Errorf("%s: expected validation error", c.name)
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
