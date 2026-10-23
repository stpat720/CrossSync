package control

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"net/http"
	"os"
	"time"
)

// backupEntry is one file inside a backup archive.
type backupEntry struct {
	name string
	data []byte
	mode int64
}

// handleBackup streams the node's restore-critical state as a tar.gz:
//
//	config.yaml   topology, folders, peers, settings (hand-edited comments too)
//	cert.pem      TLS identity certificate
//	key.pem       TLS identity private key — see the warning below
//	MANIFEST.txt  what this is, what is NOT in it, and how to restore
//
// Only identity and topology are backed up, deliberately. The per-folder index
// DBs are derived state (rebuilt from the filesystem), and events.db is history
// — neither is needed to bring a node back, and copying a live SQLite file out
// of a running daemon is exactly the kind of "backup" that restores corrupt.
//
// WARNING: the archive contains the node's PRIVATE KEY. Anyone who can fetch
// this endpoint can impersonate this node to its peers, so treat the download
// like a password (same trust boundary as the control API itself).
func (s *Service) handleBackup(w http.ResponseWriter, r *http.Request) {
	if s.ConfigPath == "" {
		writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("no config path is configured; nothing to back up"))
		return
	}
	cfg, err := os.ReadFile(s.ConfigPath)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("read config: %w", err))
		return
	}
	entries := []backupEntry{{name: "config.yaml", data: cfg, mode: 0o600}}
	if s.n.Certs != nil {
		if b, err := os.ReadFile(s.n.Certs.CertPath); err == nil {
			entries = append(entries, backupEntry{name: "cert.pem", data: b, mode: 0o644})
		}
		if b, err := os.ReadFile(s.n.Certs.KeyPath); err == nil {
			entries = append(entries, backupEntry{name: "key.pem", data: b, mode: 0o600})
		}
	}
	entries = append(entries, backupEntry{name: "MANIFEST.txt", mode: 0o644,
		data: []byte(s.backupManifest(len(cfg) > 0))})

	filename := fmt.Sprintf("crosssync-backup-%s-%s.tar.gz", s.n.Name, time.Now().Format("20060102-150405"))
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))

	// Headers are on the wire from here, so a mid-stream failure can only be
	// logged (a truncated tar.gz fails loudly at extraction, which is safe).
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	now := time.Now()
	for _, e := range entries {
		hdr := &tar.Header{
			Name: e.name, Mode: e.mode, Size: int64(len(e.data)),
			ModTime: now, Typeflag: tar.TypeReg, Format: tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			s.n.Logf("backup: write header %s: %v", e.name, err)
			break
		}
		if _, err := tw.Write(e.data); err != nil {
			s.n.Logf("backup: write %s: %v", e.name, err)
			break
		}
	}
	if err := tw.Close(); err != nil {
		s.n.Logf("backup: close tar: %v", err)
	}
	if err := gz.Close(); err != nil {
		s.n.Logf("backup: close gzip: %v", err)
	}
	s.n.Logf("backup: served %s (%d entries, device %q v%s)", filename, len(entries), s.n.Name, s.Version)
}

// backupManifest is the human-readable README that travels inside the archive;
// a restore months later should not require guessing what the files are.
func (s *Service) backupManifest(hasConfig bool) string {
	return fmt.Sprintf(`CrossSync node backup
=====================

device:  %s
version: %s
created: %s

Contents
--------
config.yaml  %s
cert.pem     TLS identity certificate (device fingerprint %s)
key.pem      TLS identity PRIVATE KEY — keep this archive secret
MANIFEST.txt this file

Not included (on purpose)
-------------------------
meta_dir/<folder>.db   per-folder index — derived from the files on disk, and
                       rebuilt automatically if missing or corrupt
meta_dir/events.db     event history — informational, not needed to restart
.sfx-hold/             soft-deleted copies inside each share (they live in the
                       share itself, so they travel with the share)

Restore
-------
1. Stop the container/daemon.
2. Extract config.yaml (and cert.pem + key.pem when this node uses TLS) over
   its previous location, e.g. /config/ for the Unraid image.
3. Keep the same meta_dir path if you can: the folder ids in config.yaml are
   the sync keys, and re-adding a folder with a new id makes every peer run a
   full first-time exchange instead of a delta.
4. Start the daemon. Missing index DBs are rebuilt by the first scan.
5. Verify: GET /api/peers shows every peer connected, GET /api/folders shows
   the expected file counts, and the peers still show this node's fingerprint
   (restoring cert.pem + key.pem keeps the identity, so no re-pairing is
   needed).
`, s.n.Name, s.Version, time.Now().Format(time.RFC3339),
		map[bool]string{true: "included", false: "MISSING — this archive is incomplete"}[hasConfig],
		s.n.Fingerprint())
}
