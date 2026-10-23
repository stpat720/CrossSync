package control

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// DaemonUpdateInfo is the result of checking the configured update manifest.
type DaemonUpdateInfo struct {
	Configured      bool   `json:"configured"`       // update_url is set
	Current         string `json:"current"`          // running daemon version
	Latest          string `json:"latest"`           // newest version in the manifest
	UpdateAvailable bool   `json:"update_available"` // latest > current
	Auto            bool   `json:"auto"`             // update_auto is on
	Target          string `json:"target,omitempty"` // where the new binary installs
	CheckedAt       int64  `json:"checked_at"`       // unix seconds of the last check
	Error           string `json:"error,omitempty"`  // fetch/parse failure (informational)
}

// updateManifest is the JSON published alongside each release: a version
// plus per-platform artifacts (url + sha256).
type updateManifest struct {
	Version   string                    `json:"version"`
	Platforms map[string]updateArtifact `json:"platforms"`
}

type updateArtifact struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size,omitempty"`
}

// updateCheckInterval throttles how often the manifest is actually fetched
// (the UI polls every 30s, but we don't want to hammer the URL).
const updateCheckInterval = 5 * time.Minute

// fetchURL downloads a payload from an http://, https:// or file:// URL.
// maxSize bounds the body for http(s) downloads.
func fetchURL(url string, maxSize int64) ([]byte, error) {
	if strings.HasPrefix(url, "file://") {
		return os.ReadFile(strings.TrimPrefix(url, "file://"))
	}
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return nil, fmt.Errorf("unsupported URL scheme %q (use http://, https:// or file://)", url)
	}
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxSize))
}

// platformKey identifies this device's binary in the manifest.
func platformKey() string { return runtime.GOOS + "/" + runtime.GOARCH }

// updateTarget returns the path the downloaded daemon binary is installed
// to and later re-executed from. It must live in a directory the daemon
// can write (never the read-only image layer): update_dir when set, else
// the directory of the config file.
func (s *Service) updateTarget() string {
	dir := s.UpdateDir
	if dir == "" {
		dir = filepath.Dir(s.ConfigPath)
	}
	name := "crosssync"
	if runtime.GOOS == "windows" {
		name = "crosssync.exe"
	}
	return filepath.Join(dir, name)
}

// fetchUpdateManifest downloads and parses the update manifest.
func (s *Service) fetchUpdateManifest() (*updateManifest, error) {
	if s.UpdateURL == "" {
		return nil, fmt.Errorf("update_url is not configured")
	}
	b, err := fetchURL(s.UpdateURL, 1<<20)
	if err != nil {
		return nil, err
	}
	var m updateManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("update manifest: %w", err)
	}
	if m.Version == "" {
		return nil, fmt.Errorf("update manifest: missing version")
	}
	if len(m.Platforms) == 0 {
		return nil, fmt.Errorf("update manifest: missing platforms")
	}
	return &m, nil
}

// versionNewer reports whether candidate is a newer version than current.
// Comparison is numeric across any number of dotted parts (so "0.1.0.3" is
// newer than "0.1.0"); unparsable parts count as 0.
func versionNewer(current, candidate string) bool {
	return compareVersions(candidate, current) > 0
}

// compareVersions compares two dotted-numeric versions, part by part
// (missing parts are 0), returning 1/0/-1 like strings.Compare. Non-numeric
// content in a part is ignored up to the first non-digit, so "v0.2.0" and
// "0.1.0-3-gabc123" still compare sensibly.
func compareVersions(a, b string) int {
	pa, pb := versionParts(a), versionParts(b)
	n := len(pa)
	if len(pb) > n {
		n = len(pb)
	}
	for i := 0; i < n; i++ {
		va, vb := 0, 0
		if i < len(pa) {
			va = pa[i]
		}
		if i < len(pb) {
			vb = pb[i]
		}
		if va == vb {
			continue
		}
		if va > vb {
			return 1
		}
		return -1
	}
	return 0
}

// versionParts splits a version into leading integers per dot-separated
// part, e.g. "v0.1.0-3-gabc" -> [0, 1, 0].
func versionParts(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ".") {
		n := 0
		seen := false
		for _, r := range p {
			if r < '0' || r > '9' {
				break
			}
			seen = true
			n = n*10 + int(r-'0')
		}
		if seen {
			out = append(out, n)
		} else {
			out = append(out, 0)
		}
	}
	return out
}

// CheckDaemonUpdate fetches the update manifest (throttled to every
// updateCheckInterval) and reports whether a newer daemon is available.
// Fetch/parse failures are reported in Error rather than returned, so the
// UI can show "check failed" without breaking.
func (s *Service) CheckDaemonUpdate() DaemonUpdateInfo {
	s.updateMu.Lock()
	defer s.updateMu.Unlock()

	now := time.Now()
	if s.updateURLConfigured() && (s.updateInfo.CheckedAt == 0 || now.Sub(time.Unix(s.updateInfo.CheckedAt, 0)) >= updateCheckInterval) {
		info := DaemonUpdateInfo{
			Configured: s.UpdateURL != "",
			Current:    s.Version,
			Auto:       s.UpdateAuto,
			Target:     s.updateTarget(),
			CheckedAt:  now.Unix(),
		}
		m, err := s.fetchUpdateManifest()
		if err != nil {
			info.Error = err.Error()
			s.updateInfo = info
			s.logUpdateCheckLocked(info)
			return info
		}
		info.Latest = m.Version
		info.UpdateAvailable = versionNewer(s.Version, m.Version)
		if _, ok := m.Platforms[platformKey()]; !ok {
			// Nothing installable for this platform: not an offerable
			// update, but surface why.
			info.UpdateAvailable = false
			info.Error = fmt.Sprintf("manifest has no binary for %s", platformKey())
		}
		s.updateInfo = info
		s.logUpdateCheckLocked(info)
		return info
	}
	// Cache still fresh (or not configured): return the last result.
	return s.updateInfo
}

func (s *Service) updateURLConfigured() bool { return s.UpdateURL != "" }

// logUpdateCheckLocked logs the outcome of a manifest check only when it
// CHANGES — the first check after start, a newly available update, a new
// failure, or a recovery. Every fetch used to log a line, which turned the
// daemon log into a wall of identical "up to date" entries (one per fetch
// while any dashboard tab was open: ~288/day on an idle box) and buried the
// events an operator actually needs. Silence is the healthy steady state.
// Caller holds updateMu.
func (s *Service) logUpdateCheckLocked(info DaemonUpdateInfo) {
	if s.n == nil {
		return
	}
	state := "up to date"
	switch {
	case info.Error != "":
		state = "check failed: " + info.Error
	case info.UpdateAvailable:
		state = "v" + info.Latest + " available"
	}
	key := info.Current + "→" + info.Latest + " | " + state
	if key == s.updateLogged {
		return
	}
	s.updateLogged = key
	s.n.Logf("self-update: %s (running v%s, latest v%s)", state, info.Current, info.Latest)
}

// StageUpdate downloads the newest binary for this platform, verifies its
// SHA-256 against the manifest, and stages it at <target>.new WITHOUT
// touching the running binary. Returns the new version. Safe to call
// repeatedly; a failure leaves no partial state behind.
func (s *Service) StageUpdate() (string, error) {
	// A rolling release publishes latest.json and the binaries in one shot, and
	// a mirror or CDN edge can serve the NEW manifest next to the OLD bytes for
	// a few seconds. Observed live: an apply failed with "checksum mismatch" and
	// the identical apply succeeded 30 seconds later. Retry ONLY a checksum
	// mismatch, with a freshly fetched manifest each time, so an unattended
	// auto-update does not lose that race; every other error is final.
	var lastErr error
	for attempt := 1; attempt <= stageAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(stageRetryDelay)
		}
		ver, err := s.stageOnce()
		if err == nil {
			return ver, nil
		}
		if !errors.Is(err, errChecksumMismatch) {
			return "", err
		}
		lastErr = err
	}
	return "", fmt.Errorf("%w (retried %d times)", lastErr, stageAttempts-1)
}

// stageAttempts / stageRetryDelay bound the manifest-vs-bytes race retry. Vars
// so tests can shorten the wait.
var (
	stageAttempts   = 3
	stageRetryDelay = 2 * time.Second
)

// errChecksumMismatch marks a staged binary whose bytes did not match the
// manifest — the only failure StageUpdate retries.
var errChecksumMismatch = errors.New("checksum mismatch")

// stageOnce is one fetch-manifest + download + verify + stage pass.
func (s *Service) stageOnce() (string, error) {
	m, err := s.fetchUpdateManifest()
	if err != nil {
		return "", err
	}
	// Never install an OLDER build: a tampered or mis-published manifest
	// would otherwise be a silent downgrade (and an attacker's easiest way to
	// reintroduce a fixed bug). Same-version restarts stay allowed — that is
	// the documented "swap the identical binary" path on boxes with no shell.
	if compareVersions(m.Version, s.Version) < 0 {
		return "", fmt.Errorf("manifest offers %s but this daemon runs %s: refusing to downgrade", m.Version, s.Version)
	}
	art, ok := m.Platforms[platformKey()]
	if !ok {
		return "", fmt.Errorf("update manifest has no binary for %s", platformKey())
	}
	if art.URL == "" {
		return "", fmt.Errorf("update manifest: missing url for %s", platformKey())
	}
	target := s.updateTarget()
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return "", err
	}
	tmp := target + ".new"
	_ = os.Remove(tmp)
	body, err := fetchURL(art.URL, 256<<20)
	if err != nil {
		return "", err
	}
	if art.SHA256 != "" {
		sum := sha256.Sum256(body)
		if got, want := hex.EncodeToString(sum[:]), strings.ToLower(art.SHA256); got != want {
			_ = os.Remove(tmp)
			return "", fmt.Errorf("%w: got %s, want %s", errChecksumMismatch, got, want)
		}
	}
	if err := os.WriteFile(tmp, body, 0o755); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return m.Version, nil
}

// CommitUpdate swaps the staged binary into place (backing up the current
// one to <target>.old, then removing the backup) and re-executes the
// daemon from the new binary with the same arguments. A successful call
// never returns on either platform (unix: exec replaces the process
// image; windows: the new process is spawned and the current one exits),
// so instead of returning an error the failure paths log it via the node
// and the running daemon keeps running untouched.
func (s *Service) CommitUpdate() {
	target := s.updateTarget()
	staged := target + ".new"
	if _, err := os.Stat(staged); err != nil {
		s.commitLogf("self-update: %v", err)
		return
	}
	if err := swapBinary(target, staged); err != nil {
		s.commitLogf("self-update: swap: %v", err)
		return
	}
	s.commitLogf("self-update: installed %s; restarting", target)
	// restartExec only returns on failure (unix: exec replaces the process
	// image; windows: os.Exit), so logging its result is enough — reaching
	// this line at all means the restart did not take effect.
	s.commitLogf("self-update: restart: %v", restartExec(target, os.Args))
}

// commitLogf logs a self-update message without failing when the service
// has no node attached (unit tests construct Service directly).
func (s *Service) commitLogf(format string, args ...any) {
	if s.n != nil {
		s.n.Logf(format, args...)
	}
}

// swapBinary atomically installs staged over target: any existing target is
// backed up to <target>.old (on unix that may be the running binary —
// rename-over-running is allowed there), then the backup is removed. On
// failure the original target is restored if it was moved away.
func swapBinary(target, staged string) error {
	backup := target + ".old"
	_ = os.Remove(backup)
	if _, err := os.Stat(target); err == nil {
		if err := os.Rename(target, backup); err != nil {
			return fmt.Errorf("backup %s: %w", target, err)
		}
	}
	if err := os.Rename(staged, target); err != nil {
		// Try to restore the backup so the target is not left missing.
		if _, serr := os.Stat(target); os.IsNotExist(serr) {
			_ = os.Rename(backup, target)
		}
		return fmt.Errorf("install %s: %w", target, err)
	}
	_ = os.Remove(backup)
	return nil
}

// ForceUpdateCheck clears the update-check cache so the next
// CheckDaemonUpdate refetches the manifest immediately (the web UI's
// "Check now" button and tests use this).
func (s *Service) ForceUpdateCheck() {
	s.updateMu.Lock()
	s.updateInfo = DaemonUpdateInfo{}
	s.updateMu.Unlock()
}

// MaybeAutoUpdate checks for a newer daemon on every daemon tick and, when
// update_auto is on and no sync/scan is in flight, applies it. Internally
// throttled by CheckDaemonUpdate.
//
// The CHECK runs even when update_auto is off: an operator-run box must still
// learn that a release exists (one log line, and a truthful /api/update/check)
// instead of only finding out while a dashboard happens to be open. Only the
// install is gated on update_auto.
func (s *Service) MaybeAutoUpdate() {
	if s.UpdateURL == "" {
		return
	}
	info := s.CheckDaemonUpdate()
	if !s.UpdateAuto || !info.UpdateAvailable {
		return
	}
	if s.n != nil && s.n.SyncRunning() {
		return // don't restart mid-transfer; next tick retries
	}
	if _, err := s.StageUpdate(); err != nil {
		if s.n != nil {
			s.n.Logf("self-update: auto stage: %v", err)
		}
		return
	}
	go func() {
		// Give the current tick cycle a moment to finish, then commit.
		time.Sleep(2 * time.Second)
		s.CommitUpdate()
	}()
}

// CleanupUpdateArtifacts removes stale staging/backup files from a
// previous self-update. Safe to call at startup.
func (s *Service) CleanupUpdateArtifacts() {
	if s.UpdateURL == "" && s.UpdateDir == "" && s.ConfigPath == "" {
		return
	}
	target := s.updateTarget()
	for _, p := range []string{target + ".new", target + ".old"} {
		_ = os.Remove(p)
	}
}
