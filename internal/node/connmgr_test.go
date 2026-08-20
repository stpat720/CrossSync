package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/version"
)

// newTestMgr builds a ConnManager with a fake sync function and a
// controllable clock, returning the manager, the clock, and accessors for
// the recorded attempts and log lines.
func newTestMgr(t *testing.T, syncErr error) (*ConnManager, *time.Time, func() int, func() []string) {
	t.Helper()
	now := time.Unix(1_700_000_000, 0)
	var attempts []uint64
	var logs []string
	m := NewConnManager(
		[]config.Peer{{ID: 7, Name: "nas-02"}},
		func(id uint64) (int, error) {
			attempts = append(attempts, id)
			return 0, syncErr
		},
		func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		},
		nil,
		nil,
	)
	m.now = func() time.Time { return now }
	return m, &now, func() int { return len(attempts) }, func() []string { return logs }
}

func TestBackoffSkipsAttemptsWhileCoolingDown(t *testing.T) {
	m, now, attempts, _ := newTestMgr(t, errors.New("connection refused"))

	m.SyncAll() // first attempt fails
	if attempts() != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts())
	}
	// Immediately retrying must be skipped (in backoff).
	m.SyncAll()
	if attempts() != 1 {
		t.Fatalf("expected no attempt during backoff, got %d", attempts())
	}
	// Just before backoffInitial elapses: still skipped.
	*now = now.Add(backoffInitial - time.Second)
	m.SyncAll()
	if attempts() != 1 {
		t.Fatalf("expected no attempt before backoff expires, got %d", attempts())
	}
	// Once the backoff elapses: attempt again.
	*now = now.Add(backoffInitial)
	m.SyncAll()
	if attempts() != 2 {
		t.Fatalf("expected attempt after backoff expired, got %d", attempts())
	}
}

func TestBackoffEscalatesAndCaps(t *testing.T) {
	m, now, attempts, _ := newTestMgr(t, errors.New("boom"))

	// Each failure schedules the next attempt with a growing delay; advance
	// past the cap each time so every iteration is due.
	for i := 0; i < 12; i++ {
		m.SyncAll()
		if attempts() != i+1 {
			t.Fatalf("iteration %d: expected %d attempts, have %d", i, i+1, attempts())
		}
		*now = now.Add(backoffMax + time.Second)
	}
	if got := backoffDelay(1); got != backoffInitial {
		t.Fatalf("backoffDelay(1) = %s, want %s", got, backoffInitial)
	}
	if got := backoffDelay(2); got != 2*backoffInitial {
		t.Fatalf("backoffDelay(2) = %s, want %s", got, 2*backoffInitial)
	}
	if got := backoffDelay(30); got != backoffMax {
		t.Fatalf("backoffDelay(30) = %s, want %s (capped)", got, backoffMax)
	}
}

func TestBackoffRecoveryLogsBackOnline(t *testing.T) {
	var attempts []uint64
	var logs []string
	now := time.Unix(1_700_000_000, 0)
	healthy := false
	m := NewConnManager(
		[]config.Peer{{ID: 7, Name: "nas-02"}},
		func(id uint64) (int, error) {
			attempts = append(attempts, id)
			if healthy {
				return 1, nil
			}
			return 0, errors.New("connection refused")
		},
		func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		},
		nil,
		nil,
	)
	m.now = func() time.Time { return now }

	m.SyncAll() // fail once
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt")
	}
	// Peer comes back: next attempt succeeds and recovery is logged.
	healthy = true
	now = now.Add(backoffInitial)
	m.SyncAll()
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "back online") {
		t.Fatalf("expected 'back online' log, got:\n%s", joined)
	}
	// And no more backoff: next tick attempts immediately.
	now = now.Add(2 * time.Second)
	m.SyncAll()
	if len(attempts) != 3 {
		t.Fatalf("expected immediate retry after recovery, got %d attempts", len(attempts))
	}
}

func TestBackoffLogsOncePerStateChange(t *testing.T) {
	m, now, _, logs := newTestMgr(t, errors.New("connection refused"))

	m.SyncAll()
	m.SyncAll() // skipped by backoff, no new log
	if n := countLogs(logs(), "connection refused"); n != 1 {
		t.Fatalf("expected 1 failure log line, got %d", n)
	}
	// Error changes -> new log line.
	*now = now.Add(backoffMax + time.Second)
	m.syncFn = func(id uint64) (int, error) { return 0, errors.New("tls: handshake failed") }
	m.SyncAll()
	if n := countLogs(logs(), "tls: handshake failed"); n != 1 {
		t.Fatalf("expected 1 log line for the new error, got %d", n)
	}
}

func countLogs(logs []string, needle string) int {
	n := 0
	for _, l := range logs {
		if strings.Contains(l, needle) {
			n++
		}
	}
	return n
}

// TestLastOnlineVsLastSync verifies that a peer that is reachable but shares
// no folders records last-online but not last-sync, while a session that
// actually syncs shared folders records both.
func TestLastOnlineVsLastSync(t *testing.T) {
	var synced int
	now := time.Unix(1_700_000_000, 0)
	m := NewConnManager(
		[]config.Peer{{ID: 7, Name: "nas-02"}},
		func(id uint64) (int, error) { return synced, nil },
		func(string, ...any) {},
		nil,
		nil,
	)
	m.now = func() time.Time { return now }

	// Peer reachable but nothing shared: online only.
	synced = 0
	m.SyncAll()
	if m.LastOnline(7).IsZero() {
		t.Fatal("last online should be set after a successful session")
	}
	if !m.LastSync(7).IsZero() {
		t.Fatal("last sync must NOT be set when no folders are shared")
	}

	// Now a session that actually syncs shared folders.
	synced = 2
	now = now.Add(30 * time.Second)
	m.SyncAll()
	if ls := m.LastSync(7); ls.IsZero() || !ls.Equal(now) {
		t.Fatalf("last sync = %v, want %v", ls, now)
	}
	if lo := m.LastOnline(7); lo.IsZero() || !lo.Equal(now) {
		t.Fatalf("last online = %v, want %v", lo, now)
	}
}

// TestRecoveryInvokesOnRecover verifies that onRecover is called with the
// peer's name exactly when a peer recovers from a failure — not on a
// steady-state success that never failed.
func TestRecoveryInvokesOnRecover(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var attempts []uint64
	var recovered []string
	healthy := false
	m := NewConnManager(
		[]config.Peer{{ID: 7, Name: "nas-02"}},
		func(id uint64) (int, error) {
			attempts = append(attempts, id)
			if healthy {
				return 1, nil
			}
			return 0, errors.New("connection refused")
		},
		func(string, ...any) {},
		nil,
		func(name string) { recovered = append(recovered, name) },
	)
	m.now = func() time.Time { return now }

	m.SyncAll() // first attempt fails (offline attention opens)
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt")
	}
	if len(recovered) != 0 {
		t.Fatalf("onRecover must NOT fire on a failure, got %v", recovered)
	}
	// Peer comes back: recovery must fire once with the peer's name.
	healthy = true
	now = now.Add(backoffInitial)
	m.SyncAll()
	if len(recovered) != 1 || recovered[0] != "nas-02" {
		t.Fatalf("onRecover = %v, want [nas-02]", recovered)
	}
	// Subsequent successes (never failed in between) must not re-fire.
	now = now.Add(2 * time.Second)
	m.SyncAll()
	if len(recovered) != 1 {
		t.Fatalf("onRecover fired %d times on steady success, want 1", len(recovered))
	}
}

// TestPeerRecoveredAutoResolvesOfflineAttention verifies the end-to-end
// contract behind the user-facing behavior: when a peer comes back online,
// every outstanding "peer offline" attention event is auto-resolved (stays
// in history, leaves the badge), and a later outage opens fresh attention.
func TestPeerRecoveredAutoResolvesOfflineAttention(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{{ID: "data", Path: t.TempDir()}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	// The peer drops: two "peer offline" warn events accumulate (a long
	// outage with changing error reasons emits more than one).
	for i := 0; i < 2; i++ {
		if _, err := n.Events.Record(&events.Event{
			TS: time.Now(), Folder: "peers", Path: "nas-02",
			Category: events.CatPeer, Severity: events.SevWarn,
			Reason: fmt.Sprintf("peer offline: connection refused (attempt %d)", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Both are open and the peer is one distinct attention condition.
	evs, err := n.Events.Query(events.Filter{Path: "nas-02"})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 {
		t.Fatalf("expected 2 offline events, got %d", len(evs))
	}
	if open, _ := n.Events.CountOpen(); open != 1 {
		t.Fatalf("attention count = %d, want 1", open)
	}

	// Peer comes back: recovery auto-resolves BOTH offline notes.
	n.peerRecovered("nas-02")
	evs, err = n.Events.Query(events.Filter{Path: "nas-02"})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range evs {
		if e.Resolution != events.ResAutoResolved {
			t.Fatalf("offline event not auto-resolved: %+v", e)
		}
	}
	if open, _ := n.Events.CountOpen(); open != 0 {
		t.Fatalf("attention count after recovery = %d, want 0", open)
	}

	// A fresh outage opens a brand-new attention condition (the old
	// auto-resolved records are not reopened — they are history).
	if _, err := n.Events.Record(&events.Event{
		TS: time.Now(), Folder: "peers", Path: "nas-02",
		Category: events.CatPeer, Severity: events.SevWarn,
		Reason: "peer offline: connection refused (new outage)",
	}); err != nil {
		t.Fatal(err)
	}
	if open, _ := n.Events.CountOpen(); open != 1 {
		t.Fatalf("attention count after new outage = %d, want 1", open)
	}
}

// TestPeerOfflineReasonFingerprint verifies the friendly rewrite for TLS
// fingerprint mismatches, so an operator can tell identity problems from a
// plain network outage.
func TestPeerOfflineReasonFingerprint(t *testing.T) {
	cases := map[string]bool{ // error -> expect fingerprint wording
		"tls: failed to verify certificate: peer certificate fingerprint is not in the allowlist": true,
		"x509: certificate signed by unknown authority":          false,
		"dial tcp 100.64.0.2:6001: connectex: refused":           false,
	}
	for errText, wantFingerprint := range cases {
		r := peerOfflineReason(errors.New(errText))
		if strings.Contains(strings.ToLower(r), "fingerprint mismatch") != wantFingerprint {
			t.Fatalf("peerOfflineReason(%q) = %q, fingerprint-wording=%v, want %v", errText, r, !wantFingerprint, wantFingerprint)
		}
	}
}

// TestGuardBlocksDeletes covers the deletion-guard threshold logic: it must
// ignore tiny delete passes (under the absolute floor) and small percentages,
// but block a genuinely large wipe, including via the absolute file cap.
func TestGuardBlocksDeletes(t *testing.T) {
	cases := []struct {
		live, dels, pct, files int
		want                   bool
	}{
		{1000, 50, 25, 0, false},   // under the 100-file floor
		{1000, 150, 25, 0, false},  // 15% < 25%
		{1000, 300, 25, 0, true},   // 30% >= 25%
		{1000, 150, 10, 0, true},   // 15% >= 10%
		{1000, 250, 99, 200, true}, // absolute cap: 250 >= 200
		{0, 500, 25, 0, false},     // nothing live -> nothing to protect
	}
	for _, c := range cases {
		if got := guardBlocksDeletes(c.live, c.dels, c.pct, c.files); got != c.want {
			t.Fatalf("guardBlocksDeletes(live=%d,dels=%d,pct=%d,files=%d) = %v, want %v",
				c.live, c.dels, c.pct, c.files, got, c.want)
		}
	}
}

// TestDeletionGuardReportAndOverride verifies the operator override path: a
// folder can be flagged by the guard (one durable error event, idempotent),
// the pending deletions are reported, and ApplyPendingDeletions removes the
// files locally and clears the guard.
func TestDeletionGuardReportAndOverride(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{{ID: "data", Path: t.TempDir()}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	f := n.Folders["data"]
	root := f.Root
	for i := 0; i < 3; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%d.txt", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// Peer 2 tombstoned all three files. A realistic peer deletion adopted
	// our live version first ({1:1}) then bumped its own counter, so the
	// tombstone {1:1,2:2} dominates our copy — no conflict copy is created.
	var peerFiles []*index.FileInfo
	for i := 0; i < 3; i++ {
		peerFiles = append(peerFiles, &index.FileInfo{
			Name: fmt.Sprintf("f%d.txt", i), Deleted: true,
			Version: version.New().Bump(1).Bump(2).Bump(2),
		})
	}
	f.Engine.SetPeerIndex(2, peerFiles, false)

	if pd, err := n.PendingDeletionsCount("data"); err != nil || pd != 3 {
		t.Fatalf("PendingDeletionsCount = %d, %v; want 3", pd, err)
	}

	// Flag the guard: one durable error event, idempotent.
	n.reportDeletionGuard("data", "3 files deleted in one sync")
	n.reportDeletionGuard("data", "3 files deleted in one sync") // must not duplicate
	if !n.DeleteGuardTripped("data") {
		t.Fatal("guard should be tripped")
	}
	if open, _ := n.Events.CountOpenFolder("data"); open != 1 {
		t.Fatalf("open folder events = %d, want 1 (the guard error)", open)
	}

	// Operator override applies the deletions and clears the guard.
	applied, err := n.ApplyPendingDeletions("data")
	if err != nil {
		t.Fatal(err)
	}
	if applied != 3 {
		t.Fatalf("applied = %d, want 3", applied)
	}
	for i := 0; i < 3; i++ {
		if _, err := os.Stat(filepath.Join(root, fmt.Sprintf("f%d.txt", i))); !os.IsNotExist(err) {
			t.Fatalf("f%d.txt should be deleted, stat err = %v", i, err)
		}
	}
	if n.DeleteGuardTripped("data") {
		t.Fatal("guard should be cleared after override")
	}
	if open, _ := n.Events.CountOpenFolder("data"); open != 0 {
		t.Fatalf("open folder events after override = %d, want 0", open)
	}
}

// TestCaseCollisionWarning verifies case-only duplicates (invisible to SMB)
// surface as a durable warning that resolves once one of them is removed.
// The index entries are injected directly because the dev machine's
// filesystem is case-insensitive (Windows), while the collision only exists
// on a case-sensitive filesystem (Unraid/Linux).
func TestCaseCollisionWarning(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{{ID: "data", Path: t.TempDir()}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	f := n.Folders["data"]

	seed := func(name string) {
		if err := f.Ix.Put(&index.FileInfo{
			Name: name, Size: 1, ModifiedS: time.Now().Unix(),
			Type: index.TypeFile, Version: version.New().Bump(1),
		}); err != nil {
			t.Fatal(err)
		}
	}
	seed("Foo.txt")
	seed("foo.txt")

	n.checkCaseCollisions("data")
	if open, _ := n.Events.CountOpenFolder("data"); open != 1 {
		t.Fatalf("open folder events = %d, want 1 (case collision warning)", open)
	}
	evs, err := n.Events.Query(events.Filter{Folder: "data", Category: events.CatWarning, OpenOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || !strings.Contains(evs[0].Reason, "case collision") {
		t.Fatalf("unexpected collision events: %+v", evs)
	}

	// Removing one of the twins resolves the warning automatically.
	if err := f.Ix.Delete("foo.txt"); err != nil {
		t.Fatal(err)
	}
	n.checkCaseCollisions("data")
	if open, _ := n.Events.CountOpenFolder("data"); open != 0 {
		t.Fatalf("open folder events after resolving = %d, want 0", open)
	}
}

// TestRunAutoArchive verifies the auto-prune job: with the feature enabled
// it removes only ROUTINE logs older than the cutoff and always preserves
// open attention/conflict events; with it disabled it is a no-op.
func TestRunAutoArchive(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{{ID: "data", Path: t.TempDir()}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	old := time.Now().Add(-200 * 24 * time.Hour) // older than 90 days
	recent := time.Now().Add(-2 * 24 * time.Hour) // newer than the cutoff
	seed := func(ts time.Time, cat events.Category, sev events.Severity, res events.Resolution, path string) {
		t.Helper()
		if _, err := n.Events.Record(&events.Event{TS: ts, Folder: "data", Path: path,
			Category: cat, Severity: sev, Resolution: res, Reason: "r"}); err != nil {
			t.Fatal(err)
		}
	}
	// Old routine events (deletable): a resolved conflict and an info applied.
	seed(old, events.CatConflict, events.SevWarn, events.ResResolved, "old-resolved.txt")
	seed(old, events.CatApplied, events.SevInfo, events.ResOpen, "old-applied.txt")
	// Old OPEN attention conflict (must be kept even though it is old).
	seed(old, events.CatConflict, events.SevWarn, events.ResOpen, "old-open-conflict.txt")
	// Recent routine event (newer than cutoff, untouched).
	seed(recent, events.CatApplied, events.SevInfo, events.ResOpen, "recent-applied.txt")

	// Disabled: no-op.
	if deleted, err := n.RunAutoArchive(); err != nil || deleted != 0 {
		t.Fatalf("disabled RunAutoArchive = (%d, %v), want (0, nil)", deleted, err)
	}

	// Enable with a 90-day cutoff.
	n.Cfg.AutoArchiveEvents = true
	n.Cfg.AutoArchiveOlderThan = 90 * 24 * 60 * 60
	deleted, err := n.RunAutoArchive()
	if err != nil {
		t.Fatal(err)
	}
	// Removes the 2 old routine events (resolved conflict + old applied);
	// keeps the old OPEN conflict and the recent event.
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	evs, err := n.Events.Query(events.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, e := range evs {
		got[e.Path] = true
	}
	if got["old-resolved.txt"] || got["old-applied.txt"] {
		t.Fatalf("routine old events should be pruned: %v", got)
	}
	if !got["old-open-conflict.txt"] || !got["recent-applied.txt"] {
		t.Fatalf("open attention / recent events must survive: %v", got)
	}
	// The open conflict still needs attention.
	if open, _ := n.Events.CountOpenFolder("data"); open != 1 {
		t.Fatalf("open attention after auto-archive = %d, want 1", open)
	}
}

// TestCorruptIndexAutoRebuild verifies the recovery path: an unreadable
// per-folder index is quarantined (kept for forensics) and transparently
// rebuilt from the filesystem at startup, with a durable system event so
// the operator can see that a recovery happened.
func TestCorruptIndexAutoRebuild(t *testing.T) {
	meta := t.TempDir()
	root := t.TempDir()
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: meta,
		Folders: []config.Folder{{ID: "data", Path: root}},
	}

	// First daemon indexes 3 files.
	n1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%d.txt", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := n1.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	n1.Close()

	// Corrupt the index (and drop WAL/SHM sidecars so nothing valid hides
	// the damage).
	dbPath := filepath.Join(meta, "data.db")
	if err := os.WriteFile(dbPath, []byte("this is definitely not a sqlite database"), 0o644); err != nil {
		t.Fatal(err)
	}
	os.Remove(dbPath + "-wal")
	os.Remove(dbPath + "-shm")

	// Reopen: must auto-quarantine + rebuild from the filesystem.
	n2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	f := n2.Folders["data"]
	files := 0
	if err := f.Ix.List(func(fi *index.FileInfo) error {
		files++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if files != 3 {
		t.Fatalf("rebuilt index has %d files, want 3", files)
	}
	matches, err := filepath.Glob(dbPath + ".corrupt-*")
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected one quarantined index, got %v (%v)", matches, err)
	}
	evs, err := n2.Events.Query(events.Filter{Folder: "data", Category: events.CatSystem})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range evs {
		if strings.Contains(e.Reason, "rebuilt from filesystem") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a rebuild system event, got %+v", evs)
	}
}

// TestRebuildFolderManual verifies the CLI-level force rebuild: the current
// index is quarantined and regenerated from the filesystem on demand.
func TestRebuildFolderManual(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{{ID: "data", Path: t.TempDir()}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	root := n.Folders["data"].Root
	for i := 0; i < 2; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("g%d.txt", i)), []byte("y"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	oldPath := n.Folders["data"].Ix.Path()

	applied, err := n.RebuildFolder("data")
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 {
		t.Fatalf("rebuilt %d change(s), want 2", applied)
	}
	// The old index was quarantined (the fresh one now occupies the path).
	matches, _ := filepath.Glob(oldPath + ".corrupt-*")
	if len(matches) != 1 {
		t.Fatalf("expected one quarantined index, got %v", matches)
	}
	// Fresh index reflects the filesystem again.
	files := 0
	n.Folders["data"].Ix.List(func(fi *index.FileInfo) error {
		files++
		return nil
	})
	if files != 2 {
		t.Fatalf("fresh index has %d files, want 2", files)
	}
}

// TestAlertsFireThrottledAndScoped verifies the page-worthy alert path:
// folder errors and peer-offline warn events post to the endpoint, the same
// condition is throttled by the cooldown, and routine info events never
// post.
func TestAlertsFireThrottledAndScoped(t *testing.T) {
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &config.Config{
		Device:   config.Device{ID: 1, Name: "nas-01"},
		MetaDir:  t.TempDir(),
		AlertURL: srv.URL,
		Folders:  []config.Folder{{ID: "data", Path: t.TempDir()}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	n.alertCooldown = 10 * time.Minute // keep default-ish; throttle via short window below

	rec := func(cat events.Category, sev events.Severity, path, reason string) {
		t.Helper()
		n.recordEvent(&events.Event{TS: time.Now(), Folder: "data", Path: path,
			Category: cat, Severity: sev, Reason: reason})
	}

	// Folder error pages once.
	rec(events.CatError, events.SevWarn, "", "folder error: disk full — space needed on the share")
	if posts.Load() != 1 {
		t.Fatalf("folder error should page once, posts = %d", posts.Load())
	}
	// Same condition again within the cooldown: throttled.
	rec(events.CatError, events.SevWarn, "", "folder error: disk full — space needed on the share")
	if posts.Load() != 1 {
		t.Fatalf("same condition should be throttled, posts = %d", posts.Load())
	}
	// Peer offline pages.
	rec(events.CatPeer, events.SevWarn, "nas-02", "peer offline: connection refused (retrying in 5s)")
	if posts.Load() != 2 {
		t.Fatalf("peer offline should page, posts = %d", posts.Load())
	}
	// Routine info events never page.
	rec(events.CatApplied, events.SevInfo, "f.txt", "pulled from peer")
	rec(events.CatConflict, events.SevWarn, "x.txt", "concurrent edit: conflict copy created") // warn but NOT in the page-worthy set
	rec(events.CatPeer, events.SevInfo, "nas-02", "peer back online")
	if posts.Load() != 2 {
		t.Fatalf("routine/info/conflict events must not page, posts = %d", posts.Load())
	}

	// After the cooldown elapses, the same error can page again.
	n.alertCooldown = time.Nanosecond
	rec(events.CatError, events.SevWarn, "", "folder error: disk full — space needed on the share")
	if posts.Load() != 3 {
		t.Fatalf("condition should re-page after cooldown, posts = %d", posts.Load())
	}
}

// TestSendTestAlert verifies the manual test-alert path (used by the CLI
// and the Settings UI button).
func TestSendTestAlert(t *testing.T) {
	var got struct {
		Title   string `json:"title"`
		Message string `json:"message"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &config.Config{
		Device:   config.Device{ID: 1, Name: "nas-01"},
		MetaDir:  t.TempDir(),
		AlertURL: srv.URL,
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.SendTestAlert(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Title, "nas-01") || !strings.Contains(got.Message, "Test alert") {
		t.Fatalf("test alert payload = %+v", got)
	}
}
