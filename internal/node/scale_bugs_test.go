package node

import (
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/events"
)

// TestPeerStaysConnectedDuringLongRound: a round can run for many minutes when a
// peer has a backlog. Connectivity must not be derived only from COMPLETED
// rounds, or a big initial sync shows the peer as offline for its whole duration
// (observed live: 16+ minutes pulling a 59k-file backlog).
func TestPeerStaysConnectedDuringLongRound(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	m := NewConnManager(
		[]config.Peer{{ID: 2, Name: "nas-02", Addresses: []string{"h:1"}}},
		func(uint64) (int, error) {
			close(started)
			<-release // a slow round
			return 1, nil
		},
		func(string, ...any) {},
		nil, nil,
	)

	go m.SyncAll()
	<-started

	lo := m.LastOnline(2)
	if lo.IsZero() {
		t.Fatal("peer must count as online while a round is in flight")
	}
	if age := time.Since(lo); age > 5*time.Second {
		t.Fatalf("round-start timestamp is %s old, want ~now", age)
	}

	close(release)
	// After the round the success timestamp takes over and stays fresh.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if now := m.LastOnline(2); !now.IsZero() && time.Since(now) < 5*time.Second {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("peer should stay online after a successful round")
}

// TestMassChangeWarningClearsWhenConverged: the heads-up exists to say "a lot
// changed, verify it"; once the backlog drained it must not leave a warn badge
// (and an open-event count) on a healthy folder.
func TestMassChangeWarningClearsWhenConverged(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{{ID: "data", Path: t.TempDir(), SettleScans: 1, SettleSeconds: -1}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Close() }()

	reason := "mass change: 900 of 1000 files changed in one sync (>50% after a prior sync) — verify"
	if _, err := n.Events.Record(&events.Event{
		TS: time.Now(), Folder: "data", Category: events.CatWarning, Severity: events.SevWarn, Reason: reason,
	}); err != nil {
		t.Fatal(err)
	}
	open, err := n.Events.CountOpen()
	if err != nil {
		t.Fatal(err)
	}
	if open == 0 {
		t.Fatal("the warning should be open before the folder settles")
	}

	n.probMu.Lock()
	n.massChange["data"] = true
	n.probMu.Unlock()

	n.settleMassChange("data")

	open, err = n.Events.CountOpen()
	if err != nil {
		t.Fatal(err)
	}
	if open != 0 {
		t.Fatalf("mass-change warning still open after settling (%d open events)", open)
	}

	// A second convergence must be a no-op (the flag was cleared).
	n.settleMassChange("data")
}
