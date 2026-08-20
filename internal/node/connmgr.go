package node

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/events"
)

// Backoff parameters for peers that are offline or failing. The delay starts
// at backoffInitial and doubles on each consecutive failure up to
// backoffMax, so an unreachable peer is not hammered (and does not spam the
// log) on every tick while the daemon runs for months unattended.
const (
	backoffInitial = 5 * time.Second
	backoffMax     = 5 * time.Minute
)

// peerState tracks the backoff state of one peer.
type peerState struct {
	nextAt   time.Time // do not attempt before this
	failures int
	lastErr  string
	lastOK   int64 // unix nanos of last successful session (0 = never)
	lastSync int64 // unix nanos of last session that actually synced shared folders
}

// ConnManager schedules outbound sync attempts per peer with exponential
// backoff. It is quiet while peers are healthy: it logs on the first
// failure, when the failure reason changes, and when a peer comes back
// online. The sync function and clock are injectable for testing.
type ConnManager struct {
	peers     []config.Peer
	syncFn    func(id uint64) (int, error) // returns shared folders synced
	logf      func(format string, args ...any)
	onEvent   func(*events.Event)
	onRecover func(name string) // peer came back: dismiss outstanding offline attention
	now       func() time.Time

	mu    sync.Mutex
	state map[uint64]*peerState
}

// NewConnManager creates a manager that calls syncFn for each configured
// peer, logging through logf and emitting peer events through onEvent.
// onRecover (may be nil) is called with the peer's name when it recovers
// from a failure, so the node can dismiss outstanding "peer offline"
// attention events. syncFn returns the number of shared folders actually
// synced (0 = the peer is reachable but no folders are shared with it).
func NewConnManager(peers []config.Peer, syncFn func(id uint64) (int, error), logf func(format string, args ...any), onEvent func(*events.Event), onRecover func(name string)) *ConnManager {
	return &ConnManager{
		peers:     peers,
		syncFn:    syncFn,
		logf:      logf,
		onEvent:   onEvent,
		onRecover: onRecover,
		now:       time.Now,
		state:     map[uint64]*peerState{},
	}
}

// SyncAll attempts to sync with every configured peer that is not currently
// in backoff. It never blocks on a peer and never aborts the daemon.
func (m *ConnManager) SyncAll() {
	now := m.now()
	for _, p := range m.peers {
		st := m.stateFor(p.ID)
		m.mu.Lock()
		due := !now.Before(st.nextAt)
		m.mu.Unlock()
		if !due {
			continue
		}
		synced, err := m.syncFn(p.ID)
		if err != nil {
			m.failed(p.ID, err)
			continue
		}
		if m.succeeded(p.ID, synced) {
			m.logf("sync with peer %s: back online", p.Name)
			// Dismiss the outstanding "peer offline" attention events FIRST
			// (they stay in history as resolved), then record the recovery
			// as a fresh info event so it is not swept up by the resolve.
			if m.onRecover != nil {
				m.onRecover(p.Name)
			}
			m.emit(events.CatPeer, events.SevInfo, p.Name, "peer back online", "")
		}
	}
}

func (m *ConnManager) stateFor(id uint64) *peerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.state[id]
	if !ok {
		st = &peerState{}
		m.state[id] = st
	}
	return st
}

// succeeded resets a peer's backoff state, reporting whether it was in
// backoff before (i.e. whether the caller should announce recovery). It
// records when the peer was last online (any successful session) and, when
// the session actually synced shared folders (synced > 0), when it last
// synced.
func (m *ConnManager) succeeded(id uint64, synced int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.state[id]
	if !ok {
		return false
	}
	recovered := st.failures > 0
	st.failures = 0
	st.lastErr = ""
	st.nextAt = time.Time{}
	now := m.now().UnixNano()
	st.lastOK = now
	if synced > 0 {
		st.lastSync = now
	}
	return recovered
}

// LastOnline returns when the peer was last reachable (any successful
// session), or the zero time if never.
func (m *ConnManager) LastOnline(id uint64) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st, ok := m.state[id]; ok && st.lastOK > 0 {
		return time.Unix(0, st.lastOK)
	}
	return time.Time{}
}

// LastSync returns when data last synced with the peer (a session that
// actually had shared folders), or the zero time if never.
func (m *ConnManager) LastSync(id uint64) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st, ok := m.state[id]; ok && st.lastSync > 0 {
		return time.Unix(0, st.lastSync)
	}
	return time.Time{}
}

// LastOK returns the time of the last successful sync with a peer, or the
// zero time if none has happened.
func (m *ConnManager) LastOK(id uint64) time.Time {
	return m.LastOnline(id)
}

// failed records a failure and schedules the next attempt with exponential
// backoff. It logs only on the first failure or when the error changes, so
// an offline peer produces a handful of lines, not one per tick.
func (m *ConnManager) failed(id uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.state[id]
	if !ok {
		return
	}
	first := st.failures == 0
	st.failures++
	delay := backoffDelay(st.failures)
	st.nextAt = m.now().Add(delay)
	msg := err.Error()
	if first || msg != st.lastErr {
		m.logf("sync with peer %s: %v (retrying in %s)", m.peerName(id), err, delay.Round(time.Second))
		reason := peerOfflineReason(err)
		m.emit(events.CatPeer, events.SevWarn, m.peerName(id), fmt.Sprintf("peer offline: %s (retrying in %s)", reason, delay.Round(time.Second)), "")
	}
	st.lastErr = msg
}

// peerOfflineReason maps a failed sync error to a human-actionable reason
// for the event store. The generic fingerprint rejection is rewritten so an
// operator can tell a real network outage from a changed peer identity.
func peerOfflineReason(err error) string {
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "fingerprint is not in the allowlist"),
		strings.Contains(s, "fingerprint mismatch"),
		strings.Contains(s, "certificate") && (strings.Contains(s, "verify") || strings.Contains(s, "not in")):
		return "TLS fingerprint mismatch — this peer's certificate changed or is not pinned. Run `crosssync fingerprint` on that server and paste the result into peers[].fingerprint"
	case strings.Contains(s, "tls"):
		return "TLS handshake failed — check the peer's fingerprint and TLS settings"
	default:
		return err.Error()
	}
}

func (m *ConnManager) emit(cat events.Category, sev events.Severity, path, reason, linked string) {
	if m.onEvent == nil {
		return
	}
	m.onEvent(&events.Event{
		TS: time.Now(), Folder: "peers", Path: path,
		Category: cat, Severity: sev, Reason: reason, Linked: linked,
	})
}

func (m *ConnManager) peerName(id uint64) string {
	for _, p := range m.peers {
		if p.ID == id {
			return p.Name
		}
	}
	return fmt.Sprintf("%d", id)
}

// backoffDelay returns the delay for the n-th consecutive failure.
func backoffDelay(n int) time.Duration {
	d := backoffInitial
	for i := 1; i < n; i++ {
		d *= 2
		if d >= backoffMax {
			return backoffMax
		}
	}
	return d
}
