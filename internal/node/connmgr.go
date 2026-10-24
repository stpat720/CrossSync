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

	// inFlight marks peers with a sync running right now, so the scheduler
	// tick, Ping, and manual syncs never run two sessions with the same
	// peer concurrently (they would corrupt backoff state and event order).
	inFlightMu sync.Mutex
	inFlight   map[uint64]bool

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
		inFlight:  map[uint64]bool{},
		state:     map[uint64]*peerState{},
	}
}

// SetPeers replaces the scheduled peer set at runtime (live add/update/
// delete of peers without a restart). State for peers that are no longer
// configured is pruned so it cannot accumulate. Safe for concurrent use.
func (m *ConnManager) SetPeers(peers []config.Peer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers = append([]config.Peer(nil), peers...)
	for id := range m.state {
		kept := false
		for _, p := range m.peers {
			if p.ID == id {
				kept = true
				break
			}
		}
		if !kept {
			delete(m.state, id)
		}
	}
}

// Ping attempts a sync with one peer immediately, so a freshly added peer
// connects without waiting for the next scheduler tick. It respects the
// peer's backoff state (a failing peer is not hammered) and skips when a
// sync with that peer is already running elsewhere.
func (m *ConnManager) Ping(id uint64) {
	if !m.inFlightMuOK(id) {
		return
	}
	defer m.clearInFlight(id)
	st := m.stateFor(id)
	if m.now().Before(st.nextAt) {
		return
	}
	synced, err := m.syncFn(id)
	if err != nil {
		m.failed(id, err)
		return
	}
	transitioned := m.succeeded(id, synced)
	name := m.peerName(id)
	if m.onRecover != nil {
		m.onRecover(name)
	}
	if transitioned {
		m.logf("sync with peer %s: back online", name)
		m.emit(events.CatPeer, events.SevInfo, name, "peer back online", "")
	}
}

// inFlightMuOK claims the in-flight slot for id, reporting whether an
// attempt should proceed. At most one sync runs per peer at a time, no
// matter how many entry points (scheduler tick, manual sync, Ping) fire
// concurrently.
func (m *ConnManager) inFlightMuOK(id uint64) bool {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	if m.inFlight[id] {
		return false
	}
	m.inFlight[id] = true
	return true
}

func (m *ConnManager) clearInFlight(id uint64) {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	delete(m.inFlight, id)
}

// snapshotPeers returns the current peer list under the lock.
func (m *ConnManager) snapshotPeers() []config.Peer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]config.Peer(nil), m.peers...)
}

// SyncAll attempts to sync with every configured peer that is not currently
// in backoff. It never blocks on a peer and never aborts the daemon.
func (m *ConnManager) SyncAll() {
	now := m.now()
	for _, p := range m.snapshotPeers() {
		st := m.stateFor(p.ID)
		m.mu.Lock()
		due := !now.Before(st.nextAt)
		m.mu.Unlock()
		if !due {
			continue
		}
		if !m.inFlightMuOK(p.ID) {
			continue
		}
		synced, err := m.syncFn(p.ID)
		m.clearInFlight(p.ID)
		if err != nil {
			m.failed(p.ID, err)
			continue
		}
		transitioned := m.succeeded(p.ID, synced)
		// Resolve any open "peer offline" for this peer on EVERY successful
		// round, not only on a transition: a disconnect seen by a session (not by
		// this manager) opens an attention event without moving the backoff
		// state, so a transition alone could never clear it. The node checks
		// cheaply whether anything is open before writing.
		if m.onRecover != nil {
			m.onRecover(p.Name)
		}
		if transitioned {
			m.logf("sync with peer %s: back online", p.Name)
			// The recovery event is recorded AFTER the resolve so it is not
			// swept up by it.
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
		name := m.peerNameLocked(id)
		m.logf("sync with peer %s: %v (retrying in %s)", name, err, delay.Round(time.Second))
		reason := peerOfflineReason(err)
		m.emit(events.CatPeer, events.SevWarn, name, fmt.Sprintf("peer offline: %s (retrying in %s)", reason, delay.Round(time.Second)), "")
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peerNameLocked(id)
}

// peerNameLocked reads the peer name assuming m.mu is already held. Use it
// from paths (failed) that run under the lock; a re-lock would deadlock.
func (m *ConnManager) peerNameLocked(id uint64) string {
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
