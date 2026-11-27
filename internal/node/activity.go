package node

import (
	"errors"
	"sort"
	std "sync"
	"time"
)

// ScanStatus is the live or last-known state of a scan for one folder. It
// is what the UI uses to answer "how much has been scanned and when did it
// start". Fields mirror scanner.Progress plus the applied-change count and
// timestamps/errors added by the node.
type ScanStatus struct {
	Folder    string `json:"folder"`
	Running   bool   `json:"running"`
	Started   int64  `json:"started"`    // unix seconds when the current scan started (0 = not running)
	Phase     string `json:"phase"`      // walking | hashing | deleting | applying | done
	Walked    int    `json:"walked"`     // directory entries walked
	HashDone  int    `json:"hash_done"`  // files hashed so far
	HashTotal int    `json:"hash_total"` // total files queued to hash
	Changed   int    `json:"changed"`    // changes applied so far (or in the last scan)
	InFlight  int    `json:"inflight"`   // changed files held back this scan: still being written
	// Committed counts the index entries this pass has BANKED, i.e. committed
	// in batches while the pass was still running rather than saved up for the
	// end. It is seeded with the folder's live entry count as the pass starts
	// (refreshed for a first pass), so a pass that RESUMES after a restart
	// reports the work it inherited instead of "committed 0" while half the
	// share sits in the index. Batches is how many commits that took.
	Committed int64 `json:"committed"`
	Batches   int   `json:"batches"`
	// Ignored are the bytes the walk deliberately did not index: FUSE/SMB
	// bookkeeping names and CrossSync's own .sfx-* areas. They occupy the
	// share without being data, and they were invisible until now.
	IgnoredFiles int    `json:"ignored_files"`
	IgnoredBytes int64  `json:"ignored_bytes"`
	Finished     int64  `json:"finished"` // unix seconds the last scan finished (0 = never)
	Error        string `json:"error"`    // error text from the last scan, if any

	// Scoped is the watcher-driven, path-scoped pass's OWN live state. A full
	// walk and a scoped pass of the same folder may run at the same time (see
	// Folder's scan slots), and the fields above describe only the walk: a
	// scoped pass must not reset walked/hash_total/committed while an operator
	// is watching a twenty-minute walk, nor clear firstPassPending. So it
	// publishes here instead, and /api/status shows both honestly. `passes`
	// counts completed scoped passes since the daemon started, so a pass too
	// fast to be caught `running` is still visible.
	Scoped ScopedScanStatus `json:"scoped"`
}

// ScopedScanStatus is one folder's last/current path-scoped (watcher) pass.
type ScopedScanStatus struct {
	Running   bool   `json:"running"`
	Started   int64  `json:"started"`   // unix seconds this pass started (0 = never)
	Phase     string `json:"phase"`     // walking | hashing | applying | cancelled | done
	Walked    int    `json:"walked"`    // entries the scoped walk covered
	HashDone  int    `json:"hash_done"` // files hashed so far
	HashTotal int    `json:"hash_total"`
	Changed   int    `json:"changed"`  // changes applied by this pass (or the last one)
	InFlight  int    `json:"inflight"` // held-back files, walk + scoped union
	Passes    int64  `json:"passes"`   // completed scoped passes since start
	Finished  int64  `json:"finished"` // unix seconds the last scoped pass finished
	Error     string `json:"error"`    // error text from the last scoped pass, if any
}

// SyncActivity is the live/last-known state of peer sync sessions.
type SyncActivity struct {
	Running  bool   `json:"running"`
	PeerID   uint64 `json:"peer_id,omitempty"` // peer currently being synced
	Started  int64  `json:"started"`           // unix seconds the current/any session started
	Finished int64  `json:"finished"`          // unix seconds of the last completed session
	Error    string `json:"error"`             // last sync error / interruption, if the last session failed
}

// activity is the internal tracker. The scan statuses are keyed by folder
// id; scans of different folders run concurrently (per-folder locking), but
// statuses are kept per folder so "last scan" info survives for every
// folder.
type activity struct {
	mu    std.Mutex
	scans map[string]*ScanStatus

	syncActive int    // concurrent peer sessions in flight
	syncPeer   uint64 // peer of the most recent session
	syncStart  int64  // start of the first active session
	syncEnd    int64  // finish of the last session
	syncErr    string // error of the last session, if it failed
}

// startScan marks a scan as running for a folder. banked is the number of
// entries the folder's index already holds (0 for a folder whose index is
// empty): it seeds the Committed figure so a resumed pass reports the work it
// inherited from the previous, interrupted pass.
func (n *Node) startScan(id string, banked int64) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, ok := n.act.scans[id]
	if !ok {
		st = &ScanStatus{Folder: id}
		n.act.scans[id] = st
	}
	st.Running = true
	st.Started = time.Now().Unix()
	st.Phase = "walking"
	st.Walked, st.HashDone, st.HashTotal, st.Changed = 0, 0, 0, 0
	st.InFlight = 0
	st.Committed = banked
	st.Batches = 0
	st.Error = ""
}

// setScan applies a mutation to a folder's live scan status.
func (n *Node) setScan(id string, fn func(*ScanStatus)) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, ok := n.act.scans[id]
	if !ok {
		return
	}
	fn(st)
}

// endScan records the outcome of a finished scan.
func (n *Node) endScan(id string, changed int, err error) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, ok := n.act.scans[id]
	if !ok {
		return
	}
	st.Running = false
	st.Finished = time.Now().Unix()
	st.Changed = changed
	switch {
	case errors.Is(err, ErrScanCancelled):
		// A pause cancelled the pass: a distinct outcome from finished and
		// from failed. It is not an error (deliberate, and nothing was
		// applied), so the status carries no error text — but the phase must
		// say "cancelled" or an operator cannot tell an interrupted pass from
		// a clean empty one.
		st.Phase = "cancelled"
		st.Error = ""
	case err != nil:
		st.Error = err.Error()
	}
}

// startScopedScan marks a path-scoped pass as running. It touches ONLY the
// Scoped sub-status: the walk's counters (and firstPassPending) belong to a walk
// that may be running right now, and resetting them would report a fresh walk
// with no progress while the operator is watching it.
func (n *Node) startScopedScan(id string) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, ok := n.act.scans[id]
	if !ok {
		st = &ScanStatus{Folder: id}
		n.act.scans[id] = st
	}
	st.Scoped.Running = true
	st.Scoped.Started = time.Now().Unix()
	st.Scoped.Phase = "walking"
	st.Scoped.Walked, st.Scoped.HashDone, st.Scoped.HashTotal, st.Scoped.Changed = 0, 0, 0, 0
	st.Scoped.Error = ""
}

// setScopedScan applies a mutation to the folder's scoped-pass status.
func (n *Node) setScopedScan(id string, fn func(*ScopedScanStatus)) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, ok := n.act.scans[id]
	if !ok {
		return
	}
	fn(&st.Scoped)
}

// endScopedScan records the outcome of a finished scoped pass.
func (n *Node) endScopedScan(id string, changed int, err error) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, ok := n.act.scans[id]
	if !ok {
		return
	}
	st.Scoped.Running = false
	st.Scoped.Finished = time.Now().Unix()
	st.Scoped.Changed = changed
	st.Scoped.Passes++
	switch {
	case errors.Is(err, ErrScanCancelled):
		st.Scoped.Phase = "cancelled"
		st.Scoped.Error = ""
	case err != nil:
		st.Scoped.Phase = "done"
		st.Scoped.Error = err.Error()
	default:
		st.Scoped.Phase = "done"
		st.Scoped.Error = ""
	}
}

// ScopedScanRunning reports whether a path-scoped pass is running for the
// folder. Separate from ScanRunning (the walk) because both may be true.
func (n *Node) ScopedScanRunning(id string) bool {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, ok := n.act.scans[id]
	return ok && st.Scoped.Running
}

// scanSnapshot returns a copy of one folder's live scan status (the zero value
// for a folder that has never been scanned). Used by the cancel path, which
// needs to say how many entries the abandoned pass had already banked.
func (n *Node) scanSnapshot(id string) ScanStatus {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	if st, ok := n.act.scans[id]; ok {
		return *st
	}
	return ScanStatus{Folder: id}
}

// ScanRunning reports whether a scan is currently running for the folder.
func (n *Node) ScanRunning(id string) bool {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	st, ok := n.act.scans[id]
	return ok && st.Running
}

// ScanStatuses returns a snapshot of every folder's scan status (running
// ones and the last finished one), including folders never scanned yet.
func (n *Node) ScanStatuses() []ScanStatus {
	n.foldersMu.RLock()
	ids := make([]string, 0, len(n.Folders))
	for id := range n.Folders {
		ids = append(ids, id)
	}
	n.foldersMu.RUnlock()
	sort.Strings(ids)
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	out := make([]ScanStatus, 0, len(ids))
	for _, id := range ids {
		st, ok := n.act.scans[id]
		if !ok {
			st = &ScanStatus{Folder: id}
		}
		out = append(out, *st)
	}
	return out
}

// SyncRunning reports whether any peer sync session is in flight.
func (n *Node) SyncRunning() bool {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	return n.act.syncActive > 0
}

// SyncSessions reports how many peer sync sessions are in flight right now.
// A session carries exactly one (peer, folder) pair, so this is the number of
// folders syncing at this instant, across all peers — the number that shows
// whether folders really are running side by side.
func (n *Node) SyncSessions() int {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	return n.act.syncActive
}

// WaitSyncIdle blocks until no sync session is in flight (or timeout elapses,
// reporting false; timeout <= 0 waits as long as needed). SyncWithPeer starts its
// folder sessions and returns as soon as they have a verdict — the daemon must not
// let one folder's long transfer hold the next scheduling decision — so a one-shot
// CLI run that is about to exit calls this to let the bytes actually move. Each
// session still ends on its own stall watchdog, so this cannot hang forever on a
// dead peer.
func (n *Node) WaitSyncIdle(timeout time.Duration) bool {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		if n.SyncSessions() == 0 {
			return true
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return false
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// beginSync marks one peer sync session as started.
func (n *Node) beginSync(peerID uint64) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	if n.act.syncActive == 0 {
		n.act.syncStart = time.Now().Unix()
	}
	n.act.syncActive++
	n.act.syncPeer = peerID
}

// endSync marks one peer sync session as finished. err is the session's
// outcome; the last error is kept only when the most recent session failed.
func (n *Node) endSync(err error) {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	if n.act.syncActive > 0 {
		n.act.syncActive--
	}
	n.act.syncEnd = time.Now().Unix()
	if n.act.syncActive == 0 {
		if err != nil {
			n.act.syncErr = err.Error()
		} else {
			n.act.syncErr = ""
		}
	}
}

// SyncActivity returns a snapshot of the sync state.
func (n *Node) SyncActivity() SyncActivity {
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	return SyncActivity{
		Running:  n.act.syncActive > 0,
		PeerID:   n.act.syncPeer,
		Started:  n.act.syncStart,
		Finished: n.act.syncEnd,
		Error:    n.act.syncErr,
	}
}
