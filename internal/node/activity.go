package node

import (
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
	Finished  int64  `json:"finished"`   // unix seconds the last scan finished (0 = never)
	Error     string `json:"error"`      // error text from the last scan, if any
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
// id; only one scan runs at a time per node (scanMu), but statuses are kept
// per folder so "last scan" info survives for every folder.
type activity struct {
	mu    std.Mutex
	scans map[string]*ScanStatus

	syncActive int    // concurrent peer sessions in flight
	syncPeer   uint64 // peer of the most recent session
	syncStart  int64  // start of the first active session
	syncEnd    int64  // finish of the last session
	syncErr    string // error of the last session, if it failed
}

// startScan marks a scan as running for a folder.
func (n *Node) startScan(id string) {
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
	if err != nil {
		st.Error = err.Error()
	}
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
	n.act.mu.Lock()
	defer n.act.mu.Unlock()
	ids := make([]string, 0, len(n.Folders))
	for id := range n.Folders {
		ids = append(ids, id)
	}
	sort.Strings(ids)
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
