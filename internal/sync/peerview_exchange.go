package sync

import "time"

// Peer-view completeness.
//
// A full index exchange is CHUNKED (indexChunkSize entries per frame), and the
// receiver applies it as it arrives: the first frame replaces the stored view
// and later frames append. If the session dies mid-stream — a dropped VPN, a
// peer restart, an operator pausing — the view left behind is a PREFIX of the
// peer's index.
//
// That prefix is harmless for pulling (it makes us pull less, never more), but
// it is ruinous for the upload number: PendingUploads asks "which of my files
// is not in the peer's view", and every file past the truncation point answers
// yes. Measured on the live pair: views truncated at 54,353 and 37,675 entries
// out of 84,353 produced "pending ↑ 30,000" on one node and "46,678" on the
// other while both trees were byte-identical, and nothing was actually owed.
//
// So an exchange marks the view PENDING when its first frame lands and
// COMPLETE only on the FINAL frame. PendingUploads skips a pending view: the
// honest answer to "what does the peer lack" while the peer's index is half
// received is "unknown", not a number.

// BeginPeerViewExchange marks peerID's stored view as a partial snapshot. It is
// called when the first frame of a FULL (non-delta) index send arrives.
func (e *Engine) BeginPeerViewExchange(peerID uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.viewPending == nil {
		e.viewPending = map[uint64]bool{}
	}
	e.viewPending[peerID] = true
	// Upload work ignores a view that is mid-exchange, and a name is upload work
	// as soon as ONE authoritative view lacks it - so dropping a view from the
	// authoritative set changes the answer for names that are not in that view
	// at all (the equivalence test caught exactly that). The whole set is stale
	// until a full walk, which is fine: exchanges are once per session.
	e.markPendingAll()
}

// EndPeerViewExchange marks peerID's view authoritative again. It is called on
// the FINAL frame of a full send, and only then: an exchange that dies halfway
// leaves the flag set so its truncated view is never read as truth.
//
// The completion time is also written to this folder's index DB. It is the
// "as of" half of every pending figure, and it used to live only in this map —
// so a restart blanked it for EVERY folder at once and each one read as "no
// completed index exchange with a peer yet" until its own exchange landed.
// Measured live: one folder's long first index left the others, including an
// in-sync 741-file folder, showing "awaiting sync" for 50 minutes.
func (e *Engine) EndPeerViewExchange(peerID uint64) {
	now := time.Now()
	e.mu.Lock()
	delete(e.viewPending, peerID)
	e.markPendingAll()
	if e.viewAt == nil {
		e.viewAt = map[uint64]time.Time{}
	}
	e.viewAt[peerID] = now
	ix := e.Ix
	e.mu.Unlock()
	// Persisted outside the lock: this runs on a session goroutine once per
	// completed exchange, and the pending gate (which polls this state every
	// couple of seconds from /api/status) must never wait on a DB write.
	if ix == nil {
		return
	}
	if err := ix.SetPeerViewExchange(peerID, now); err != nil {
		e.Logf("folder %s: record the peer view exchange time: %v", e.ID, err)
	}
}

// PeerViewsPending reports whether any stored peer view is a partial snapshot.
// Callers that present "what a peer needs from us" use it to say "unknown"
// rather than a number that is really an artefact of a broken exchange.
func (e *Engine) PeerViewsPending() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.viewPending) > 0
}

// loadPeerViewExchanges restores the per-peer exchange times this folder's index
// DB recorded, so a restart does not blank the "as of" half of the pending
// figures. Called once from New, i.e. at folder open, never from a poll path.
//
// It restores TIMES only, never views: a view is a copy of the peer's index and
// is deliberately not persisted (it is cheap to re-exchange and would have to be
// invalidated per peer session). A remembered time therefore says "when we last
// knew this peer's index", which is what the download figure needs and what the
// upload figure can never use — see PendingBasisState.
func (e *Engine) loadPeerViewExchanges() {
	ats, err := e.Ix.PeerViewExchanges()
	if err != nil {
		e.Logf("folder %s: load the recorded peer view exchange times: %v", e.ID, err)
		return
	}
	if len(ats) == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.viewAt == nil {
		e.viewAt = map[uint64]time.Time{}
	}
	for id, at := range ats {
		// Never move a live timestamp backwards: an exchange that completed
		// while this process was already running is newer than the disk.
		if cur, ok := e.viewAt[id]; ok && !cur.Before(at) {
			continue
		}
		e.viewAt[id] = at
	}
}
