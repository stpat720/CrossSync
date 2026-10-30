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
}

// EndPeerViewExchange marks peerID's view authoritative again. It is called on
// the FINAL frame of a full send, and only then: an exchange that dies halfway
// leaves the flag set so its truncated view is never read as truth.
func (e *Engine) EndPeerViewExchange(peerID uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.viewPending, peerID)
	if e.viewAt == nil {
		e.viewAt = map[uint64]time.Time{}
	}
	e.viewAt[peerID] = time.Now()
}

// PeerViewsPending reports whether any stored peer view is a partial snapshot.
// Callers that present "what a peer needs from us" use it to say "unknown"
// rather than a number that is really an artefact of a broken exchange.
func (e *Engine) PeerViewsPending() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.viewPending) > 0
}
