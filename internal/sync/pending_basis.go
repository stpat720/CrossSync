package sync

import (
	"fmt"
	"time"
)

// pendingViewMaxAge is how old a peer view may be and still be treated as "the
// peer has not moved". A round runs every few seconds when the link is healthy;
// a view older than this, with no round in flight to that peer, means the peer
// may have changed with nothing having refreshed our picture of it.
const pendingViewMaxAge = 90 * time.Second

// roundState is one peer's round activity for this folder (see Engine.rounds).
type roundState struct {
	busy    bool
	started time.Time
}

// BeginRound records that a sync round with peerID starts now for this folder.
// The node calls it before the pull phase of every session, for every folder the
// session covers, and EndRound when the session ends.
func (e *Engine) BeginRound(peerID uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.rounds == nil {
		e.rounds = map[uint64]*roundState{}
	}
	r := e.rounds[peerID]
	if r == nil {
		r = &roundState{}
		e.rounds[peerID] = r
	}
	r.busy = true
	r.started = time.Now()
}

// EndRound records that the round with peerID is over for this folder.
func (e *Engine) EndRound(peerID uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if r := e.rounds[peerID]; r != nil {
		r.busy = false
	}
}

// roundBusyLocked reports whether a round with peerID is in flight.
func (e *Engine) roundBusyLocked(peerID uint64) bool {
	r := e.rounds[peerID]
	return r != nil && r.busy
}

// PendingBasis says how much of the cached pending counters may be published,
// and as of when.
//
// The invariant the operator cares about is that "pending up" on one side and
// "pending down" on the other are the same fact seen from both ends. That only
// holds when both numbers come from the same pair of index snapshots. It is not
// a fact about files that can be computed in isolation: it is a statement about
// my index versus my copy of yours, so a stale copy silently falsifies it.
//
// Measured failure the gate exists to prevent: with the peer link down for nine
// minutes, one side reported "741 files pending upload" (its copy of the peer's
// index predated the peer's scan) while the other reported "0 pending" -- two
// numbers, same folder, contradicting each other, and neither was a lie about
// the snapshot it came from.
//
// The gate has TWO answers, not one, because the two figures fail differently:
//
//   - The DOWNLOAD figure is "what my index needs from the peer's view". An
//     older peer view can only make it pull less (and the next round fixes it);
//     it can never invent work. So it is publishable whenever a complete peer
//     view exists and is either recent OR backed by a round with that peer that
//     is running right now — a live round is freshness, because the peer is
//     demonstrably exchanging with us and the counters are recomputed when the
//     round ends. This is the case that used to be reported as unknown: a
//     74-minute sync round left EVERY folder on BOTH nodes showing "awaiting
//     sync" (reason "peer view is 1h13m old"), including folders that were idle
//     and perfectly in sync.
//
//   - The UPLOAD figure is "what the peer's view lacks of mine". That reads a
//     file past the end of a truncated view as owed, so it needs the strict
//     conditions: no view mid-exchange, AND this index not yet advanced past
//     what we last TOLD the peer (sentSeq >= ownSeq). Mid-exchange is worse than
//     stale: a view truncated at 54,353 of 84,353 entries produced "pending ↑
//     30,000" while both trees were byte-identical. Note sentSeq < ownSeq is
//     ALWAYS true while a round is applying received changes, so gating the
//     whole row on it (the old flat rule) is what blanked the download figure
//     exactly when it was most informative.
type PendingBasis struct {
	// Down reports that pending_files/pending_bytes may be published.
	Down bool
	// Up reports that pending_up may be published. It implies Down. False with
	// Down true is a normal row: a download count and no upload count.
	Up bool
	// AsOf is the unix second of the OLDEST view exchange the numbers rest on;
	// 0 when no exchange has completed. While a round is in flight this is when
	// that peer's view was last completed, i.e. "as of round start".
	AsOf int64
	// Reason says why a figure is withheld ("" when both are published).
	Reason string
}

// PendingBasisState is the current answer (see PendingBasis). It reads only
// in-memory state: no index walk, no I/O — /api/status calls it per folder per
// poll.
func (e *Engine) PendingBasisState() PendingBasis {
	e.mu.Lock()
	defer e.mu.Unlock()
	var b PendingBasis
	if len(e.peers) == 0 {
		b.Reason = "no completed index exchange with a peer yet"
		return b
	}
	// A truncated view is never truth, in either direction: the exchange is
	// still appending, so nothing it says is final yet.
	for id := range e.peers {
		if e.viewPending[id] {
			b.Reason = "a peer index exchange is in progress"
			return b
		}
	}
	now := time.Now()
	var oldest, staleAt time.Time
	for id := range e.peers {
		at, ok := e.viewAt[id]
		if !ok {
			b.Reason = "no completed index exchange with a peer yet"
			return b
		}
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
		if now.Sub(at) <= pendingViewMaxAge {
			continue // recent: the peer cannot have moved on unseen
		}
		if e.roundBusyLocked(id) {
			continue // a round with that peer is running: fresh as of its start
		}
		if staleAt.IsZero() || at.Before(staleAt) {
			staleAt = at
		}
	}
	b.AsOf = oldest.Unix()
	if !staleAt.IsZero() {
		b.Reason = fmt.Sprintf("peer view is %s old (no recent exchange)", now.Sub(staleAt).Round(time.Second))
		return b
	}
	b.Down = true
	// What we have told each peer must be everything we have: otherwise their
	// "pending down" is computed against an older copy of this index than the one
	// our "pending up" is computed from.
	ownSeq := e.Ix.MaxSeq()
	for id := range e.peers {
		_, sentSeq, _, ok, err := e.Ix.GetPeerIndex(id)
		if err != nil {
			b.Reason = "peer index state unavailable"
			return b
		}
		if !ok {
			b.Reason = "this index has not been sent to the peer yet"
			return b
		}
		if sentSeq < ownSeq {
			b.Reason = fmt.Sprintf("this index has %d change(s) the peer has not been sent, so the upload figure is withheld", ownSeq-sentSeq)
			return b
		}
	}
	b.Up = true
	return b
}
