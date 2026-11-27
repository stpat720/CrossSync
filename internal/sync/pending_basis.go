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

// roundBusyAnyLocked reports whether ANY round is in flight for this folder.
// The 5 s pending refresher uses it to stay out of a round's way: a rebuild on a
// million-file index holds the engine lock for minutes, and a round needs that
// lock to apply the peer's index frames and to plan its pulls. Caller holds e.mu.
func (e *Engine) roundBusyAnyLocked() bool {
	for _, r := range e.rounds {
		if r != nil && r.busy {
			return true
		}
	}
	return false
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
//     it can never invent work. So it is published whenever a COMPLETE peer view
//     exists, whatever its age, and always WITH that age (AsOf): a 90 s age rule
//     that withheld it is what made a 74-minute sync round report EVERY folder
//     on BOTH nodes as "awaiting sync", including folders that were idle and
//     perfectly in sync. The age is the honesty mechanism, not a mute switch.
//     The timestamp also survives a restart (it is persisted beside the
//     peer-index record and restored at folder open), because blanking it on
//     restart blanked the figure for every folder until its own exchange landed.
//
//   - The UPLOAD figure is "what the peer's view lacks of mine". That reads a
//     file past the end of a truncated view as owed, so it needs the strict
//     conditions: no view mid-exchange, a view actually HELD in this process
//     (a restored timestamp describes a view we no longer have — with no view in
//     memory every local file reads as owed), a RECENT view (an old one may
//     claim work the peer has since done), AND this index not yet advanced past
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
	// Reason says why a figure is withheld ("" when both are published). It also
	// names the view age when a stale view is published with the download figure:
	// the number is shown, but never as if it were current.
	Reason string
}

// PendingBasisState is the current answer (see PendingBasis). It reads only
// in-memory state: no index walk, no I/O — /api/status calls it per folder per
// poll. The persisted exchange times it relies on are loaded once, at folder
// open (see loadPeerViewExchanges).
//
// It must never BLOCK, and that is not a micro-optimisation: a live goroutine
// dump on Tower (146k-entry folder, 0.1.141) showed 21 of 24 goroutines queued
// on this call's e.mu.Lock while a scan held the engine lock, so every /api/status
// poll stalled for minutes and the UI reported it could not reach the API at all
// — a dead page behind a hashing pass. The lock is therefore a TryLock: when the
// engine is busy the answer comes from the last snapshot taken with the lock held,
// which is what a status poll wants anyway (a slightly old, honest figure instead
// of a hung request). The upload gating is unchanged: a stale basis withholds the
// upload figure, it never publishes one.
func (e *Engine) PendingBasisState() PendingBasis {
	if !e.mu.TryLock() {
		if snap := e.basisSnapshot.Load(); snap != nil {
			b := *snap
			// The upload figure is the strong claim ("every peer has this whole
			// index"). A snapshot taken before the engine last changed cannot
			// support it, so a busy engine always withholds it.
			b.Up = false
			if b.Reason == "" {
				b.Reason = "engine busy: upload figure not verified while a scan or sync round holds the lock"
			}
			return b
		}
		return PendingBasis{Reason: "engine busy: a scan or sync round holds the lock"}
	}
	defer e.mu.Unlock()
	b := e.pendingBasisLocked()
	e.basisSnapshot.Store(&b)
	return b
}

// pendingBasisLocked is the computation behind PendingBasisState; the caller
// holds e.mu.
func (e *Engine) pendingBasisLocked() PendingBasis {
	var b PendingBasis
	// The peers whose state these numbers may rest on: a view exchanged in this
	// process (e.peers) and one whose exchange TIME survived a restart (e.viewAt,
	// restored from the index DB). A view that is only remembered, not held, can
	// still date the download figure — and can never support the upload one.
	ids := make([]uint64, 0, len(e.peers)+len(e.viewAt))
	for id := range e.peers {
		ids = append(ids, id)
	}
	for id := range e.viewAt {
		if _, live := e.peers[id]; !live {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		b.Reason = "no completed index exchange with a peer yet"
		return b
	}
	// A truncated view is never truth, in either direction: the exchange is
	// still appending, so nothing it says is final yet.
	for _, id := range ids {
		if e.viewPending[id] {
			b.Reason = "a peer index exchange is in progress"
			return b
		}
	}
	now := time.Now()
	var oldest, staleAt time.Time
	for _, id := range ids {
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
	// A complete view is enough for the DOWNLOAD figure, however old it is: an
	// old view under-reports (we pull less than we could) and never invents
	// work, and the row carries AsOf so nobody reads the number as current.
	b.Down = true
	if !staleAt.IsZero() {
		// The view is old and no round is refreshing it: the number is still
		// published, WITH its age, and the upload figure stays withheld.
		b.Reason = fmt.Sprintf("peer view is %s old (no recent exchange)", now.Sub(staleAt).Round(time.Second))
		return b
	}
	// What we have told each peer must be everything we have: otherwise their
	// "pending down" is computed against an older copy of this index than the one
	// our "pending up" is computed from.
	ownSeq := e.Ix.MaxSeq()
	for _, id := range ids {
		if _, live := e.peers[id]; !live {
			// The timestamp outlived the process, the VIEW did not. With no view
			// in memory every local file reads as "the peer lacks this", which is
			// the phantom upload count this gate exists to prevent.
			b.Reason = fmt.Sprintf("peer view was recorded %s ago but has not been re-exchanged since this process started, so the upload figure is withheld", now.Sub(e.viewAt[id]).Round(time.Second))
			return b
		}
		sentSeq, sent := e.peerSent[id]
		if !sent {
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
