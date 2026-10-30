package sync

import (
	"fmt"
	"time"
)

// pendingViewMaxAge is how old a peer view may be and still be treated as "the
// peer has not moved". A round runs every few seconds when the link is healthy;
// a view older than this means the peer may have changed with nothing having
// refreshed our picture of it.
const pendingViewMaxAge = 90 * time.Second

// PendingBasis reports whether the cached pending counters describe the CURRENT
// pair of indexes, and if not, why not.
//
// The invariant the operator cares about is that "pending up" on one side and
// "pending down" on the other are the same fact seen from both ends. That only
// holds when both numbers come from the same pair of index snapshots. It is not
// a fact about files that can be computed in isolation: it is a statement about
// my index versus my copy of yours, so a stale copy silently falsifies it.
//
// Measured failure this exists to prevent: with the peer link down for nine
// minutes, one side reported "741 files pending upload" (its copy of the peer's
// index predated the peer's scan) while the other reported "0 pending" -- two
// numbers, same folder, contradicting each other, and neither was a lie about
// the snapshot it came from.
//
// Verified means all of:
//   - at least one peer view exists and none is mid-exchange (a truncated view
//     is never truth);
//   - every view was completed recently, so the peer is not believed to have
//     moved on unseen;
//   - this index has not advanced past what we last TOLD the peer, otherwise
//     the peer's copy of us is stale and their "down" cannot match our "up".
func (e *Engine) PendingBasis() (bool, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.peers) == 0 {
		return false, "no peer has exchanged an index for this folder yet"
	}
	now := time.Now()
	for id := range e.peers {
		if e.viewPending[id] {
			return false, "a peer index exchange is in progress"
		}
	}
	for id := range e.peers {
		at, ok := e.viewAt[id]
		if !ok {
			return false, "no completed index exchange with a peer yet"
		}
		if age := now.Sub(at); age > pendingViewMaxAge {
			return false, fmt.Sprintf("peer view is %s old (no recent exchange)", age.Round(time.Second))
		}
	}
	// What we have told each peer must be everything we have: otherwise their
	// "pending down" is computed against an older copy of this index than the one
	// our "pending up" is computed from.
	ownSeq := e.Ix.MaxSeq()
	for id := range e.peers {
		_, sentSeq, _, ok, err := e.Ix.GetPeerIndex(id)
		if err != nil {
			return false, "peer index state unavailable"
		}
		if !ok {
			return false, "this index has not been sent to the peer yet"
		}
		if sentSeq < ownSeq {
			return false, fmt.Sprintf("index has %d change(s) the peer has not been sent", ownSeq-sentSeq)
		}
	}
	return true, ""
}

// UnverifiedPendingPulls/Uploads report the counts and the reason they cannot be
// trusted, for the API to present as "awaiting sync" instead of a number.
func (e *Engine) PendingUnverifiedReason() string {
	if ok, reason := e.PendingBasis(); !ok {
		return reason
	}
	return ""
}
