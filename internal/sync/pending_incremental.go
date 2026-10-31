package sync

import (
	"sort"
	"sync"
	"time"

	"crosssync/internal/index"
)

// Incremental pending computation.
//
// Both pending lists used to be rebuilt by walking the whole index after every
// round and by the 5s refresher. A cache keyed on (index sequence, view revision)
// made an IDLE folder free, but a busy one still paid a full walk per round — and
// on a folder of a hundred thousand files that is the whole round.
//
// The lists are a pure function of the local entry for a name plus the peer views,
// so a name's membership can only change when that name is written locally or
// appears in a peer view that changed. This file keeps exactly that set of names
// dirty (the index calls a hook on every put/delete, the engine marks names when a
// view is replaced or a file enters/leaves the stability gate) and recomputes only
// those names. The full walk remains, as the initial build, as the fallback when
// the dirty set is large enough that it is cheaper, and as a periodic safety net.

// pendingRebuildAfter bounds how long the incremental lists are trusted while
// there is activity: past this age, both lists are recomputed from the whole index,
// so an invalidation this file failed to observe cannot persist.
const pendingRebuildAfter = 15 * time.Minute

// pendingDirtyShare is the fraction of the index above which a full walk costs
// less than one index lookup per changed name.
const pendingDirtyShare = 4

// pendingDirty is the set of names whose membership may have changed. It has its
// own mutex, and it is only ever locked as a leaf: the index write hook calls it
// while the index lock is held, so it must never take the engine lock.
type pendingDirty struct {
	mu    sync.Mutex
	names map[string]struct{}
	all   bool
}

// markPendingDirty records one name as possibly changed.
func (e *Engine) markPendingDirty(name string) {
	if name == "" {
		return
	}
	d := &e.pend
	d.mu.Lock()
	if d.names == nil {
		d.names = make(map[string]struct{}, 64)
	}
	d.names[name] = struct{}{}
	d.mu.Unlock()
}

// markPendingRenamesLocked records several names at once.
func (e *Engine) markPendingNames(names ...string) {
	for _, n := range names {
		e.markPendingDirty(n)
	}
}

// markPendingAll forces the next computation to walk the whole index. Used when
// something changed that cannot be attributed to individual names (a peer view
// dropped wholesale, the incremental path was switched off mid-flight).
func (e *Engine) markPendingAll() {
	d := &e.pend
	d.mu.Lock()
	d.all = true
	d.mu.Unlock()
}

// takePendingDirty drains the dirty set.
func (e *Engine) takePendingDirty() ([]string, bool) {
	d := &e.pend
	d.mu.Lock()
	defer d.mu.Unlock()
	all := d.all
	d.all = false
	if len(d.names) == 0 {
		return nil, all
	}
	out := make([]string, 0, len(d.names))
	for n := range d.names {
		out = append(out, n)
	}
	d.names = nil
	return out, all
}

// SetIncrementalPending turns the incremental computation on or off. Off means
// every call walks the whole index, which is the pre-optimisation behaviour and
// the reference the equivalence test compares against.
func (e *Engine) SetIncrementalPending(on bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.incrementalPending = on
	e.pendingCache.valid = false
}


// ensurePendingLocked brings both pending lists up to date and is the only place
// they are built.
func (e *Engine) ensurePendingLocked() error {
	c := &e.pendingCache
	if !e.incrementalPending {
		return e.rebuildPendingLocked()
	}
	names, all := e.takePendingDirty()
	if !c.valid || all {
		return e.rebuildPendingLocked()
	}
	if c.builtAt.IsZero() || (len(names) > 0 && time.Since(c.builtAt) > pendingRebuildAfter) {
		return e.rebuildPendingLocked()
	}
	if len(names) == 0 {
		return nil // nothing changed: the lists stand
	}
	// One lookup per changed name beats a full walk only while the change is
	// small relative to the index.
	if c.indexed > 0 && len(names) >= c.indexed/pendingDirtyShare+1 {
		return e.rebuildPendingLocked()
	}

	views := e.authoritativeViewsLocked()
	for _, name := range names {
		local, ok, err := e.Ix.Get(name)
		if err != nil {
			return err
		}
		e.setMember(c.pullSet, name, e.pullWanted(name, local, ok))
		want := false
		if len(views) > 0 {
			want = e.uploadWanted(name, local, ok, views)
		}
		e.setMember(c.upSet, name, want)
	}
	c.pulls = sortedSet(c.pullSet)
	c.ups = sortedSet(c.upSet)
	return nil
}

// rebuildPendingLocked recomputes both lists from the whole index.
func (e *Engine) rebuildPendingLocked() error {
	pulls, err := e.computeNeedPullsLocked()
	if err != nil {
		return err
	}
	ups, err := e.computePendingUploadsLocked()
	if err != nil {
		return err
	}
	c := &e.pendingCache
	c.pulls, c.ups = pulls, ups
	c.pullSet = setOf(pulls)
	c.upSet = setOf(ups)
	c.builtAt = time.Now()
	c.valid = true
	e.takePendingDirty() // the walk covered everything queued so far
	return nil
}

// setMember adds or removes a name without disturbing the rest of the set.
func (e *Engine) setMember(set map[string]struct{}, name string, want bool) {
	if set == nil {
		return
	}
	if want {
		set[name] = struct{}{}
		return
	}
	delete(set, name)
}

func setOf(names []string) map[string]struct{} {
	out := make(map[string]struct{}, len(names))
	for _, n := range names {
		out[n] = struct{}{}
	}
	return out
}

func sortedSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// authoritativeViewsLocked returns the peer views that may be used to decide
// upload work, skipping any that is mid-exchange (a prefix would read as "the
// peer lacks this file" for everything past the truncation point).
func (e *Engine) authoritativeViewsLocked() []map[string]*index.FileInfo {
	out := make([]map[string]*index.FileInfo, 0, len(e.peers))
	for id, view := range e.peers {
		if e.viewPending[id] {
			continue
		}
		out = append(out, view)
	}
	return out
}

// pullWanted reports whether name belongs in the pull list: the acquire path, the
// delete pass, and the two version comparisons, exactly as the full walk decided
// them. Kept in one place so the walk and the incremental path cannot disagree.
func (e *Engine) pullWanted(name string, local *index.FileInfo, ok bool) bool {
	// send-only never acquires anything from peers.
	if e.Direction == "send-only" {
		return false
	}
	// A file whose local content is still changing must never be pulled over:
	// PlanOverwrite would archive/conflict-copy it and Commit would rename the
	// remote version straight over a half-written file.
	if e.InFlight(name) {
		return false
	}
	global, _ := e.globalFrom(local, ok, name)
	if global == nil {
		return false
	}
	switch {
	case !ok && !global.Deleted:
		// Only the acquire path is gated: a name we do not own and the include
		// list does not cover is out of scope. Owned entries keep following peer
		// updates and deletions.
		if e.Include != nil {
			isDir := false
			for _, view := range e.peers {
				if g, eok := view[name]; eok {
					isDir = g.Type == index.TypeDirectory
					break
				}
			}
			if matched, _ := e.Include.Match(name, isDir); !matched {
				return false
			}
		}
		return true
	case ok && !local.Deleted && global.Deleted:
		// The global model says DELETED. The session needs the name in this list
		// so the delete pass consults it; the counters exclude it.
		return true
	case ok && !local.Deleted && !global.Deleted:
		switch global.Version.Compare(local.Version) {
		case 1:
			// A dominating version is a fast-forward download — unless the bytes
			// it describes are already ours, in which case only the vector is
			// behind and JoinIdentical adopts it.
			return !identicalContent(local, global)
		case 2:
			// Concurrent edit: need the winner unless the content is identical.
			return !sameContent(local, global)
		}
	case ok && local.Deleted && !global.Deleted:
		// Local tombstone but a peer resurrected the file.
		return true
	}
	return false
}

// uploadWanted reports whether name is outstanding upload work for a peer.
//
// It is deliberately order-independent: ANY authoritative view that lacks the
// bytes, or that we hold a dominating version of different content for, makes
// the file upload work. An earlier shape returned on the first view that had an
// entry, so two peers (one with a tombstone, one without) produced an answer that
// depended on Go's map iteration order - the same state, two different numbers.
func (e *Engine) uploadWanted(name string, local *index.FileInfo, ok bool, views []map[string]*index.FileInfo) bool {
	if !ok || local == nil || local.Deleted {
		return false
	}
	// An unsettled file is not actionable upload work: the serve gate would
	// refuse its blocks anyway.
	if e.InFlight(name) {
		return false
	}
	for _, view := range views {
		pf, has := view[name]
		if !has {
			return true
		}
		if pf.Deleted {
			// The peer tombstoned this path: that is work for us to APPLY. It is
			// only upload work when our version dominates the tombstone, i.e. a
			// real local edit after the deletion, which resurrects the file.
			if local.Version.Compare(pf.Version) == 1 {
				return true
			}
			continue
		}
		// A peer that holds these exact bytes will not pull them, so a dominating
		// version alone is not upload work.
		if local.Version.Compare(pf.Version) == 1 && !identicalContent(local, pf) {
			return true
		}
	}
	return false
}
