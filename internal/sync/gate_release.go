package sync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"

	"crosssync/internal/events"
	"crosssync/internal/index"
)

// ErrMoveReleased reports that ApplyMove did not relocate the file because the
// move's destination is outside THIS device's include gate: the local copy was
// released instead (removed, and no longer tracked) because this device has
// not opted into the new name. It is not a failure — the caller reports it as
// a release, not as a skipped move.
var ErrMoveReleased = errors.New("move released: destination outside this device's include gate")

// releaseMetaKey is the index meta row that persists the pending out-of-gate
// releases, so a restart in the middle of the handshake cannot lose them
// (losing one would leave the paths tracked forever).
const releaseMetaKey = "gate_releases"

// pendingRelease is one local rename whose target path is outside this
// device's include gate: an in-scope path moved to a name this device no
// longer tracks. It is advertised to peers (old path tombstoned, new path
// live) so they can apply it as a MOVE, but this device may only drop its
// tracking of both paths once every peer that owned the old path has
// confirmed — otherwise a peer that is offline or mid-round would be told to
// delete a file it never received the replacement for.
type pendingRelease struct {
	From   string   `json:"from"`             // old, in-scope path
	To     string   `json:"to"`               // new, out-of-gate path
	Size   int64    `json:"size"`             // content identity of the moved file
	Blocks [][]byte `json:"blocks,omitempty"` //
	// Peers are the devices that owned From when the rename was applied. The
	// release waits for each of them; a peer that is no longer configured is
	// dropped by CleanupPeerIndex.
	Peers []uint64 `json:"peers,omitempty"`
}

// loadReleasesLocked lazily reads the persisted release list. Called with
// e.mu held.
func (e *Engine) loadReleasesLocked() {
	if e.releasesLoaded {
		return
	}
	e.releasesLoaded = true
	raw, ok, err := e.Ix.GetMeta(releaseMetaKey)
	if err != nil || !ok {
		return
	}
	var list []pendingRelease
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		e.Logf("folder %s: pending releases unreadable (%v); dropping them", e.ID, err)
		return
	}
	e.releases = list
}

// saveReleasesLocked persists the release list. Called with e.mu held.
func (e *Engine) saveReleasesLocked() error {
	raw := []byte("[]")
	if len(e.releases) > 0 {
		var err error
		raw, err = json.Marshal(e.releases)
		if err != nil {
			return err
		}
	}
	return e.Ix.SetMeta(releaseMetaKey, string(raw))
}

// recordReleaseLocked appends a pending release for one out-of-gate rename
// applied locally, capturing which peers currently own the old path (from the
// per-peer index views). Called with e.mu held.
func (e *Engine) recordReleaseLocked(from, to string, size int64, blocks [][]byte) {
	e.loadReleasesLocked()
	var owners []uint64
	for pid, view := range e.peers {
		if fi, ok := view[from]; ok && !fi.Deleted {
			owners = append(owners, pid)
		}
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i] < owners[j] })
	blocksCopy := make([][]byte, len(blocks))
	for i, b := range blocks {
		blocksCopy[i] = append([]byte(nil), b...)
	}
	e.releases = append(e.releases, pendingRelease{From: from, To: to, Size: size, Blocks: blocksCopy, Peers: owners})
}

// RecordOutOfGateRename records the pending release for a rename that moved
// content out of this device's include gate. It is the per-change counterpart
// of the batch path inside ApplyLocalChanges (used by the scan fallback and by
// a rebuild from the filesystem), which applies renames one at a time.
func (e *Engine) RecordOutOfGateRename(from, to string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	fi, ok, err := e.Ix.Get(to)
	if err != nil {
		return err
	}
	if !ok || fi.Deleted {
		return fmt.Errorf("record release %s -> %s: target not indexed", from, to)
	}
	e.recordReleaseLocked(from, to, fi.Size, fi.Blocks)
	return e.saveReleasesLocked()
}

// PendingReleases counts the out-of-gate renames still waiting for peer
// confirmation (exposed for tests and diagnostics).
func (e *Engine) PendingReleases() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.loadReleasesLocked()
	return len(e.releases)
}

// ConfirmReleases releases every pending out-of-gate rename that every owning
// peer has confirmed: the peer either holds the content under the new name, or
// no longer owns the old path either (it released it under its own gate).
// Released paths leave this index WITHOUT any deletion or tombstone being
// broadcast — the move was already advertised — and this device's files on
// disk are left exactly where they are. Returns the released new-path names;
// one durable system/info event records the release.
//
// It must only be called after a completed round (or a scan with no peers to
// wait for): the per-peer views it reads are refreshed by sessions.
func (e *Engine) ConfirmReleases() ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.loadReleasesLocked()
	if len(e.releases) == 0 {
		return nil, nil
	}
	keep := e.releases[:0:0]
	var released []string
	for _, rel := range e.releases {
		if !e.releaseConfirmedLocked(rel) {
			keep = append(keep, rel)
			continue
		}
		// Both rows leave the index: the new path was never this device's to
		// keep, and the old path's tombstone has served its purpose (every
		// owner confirmed). No tombstone is written, so nothing propagates.
		for _, name := range []string{rel.To, rel.From} {
			if err := e.Ix.Delete(name); err != nil {
				return released, err
			}
		}
		released = append(released, rel.To)
	}
	if len(keep) != len(e.releases) {
		e.releases = keep
		if err := e.saveReleasesLocked(); err != nil {
			return released, err
		}
	}
	if len(released) > 0 {
		sort.Strings(released)
		e.emit(events.CatSystem, events.SevInfo, "",
			fmt.Sprintf("released %d paths renamed out of gate (content confirmed on peers)", len(released)), "", 0)
	}
	return released, nil
}

// releaseConfirmedLocked reports whether every owning peer has confirmed the
// rename. A peer confirms by holding the content under the new name; a peer
// whose view no longer owns the old path and does not hold the new one has
// released it under its own gate (that is a confirmation too).
func (e *Engine) releaseConfirmedLocked(rel pendingRelease) bool {
	for _, pid := range rel.Peers {
		view := e.peers[pid]
		if view == nil {
			return false // no session with it since the rename: unknown
		}
		if to, ok := view[rel.To]; ok && !to.Deleted {
			if to.Size == rel.Size && equalBlocks(to.Blocks, rel.Blocks) {
				continue // peer holds the content under the new name
			}
			return false // peer holds something else there
		}
		// No live new-path entry in the peer's view: it released the path
		// (out of its own scope) only if it also stopped owning the old one.
		// A peer that still advertises the old path has not applied the move.
		if from, ok := view[rel.From]; ok && !from.Deleted {
			return false
		}
	}
	return true
}

// dropPeerFromReleasesLocked forgets a removed peer in every pending release,
// so a device that is no longer configured can never block a release forever.
// Called with e.mu held.
func (e *Engine) dropPeerFromReleasesLocked(peerID uint64) {
	e.loadReleasesLocked()
	changed := false
	for i := range e.releases {
		peers := e.releases[i].Peers[:0]
		for _, p := range e.releases[i].Peers {
			if p == peerID {
				changed = true
				continue
			}
			peers = append(peers, p)
		}
		e.releases[i].Peers = peers
	}
	if changed {
		if err := e.saveReleasesLocked(); err != nil {
			e.Logf("folder %s: save releases: %v", e.ID, err)
		}
	}
}

// releaseMoveLocked handles a peer move whose destination is outside this
// device's include gate: the local copy is released (removed from disk, old
// path retired, new path never indexed). No content is transferred and no
// deletion is broadcast for the new path — the peer holding the content keeps
// it, and this device simply has not opted into the new name. The old path is
// retired with a local tombstone so peers waiting for confirmation can see
// that this device let go of it.
//
// preserve moves the local copy into the hold area instead of unlinking it
// (the over-threshold case: a folder renamed out of a gated device looks
// exactly like a mass deletion here, so it gets the same undo window).
func (e *Engine) releaseMoveLocked(m Move, local *index.FileInfo, preserve bool) error {
	held := false
	if preserve && e.HoldLocal != nil {
		if err := e.HoldLocal(m.From); err != nil {
			e.Logf("hold %s: %v (releasing instead)", m.From, err)
		} else {
			held = true
		}
	}
	if !held {
		if err := os.Remove(e.abs(m.From)); err != nil && !os.IsNotExist(err) {
			e.Logf("release %s: remove local copy: %v", m.From, err)
		}
	}
	tomb := &index.FileInfo{Name: m.From, Type: local.Type, Deleted: true,
		Version: local.Version.Bump(e.NodeID)}
	if err := e.Ix.Put(tomb); err != nil {
		return err
	}
	// No per-file event here: the node records one folder-level release
	// summary (a folder-wide reorganisation can release thousands of paths).
	e.Logf("folder %s: released %s (peer move landed outside the include gate)", e.ID, m.From)
	return ErrMoveReleased
}

// equalBlocks reports whether two block-hash lists are identical.
func equalBlocks(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
