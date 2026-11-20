package node

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/transfer"
)

// Peer returns the configured peer with the given id.
func (n *Node) Peer(id uint64) (*config.Peer, bool) {
	for i := range n.Cfg.Peers {
		if n.Cfg.Peers[i].ID == id {
			return &n.Cfg.Peers[i], true
		}
	}
	return nil, false
}

// SyncWithPeer syncs every folder shared with a peer, giving EACH folder its own
// session: up to Node.FolderSessions() sessions run at the same time, and a
// folder that is already syncing is skipped rather than queued twice.
//
// This is the outbound half of "separate folders are separate syncs". One
// peer-wide round used to carry every folder, so a folder's work could only
// start (or report) after the folders before it in the round, and a long folder
// held the peer's only session. Now the session slot is per (peer, folder): the
// folder with work starts at once, on its own connection, with its own transfer
// pipeline. What stays exclusive is the folder itself — no two rounds on the SAME
// folder, inbound or outbound (acquireFolderSession), which is what kept two
// writers off one .part staging temp.
//
// A folder that cannot start (its slot is held) is not a failure: the session
// holding it syncs both directions for that folder, so this call reports the
// number of folder sessions it STARTED (0 = none, which is a skip, not an
// error). It returns ErrPeerBusy only when the peer itself declined every
// session, which the ConnManager classifies as reachable-and-busy; a genuine
// transport failure is still an error, so an unreachable peer backs off exactly as
// before.
func (n *Node) SyncWithPeer(id uint64) (synced int, err error) {
	peer, ok := n.Peer(id)
	if !ok {
		return 0, fmt.Errorf("unknown peer %d", id)
	}
	// Transfers paused for every folder? Then there is nothing to do. Checked per
	// folder inside the session as well, because a node can pause one share and
	// keep syncing another.
	if n.allTransfersPaused() {
		return 0, nil
	}
	folders := n.syncCandidates(id)
	if len(folders) == 0 {
		// Nothing to offer this peer, but a dial still proves it is up — which is
		// what /api/status reports as connected.
		return n.probePeer(*peer, id)
	}

	// Claim a slot per folder first, so the cap and the "already syncing" rule
	// are decided HERE, once, and the sessions start together.
	launched := make([]string, 0, len(folders))
	releases := make([]func(), 0, len(folders))
	for _, fid := range folders {
		release, ok := n.acquireFolderSession(id, fid, 0)
		if !ok {
			continue
		}
		launched = append(launched, fid)
		releases = append(releases, release)
	}
	if len(launched) == 0 {
		// Every folder is already syncing with this peer (the peer's own inbound
		// sessions cover both directions) or the cap is full. Skipping is a
		// no-op, not a failure, so the peer is not pushed into backoff.
		n.Logf("sync with peer %s: every folder is already syncing (cap %d); skipping this tick",
			peer.Name, n.FolderSessions())
		return 0, nil
	}

	// Each session reports its VERDICT — the peer answered the exchange and said
	// what the session is about — and then keeps running in the background. This
	// function must not wait for the transfers: a folder with a long transfer
	// holding the next scheduling decision is the very "one folder waiting for
	// another" this change removes.
	verdicts := make(chan error, len(launched))
	for i, fid := range launched {
		go func(fid string, release func()) {
			n.syncFolderSession(*peer, fid, release, verdicts)
		}(fid, releases[i])
	}
	deadline := time.After(sessionVerdictWait)
	up, busy := 0, 0
	var firstErr error
verdicts:
	for i := 0; i < len(launched); i++ {
		select {
		case verr := <-verdicts:
			switch {
			case verr == nil:
				up++
			case errors.Is(verr, ErrPeerBusy):
				// The peer is up and declined because it is already syncing this
				// folder with us: liveness, not a failure.
				busy++
			case firstErr == nil:
				firstErr = verr
			}
		case <-deadline:
			// A peer that accepts nothing and answers nothing: back off rather
			// than pretend the round worked. A session still in flight reports on
			// its own and MarkReachable clears this if it turns out to be alive.
			if firstErr == nil {
				firstErr = fmt.Errorf("peer %s did not answer a session within %s", peer.Name, sessionVerdictWait)
			}
			break verdicts
		}
	}
	switch {
	case up > 0:
		// Sessions are running; each one records its own bytes, last-sync and
		// errors when it finishes.
		return up, nil
	case busy > 0 && firstErr == nil:
		return 0, ErrPeerBusy
	case firstErr != nil:
		return 0, firstErr
	default:
		return 0, nil
	}
}

// sessionVerdictWait bounds how long one scheduling decision waits for its
// sessions to report that the peer answered. Long enough for a TCP connect plus
// three round trips on a bad link, short enough that a black-holed peer cannot
// stall the scheduler for the whole session stall window.
const sessionVerdictWait = 30 * time.Second

// syncFolderSession runs ONE folder's session with a peer over its own
// connection, holding the slot the scheduler already claimed. It reports the
// session's verdict (nil once the peer has answered the exchange, or the error
// that stopped it) and then carries on with the transfer in the background.
func (n *Node) syncFolderSession(peer config.Peer, folder string, release func(), verdict chan<- error) {
	defer release()
	var err error
	n.beginSync(peer.ID)
	defer func() { n.endSync(err) }()

	notified := false
	notify := func(verr error) {
		if notified {
			return
		}
		notified = true
		verdict <- verr
	}
	var lastErr error
	for _, addr := range peer.Addresses {
		conn, derr := n.Dial(addr)
		if derr != nil {
			lastErr = derr
			continue
		}
		conn.SetStall(sessionStall)
		peerFolders, stop, oerr := n.openSession(conn, peer.ID, []string{folder})
		if oerr != nil {
			conn.SetStall(0)
			_ = conn.Close()
			lastErr = oerr
			continue
		}
		// The peer answered: this tick is NOT a failure, whatever the transfer
		// does next.
		notify(nil)
		got, serr := n.runSession(conn, peerFolders, []string{folder})
		stop()
		conn.SetStall(0)
		_ = conn.Close()
		if serr == nil {
			// A completed session is proof the peer is up; when it really moved
			// this folder's data, it also advances last-sync.
			n.markPeerReachable(peer.ID, got > 0)
			return
		}
		// A failed round leaves us with no trustworthy picture of the peer's
		// outstanding work: drop the stored view rather than show stale entries
		// until the next successful round.
		n.clearPeerUnsynced(peer.ID)
		err = serr
		notify(serr)
		return
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("peer %s has no address to dial", peer.Name)
	}
	err = lastErr
	notify(lastErr)
}

// probePeer dials a peer with an empty cluster config: nothing to exchange, but
// the dial and the cluster exchange still prove the peer is up. That matters for
// reachability (connected = last-online < 2 min, and this node may win every
// dial race), so a node with nothing to offer must not simply skip the peer.
func (n *Node) probePeer(peer config.Peer, id uint64) (synced int, err error) {
	n.beginSync(id)
	defer func() { n.endSync(err) }()
	var lastErr error
	for _, addr := range peer.Addresses {
		conn, derr := n.Dial(addr)
		if derr != nil {
			lastErr = derr
			continue
		}
		_, serr := n.syncSession(conn, id, []string{})
		_ = conn.Close()
		if serr == nil {
			return 0, nil
		}
		lastErr = serr
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("peer %s has no address to dial", peer.Name)
	}
	return 0, lastErr
}

// syncCandidates is the folder set a fan-out offers this peer: every folder that
// may sync with it, least recently synced FIRST.
//
// Every advertised folder is offered on EVERY round, even one the peer may not
// have: a per-folder session advertises only its own folder, so the peer's
// remembered set is a record of what we have already exchanged with it, not its
// full list — filtering by it would mean a folder the peer keeps to itself (or
// that only we dial about) is never offered at all. A session that turns out to
// share nothing is the cheap, clean "no shared folders" no-op.
//
// The ORDER is the fairness rule when the cap (or a busy slot) limits how many
// sessions start at once: without it the same folder would always take the first
// free slot and the others would wait for a tick where it happens to have no
// work. A cap of 1 therefore still round-robins across folders.
func (n *Node) syncCandidates(peerID uint64) []string {
	ids := n.advertisedFolders(peerID)
	sort.Slice(ids, func(i, j int) bool {
		li, lj := n.folderLastSync(ids[i]), n.folderLastSync(ids[j])
		if li != lj {
			return li < lj
		}
		return ids[i] < ids[j]
	})
	return ids
}

// folderLastSync is a folder's last completed sync (unix nanos, 0 = never), read
// from the engine's atomic so scheduling never walks an index.
func (n *Node) folderLastSync(id string) int64 {
	if f, ok := n.lookupFolder(id); ok && f.Engine != nil {
		return f.Engine.Stats.LastSync.Load()
	}
	return 0
}

// SyncAllPeers syncs with every configured peer, logging failures without
// aborting (a peer being offline is normal).
func (n *Node) SyncAllPeers() error {
	defer n.RefreshPendingStats()
	for _, p := range n.Cfg.Peers {
		if _, err := n.SyncWithPeer(p.ID); err != nil {
			n.Logf("sync with peer %s: %v", p.Name, err)
		}
	}
	return nil
}

// RefreshPendingStats recomputes the cached pending counters for every folder.
func (n *Node) RefreshPendingStats() {
	for _, f := range n.Folders {
		if f.Engine != nil {
			f.Engine.RefreshPendingStats()
		}
	}
}

// StartPendingRefresher keeps the cached pending counters AND the cached disk
// figures current (free space of the share, size of the .sfx-versions area).
// Without it the numbers would only move when a round runs, so a paused folder
// or an offline peer would show stale values forever — while computing them on
// demand is exactly the thing that stalled the control plane: the UI polls
// /api/status every couple of seconds, and one statfs plus one directory walk per
// folder per request made that call take 5.8-8.3 s on Tower.
func (n *Node) StartPendingRefresher(every time.Duration) {
	if every <= 0 {
		every = 5 * time.Second
	}
	go func() {
		// One immediate pass: a status request that arrives before the first
		// tick must already see real values (the node may have been running
		// for days before this goroutine started).
		n.RefreshPendingStats()
		n.RefreshDiskStats()
		n.RefreshStatusCounts()
		t := time.NewTicker(every)
		defer t.Stop()
		for range t.C {
			n.RefreshPendingStats()
			n.RefreshDiskStats()
			n.RefreshStatusCounts()
		}
	}()
}

// Run accepts inbound connections and serves them until the listener
// closes. Each session runs in its own goroutine. Accept errors (e.g. a
// TLS handshake rejected because the peer's fingerprint is not pinned) are
// logged and the loop continues so authorized peers keep being served.
func (n *Node) Run(ln net.Listener) error {
	for {
		conn, err := transfer.Accept(ln)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			n.Logf("accept: %v", err)
			continue
		}
		go func(c *transfer.Conn) {
			defer func() { _ = c.Close() }()
			// Record the round in the sync activity the operator sees, exactly
			// like an outbound one. Only SyncWithPeer used to do it, so on a
			// node whose peer does the dialling the panel froze at the last
			// outbound attempt: "idle - last 3m ago - interrupted" while syncs
			// completed every few seconds (the folder's last_sync read 5s), and
			// the error could not clear until this node happened to dial out.
			// The peer id is not known before the handshake, so the running
			// line shows no peer; 0 is never displayed.
			n.beginSync(0)
			err := n.Serve(c)
			n.endSync(err)
			if err != nil {
				// The peer's unsynced view describes its live state; an
				// interrupted session must not leave a stale snapshot.
				if c.PeerID != 0 {
					n.clearPeerUnsynced(c.PeerID)
				}
				n.Logf("inbound session: %v", err)
			}
		}(conn)
	}
}
