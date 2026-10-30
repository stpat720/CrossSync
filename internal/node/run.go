package node

import (
	"errors"
	"fmt"
	"net"
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

// SyncWithPeer connects to a peer (trying each address) and performs one
// full exchange. It returns the number of shared folders actually synced
// (0 = the peer was reachable but no folders are shared with it). Sync
// activity is tracked so the control plane can report "sync running since
// …" and detect interruptions.
func (n *Node) SyncWithPeer(id uint64) (synced int, err error) {
	peer, ok := n.Peer(id)
	if !ok {
		return 0, fmt.Errorf("unknown peer %d", id)
	}
	// Transfers paused for every folder? Then there is nothing to do. Checked per
	// folder inside the round as well, because a node can pause one share and
	// keep syncing another.
	if n.allTransfersPaused() {
		return 0, nil
	}
	// Never run two rounds with the same peer: the peer's own inbound round
	// syncs both directions anyway, and overlapping rounds stage identical
	// .part temps (the documented chtimes churn). Skipping is a no-op, not a
	// failure, so the peer is not pushed into backoff.
	release, ok := n.acquirePeerSession(id, 0)
	if !ok {
		n.Logf("sync with peer %s: a round is already in progress; skipping this tick", peer.Name)
		return 0, nil
	}
	defer release()
	n.beginSync(id)
	defer func() { n.endSync(err) }()
	var lastErr error
	for _, addr := range peer.Addresses {
		conn, err := n.Dial(addr)
		if err != nil {
			lastErr = err
			continue
		}
		synced, err = n.SyncOnce(conn, id)
		_ = conn.Close()
		if err == nil {
			return synced, nil
		}
		// A failed round leaves us with no trustworthy picture of the
		// peer's outstanding work: drop the stored view rather than show
		// stale entries until the next successful round.
		n.clearPeerUnsynced(id)
		lastErr = err
	}
	return 0, lastErr
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

// StartPendingRefresher keeps the cached pending counters current. Without it
// the numbers would only move when a round runs, so a paused folder or an
// offline peer would show stale values forever — while computing them on demand
// is exactly the thing that stalled the control plane.
func (n *Node) StartPendingRefresher(every time.Duration) {
	if every <= 0 {
		every = 5 * time.Second
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for range t.C {
			n.RefreshPendingStats()
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
