package node

import (
	"errors"
	"fmt"
	"net"

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
		conn.Close()
		if err == nil {
			return synced, nil
		}
		lastErr = err
	}
	return 0, lastErr
}

// SyncAllPeers syncs with every configured peer, logging failures without
// aborting (a peer being offline is normal).
func (n *Node) SyncAllPeers() error {
	for _, p := range n.Cfg.Peers {
		if _, err := n.SyncWithPeer(p.ID); err != nil {
			n.Logf("sync with peer %s: %v", p.Name, err)
		}
	}
	return nil
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
			defer c.Close()
			if err := n.Serve(c); err != nil {
				n.Logf("inbound session: %v", err)
			}
		}(conn)
	}
}
