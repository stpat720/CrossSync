package node

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"crosssync/internal/index"
	"crosssync/internal/protocol"
	"crosssync/internal/transfer"
)

// minGuardDeletes is the absolute floor before the deletion guard engages:
// tiny folders may legitimately be cleaned out wholesale, so the guard only
// trips for a genuinely large delete pass.
const minGuardDeletes = 100

// guardBlocksDeletes reports whether a delete pass should be blocked: at
// least minGuardDeletes deletions AND either the percentage threshold or the
// absolute file cap is exceeded.
func guardBlocksDeletes(live, dels, pct, files int) bool {
	if live <= 0 || dels < minGuardDeletes {
		return false
	}
	return dels*100 >= live*pct || (files > 0 && dels >= files)
}

// sharedFolders returns the intersection of our folder ids with the peer's,
// honoring each folder's peer scoping (empty folder.Peers = all peers). A
// folder is only shared with this peer when the folder's Peers list is empty
// or contains the peer's device id.
func (n *Node) sharedFolders(peerFolders map[string]bool, peerID uint64) []string {
	var out []string
	for id := range n.Folders {
		if peerFolders[id] && n.Folders[id].Cfg.AllowsPeer(peerID) {
			out = append(out, id)
		}
	}
	return out
}

// handshake exchanges Hello messages. The connection is strictly
// alternating (one writer at a time) to avoid deadlocks on any transport:
// the initiator sends first, the responder receives first.
func (n *Node) handshake(conn *transfer.Conn, expectedPeer uint64, sendFirst bool) (uint64, string, error) {
	send := func() error {
		return conn.Send(&protocol.Message{Type: protocol.MsgHello, Hello: &protocol.Hello{
			DeviceID: n.ID, DeviceName: n.Name,
		}})
	}
	var m *protocol.Message
	var err error
	if sendFirst {
		if err = send(); err != nil {
			return 0, "", err
		}
		m, err = conn.Recv()
	} else {
		m, err = conn.Recv()
		if err != nil {
			return 0, "", err
		}
		if err = send(); err != nil {
			return 0, "", err
		}
	}
	if err != nil {
		return 0, "", err
	}
	if m.Type != protocol.MsgHello || m.Hello == nil {
		return 0, "", fmt.Errorf("expected hello, got %v", m.Type)
	}
	if expectedPeer != 0 && m.Hello.DeviceID != expectedPeer {
		return 0, "", fmt.Errorf("peer identity mismatch: got %d, want %d", m.Hello.DeviceID, expectedPeer)
	}
	conn.PeerID = m.Hello.DeviceID
	conn.PeerName = m.Hello.DeviceName
	return conn.PeerID, conn.PeerName, nil
}

// advertisedFolders returns the folder ids this node offers to a given peer,
// honoring per-folder peer scoping: a folder restricted to other peers is
// never advertised to (or visible from) this peer.
func (n *Node) advertisedFolders(peerID uint64) []string {
	var out []string
	for id, f := range n.Folders {
		if f.Cfg.AllowsPeer(peerID) {
			out = append(out, id)
		}
	}
	return out
}

func (n *Node) sendCluster(conn *transfer.Conn) error {
	cc := &protocol.ClusterConfig{}
	for _, id := range n.advertisedFolders(conn.PeerID) {
		cc.Folders = append(cc.Folders, protocol.ClusterFolder{ID: id})
	}
	return conn.Send(&protocol.Message{Type: protocol.MsgClusterConfig, Cluster: cc})
}

func (n *Node) recvCluster(conn *transfer.Conn) (map[string]bool, error) {
	m, err := conn.Recv()
	if err != nil {
		return nil, err
	}
	if m.Type != protocol.MsgClusterConfig || m.Cluster == nil {
		return nil, fmt.Errorf("expected cluster config, got %v", m.Type)
	}
	out := map[string]bool{}
	for _, f := range m.Cluster.Folders {
		out[f.ID] = true
	}
	return out, nil
}

// indexSnapshot remembers the index state sent for one folder during a
// session, so it can be persisted once the session completes successfully.
type indexSnapshot struct {
	folder  string
	indexID string
	maxSeq  int64
}

// sendIndexes sends this node's view of each folder to the peer, using a
// delta when safe: if we have already told this peer about this exact index
// generation up to some max sequence, only entries newer than that are
// sent; otherwise the full index is sent. The returned snapshots record the
// state actually transmitted and must be persisted only if the whole
// session succeeds (see recordPeerIndex).
func (n *Node) sendIndexes(conn *transfer.Conn, folders []string, peerID uint64) ([]indexSnapshot, error) {
	snaps := make([]indexSnapshot, 0, len(folders))
	for _, id := range folders {
		f := n.Folders[id]
		indexID, err := f.Ix.IndexID()
		if err != nil {
			return nil, err
		}
		maxSeq := f.Ix.MaxSeq()
		var files []protocol.FileMessage
		update := false
		if peerIndexID, peerMaxSeq, ok, err := f.Ix.GetPeerIndex(peerID); err != nil {
			return nil, err
		} else if ok && peerIndexID == indexID {
			// Same generation we already told this peer about: delta only.
			update = true
			if err := f.Ix.ListAfter(peerMaxSeq, func(fi *index.FileInfo) error {
				files = append(files, toFileMessage(fi))
				return nil
			}); err != nil {
				return nil, err
			}
		} else {
			if err := f.Ix.ListAll(func(fi *index.FileInfo) error {
				files = append(files, toFileMessage(fi))
				return nil
			}); err != nil {
				return nil, err
			}
		}
		n.SentEntries.Add(int64(len(files)))
		if err := conn.Send(&protocol.Message{Type: protocol.MsgIndex, Index: &protocol.IndexMsg{
			Folder: id, Files: files, Update: update, IndexID: indexID, MaxSeq: maxSeq,
		}}); err != nil {
			return nil, err
		}
		snaps = append(snaps, indexSnapshot{folder: id, indexID: indexID, maxSeq: maxSeq})
	}
	return snaps, nil
}

// recordPeerIndex persists the index state transmitted to peerID for each
// folder. It is called only after a session completed end-to-end, so a
// dropped session never advances the marker past what the peer received.
func (n *Node) recordPeerIndex(peerID uint64, snaps []indexSnapshot) error {
	for _, s := range snaps {
		f, ok := n.Folders[s.folder]
		if !ok {
			continue
		}
		if err := f.Ix.SetPeerIndex(peerID, s.indexID, s.maxSeq); err != nil {
			return err
		}
	}
	return nil
}

// recvFullIndexes reads full Index messages for every shared folder.
func (n *Node) recvFullIndexes(conn *transfer.Conn, folders []string) error {
	remaining := map[string]bool{}
	for _, id := range folders {
		remaining[id] = true
	}
	for len(remaining) > 0 {
		m, err := conn.Recv()
		if err != nil {
			return err
		}
		switch m.Type {
		case protocol.MsgIndex:
			if m.Index == nil || !remaining[m.Index.Folder] {
				continue
			}
			if err := n.mergeIndex(conn, m.Index); err != nil {
				return err
			}
			delete(remaining, m.Index.Folder)
		case protocol.MsgClose:
			return fmt.Errorf("peer closed during index exchange")
		default:
			// tolerate progress/ping interleaved
		}
	}
	return nil
}

func (n *Node) mergeIndex(conn *transfer.Conn, msg *protocol.IndexMsg) error {
	f, ok := n.Folders[msg.Folder]
	if !ok {
		return nil
	}
	files := make([]*index.FileInfo, 0, len(msg.Files))
	for i := range msg.Files {
		files = append(files, fromFileMessage(&msg.Files[i]))
	}
	f.Engine.SetPeerIndex(conn.PeerID, files, msg.Update)
	return nil
}

// pullAll pulls everything this node needs in the given folders.
func (n *Node) pullAll(conn *transfer.Conn, folders []string) error {
	for _, id := range folders {
		f := n.Folders[id]

		// Same-content move/rename detection: paths this peer deleted that
		// pair one-to-one with paths it added of identical content are
		// applied as local renames — no transfer, and (because the moved
		// paths are no longer pending after the index update) they are
		// naturally excluded from the deletion guard and mass-change signal
		// below. A move that fails to apply (source changed, target
		// appeared, I/O error) stays in the normal pull+delete path.
		moves, err := f.Engine.PlanMoves(conn.PeerID)
		if err != nil {
			return err
		}
		applied := moves[:0]
		for _, m := range moves {
			if err := f.Engine.ApplyMove(m); err != nil {
				n.Logf("folder %s: move %s -> %s skipped (%v); falling back to pull+delete", id, m.From, m.To, err)
				continue
			}
			applied = append(applied, m)
		}
		if len(applied) > 0 {
			n.emitMoves(id, applied, conn.PeerID)
		}

		needs, err := f.Engine.NeedPulls()
		if err != nil {
			return err
		}

		// Live file count (for the deletion guard and mass-change signal).
		live := 0
		_ = f.Ix.List(func(fi *index.FileInfo) error {
			if !fi.Deleted {
				live++
			}
			return nil
		})

		// Deletion guard: a single sync must never silently wipe a folder.
		// If the peer's index proposes deleting more than the configured
		// threshold, block the deletions (they stay pending) and require an
		// explicit operator override.
		dels, err := f.Engine.PendingDeletions()
		if err != nil {
			return err
		}
		pct, files := f.Cfg.DeleteGuard()
		blockDeletes := guardBlocksDeletes(live, len(dels), pct, files)
		if blockDeletes {
			n.reportDeletionGuard(id, fmt.Sprintf(
				"%d files deleted by peers in one sync (>%d%% of %d live files). Nothing was deleted. Verify the change, then use 'Apply pending deletions' in the folder actions to proceed.",
				len(dels), pct, live))
			// Keep pulling non-deletion changes, but skip the deletions.
			delSet := map[string]bool{}
			for _, d := range dels {
				delSet[d] = true
			}
			filtered := needs[:0]
			for _, name := range needs {
				if !delSet[name] {
					filtered = append(filtered, name)
				}
			}
			needs = filtered
		} else if len(dels) == 0 && n.DeleteGuardTripped(id) {
			// The peer's deletions are no longer pending: the guard can
			// stand down and its error event clears.
			n.clearDeletionGuard(id)
		}
		// Mass-change signal (non-blocking): a session that rewrites most
		// of the folder after a prior completed sync is a heads-up (rename /
		// format shift / migration), never a silent surprise.
		if !blockDeletes {
			n.checkMassChange(id, len(needs), live, conn.PeerID)
		}

		for _, name := range needs {
			if err := n.pullOne(conn, f, name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *Node) pullOne(conn *transfer.Conn, f *Folder, name string) error {
	plan, err := f.Engine.PlanOverwrite(name)
	if err != nil {
		return err
	}
	plan.PeerID = conn.PeerID
	if _, err := f.Engine.Execute(plan); err != nil {
		n.reportProblem(f.Cfg.ID, name, err)
		return err
	}
	if plan.Delete {
		if err := f.Engine.ApplyDeletion(name, conn.PeerID); err != nil {
			n.reportProblem(f.Cfg.ID, name, err)
			return err
		}
		n.clearProblem(f.Cfg.ID, name)
		return n.sendIndexUpdate(conn, f.Cfg.ID, name)
	}
	// Directories and symlinks carry no content: create/adopt them rather
	// than transferring blocks.
	switch plan.Global.Type {
	case index.TypeDirectory:
		if err := f.Engine.EnsureDir(plan.Global); err != nil {
			n.reportProblem(f.Cfg.ID, name, err)
			return err
		}
		n.clearProblem(f.Cfg.ID, name)
		return n.sendIndexUpdate(conn, f.Cfg.ID, name)
	case index.TypeSymlink:
		n.Logf("folder %s: symlink %q not followed (v1 limitation); indexing it", f.Cfg.ID, name)
		if err := f.Engine.AdoptMetadata(plan.Global); err != nil {
			n.reportProblem(f.Cfg.ID, name, err)
			return err
		}
		n.clearProblem(f.Cfg.ID, name)
		return n.sendIndexUpdate(conn, f.Cfg.ID, name)
	}

	pull, err := f.Engine.StartPull(name, conn.PeerID)
	if err != nil {
		n.reportProblem(f.Cfg.ID, name, err)
		return err
	}
	var reqID uint64 = 1
	for _, need := range pull.NeedBlocks() {
		req := &protocol.Request{ID: reqID, Folder: f.Cfg.ID, Name: name, Offset: need.Offset, Size: need.Size}
		if err := conn.Send(&protocol.Message{Type: protocol.MsgRequest, Request: req}); err != nil {
			pull.Abort()
			return err
		}
	recvBlock:
		for {
			m, err := conn.Recv()
			if err != nil {
				pull.Abort()
				return err
			}
			switch m.Type {
			case protocol.MsgResponse:
				if m.Response == nil || m.Response.ID != reqID {
					continue
				}
				if m.Response.Code != protocol.RespNoError {
					pull.Abort()
					n.reportProblem(f.Cfg.ID, name, fmt.Errorf("block request failed: code %d", m.Response.Code))
					return fmt.Errorf("block request failed: code %d", m.Response.Code)
				}
				if err := pull.ReceiveBlock(need.Index, m.Response.Data); err != nil {
					pull.Abort()
					n.reportProblem(f.Cfg.ID, name, err)
					return err
				}
				reqID++
				break recvBlock
			case protocol.MsgIndex, protocol.MsgIndexUpdate:
				if m.Index != nil {
					if err := n.mergeIndex(conn, m.Index); err != nil {
						pull.Abort()
						return err
					}
				}
			default:
				// ignore interleaved control messages
			}
		}
	}
	if _, err := pull.Finish(); err != nil {
		n.reportProblem(f.Cfg.ID, name, err)
		return err
	}
	n.clearProblem(f.Cfg.ID, name)
	return n.sendIndexUpdate(conn, f.Cfg.ID, name)
}

// sendIndexUpdate pushes the current local entry for name to the peer.
func (n *Node) sendIndexUpdate(conn *transfer.Conn, folderID, name string) error {
	f, ok := n.Folders[folderID]
	if !ok {
		return nil
	}
	fi, ok, err := f.Ix.Get(name)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	msg := &protocol.IndexMsg{Folder: folderID, Update: true, Files: []protocol.FileMessage{toFileMessage(fi)}}
	return conn.Send(&protocol.Message{Type: protocol.MsgIndexUpdate, Index: msg})
}

// serveUntil answers block requests and merges index updates until it
// receives the terminal message type (MsgPhaseDone or MsgClose).
func (n *Node) serveUntil(conn *transfer.Conn, terminal protocol.MsgType) error {
	for {
		m, err := conn.Recv()
		if err != nil {
			return err
		}
		switch m.Type {
		case protocol.MsgRequest:
			resp := n.serveBlock(m.Request)
			if err := conn.Send(&protocol.Message{Type: protocol.MsgResponse, Response: resp}); err != nil {
				return err
			}
		case protocol.MsgIndex, protocol.MsgIndexUpdate:
			if m.Index != nil {
				if err := n.mergeIndex(conn, m.Index); err != nil {
					return err
				}
			}
		case terminal:
			return nil
		default:
			// ignore pings/progress
		}
	}
}

// serveBlock reads one block from the local file for a peer request.
func (n *Node) serveBlock(req *protocol.Request) *protocol.Response {
	resp := &protocol.Response{ID: req.ID, Code: protocol.RespNoSuchFile}
	if req == nil {
		resp.Code = protocol.RespGeneric
		return resp
	}
	f, ok := n.Folders[req.Folder]
	if !ok {
		return resp
	}
	abs := filepath.Join(f.Root, filepath.FromSlash(req.Name))
	file, err := os.Open(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return resp
		}
		resp.Code = protocol.RespGeneric
		return resp
	}
	defer file.Close()
	buf := make([]byte, req.Size)
	nn, err := file.ReadAt(buf, req.Offset)
	if err != nil && err != io.EOF {
		resp.Code = protocol.RespGeneric
		return resp
	}
	if int64(nn) != int64(req.Size) {
		// The file changed/truncated under us: report unavailable so the
		// puller retries (graceful handling of busy files).
		resp.Code = protocol.RespNoSuchFile
		return resp
	}
	f.Engine.Stats.BytesUp.Add(int64(nn))
	resp.Code = protocol.RespNoError
	resp.Data = buf
	return resp
}

// SyncOnce performs a full exchange as the INITIATOR. The exchange is
// strictly alternating (one writer at a time) to stay deadlock-free on any
// transport at any message size. It returns the number of shared folders
// that were actually synced (0 when nothing was shared).
func (n *Node) SyncOnce(conn *transfer.Conn, expectedPeer uint64) (int, error) {
	if _, _, err := n.handshake(conn, expectedPeer, true); err != nil {
		return 0, err
	}
	if err := n.sendCluster(conn); err != nil {
		return 0, err
	}
	peerFolders, err := n.recvCluster(conn)
	if err != nil {
		return 0, err
	}
	n.RecordPeerFolders(conn.PeerID, peerFolders)
	shared := n.sharedFolders(peerFolders, conn.PeerID)
	if len(shared) == 0 {
		return 0, conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "no shared folders"}})
	}
	// We write our indexes, then read theirs.
	snaps, err := n.sendIndexes(conn, shared, conn.PeerID)
	if err != nil {
		return 0, err
	}
	if err := n.recvFullIndexes(conn, shared); err != nil {
		return 0, err
	}
	// Phase 1: we pull; the peer serves.
	if err := n.pullAll(conn, shared); err != nil {
		return 0, err
	}
	if err := conn.Send(&protocol.Message{Type: protocol.MsgPhaseDone}); err != nil {
		return 0, err
	}
	// Phase 2: we serve while the peer pulls.
	if err := n.serveUntil(conn, protocol.MsgClose); err != nil {
		return 0, err
	}
	// Send our final Close; the responder acknowledges it before closing.
	if err := conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "done"}}); err != nil {
		return 0, err
	}
	// The exchange succeeded end-to-end: record what the peer now holds.
	if err := n.recordPeerIndex(conn.PeerID, snaps); err != nil {
		return 0, err
	}
	n.markFoldersSynced(shared)
	return len(shared), nil
}

// Serve handles an inbound connection as the RESPONDER.
func (n *Node) Serve(conn *transfer.Conn) error {
	if _, _, err := n.handshake(conn, 0, false); err != nil {
		return err
	}
	peerFolders, err := n.recvCluster(conn)
	if err != nil {
		return err
	}
	n.RecordPeerFolders(conn.PeerID, peerFolders)
	if err := n.sendCluster(conn); err != nil {
		return err
	}
	shared := n.sharedFolders(peerFolders, conn.PeerID)
	if len(shared) == 0 {
		return conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "no shared folders"}})
	}
	// We read their indexes first, then write ours.
	if err := n.recvFullIndexes(conn, shared); err != nil {
		return err
	}
	snaps, err := n.sendIndexes(conn, shared, conn.PeerID)
	if err != nil {
		return err
	}
	// Phase 1: serve the initiator's pulls until it signals phase-done.
	if err := n.serveUntil(conn, protocol.MsgPhaseDone); err != nil {
		return err
	}
	// Phase 2: we pull; the initiator serves.
	if err := n.pullAll(conn, shared); err != nil {
		return err
	}
	if err := conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "done"}}); err != nil {
		return err
	}
	// Wait for the initiator's final Close so we do not abort their write
	// by closing the socket underneath them. EOF is acceptable.
	if _, err := conn.Recv(); err != nil {
		// The initiator closed the connection; the exchange is complete.
	}
	// The exchange succeeded end-to-end: record what the peer now holds.
	if err := n.recordPeerIndex(conn.PeerID, snaps); err != nil {
		return err
	}
	n.markFoldersSynced(shared)
	return nil
}

// markFoldersSynced updates per-folder stats after a successful session.
func (n *Node) markFoldersSynced(folders []string) {
	now := time.Now().UnixNano()
	for _, id := range folders {
		if f, ok := n.Folders[id]; ok {
			f.Engine.Stats.Syncs.Add(1)
			f.Engine.Stats.LastSync.Store(now)
		}
	}
}
