package node

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"crosssync/internal/events"
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
			DeviceID: n.ID, DeviceName: n.Name, SessionID: n.SessionID, Protocol: protocol.Current,
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
	conn.PeerSession = m.Hello.SessionID
	conn.PeerProtocol = m.Hello.Protocol
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
	var m *protocol.Message
	for {
		var err error
		m, err = conn.Recv()
		if err != nil {
			return nil, err
		}
		// A keepalive frame can be in flight while the peer finishes its own
		// cluster exchange; it carries no state, so skip it rather than
		// failing the session on an unexpected type.
		if m.Type == protocol.MsgPing {
			continue
		}
		break
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

// indexSnapshot remembers the index state sent for one folder during an
// session, so it can be persisted once the session completes successfully.
type indexSnapshot struct {
	folder  string
	indexID string
	maxSeq  int64
}

// sessionValid reports whether a persisted "peer already has this index up
// to maxSeq" marker may be trusted for a delta. The marker lives in OUR
// index DB and records the state of the PEER's in-memory view of our
// files. That view is wiped whenever the PEER's process restarts — our own
// restart does not touch it. The marker therefore stores the peer's
// session id (the random per-process id the peer advertised in Hello); a
// delta is only correct while the peer is still in that session. A
// mismatch (peer restarted, or a legacy marker with no session id) forces
// a full re-exchange instead of an empty delta that would merge into the
// peer's now-empty view and leave every local file flagged as "pending
// upload" (the phantom 0/3).
func (n *Node) sessionValid(storedPeerSession, livePeerSession string) bool {
	return livePeerSession != "" && storedPeerSession == livePeerSession
}

// sendIndexes sends this node's view of each folder to the peer, using a
// delta when safe: if we have already told this peer about this exact index
// generation up to some max sequence, only entries newer than that are
// sent; otherwise the full index is sent. The returned snapshots record the
// state actually transmitted and must be persisted only if the whole
// session succeeds (see recordPeerIndex).
// indexChunkSize caps the entries per IndexMsg frame so no single frame
// approaches the protocol's maxMessageSize, even for folders with millions
// of files. It is a variable so tests can force chunking with small folders.
var indexChunkSize = 4096

func (n *Node) sendIndexes(conn *transfer.Conn, folders []string, peerID uint64) ([]indexSnapshot, error) {
	// The peer's live session id, advertised in its Hello. A stored marker
	// is only a valid delta basis if the peer is still in the session the
	// marker was recorded under (its in-memory view survives only for the
	// life of that process).
	livePeerSession := conn.PeerSession
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
		if peerIndexID, peerMaxSeq, storedPeerSession, ok, err := f.Ix.GetPeerIndex(peerID); err != nil {
			return nil, err
		} else if ok && peerIndexID == indexID && n.sessionValid(storedPeerSession, livePeerSession) {
			// Same generation we already told this peer about, and the
			// peer is still in the session it was told in: delta only.
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
		// Stream the folder in frames when the peer supports chunked index
		// messages (protocol rev >= 1). A revision-0 peer sweeps its view on
		// EVERY full frame, so it must receive the whole folder in one frame
		// (the legacy behavior, capped by maxMessageSize).
		if conn.PeerProtocol >= 1 {
			total := len(files)
			for start := 0; ; {
				end := start + indexChunkSize
				if end > total {
					end = total
				}
				fm := &protocol.IndexMsg{
					Folder: id, Files: files[start:end], Update: update,
					IndexID: indexID, MaxSeq: maxSeq, Final: end == total,
				}
				if err := conn.Send(&protocol.Message{Type: protocol.MsgIndex, Index: fm}); err != nil {
					return nil, err
				}
				if end == total {
					break
				}
				start = end
			}
		} else if err := conn.Send(&protocol.Message{Type: protocol.MsgIndex, Index: &protocol.IndexMsg{
			Folder: id, Files: files, Update: update, IndexID: indexID, MaxSeq: maxSeq, Final: true,
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
// The marker stores the PEER's live session id (conn.PeerSession): that is
// the lifetime of the peer's in-memory view this marker describes.
func (n *Node) recordPeerIndex(peerID uint64, conn *transfer.Conn, snaps []indexSnapshot) error {
	for _, s := range snaps {
		f, ok := n.Folders[s.folder]
		if !ok {
			continue
		}
		if err := f.Ix.SetPeerIndex(peerID, s.indexID, s.maxSeq, conn.PeerSession); err != nil {
			return err
		}
	}
	return nil
}

// recvFullIndexes reads full Index messages for every shared folder. A
// folder's index may arrive in several frames (chunked index exchange):
// the first frame replaces the peer view, the final frame completes it, and
// frames in between append without sweeping.
func (n *Node) recvFullIndexes(conn *transfer.Conn, folders []string) error {
	remaining := map[string]bool{}
	for _, id := range folders {
		remaining[id] = true
	}
	streaming := map[string]bool{}
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
			if err := n.mergeIndex(conn, m.Index, streaming); err != nil {
				return err
			}
			if m.Index.Final {
				delete(remaining, m.Index.Folder)
				delete(streaming, m.Index.Folder)
			} else {
				streaming[m.Index.Folder] = true
			}
		case protocol.MsgClose:
			return fmt.Errorf("peer closed during index exchange")
		case protocol.MsgPing:
			// Keepalive liveness frame: ignored, never an error (see the
			// keepalive note above SyncOnce).
		default:
			// tolerate progress/ping interleaved
		}
	}
	return nil
}

// mergeIndex merges one Index frame into the local peer view. streaming is
// the set of folders currently mid-exchange (chunked index); it is nil
// outside the full-index exchange, where every index frame is complete on
// its own.
func (n *Node) mergeIndex(conn *transfer.Conn, msg *protocol.IndexMsg, streaming map[string]bool) error {
	f, ok := n.Folders[msg.Folder]
	if !ok {
		return nil
	}
	files := make([]*index.FileInfo, 0, len(msg.Files))
	for i := range msg.Files {
		files = append(files, fromFileMessage(&msg.Files[i]))
	}
	if streaming != nil {
		if streaming[msg.Folder] {
			// A non-final frame of a chunked exchange: append to the view
			// established by the folder's first frame.
			f.Engine.SetPeerIndexAppend(conn.PeerID, files)
		} else {
			// First frame of the exchange for this folder: a full send
			// (Update=false) replaces the view wholesale, a delta send
			// (Update=true) only changes the named entries.
			f.Engine.SetPeerIndex(conn.PeerID, files, msg.Update)
		}
		return nil
	}
	// Self-contained frame (index updates during the pull/serve phase, or a
	// legacy single-frame exchange).
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
				if m.Response.Code == protocol.RespInvalidFile {
					// The peer is still writing this file (stability gate on
					// the source). Skip it and keep the round alive for
					// everything else; it is retried next round. No
					// reportProblem — this is transient, not a failure.
					pull.Abort()
					n.Logf("folder %s: %s is still being written on the peer; will retry", f.Cfg.ID, name)
					return nil
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
					if err := n.mergeIndex(conn, m.Index, nil); err != nil {
						pull.Abort()
						return err
					}
				}
			case protocol.MsgPing:
				// Keepalive liveness frame: ignored, never an error (see the
				// keepalive note above SyncOnce).
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
				if err := n.mergeIndex(conn, m.Index, nil); err != nil {
					return err
				}
			}
		case terminal:
			return nil
		case protocol.MsgPing:
			// Keepalive liveness frame: ignored, never an error (see the
			// keepalive note above SyncOnce).
		case protocol.MsgEvent:
			// The peer's open unsynced set for one folder (protocol rev
			// >= 2). It arrives just before its terminal Close and is
			// stored, never answered.
			n.storeEventMsg(conn.PeerID, m)
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
	// Stability gate: the source is still writing this file, so any block we
	// handed out would be a snapshot of a partial. Refuse it (reusing the
	// existing, previously unused code — no protocol change); the puller
	// treats that as "skip this file, retry next round".
	if f.Engine.InFlight(req.Name) {
		return &protocol.Response{ID: req.ID, Code: protocol.RespInvalidFile}
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
	defer func() { _ = file.Close() }()
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

// keepaliveInterval is how often an active session writes a MsgPing (see
// startKeepalive). A variable so tests can shorten it; production value is
// transfer.KeepaliveInterval (20s) and it is deliberately not configurable.
var keepaliveInterval = transfer.KeepaliveInterval

// keepaliveStopGrace bounds how long stopping the keepalive waits for the ping
// goroutine. It is only ever inside a tiny frame write, so it stops at once;
// the bound exists so a goroutine stuck writing to a black-holed link can
// never block session teardown (the write fails and it exits on its own).
const keepaliveStopGrace = time.Second

// startKeepalive starts the session's ping sender and returns the function
// that stops it. Callers defer the returned stop, so the goroutine lives
// exactly as long as the session.
//
// A round reuses ONE connection for the whole folder exchange and its data
// phases (hashing, reading a big block) can be silent for minutes; that is
// long enough for a NAT middlebox on a CGNAT/Tailscale path to drop the idle
// flow, after which the next read returns a clean EOF and the round is
// abandoned. Pinging on both sides means whichever direction goes silent
// still refreshes the mapping. Ping frames go through transfer.Conn.Send, so
// they share the session's write mutex and can never interleave with a frame.
func startKeepalive(conn *transfer.Conn) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn.PingLoop(ctx, keepaliveInterval)
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(keepaliveStopGrace):
		}
	}
}

// Note on incoming MsgPing: it is IGNORED, never answered. The wire envelope
// has no ping/pong discriminator (no Ping field on Message), so a reply would
// be indistinguishable from a ping and each side answering the other's answer
// would ping-pong forever. Receiving a ping is itself proof the link is alive,
// which is all the sender needs, so every read path tolerates it explicitly:
// recvCluster skips it, and the data-phase loops (recvFullIndexes, pullOne,
// serveUntil) ignore it instead of failing the session.

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
	// The cluster exchange is over, so the peer may now interleave pings and
	// the long data phases below start: keep this connection warm.
	stopKeepalive := startKeepalive(conn)
	defer stopKeepalive()
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
	// Phase 2: we serve while the peer pulls. serveUntil also stores the
	// peer's unsynced-state announcements (MsgEvent), which it sends just
	// before its terminal Close.
	if err := n.serveUntil(conn, protocol.MsgClose); err != nil {
		return 0, err
	}
	// The exchange succeeded end-to-end: record what the peer now holds.
	if err := n.recordPeerIndex(conn.PeerID, conn, snaps); err != nil {
		return 0, err
	}
	n.markFoldersSynced(shared)
	// Tell the peer what we still have not synced (rev-2 peers only). The
	// reconciliation in markFoldersSynced has just run, so this is our
	// post-round state, and it goes after the serve phase because that is
	// where the peer's last pulls of our files land: only then is our own
	// upload backlog settled.
	if err := n.sendUnsyncedStates(conn, shared); err != nil {
		return 0, err
	}
	// Send our final Close; the responder acknowledges it before closing.
	if err := conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "done"}}); err != nil {
		return 0, err
	}
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
	// Cluster exchange done: the peer may interleave pings from here on, and
	// this side must keep the connection warm while it waits on the peer's
	// index and while it reads blocks from disk.
	stopKeepalive := startKeepalive(conn)
	defer stopKeepalive()
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
	// Our pull phase is done, so our own unsynced set is final for this
	// round. Reconcile first: an event whose path converged in this round
	// must not be advertised as still outstanding, or the peer would show a
	// phantom for a whole round.
	for _, id := range shared {
		n.reconcileUnsynced(id)
	}
	// Announce it before the Close, which the initiator's serveUntil treats
	// as terminal (rev-2 peers only).
	if err := n.sendUnsyncedStates(conn, shared); err != nil {
		return err
	}
	if err := conn.Send(&protocol.Message{Type: protocol.MsgClose, Close: &protocol.Close{Reason: "done"}}); err != nil {
		return err
	}
	// Wait for the initiator's final Close so we do not abort their write
	// by closing the socket underneath them. A rev-2 initiator sends its
	// unsynced-state announcements first, so this reads a small sequence:
	// MsgEvent (stored) ... then MsgClose. A rev-1 initiator sends Close
	// directly, which ends the loop at once. Old RESPONDERS never run this
	// loop at all — a new initiator only sends MsgEvent when the Hello
	// advertised rev >= 2, and an old responder advertises 1 — so no old
	// daemon is ever exposed to a frame it does not expect.
	//
	// By the time we get here the initiator has already closed the
	// connection, so a Recv error just means "done" and is intentionally
	// discarded, exactly as the single Recv it replaces did.
waitDone:
	for {
		m, err := conn.Recv()
		if err != nil {
			break
		}
		switch m.Type {
		case protocol.MsgEvent:
			n.storeEventMsg(conn.PeerID, m)
		case protocol.MsgClose:
			break waitDone
		case protocol.MsgPing:
			// Keepalive liveness frame: ignored, never an error.
		default:
			// Unknown type from a newer peer: ignore (rolling-upgrade
			// safety — never fail a completed round on it).
		}
	}
	// The exchange succeeded end-to-end: record what the peer now holds.
	if err := n.recordPeerIndex(conn.PeerID, conn, snaps); err != nil {
		return err
	}
	n.markFoldersSynced(shared)
	return nil
}

// markFoldersSynced updates per-folder stats after a successful session and
// reconciles the folder's stale "unsynced" attention events (see
// reconcileUnsynced): a round that completed end-to-end is exactly the
// moment we know what is and is not still outstanding.
func (n *Node) markFoldersSynced(folders []string) {
	now := time.Now().UnixNano()
	for _, id := range folders {
		if f, ok := n.lookupFolder(id); ok {
			f.Engine.Stats.Syncs.Add(1)
			f.Engine.Stats.LastSync.Store(now)
		}
		n.reconcileUnsynced(id)
	}
}

// reconcileUnsynced auto-resolves open warn+ "unsynced" events for paths
// that are now verifiably in sync on this device. Without this a stale
// event outlives its condition: it is only ever resolved on the success
// path of pullOne, and a path that becomes identical on both sides WITHOUT
// a later pull (it was pulled and then modified locally, or the peer
// already had it) never reaches that path again, so the event stays open
// forever and keeps the attention badge red.
//
// A path is only resolved when it is honestly converged: it must still be
// a live index entry, must not be waiting on a peer in either direction,
// and must not be held back by the stability gate (a file still being
// written is excluded from NeedPulls, so it would otherwise look in sync).
//
// Locking: the folder map is only read through lookupFolder (which drops
// foldersMu immediately), then per-folder engine/index work runs without
// any node-level lock held; probMu is taken alone, briefly, never across a
// store call (see the probMu note above checkMassChange).
func (n *Node) reconcileUnsynced(id string) {
	if n.Events == nil {
		return
	}
	f, ok := n.lookupFolder(id)
	if !ok {
		return
	}
	evs, err := n.Events.Query(events.Filter{Folder: id, Category: events.CatUnsynced, OpenOnly: true})
	if err != nil {
		n.Logf("folder %s: reconcile unsynced: %v", id, err)
		return
	}
	if len(evs) == 0 {
		return
	}
	paths := make([]string, 0, len(evs))
	seen := make(map[string]bool, len(evs))
	for _, e := range evs {
		if e.Path == "" || seen[e.Path] {
			continue
		}
		seen[e.Path] = true
		paths = append(paths, e.Path)
	}
	down, up := f.unsyncedDirections()
	for _, p := range paths {
		// (a)/(b): still owed in either direction, so not in sync yet.
		if down[p] || up[p] {
			continue
		}
		// (c): still being written; the gate hides it from NeedPulls.
		if f.Engine.InFlight(p) {
			continue
		}
		// (d): no live local entry (tombstoned or never indexed).
		fi, ok, err := f.Ix.Get(p)
		if err != nil || !ok || fi.Deleted {
			continue
		}
		if err := n.Events.ResolveAutoCondition(id, p, events.CatUnsynced, "system"); err != nil {
			n.Logf("folder %s: resolve unsynced %s: %v", id, p, err)
			continue
		}
		// Forget the in-memory throttle entry too: reportProblem only emits
		// on a reason CHANGE, so a leftover entry would silence the next
		// genuine failure of this path.
		key := id + "\x00" + p
		n.probMu.Lock()
		delete(n.problems, key)
		n.probMu.Unlock()
	}
}

// maxUnsyncedPerFolder caps the entries carried in one EventMsg (and the
// stored per-peer view). A folder with a long backlog of stuck files must
// never produce an unbounded frame; paths are sorted first, so the cap
// always keeps the same deterministic prefix.
const maxUnsyncedPerFolder = 200

// protocolUnsyncedState is the protocol revision that introduced the
// end-of-round unsynced-state announcement (MsgEvent). It is only ever SENT
// when the peer advertised at least this revision (see sendUnsyncedStates);
// receiving is always accepted, because an unexpected MsgEvent from a peer
// that advertised more than we know about is harmless.
const protocolUnsyncedState = 2

// unsyncedDirections returns the two name sets that decide a path's
// direction tag: paths this device still needs from a peer ("down") and
// paths a peer still needs from this device ("up"). Both are read from the
// engine's existing computations, never from a fresh index walk.
func (f *Folder) unsyncedDirections() (down, up map[string]bool) {
	down = map[string]bool{}
	up = map[string]bool{}
	if pulls, err := f.Engine.NeedPulls(); err == nil {
		for _, name := range pulls {
			down[name] = true
		}
	}
	if ups, err := f.Engine.PendingUploads(); err == nil {
		for _, name := range ups {
			up[name] = true
		}
	}
	return down, up
}

// dirTag renders the direction tag shared by the API (AttentionFile.Dir)
// and the wire (protocol.UnsyncedEntry.Dir): "down" = we need that path
// from a peer, "up" = a peer needs it from us, "both" = both, "" = neither.
func dirTag(down, up bool) string {
	switch {
	case down && up:
		return "both"
	case down:
		return "down"
	case up:
		return "up"
	default:
		return ""
	}
}

// eventMsgFor builds this device's complete open unsynced set for one
// folder: one entry per path that still has an open warn+ unsynced event,
// with the direction tag the receiving peer will display. An empty (but
// non-nil) message is meaningful — it tells the peer we are fully synced —
// so a known folder always produces a message.
func (n *Node) eventMsgFor(id string) *protocol.EventMsg {
	f, ok := n.lookupFolder(id)
	if !ok {
		return nil
	}
	if n.Events == nil {
		return &protocol.EventMsg{Folder: id}
	}
	evs, err := n.Events.Query(events.Filter{Folder: id, Category: events.CatUnsynced, OpenOnly: true})
	if err != nil {
		n.Logf("folder %s: unsynced summary: %v", id, err)
		return &protocol.EventMsg{Folder: id}
	}
	down, up := f.unsyncedDirections()
	seen := make(map[string]bool, len(evs))
	out := make([]protocol.UnsyncedEntry, 0, len(evs))
	for _, e := range evs {
		if e.Path == "" || seen[e.Path] {
			continue // one entry per condition; newest reason wins
		}
		seen[e.Path] = true
		out = append(out, protocol.UnsyncedEntry{
			Path:   e.Path,
			Reason: e.Reason,
			Dir:    dirTag(down[e.Path], up[e.Path]),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	if len(out) > maxUnsyncedPerFolder {
		out = out[:maxUnsyncedPerFolder]
	}
	return &protocol.EventMsg{Folder: id, Entries: out}
}

// sendUnsyncedStates advertises our open unsynced set for every shared
// folder, one message per folder. It is sent at the end of every
// successful round (never only on change) so each peer always holds a
// current picture, and the receiver replaces its stored set wholesale.
//
// Gating: only a peer that advertised protocol rev >= 2 gets these frames.
// An old daemon would treat MsgEvent as an unknown type — harmless in the
// read loops, but it also never waits for one, and sending frames it did
// not expect near the end of the round is exactly the kind of avoidable
// surprise rolling upgrades must not have. So nothing is sent to a rev < 2
// peer; the round is byte-for-byte the old exchange.
func (n *Node) sendUnsyncedStates(conn *transfer.Conn, folders []string) error {
	if conn.PeerProtocol < protocolUnsyncedState {
		return nil
	}
	for _, id := range folders {
		em := n.eventMsgFor(id)
		if em == nil {
			continue
		}
		if err := conn.Send(&protocol.Message{Type: protocol.MsgEvent, Event: em}); err != nil {
			return err
		}
	}
	return nil
}

// storeEventMsg records one received unsynced-state announcement for the
// peer that sent it. Malformed frames are ignored rather than failing a
// round that already succeeded.
func (n *Node) storeEventMsg(peerID uint64, m *protocol.Message) {
	if m == nil || m.Event == nil {
		return
	}
	n.StorePeerUnsynced(peerID, m.Event.Folder, m.Event.Entries)
}

