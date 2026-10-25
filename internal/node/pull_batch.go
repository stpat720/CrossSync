package node

import (
	"fmt"

	"crosssync/internal/index"
	"crosssync/internal/protocol"
	syncpkg "crosssync/internal/sync"
	"crosssync/internal/transfer"
)

// Concurrent pulling.
//
// Blocks of ONE file are already pipelined (pullWindowSize requests in flight,
// which is what turns a large file from "block size / RTT" into a sustained
// stream), but the files themselves used to be pulled strictly one after
// another: every file cost at least one full round trip (request out, response
// back) plus its commit, so a tree of small files ran at roughly one file per
// RTT — measured 32 files/s over the 18 ms Tailscale hop between the two live
// nodes, i.e. ~2 round trips per file.
//
// A batch keeps up to K files in flight at once (config pull_concurrency,
// default 4, hard-capped at 8), each with its own block window, so while one
// file's response is on the wire the other files' requests are already queued
// at the peer. The round trip is then paid once per K files instead of once
// per file.
//
// MEMORY PER PEER: K x pullWindowSize block buffers, i.e. 32 blocks by default
// and 64 at the cap. At the usual 256 KiB block size that is 8 MiB (16 MiB at
// the cap); at the 16 MiB block-size ceiling — a file over ~32 GiB, where the
// 2000-block rule picks the largest block — it is 1 GiB at the cap, which is
// the other half of why K is capped at all. The peer's serving side
// (serveUntil) answers one request at a time, so a larger K only deepens ITS
// socket queue — K can never make that side allocate more than one block per
// request — but it can starve that loop. Only one round per peer runs at a time
// (acquirePeerSession) and folders are pulled in sequence, so this is the whole
// per-peer budget.
//
// CORRELATION: a block response carries only {ID, Data, Code}. IDs come from
// the CONNECTION (conn.NextRequestID), so they are unique across files, and
// the batch keeps ONE map from request ID to (job, block) — there is no
// per-file ID space a response could be matched into twice. A response whose
// ID is not in the map is dropped (an abandoned file's answer arriving late),
// and one whose job is already dead is dropped too: bytes can only ever land
// in the temp file of the job that asked for them.
//
// ORDERING: moves stay planned and applied before the batch starts, and pending
// deletions are still applied after it returns (pullAll's phase order is
// unchanged). Only the order WITHIN the pull list becomes interleaved; the set
// of names pulled in a round is identical.

// pullJob is one file's pull inside a batch.
type pullJob struct {
	name     string
	pull     *syncpkg.Pull
	needs    []syncpkg.BlockNeed
	sent     int
	received int
	// dead marks a job that has been completed or abandoned. Responses for
	// its still-outstanding requests are dropped instead of written.
	dead bool
}

// pullRef attributes one outstanding request to a file and a block in it.
type pullRef struct {
	job *pullJob
	idx int // index into job.needs
}

// pullBatch is the state of one folder's pull phase over one connection.
type pullBatch struct {
	n    *Node
	conn *transfer.Conn
	f    *Folder
	k    int

	jobs []*pullJob
	// inFlight holds every request on this connection that has not been
	// answered yet, across all files. Its length is the number of blocks the
	// peer still owes us, which is also the loop's stop condition.
	inFlight map[uint64]pullRef
}

// pullFiles pulls every name in names from the peer (the global winner of each)
// keeping up to pull_concurrency files in flight.
//
// It returns nil for every per-file outcome: plain success, and per-file
// difficulties (local write/commit failed, the peer's disk couldn't serve this
// one file, a block arrived corrupted) which are already recorded as a visible,
// retrying unsynced event. Those are NOT reasons to abort the round — aborting
// propagates to SyncOnce and marks a healthy peer offline over one stubborn
// file (the live-share bug) — so the file is skipped and the round keeps going.
// A non-nil return means the connection itself broke (Send/Recv/merge transport
// failure), which must still abort the round.
func (n *Node) pullFiles(conn *transfer.Conn, f *Folder, names []string) error {
	b := &pullBatch{
		n:        n,
		conn:     conn,
		f:        f,
		k:        n.Cfg.PullFiles(),
		inFlight: make(map[uint64]pullRef, pullWindowSize),
	}
	return b.run(names)
}

// run admits files until K are in flight, then dispatches responses until every
// name has been handled and every request has been answered.
func (b *pullBatch) run(names []string) error {
	next := 0
	for {
		// Admit while there is a free slot and something left to start. A
		// name that needs no network (directory, symlink, deletion, a pull
		// satisfied from a local copy) is handled here and never takes a slot.
		for len(b.jobs) < b.k && next < len(names) {
			// A pause that lands mid-batch takes effect here: the files
			// already in flight finish (their blocks are on the wire and
			// committing them is cheaper than throwing them away), and
			// nothing new is started, so the operator's pause is bounded by
			// K files instead of by the rest of the folder. The rest of the
			// list stays pending; the next round resumes it with no re-walk.
			if b.f.TransfersPaused() {
				b.n.Logf("folder %s: transfers paused mid-round; %d file(s) left for the next round",
					b.f.Cfg.ID, len(names)-next)
				next = len(names)
				break
			}
			name := names[next]
			next++
			if b.active(name) {
				// Never two pulls of one name at the same time: they would
				// stage into the same temp file, interleave their blocks into
				// it and race for the same commit.
				b.n.Logf("folder %s: %s is already being pulled in this round; skipping the duplicate", b.f.Cfg.ID, name)
				continue
			}
			job, err := b.start(name)
			if err != nil {
				b.abortAll()
				return err
			}
			if job != nil {
				b.jobs = append(b.jobs, job)
			}
		}
		if len(b.jobs) == 0 && len(b.inFlight) == 0 {
			return nil
		}
		// The peer answers every request exactly once, so the answers of an
		// abandoned file always drain: no wait here is unbounded.
		m, err := b.conn.Recv()
		if err != nil {
			b.abortAll()
			return err
		}
		if err := b.handle(m); err != nil {
			b.abortAll()
			return err
		}
	}
}

// active reports whether name is already in flight in this batch. The round's
// pending list is unique by construction (NeedPulls walks the index once per
// name), so this is a guard rather than a dedupe pass — deliberately O(K) and
// not a map over the whole list: a first sync of a multi-million-file tree
// would pay hundreds of megabytes for an index of names it already holds.
func (b *pullBatch) active(name string) bool {
	for _, job := range b.jobs {
		if job.name == name {
			return true
		}
	}
	return false
}

// start begins the pull of one name. A nil job with a nil error means the name
// is finished (or failed per-file, already reported); a non-nil error is a
// transport failure.
func (b *pullBatch) start(name string) (*pullJob, error) {
	n, f, conn := b.n, b.f, b.conn
	plan, err := f.Engine.PlanOverwrite(name)
	if err != nil {
		n.reportProblem(f.Cfg.ID, name, err)
		return nil, nil
	}
	plan.PeerID = conn.PeerID
	if _, err := f.Engine.Execute(plan); err != nil {
		n.reportProblem(f.Cfg.ID, name, err)
		return nil, nil
	}
	if plan.Delete {
		if err := f.Engine.ApplyDeletion(name, conn.PeerID); err != nil {
			n.reportProblem(f.Cfg.ID, name, err)
			return nil, nil
		}
		n.clearProblem(f.Cfg.ID, name)
		return nil, n.sendIndexUpdate(conn, f.Cfg.ID, name)
	}
	// Directories and symlinks carry no content: create/adopt them rather
	// than transferring blocks.
	switch plan.Global.Type {
	case index.TypeDirectory:
		if err := f.Engine.EnsureDir(plan.Global); err != nil {
			n.reportProblem(f.Cfg.ID, name, err)
			return nil, nil
		}
		n.clearProblem(f.Cfg.ID, name)
		return nil, n.sendIndexUpdate(conn, f.Cfg.ID, name)
	case index.TypeSymlink:
		n.Logf("folder %s: symlink %q not followed (v1 limitation); indexing it", f.Cfg.ID, name)
		if err := f.Engine.AdoptMetadata(plan.Global); err != nil {
			n.reportProblem(f.Cfg.ID, name, err)
			return nil, nil
		}
		n.clearProblem(f.Cfg.ID, name)
		return nil, n.sendIndexUpdate(conn, f.Cfg.ID, name)
	}

	pull, err := f.Engine.StartPull(name, conn.PeerID)
	if err != nil {
		n.reportProblem(f.Cfg.ID, name, err)
		return nil, nil
	}
	job := &pullJob{name: name, pull: pull, needs: pull.NeedBlocks()}
	if len(job.needs) == 0 {
		// Nothing to fetch: a resumed temp already held every valid block, or
		// the content came from a byte-identical local copy. Commit it now; it
		// must not hold a concurrency slot.
		return nil, b.complete(job)
	}
	for job.sent < pullWindowSize && job.sent < len(job.needs) {
		if err := b.sendNext(job); err != nil {
			pull.Abort()
			return nil, err
		}
	}
	return job, nil
}

// sendNext asks for the next block this job needs and records the ID.
func (b *pullBatch) sendNext(job *pullJob) error {
	if job.dead || job.sent >= len(job.needs) {
		return nil
	}
	need := job.needs[job.sent]
	id := b.conn.NextRequestID()
	req := &protocol.Request{ID: id, Folder: b.f.Cfg.ID, Name: job.name, Offset: need.Offset, Size: need.Size}
	if err := b.conn.Send(&protocol.Message{Type: protocol.MsgRequest, Request: req}); err != nil {
		return err
	}
	b.inFlight[id] = pullRef{job: job, idx: job.sent}
	job.sent++
	return nil
}

// handle dispatches one message from the peer.
func (b *pullBatch) handle(m *protocol.Message) error {
	switch m.Type {
	case protocol.MsgResponse:
		if m.Response == nil {
			return nil
		}
		ref, ok := b.inFlight[m.Response.ID]
		if !ok {
			// Unknown ID: never produced by this batch. Dropping is the only
			// safe choice — it cannot be attributed to any file.
			return nil
		}
		delete(b.inFlight, m.Response.ID)
		job := ref.job
		if job.dead {
			// The file was abandoned mid-flight; its answers are drained here
			// and go nowhere.
			return nil
		}
		switch m.Response.Code {
		case protocol.RespNoError:
			// fall through to the write below
		case protocol.RespInvalidFile:
			// The peer is still writing this file (stability gate on the
			// source). Skip it and keep the round alive for everything else;
			// it is retried next round. No reportProblem — transient.
			b.n.Logf("folder %s: %s is still being written on the peer; will retry", b.f.Cfg.ID, job.name)
			b.abandon(job)
			return nil
		case protocol.RespNoSuchFile:
			// The source deleted or renamed the file away under an in-flight
			// pull (a converging delete). Transient, not a failure: the
			// tombstone merges in this round and the file is not re-pulled.
			b.n.Logf("folder %s: %s is gone on the peer; skipping, will re-check next round", b.f.Cfg.ID, job.name)
			b.abandon(job)
			return nil
		default:
			// The peer's disk refused this specific file (code 1): per-file
			// difficulty, visible and retrying, never a broken connection.
			b.n.reportProblem(b.f.Cfg.ID, job.name, fmt.Errorf("block request failed: code %d", m.Response.Code))
			b.abandon(job)
			return nil
		}
		// ReceiveBlock verifies the hash before writing, so a block carrying
		// another file's content (or a corrupted transfer) is refused here
		// instead of being staged.
		if err := job.pull.ReceiveBlock(job.needs[ref.idx].Index, m.Response.Data); err != nil {
			b.n.reportProblem(b.f.Cfg.ID, job.name, err)
			b.abandon(job)
			return nil
		}
		job.received++
		if job.received == len(job.needs) {
			return b.complete(job)
		}
		return b.sendNext(job)
	case protocol.MsgIndex, protocol.MsgIndexUpdate:
		if m.Index != nil {
			return b.n.mergeIndex(b.conn, m.Index, nil)
		}
	case protocol.MsgPing:
		// Keepalive liveness frame: ignored, never an error.
	default:
		// ignore interleaved control messages
	}
	return nil
}

// complete commits a finished job and tells the peer.
func (b *pullBatch) complete(job *pullJob) error {
	job.dead = true
	b.remove(job)
	if _, err := job.pull.Finish(); err != nil {
		b.n.reportProblem(b.f.Cfg.ID, job.name, err)
		return nil
	}
	b.n.clearProblem(b.f.Cfg.ID, job.name)
	return b.n.sendIndexUpdate(b.conn, b.f.Cfg.ID, job.name)
}

// abandon drops a job without committing it. Its staged temp is kept (a later
// pull resumes from it) and its outstanding answers are drained and ignored.
func (b *pullBatch) abandon(job *pullJob) {
	if job.dead {
		return
	}
	job.dead = true
	job.pull.Abort()
	b.remove(job)
}

func (b *pullBatch) remove(job *pullJob) {
	for i, j := range b.jobs {
		if j == job {
			b.jobs = append(b.jobs[:i], b.jobs[i+1:]...)
			return
		}
	}
}

// abortAll releases every staged temp on a transport failure. The pulls are
// resumable, so nothing is lost but the round.
func (b *pullBatch) abortAll() {
	for _, job := range b.jobs {
		job.dead = true
		job.pull.Abort()
	}
	b.jobs = nil
}
