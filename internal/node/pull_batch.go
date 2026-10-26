package node

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"crosssync/internal/hash"
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

// Coalescing: one request per RUN of consecutive blocks.
//
// The wire request already carries an arbitrary {offset, size} (the peer reads
// exactly that many bytes, and its only limit is hash.MaxBlockSize = 16 MiB),
// so a puller can ask for 32 consecutive 128 KiB blocks in one request instead
// of 32 requests. That matters because the cost on a FUSE share (Unraid's shfs)
// is per I/O, not per byte: measured ~43-47 ms per 128 KiB block either way, so
// the block count, not the byte count, was setting throughput (2.7-3.1 MB/s on a
// 64 MiB file, 23 MB/s with 1 MiB blocks). Runs cut both the request count and
// the peer's read syscalls; block hashes and verification are unchanged, so the
// index format and every integrity guarantee stay exactly as they were.
//
// A run never crosses a gap (a resumed temp makes needs non-contiguous) and never
// exceeds the configured pull_run_kb, so memory stays predictable. Response
// bytes outstanding per peer are bounded by pull_inflight_mb: a count of
// requests is not a memory bound once one request can be megabytes.
// Both are per-config knobs (see config.PullRunBytesOrDefault).

// planPulls groups consecutive block needs into wire runs of at most maxRun
// bytes. Order is preserved, so a run is always a contiguous byte range.
func planPulls(needs []syncpkg.BlockNeed, maxRun int) []pullRun {
	if len(needs) == 0 {
		return nil
	}
	if maxRun <= 0 {
		maxRun = hash.MaxBlockSize
	}
	runs := make([]pullRun, 0, len(needs))
	cur := pullRun{needs: []syncpkg.BlockNeed{needs[0]}, offset: needs[0].Offset, size: needs[0].Size}
	for _, need := range needs[1:] {
		last := cur.needs[len(cur.needs)-1]
		// Consecutive in the file, and the run must stay under the cap and
		// under the peer's per-request limit.
		if need.Index == last.Index+1 && cur.size+need.Size <= maxRun && cur.size+need.Size <= hash.MaxBlockSize {
			cur.needs = append(cur.needs, need)
			cur.size += need.Size
			continue
		}
		runs = append(runs, cur)
		cur = pullRun{needs: []syncpkg.BlockNeed{need}, offset: need.Offset, size: need.Size}
	}
	return append(runs, cur)
}

// pullJob is one file's pull inside a batch.
type pullJob struct {
	name  string
	pull  *syncpkg.Pull
	needs []syncpkg.BlockNeed
	// runs is needs grouped into consecutive ranges: one wire request
	// per run instead of one per block (see planPulls).
	runs     []pullRun
	sent     int // runs sent
	received int // blocks received
	total    int // blocks the file needs
	// dead marks a job that has been completed or abandoned. Responses for
	// its still-outstanding requests are dropped instead of written.
	dead bool
}

// pullRun is one wire request covering one or more CONSECUTIVE blocks of a
// file. The block list still decides hashing and verification; a run only
// changes how many bytes one request asks for.
type pullRun struct {
	needs  []syncpkg.BlockNeed
	offset int64
	size   int
}

// pullRef attributes one outstanding request to a file and the run it is for.
type pullRef struct {
	job *pullJob
	run int // index into job.runs
}

// pullBatch is the state of one folder's pull phase over one connection.
type pullBatch struct {
	n    *Node
	conn *transfer.Conn
	f    *Folder
	k    int

	jobs []*pullJob
	// inflightBytes is how many response bytes the peer owes us right now.
	// Requests are issued while this is under the batch's byte budget, which is
	// what keeps a few large runs from pinning tens of megabytes of buffers
	// (the count window alone cannot: a run is up to pull_run_kb).
	inflightBytes int
	// inFlight holds every request on this connection that has not been
	// answered yet, across all files. Its length is the number of blocks the
	// peer still owes us, which is also the loop's stop condition.
	inFlight map[uint64]pullRef
	// Tuning from config: the largest run requested, the response-byte
	// budget, and whether a block may be copied from another local file.
	runBytes int
	budget   int
	reuse    bool
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
		runBytes: n.Cfg.PullRunBytesOrDefault(),
		budget:   n.Cfg.PullInFlightBytesOrDefault(),
		reuse:    n.Cfg.ReuseLocalBlocks(),
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
		// abandoned file always drain: no wait here is unbounded — EXCEPT when
		// the socket is half-open (the peer restarted, so no FIN ever arrives).
		// Then Recv blocks forever and the round hangs with files still pending
		// and nothing to show for it: measured live, a 31 GiB folder stalled
		// with 3 files left, 0 bytes for minutes, no error, on both nodes.
		if len(b.inFlight) > 0 {
			_ = b.conn.SetReadDeadline(time.Now().Add(pullStallTimeout))
		} else {
			_ = b.conn.SetReadDeadline(time.Time{})
		}
		m, err := b.conn.Recv()
		_ = b.conn.SetReadDeadline(time.Time{})
		if err != nil {
			b.abortAll()
			if isTimeout(err) {
				return fmt.Errorf("peer sent nothing for %s with %d request(s) in flight: %w",
					pullStallTimeout, len(b.inFlight), err)
			}
			return err
		}
		if err := b.handle(m); err != nil {
			b.abortAll()
			return err
		}
	}
}

// orderPulls applies the configured pull order to one folder's pending list.
// "index" keeps the previous behaviour (name order, the order the index walks
// it), "smallest" (default) puts small files first, "largest" streams big ones
// early. Names the peer's view has no size for sort last under "smallest": an
// unknown cost should not jump the queue.
func (n *Node) orderPulls(f *Folder, peerID uint64, names []string) []string {
	order := n.Cfg.PullOrderOrDefault()
	if order == "index" || len(names) < 2 {
		return names
	}
	sizes := f.Engine.PeerSizes(peerID, names)
	out := make([]string, len(names))
	copy(out, names)
	sort.SliceStable(out, func(i, j int) bool {
		si, oki := sizes[out[i]]
		sj, okj := sizes[out[j]]
		if order == "largest" {
			if si != sj {
				return si > sj
			}
			return out[i] < out[j]
		}
		// smallest: unknown sizes last, then by size, then by name for a
		// deterministic order.
		if oki != okj {
			return oki
		}
		if si != sj {
			return si < sj
		}
		return out[i] < out[j]
	})
	return out
}

// pullStallTimeout is how long a pull round waits for the peer with requests in
// flight before treating the session as dead. A half-open socket (the peer
// restarted, so no FIN ever arrives) otherwise blocks Recv forever and leaves the
// round hanging silently with files still pending; this turns it into a transport
// error, which the round already retries with backoff. Generous on purpose: a
// 4 MiB run served from a saturated array is slow, but not two minutes slow.
var pullStallTimeout = 120 * time.Second

// isTimeout reports whether err is a deadline (no progress) rather than a broken
// connection.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
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
	// Local block reuse first: a block whose bytes already exist in another
	// local file never has to cross the wire. It runs after the whole-file
	// reuse inside StartPull, so duplicates are already free and this catches
	// the partial overlap (an edited copy, a shared prefix, a .venv tree).
	if b.reuse {
		if _, _, rerr := f.Engine.FillBlocksFromLocal(pull); rerr != nil {
			b.n.Logf("folder %s: local block reuse for %s: %v", f.Cfg.ID, name, rerr)
		}
	}
	job := &pullJob{name: name, pull: pull, needs: pull.NeedBlocks()}
	job.total = len(job.needs)
	job.runs = planPulls(job.needs, b.runBytes)
	if len(job.needs) == 0 {
		// Nothing to fetch: a resumed temp already held every valid block, or
		// the content came from a byte-identical local copy. Commit it now; it
		// must not hold a concurrency slot.
		return nil, b.complete(job)
	}
	for job.sent < pullWindowSize && job.sent < len(job.runs) {
		before := job.sent
		if err := b.sendNext(job); err != nil {
			pull.Abort()
			return nil, err
		}
		if job.sent == before {
			// The byte budget is exhausted: wait for a response to free
			// room instead of spinning here. The response path refills.
			break
		}
	}
	return job, nil
}

// sendNext asks for the next RUN this job needs and records the ID. It is a
// no-op (not an error) when the byte budget is exhausted and something is
// already in flight; the caller retries after a response frees room.
func (b *pullBatch) sendNext(job *pullJob) error {
	if job.dead || job.sent >= len(job.runs) {
		return nil
	}
	run := job.runs[job.sent]
	// The budget may not block the ONLY request in flight, or the batch
	// would wait for a response that was never asked for.
	if b.inflightBytes > 0 && b.inflightBytes+run.size > b.budget {
		return nil
	}
	id := b.conn.NextRequestID()
	req := &protocol.Request{ID: id, Folder: b.f.Cfg.ID, Name: job.name, Offset: run.offset, Size: run.size}
	if err := b.conn.Send(&protocol.Message{Type: protocol.MsgRequest, Request: req}); err != nil {
		return err
	}
	b.inFlight[id] = pullRef{job: job, run: job.sent}
	b.inflightBytes += run.size
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
		// A run answers several blocks with one buffer. Split it back into the
		// blocks it covers and verify each against its own hash - the block
		// list is still the unit of integrity, so a run that is short or
		// misaligned is refused exactly like a corrupted block.
		run := job.runs[ref.run]
		b.inflightBytes -= run.size
		if b.inflightBytes < 0 {
			b.inflightBytes = 0
		}
		if len(m.Response.Data) != run.size {
			b.n.reportProblem(b.f.Cfg.ID, job.name, fmt.Errorf("run of %d block(s) came back with %d of %d byte(s)",
				len(run.needs), len(m.Response.Data), run.size))
			b.abandon(job)
			return nil
		}
		// One verification pass over the run, then ONE write for the whole
		// range: the receiving array charges ~40 ms per write whether it
		// carries 128 KiB or 4 MiB, so per-block writes were the ceiling.
		if err := job.pull.ReceiveBlocks(run.needs[0].Index, m.Response.Data); err != nil {
			b.n.reportProblem(b.f.Cfg.ID, job.name, err)
			b.abandon(job)
			return nil
		}
		job.received += len(run.needs)
		if job.received == job.total {
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
