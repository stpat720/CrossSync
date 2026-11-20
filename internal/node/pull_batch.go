package node

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
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
	// Stage timestamps for the pull metrics (internal/sync.PullStageTiming):
	// when the batch admitted the file, when its first block landed and when
	// its last block landed. They cost three timestamps on a struct that is
	// allocated per file anyway — no per-file allocation, no logging.
	admitAt      time.Time
	firstBlockAt time.Time
	allBlocksAt  time.Time
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
	// lastProgress is the last moment this round did real WORK: a block run
	// carrying data, a file committed, an index frame merged. Keepalive pings
	// deliberately do not move it — see the watchdog note in run.
	lastProgress time.Time
}

// noteProgress marks a piece of real work on this round. Only the paths that
// actually move a file forward call it: a response whose data was written, a
// job that committed, an index that merged. A ping is proof of a live socket,
// never proof of progress, so it must not call this.
func (b *pullBatch) noteProgress() { b.lastProgress = time.Now() }

// stallDeadline is when the next read must give up: lastProgress plus the stall
// window, never later than "right now". Re-arming it per frame would let a
// keepalive ping (every 20 s) extend the budget forever — that is the live
// wedge this exists to end — so the deadline is derived from progress and
// monotonically fails to move while only pings arrive.
func (b *pullBatch) stallDeadline(now time.Time) time.Time {
	d := b.lastProgress.Add(peerStallTimeout)
	if !d.After(now) {
		// The budget is already spent: fail this read at once rather than
		// granting another full window per ping.
		return now
	}
	return d
}

// stuckFiles names the distinct files with at least one request still
// outstanding, capped at max (more reports how many did not fit). Sorted so the
// operator's error text is stable between runs. Jobs already abandoned on
// purpose (a file the peer refused, a converging delete) are skipped: their
// answers are just draining, and blaming those files would be a false alarm.
func (b *pullBatch) stuckFiles(max int) (names []string, more int) {
	seen := make(map[string]struct{}, len(b.inFlight))
	for _, ref := range b.inFlight {
		if ref.job.dead {
			continue
		}
		name := ref.job.name
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	if max > 0 && len(names) > max {
		more = len(names) - max
		names = names[:max]
	}
	return names, more
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
	// Requests that cannot be written mean the peer is gone; bound the sends so
	// a half-open socket cannot wedge the round.
	conn.SetWriteTimeout(peerStallTimeout)
	defer conn.SetWriteTimeout(0)
	// The session's stall budget is armed PER FRAME (transfer.Conn.SetStall), so
	// the keepalive pings that keep the link warm also re-arm it; it can never
	// fire while the peer is merely quiet. This batch runs its own
	// progress-based watchdog instead, and puts the session's budget back when
	// it is done.
	prevStall := conn.Stall()
	conn.SetStall(0)
	defer conn.SetStall(prevStall)

	b := n.newPullBatch(conn, f)
	start := time.Now()
	err := b.run(names)
	// Batch wall time: the denominator for "how much of a round is per-file
	// cost". Recorded for batches that had something to do, so an idle folder
	// does not drag the average down.
	if len(names) > 0 {
		f.Engine.Stats.PullBatchNs.Add(time.Since(start).Nanoseconds())
		f.Engine.Stats.PullBatches.Add(1)
	}
	return err
}

// newPullBatch builds one folder's pull batch, SAMPLING every tuning value once.
//
// K is read from the node's live atomic (see Node.PullConcurrency) rather than
// from the config struct, because PUT /api/settings/sync can change it on a
// running daemon — and sampling it here is exactly what makes a change apply to
// the NEXT batch without disturbing this one: the jobs already admitted keep
// their slots, and the admitted count is what bounds memory.
func (n *Node) newPullBatch(conn *transfer.Conn, f *Folder) *pullBatch {
	return &pullBatch{
		n:        n,
		conn:     conn,
		f:        f,
		k:        n.PullConcurrency(),
		runBytes: n.Cfg.PullRunBytesOrDefault(),
		budget:   n.Cfg.PullInFlightBytesOrDefault(),
		reuse:    n.Cfg.ReuseLocalBlocks(),
		inFlight: make(map[uint64]pullRef, pullWindowSize),
	}
}

// run admits files until K are in flight, then dispatches responses until every
// name has been handled and every request has been answered.
func (b *pullBatch) run(names []string) error {
	b.noteProgress()
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
		// abandoned file always drain: no wait HERE is unbounded — provided the
		// deadline is anchored on PROGRESS. It used to be re-armed on every
		// loop iteration, i.e. on every frame, and the keepalive goroutine
		// emits a ping every 20 s independently of this code path: every ping
		// returned from Recv, the loop re-armed, and a 120 s budget could never
		// elapse while the peer was merely quiet rather than gone. Measured
		// live: a round running:true for 74 minutes with bytes_down frozen,
		// inflight_files 0 on both nodes, no error, no event — and the OTHER
		// folder syncing to that peer held up for 73 of those minutes because
		// the per-peer session slot was still taken.
		if len(b.inFlight) > 0 {
			_ = b.conn.SetReadDeadline(b.stallDeadline(time.Now()))
		} else {
			_ = b.conn.SetReadDeadline(time.Time{})
		}
		m, err := b.conn.Recv()
		_ = b.conn.SetReadDeadline(time.Time{})
		if err != nil {
			if isTimeout(err) {
				outstanding := len(b.inFlight)
				// One visible, retrying unsynced event per stuck file; the
				// error text only names a handful (a K x window batch can hold
				// dozens) and says how many are left out.
				all, _ := b.stuckFiles(0)
				for _, name := range all {
					b.n.reportProblem(b.f.Cfg.ID, name, fmt.Errorf(
						"peer sent nothing for %s while serving this file — the peer may be stalled reading it; the round will retry",
						peerStallTimeout))
				}
				names, more := all, 0
				if len(names) > stallNameLimit {
					more = len(names) - stallNameLimit
					names = names[:stallNameLimit]
				}
				b.abortAll()
				where := "no live file had a request outstanding"
				if len(names) > 0 {
					where = strings.Join(names, ", ")
					if more > 0 {
						where += fmt.Sprintf(" …and %d more", more)
					}
				}
				return fmt.Errorf("peer sent nothing for %s with %d request(s) in flight (%s): %w",
					peerStallTimeout, outstanding, where, err)
			}
			b.abortAll()
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

// peerStallTimeout is how long a pull round waits for the peer with requests in
// flight before treating the session as dead. A half-open socket (the peer
// restarted, so no FIN ever arrives) otherwise blocks Recv forever and leaves the
// round hanging silently with files still pending; this turns it into a transport
// error, which the round already retries with backoff. Generous on purpose: a
// 4 MiB run served from a saturated array is slow, but not two minutes slow.
var peerStallTimeout = 120 * time.Second

// stallNameLimit bounds how many file names a stall error prints. Every stuck
// file still gets its own unsynced event; the error is what an operator reads in
// a log line, and a 64-request batch must not turn that into a wall of names.
const stallNameLimit = 5

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
	job := &pullJob{name: name, pull: pull, needs: pull.NeedBlocks(), admitAt: time.Now()}
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
		// Data landed and verified: this is progress, whatever else is
		// outstanding. A ping never is (see noteProgress).
		b.noteProgress()
		// Stage boundaries: the first block ends the "wait" stage and starts
		// the transfer stage; the last block ends the transfer stage.
		if job.firstBlockAt.IsZero() {
			job.firstBlockAt = time.Now()
		}
		if job.received == job.total {
			job.allBlocksAt = time.Now()
			return b.complete(job)
		}
		return b.sendNext(job)
	case protocol.MsgIndex, protocol.MsgIndexUpdate:
		if m.Index != nil {
			if err := b.n.mergeIndex(b.conn, m.Index, nil); err != nil {
				return err
			}
			b.noteProgress()
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
	b.noteProgress()
	_, err := job.pull.Finish()
	done := time.Now()
	if err != nil {
		// A failed commit contributes no stage samples: its half-measured
		// timings would skew the averages it is supposed to explain.
		b.n.reportProblem(b.f.Cfg.ID, job.name, err)
		return nil
	}
	b.recordPullStages(job, done)
	b.n.clearProblem(b.f.Cfg.ID, job.name)
	return b.n.sendIndexUpdate(b.conn, b.f.Cfg.ID, job.name)
}

// recordPullStages publishes the four-stage split of one COMMITTED file to the
// folder's counters (see sync.PullStageTiming). It is pure arithmetic on
// timestamps already sampled on the pull path: no allocation, no logging, no
// lock — four atomic adds and a counter, per file.
//
// A job that needed no blocks (a resumed temp already held them all, or the
// content came from a byte-identical local copy) has no first/last block, so its
// wait and transfer stages are zero and its whole cost is fsync + commit: that is
// the honest reading, and it keeps the counters comparable across files.
func (b *pullBatch) recordPullStages(job *pullJob, finishDone time.Time) {
	first, all := job.firstBlockAt, job.allBlocksAt
	if first.IsZero() || all.IsZero() || all.Before(first) {
		first, all = job.admitAt, job.admitAt
	}
	fsync := job.pull.FsyncAt()
	if fsync.IsZero() || fsync.Before(all) {
		// No observed fsync boundary (Finish failed before it, or the clock
		// read is inconsistent): attribute everything after the transfer to
		// the commit, so the stages still sum to the file's measured life.
		fsync = finishDone
	}
	b.f.Engine.Stats.AddPullStageTiming(syncpkg.PullStageTiming{
		WaitFirstBlock: first.Sub(job.admitAt).Nanoseconds(),
		Transfer:       all.Sub(first).Nanoseconds(),
		Fsync:          fsync.Sub(all).Nanoseconds(),
		Commit:         finishDone.Sub(fsync).Nanoseconds(),
	})
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
