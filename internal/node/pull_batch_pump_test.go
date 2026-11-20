package node

import (
	"fmt"
	"net"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/protocol"
	"crosssync/internal/transfer"
)

// Request bookkeeping in one pull batch.
//
// These pin the two halves of the wedge the twin-corpus test reproduces end to
// end: a job admitted while the response-byte budget was full used to be left
// with NO request outstanding and nothing that would ever ask for its first
// block (the batch loop then waits on a socket nobody will write to, with the
// read deadline disarmed), and an abandoned file's drained answers used to keep
// their bytes charged to the budget, starving the files that were still live.

// newDrainedBatchConn returns a batch connection whose frames go nowhere: a
// net.Pipe with a reader that discards every frame, so request bookkeeping can
// be exercised without a peer.
func newDrainedBatchConn(t *testing.T) *transfer.Conn {
	t.Helper()
	cli, srv := net.Pipe()
	t.Cleanup(func() {
		_ = cli.Close()
		_ = srv.Close()
	})
	go func() {
		peer := transfer.NewConn(srv)
		for {
			if _, err := peer.Recv(); err != nil {
				return
			}
		}
	}()
	return transfer.NewConn(cli)
}

// drainedBatch builds a pullBatch over an unread peer link, with one folder and
// the given byte budget, so request bookkeeping can be exercised without a real
// round.
func drainedBatch(t *testing.T, k, budget int) *pullBatch {
	t.Helper()
	return &pullBatch{
		conn:     newDrainedBatchConn(t),
		f:        &Folder{Cfg: config.Folder{ID: "data"}},
		k:        k,
		runBytes: 4 << 20,
		budget:   budget,
		inFlight: make(map[uint64]pullRef),
	}
}

// runsOf gives one job n runs of size bytes (only the byte accounting matters
// here; the block lists belong to the pull itself).
func runsOf(job *pullJob, n, size int) {
	for r := 0; r < n; r++ {
		job.runs = append(job.runs, pullRun{offset: int64(r * size), size: size})
	}
	job.total = n
}

// TestPullBatchPumpNeverStarvesAFile: three files wanting 4 MiB runs against a
// 16 MiB budget. The budget holds four runs, so every file must get at least
// one — that is exactly the state the wedge left behind (a file admitted with
// nothing outstanding).
func TestPullBatchPumpNeverStarvesAFile(t *testing.T) {
	const run = 4 << 20
	b := drainedBatch(t, 4, 16<<20)
	for i := 0; i < 3; i++ {
		job := &pullJob{name: fmt.Sprintf("tex/twin-%d.dds", i)}
		runsOf(job, 3, run)
		b.jobs = append(b.jobs, job)
	}

	if err := b.pump(); err != nil {
		t.Fatalf("pump: %v", err)
	}
	for _, job := range b.jobs {
		if job.sent == 0 {
			t.Fatalf("%s was admitted with no request issued (budget %d, %d bytes already promised)",
				job.name, b.budget, b.inflightBytes)
		}
	}
	if b.inflightBytes > b.budget {
		t.Fatalf("%d bytes promised against a %d byte budget", b.inflightBytes, b.budget)
	}
	if got, want := b.inflightBytes, 4*run; got != want {
		t.Fatalf("promised %d bytes, want %d (the budget must be filled, not abandoned)", got, want)
	}
	// Four runs outstanding over three files: the fair share is 2/1/1 in
	// admission order, never 3/1/0.
	counts := []int{b.jobs[0].sent, b.jobs[1].sent, b.jobs[2].sent}
	if counts[2] == 0 || counts[0] > 2 {
		t.Fatalf("run shares = %v, want every file served and no file holding the whole budget", counts)
	}
}

// TestPullBatchPumpRespectsThePerFileWindow: whatever the budget allows, ONE
// file never has more than pullWindowSize runs on the wire.
func TestPullBatchPumpRespectsThePerFileWindow(t *testing.T) {
	const run = 1 << 20
	b := drainedBatch(t, 1, 1<<30)
	job := &pullJob{name: "tex/big.dds"}
	runsOf(job, pullWindowSize*3, run)
	b.jobs = append(b.jobs, job)

	if err := b.pump(); err != nil {
		t.Fatalf("pump: %v", err)
	}
	if job.sent != pullWindowSize {
		t.Fatalf("%d runs on the wire, want the window %d", job.sent, pullWindowSize)
	}
	// An answered run frees exactly one slot for one more run (this is the
	// accounting handle does on a live job), never a second window's worth.
	delete(b.inFlight, 99)
	job.answered++
	b.inflightBytes -= run
	if err := b.pump(); err != nil {
		t.Fatalf("pump after an answer: %v", err)
	}
	if job.sent != pullWindowSize+1 {
		t.Fatalf("%d runs on the wire after one answer, want %d", job.sent, pullWindowSize+1)
	}
}

// TestPullBatchDeadJobResponseFreesBudget: the answer that arrives for a file
// that was already abandoned must give its bytes back. Otherwise the rest of
// the batch is short of room for the whole round — and with the budget held by
// phantom bytes, no file can be asked for anything.
func TestPullBatchDeadJobResponseFreesBudget(t *testing.T) {
	const run = 4 << 20
	b := &pullBatch{
		k:             4,
		budget:        16 << 20,
		inflightBytes: run,
		inFlight:      make(map[uint64]pullRef),
	}
	gone := &pullJob{name: "tex/gone.dds", dead: true}
	runsOf(gone, 1, run)
	b.inFlight[7] = pullRef{job: gone, run: 0}

	if err := b.handle(&protocol.Message{Type: protocol.MsgResponse, Response: &protocol.Response{
		ID: 7, Code: protocol.RespNoSuchFile,
	}}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(b.inFlight) != 0 {
		t.Fatalf("%d request(s) still marked outstanding after the answer", len(b.inFlight))
	}
	if b.inflightBytes != 0 {
		t.Fatalf("inflightBytes = %d after a drained answer, want 0 (the budget would stay charged for the rest of the round)", b.inflightBytes)
	}
}
