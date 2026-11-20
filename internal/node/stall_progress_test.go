package node

import (
	"bytes"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/hash"
	"crosssync/internal/index"
	"crosssync/internal/protocol"
	"crosssync/internal/transfer"
	"crosssync/internal/version"
)

// The tests in this file pin the progress-based stall watchdog.
//
// The bug they cover: the stall deadline used to be re-armed on every frame
// (per loop iteration in pullBatch.run, per Recv in serveUntil), while the
// keepalive goroutine writes a MsgPing every 20 s independently of the blocked
// code path. Every ping returned from Recv, the loop re-armed its whole budget,
// and no timeout could ever fire while the peer was merely QUIET rather than
// dead. Measured live: a round running:true for 74 minutes with bytes_down
// frozen, inflight_files 0 on both nodes, error:"" and no open events; the other
// folder syncing to that peer had not run for 73 of those minutes because the
// per-peer session slot was still held.

// pingOnlyPeer accepts one connection and answers NOTHING: it only emits
// keepalive pings, which is exactly how a wedged-but-reachable peer looks on the
// wire (its keepalive goroutine keeps writing while its serving loop is stuck in
// a read). sent counts the pings it wrote.
func pingOnlyPeer(t *testing.T, every time.Duration, sent *atomic.Int64) string {
	t.Helper()
	srv, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	go func() {
		c, err := srv.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		pc := transfer.NewConn(c)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				if _, err := pc.Recv(); err != nil {
					return
				}
			}
		}()
		tick := time.NewTicker(every)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				if err := pc.Send(&protocol.Message{Type: protocol.MsgPing}); err != nil {
					return
				}
				sent.Add(1)
			}
		}
	}()
	return srv.Addr().String()
}

// stalledPullNode builds the puller side plus the peer's view of ONE block-sized
// file ("stalled.bin"), so a round really has something outstanding to wait for.
func stalledPullNode(t *testing.T) (*Node, []byte, uint64) {
	t.Helper()
	b := chunkTestNode(t, 2, "b", t.TempDir(), t.TempDir())
	data := pattern(101, 256<<10, rand.New(rand.NewSource(101)))
	blocks, err := hash.BlockHashes(bytes.NewReader(data), hash.MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	const peerID = 9
	b.Folders["data"].Engine.SetPeerIndex(peerID, []*index.FileInfo{{
		Name:      "stalled.bin",
		Size:      int64(len(data)),
		Type:      index.TypeFile,
		Version:   version.New().Bump(peerID),
		BlockSize: hash.MinBlockSize,
		Blocks:    blocks,
	}}, false)
	return b, data, peerID
}

// stalledPullTimeout shrinks peerStallTimeout for one test and restores it.
func stalledPullTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := peerStallTimeout
	peerStallTimeout = d
	t.Cleanup(func() { peerStallTimeout = old })
}

// pullAgainstPingPeer drives one pull round toward a peer that only pings and
// returns the round error. It never lets the test itself hang: a round that does
// not abort within the bound fails with the ping count, which is the signature
// of the per-frame deadline bug.
func pullAgainstPingPeer(t *testing.T, b *Node, peerID uint64, bound time.Duration) (int64, error) {
	t.Helper()
	var pings atomic.Int64
	addr := pingOnlyPeer(t, 20*time.Millisecond, &pings)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn := transfer.NewConn(c)
	conn.PeerID = peerID
	// Mirror a real session: SyncOnce/Serve arm the coarse session stall. It is
	// per FRAME, so on its own it is re-armed by every ping too — the batch must
	// take over with its own progress deadline (and put this back).
	conn.SetStall(sessionStall)
	t.Cleanup(func() { _ = c.Close() })

	done := make(chan error, 1)
	go func() {
		done <- b.pullFiles(conn, b.Folders["data"], []string{"stalled.bin"})
	}()
	select {
	case err := <-done:
		return pings.Load(), err
	case <-time.After(bound):
		t.Fatalf("the pull round never aborted after %s while the peer sent %d keepalive pings: the stall deadline is being re-armed by pings (it must be anchored on progress)",
			bound, pings.Load())
		return pings.Load(), nil
	}
}

// TestPullStallPingsDoNotExtendBudget: a peer that answers only PINGS and never
// the block request must abort the round within the (test-shortened) timeout,
// name the file it is stuck on, and report enough pings to prove that keepalive
// traffic was flowing the whole time.
func TestPullStallPingsDoNotExtendBudget(t *testing.T) {
	b, _, peerID := stalledPullNode(t)
	const window = 400 * time.Millisecond
	stalledPullTimeout(t, window)

	start := time.Now()
	pings, err := pullAgainstPingPeer(t, b, peerID, 8*time.Second)
	waited := time.Since(start)

	if err == nil {
		t.Fatalf("pulling from a peer that only pings returned no error after %s", waited)
	}
	if !strings.Contains(err.Error(), "sent nothing") {
		t.Fatalf("error = %v, want the no-progress timeout text", err)
	}
	if !strings.Contains(err.Error(), "stalled.bin") {
		t.Fatalf("error = %v, want it to name the file with the outstanding request", err)
	}
	if pings < 5 {
		t.Fatalf("the peer sent only %d pings; the test did not reproduce the keepalive-on-a-quiet-link case", pings)
	}
	// Pings every 20 ms: before the fix each one bought another full window, so
	// the round never returned at all. A few windows is the most a progress
	// deadline may cost.
	if waited > 3*time.Second {
		t.Fatalf("round took %s with a %s window and %d pings flowing: pings are still extending the budget",
			waited, window, pings)
	}
}

// TestPullStallReportsUnsyncedEvent: the stall must be diagnosable from the
// event store alone — one visible, retrying unsynced event for the file that was
// left hanging, with a reason that says what to look at.
func TestPullStallReportsUnsyncedEvent(t *testing.T) {
	b, _, peerID := stalledPullNode(t)
	stalledPullTimeout(t, 400*time.Millisecond)

	_, err := pullAgainstPingPeer(t, b, peerID, 8*time.Second)
	if err == nil {
		t.Fatal("pulling from a peer that only pings returned no error")
	}

	evs, qerr := b.Events.Query(events.Filter{
		Folder: "data", Path: "stalled.bin", Category: events.CatUnsynced, OpenOnly: true,
	})
	if qerr != nil {
		t.Fatal(qerr)
	}
	if len(evs) != 1 {
		t.Fatalf("open unsynced events for stalled.bin = %d, want exactly 1 (the round must report the stuck file)", len(evs))
	}
	if !strings.Contains(evs[0].Reason, "peer may be stalled") {
		t.Fatalf("event reason = %q, want an actionable stall reason", evs[0].Reason)
	}
}

// TestServeUntilAdmissionRefusesInsteadOfBlocking: with every serve worker stuck
// (the hook parks the only worker), the read loop must still consume frames and
// answer a new request with a retryable code, in bounded time, instead of
// blocking in `slots <- struct{}{}` and freezing the whole session.
func TestServeUntilAdmissionRefusesInsteadOfBlocking(t *testing.T) {
	oldWorkers, oldAdmission := serveWorkers, serveAdmissionTimeout
	serveWorkers, serveAdmissionTimeout = 1, 150*time.Millisecond
	t.Cleanup(func() {
		serveWorkers, serveAdmissionTimeout = oldWorkers, oldAdmission
	})

	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	body := pattern(7, 300<<10, rand.New(rand.NewSource(7)))
	writePattern(t, root, "one.bin", body)
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	serveWorkerHook = func(delta int) {
		if delta > 0 {
			entered <- struct{}{}
			<-release
		}
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		serveWorkerHook = nil
	})

	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	served := make(chan error, 1)
	go func() {
		c, err := transfer.Accept(ln)
		if err != nil {
			served <- err
			return
		}
		defer func() { _ = c.Close() }()
		served <- n.serveUntil(c, protocol.MsgClose)
	}()
	cli, err := transfer.Dial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	// Request 1 takes the only worker and parks it in the hook.
	if err := cli.Send(&protocol.Message{Type: protocol.MsgRequest, Request: &protocol.Request{
		ID: 1, Folder: "data", Name: "one.bin", Offset: 0, Size: len(body),
	}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first request never reached a worker")
	}

	// Request 2 arrives while the pool is full: the read loop must answer it
	// itself rather than wait for a worker that cannot free up.
	start := time.Now()
	if err := cli.Send(&protocol.Message{Type: protocol.MsgRequest, Request: &protocol.Request{
		ID: 2, Folder: "data", Name: "one.bin", Offset: 0, Size: len(body),
	}}); err != nil {
		t.Fatal(err)
	}
	_ = cli.SetReadDeadline(time.Now().Add(5 * time.Second))
	m, err := cli.Recv()
	if err != nil {
		t.Fatalf("the serve loop did not answer a request while its pool was stuck: %v", err)
	}
	waited := time.Since(start)
	if m.Type != protocol.MsgResponse || m.Response == nil {
		t.Fatalf("got %v, want a block response", m.Type)
	}
	if m.Response.ID != 2 {
		t.Fatalf("refusal carries id %d, want 2 (the request the loop could not admit)", m.Response.ID)
	}
	if m.Response.Code != protocol.RespGeneric {
		t.Fatalf("refusal code = %d, want RespGeneric (retryable: the puller must not treat the peer as offline)", m.Response.Code)
	}
	if waited > 3*time.Second {
		t.Fatalf("refusal took %s, want it bounded by the admission timeout", waited)
	}

	// The stuck worker still answers its own request once it is released, and
	// the session ends normally.
	close(release)
	_ = cli.SetReadDeadline(time.Now().Add(10 * time.Second))
	m, err = cli.Recv()
	if err != nil {
		t.Fatalf("the released worker never answered: %v", err)
	}
	if m.Response == nil || m.Response.ID != 1 || m.Response.Code != protocol.RespNoError {
		t.Fatalf("got %+v, want the first request served with data", m.Response)
	}
	if err := cli.Send(&protocol.Message{Type: protocol.MsgClose}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serveUntil returned %v after the refusal path", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveUntil did not return after the terminal frame")
	}
}

// TestServeUntilPingsDoNotExtendBudget pins the same progress rule on the
// SERVING side: a peer that has a round open and then only pings (its puller is
// wedged, so it asks for nothing and answers nothing) must be let go after the
// stall window, not kept alive frame by frame forever.
func TestServeUntilPingsDoNotExtendBudget(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	// Only an ACTIVE round arms the serve deadline; an idle session waits.
	n.beginSync(2)
	defer n.endSync(nil)
	stalledPullTimeout(t, 400*time.Millisecond)

	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	served := make(chan error, 1)
	go func() {
		c, err := transfer.Accept(ln)
		if err != nil {
			served <- err
			return
		}
		defer func() { _ = c.Close() }()
		served <- n.serveUntil(c, protocol.MsgClose)
	}()
	cli, err := transfer.Dial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	// A real session has the coarse stall armed; the serve loop must replace it
	// with its own progress deadline for the duration.
	cli.SetStall(sessionStall)

	var pings atomic.Int64
	var sendErr atomic.Bool
	go func() {
		for {
			if err := cli.Send(&protocol.Message{Type: protocol.MsgPing}); err != nil {
				sendErr.Store(true)
				return
			}
			pings.Add(1)
			time.Sleep(20 * time.Millisecond)
		}
	}()

	select {
	case err := <-served:
		if err == nil {
			t.Fatal("serveUntil returned nil, want the no-progress timeout")
		}
		if !strings.Contains(err.Error(), "sent nothing") {
			t.Fatalf("error = %v, want the no-progress timeout text", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatalf("serveUntil never aborted after %d keepalive pings: its deadline is re-armed per frame", pings.Load())
	}
	if pings.Load() < 5 {
		t.Fatalf("the peer only sent %d pings; the keepalive case was not reproduced", pings.Load())
	}
}
