package node

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/protocol"
	"crosssync/internal/transfer"
)

// ---------------------------------------------------------------------------
// A link with latency.
//
// Loopback RTT is microseconds, so a pull of 200 tiny files completes before
// the per-file round trip can show up at all — the concurrency win is
// invisible and the measurement would prove nothing. Every test in this file
// therefore runs the round over a real TCP pair with a delay line in the
// middle: each frame is forwarded d later, in order, so the link behaves like
// the 18 ms Tailscale hop between the two live nodes while keeping TCP's
// buffering (a delay BATCH must not serialize a pipelined window into one
// frame per d).
// ---------------------------------------------------------------------------

// wireLog records the block requests a link carried, in the order they
// crossed it. Test-only: it is how the integrity test proves that requests
// for DIFFERENT files really were outstanding at the same time (a pull that
// silently degenerated to one file at a time would show a run of one name).
type wireLog struct {
	mu    sync.Mutex
	names []string
	sizes []int
}

// maxRequestSize reports the largest single block request seen (0 = none).
func (w *wireLog) maxRequestSize() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	max := 0
	for _, s := range w.sizes {
		if s > max {
			max = s
		}
	}
	return max
}

// requests reports how many block requests the link carried.
func (w *wireLog) requests() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.names)
}

func (w *wireLog) add(name string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.names = append(w.names, name)
	w.mu.Unlock()
}

// addSize records one request's byte size (nil-safe).
func (w *wireLog) addSize(size int) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.sizes = append(w.sizes, size)
	w.mu.Unlock()
}

// transitions counts how often the requested file changes between consecutive
// block requests: 0 means strictly one file at a time.
func (w *wireLog) transitions() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for i := 1; i < len(w.names); i++ {
		if w.names[i] != w.names[i-1] {
			n++
		}
	}
	return n
}

func (w *wireLog) distinct() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	seen := map[string]bool{}
	for _, n := range w.names {
		seen[n] = true
	}
	return len(seen)
}

// delayCopy forwards every frame from src to dst d later, preserving order.
// Frames are queued as they are read (so a pipelined burst is NOT throttled to
// one frame per d) and written when their arrival time comes.
func delayCopy(src, dst *transfer.Conn, d time.Duration, log *wireLog) {
	type frame struct {
		m  *protocol.Message
		at time.Time
	}
	q := make(chan frame, 8192)
	go func() {
		defer close(q)
		for {
			m, err := src.Recv()
			if err != nil {
				return
			}
			if m.Type == protocol.MsgRequest && m.Request != nil {
				log.add(m.Request.Name)
				log.addSize(m.Request.Size)
			}
			q <- frame{m, time.Now().Add(d)}
		}
	}()
	for f := range q {
		if w := time.Until(f.at); w > 0 {
			time.Sleep(w)
		}
		if err := dst.Send(f.m); err != nil {
			// Drain so the reader can finish instead of leaking a goroutine.
			for range q {
			}
			return
		}
	}
}

// roundRun is a sync round in flight: the test can act on the link (pause the
// folder, cut the connection) while b is pulling, then wait for the round.
type roundRun struct {
	client *transfer.Conn
	done   chan error
	served chan error
	bridge chan struct{}

	// aux holds the two bridge sockets. Cut closes them, which is exactly what
	// a network partition looks like to both daemons: their own sockets stay
	// open, the stream just dies.
	auxMu sync.Mutex
	aux   []*transfer.Conn
	cut   bool
}

// track registers a bridge socket (and closes it at once if the link was
// already cut).
func (r *roundRun) track(c *transfer.Conn) {
	r.auxMu.Lock()
	defer r.auxMu.Unlock()
	if r.cut {
		_ = c.Close()
		return
	}
	r.aux = append(r.aux, c)
}

// Cut kills the link mid-round.
func (r *roundRun) Cut() {
	r.auxMu.Lock()
	r.cut = true
	conns := append([]*transfer.Conn(nil), r.aux...)
	r.auxMu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// startLatencyRound wires the link and starts the round without waiting.
func startLatencyRound(t *testing.T, a, b *Node, d time.Duration, log *wireLog) *roundRun {
	t.Helper()
	srv, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	r := &roundRun{done: make(chan error, 1), served: make(chan error, 1), bridge: make(chan struct{})}
	go func() {
		conn, err := transfer.Accept(srv)
		if err != nil {
			r.served <- err
			return
		}
		defer func() { _ = conn.Close() }()
		r.served <- a.Serve(conn)
	}()

	front, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = front.Close() })
	go func() {
		defer close(r.bridge)
		cli, err := transfer.Accept(front)
		if err != nil {
			return
		}
		defer func() { _ = cli.Close() }()
		back, err := transfer.Dial(srv.Addr().String())
		if err != nil {
			return
		}
		defer func() { _ = back.Close() }()
		r.track(cli)
		r.track(back)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); delayCopy(cli, back, d, log) }()
		go func() { defer wg.Done(); delayCopy(back, cli, d, log) }()
		wg.Wait()
	}()

	client, err := transfer.Dial(front.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	r.client = client
	go func() {
		_, err := b.SyncOnce(client, a.ID)
		r.done <- err
	}()
	return r
}

// Wait returns the initiator's round result once both sides have finished.
func (r *roundRun) Wait(t *testing.T) error {
	t.Helper()
	var roundErr error
	select {
	case roundErr = <-r.done:
	case <-time.After(60 * time.Second):
		t.Fatalf("round did not finish")
	}
	_ = r.client.Close()
	select {
	case <-r.bridge:
	case <-time.After(20 * time.Second):
		t.Fatalf("latency bridge did not finish")
	}
	select {
	case err := <-r.served:
		if roundErr == nil && err != nil {
			return fmt.Errorf("server side: %w", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("server side did not finish")
	}
	return roundErr
}

// latencyRound runs one full sync round with b as the initiator (puller) and a
// as the responder (server), over a link with d of one-way delay.
func latencyRound(t *testing.T, a, b *Node, d time.Duration, log *wireLog) error {
	t.Helper()
	return startLatencyRound(t, a, b, d, log).Wait(t)
}

// pattern returns size bytes that are unique per (file tag, block index): a
// block written into the wrong file — or into the wrong offset of the same
// file — can never compare equal to the expected bytes.
func pattern(tag byte, size int, rng *rand.Rand) []byte {
	b := make([]byte, size)
	fill := make([]byte, 256)
	rng.Read(fill)
	for i := range b {
		b[i] = fill[i%len(fill)] ^ tag ^ byte(i>>9)
	}
	return b
}

func writePattern(t *testing.T, root, rel string, data []byte) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(p, time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC), time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC))
}

// pullEventsFor counts the applied "pulled from peer" events on n: one per
// file actually transferred (a file pulled twice would show two).
func pullEventsFor(t *testing.T, n *Node) int {
	t.Helper()
	evs, err := n.Events.Query(events.Filter{Folder: "data", Category: events.CatApplied})
	if err != nil {
		t.Fatal(err)
	}
	c := 0
	for _, e := range evs {
		if strings.Contains(e.Reason, "pulled from peer") {
			c++
		}
	}
	return c
}

// regularFiles counts the regular files directly under root (these corpora are
// flat). Staging temps live in the reserved .sfx-* namespace, so a partial pull
// is never counted as an arrived file.
func regularFiles(t *testing.T, root string) int {
	t.Helper()
	ents, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	c := 0
	for _, e := range ents {
		if e.Type().IsRegular() {
			c++
		}
	}
	return c
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}
