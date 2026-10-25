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
}

func (w *wireLog) add(name string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.names = append(w.names, name)
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

// latencyRound runs one full sync round with b as the initiator (puller) and a
// as the responder (server), over a link with d of one-way delay.
func latencyRound(t *testing.T, a, b *Node, d time.Duration, log *wireLog) error {
	t.Helper()
	srv, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	served := make(chan error, 1)
	go func() {
		conn, err := transfer.Accept(srv)
		if err != nil {
			served <- err
			return
		}
		defer func() { _ = conn.Close() }()
		served <- a.Serve(conn)
	}()

	front, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = front.Close() })
	bridgeDone := make(chan struct{})
	go func() {
		defer close(bridgeDone)
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
	_, roundErr := b.SyncOnce(client, a.ID)
	_ = client.Close()
	select {
	case <-bridgeDone:
	case <-time.After(30 * time.Second):
		return fmt.Errorf("latency bridge did not finish")
	}
	select {
	case err := <-served:
		if roundErr == nil && err != nil {
			return fmt.Errorf("server side: %w", err)
		}
	case <-time.After(30 * time.Second):
		return fmt.Errorf("server side did not finish")
	}
	return roundErr
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
