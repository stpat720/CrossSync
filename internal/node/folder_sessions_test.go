package node

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/transfer"
)

// ---------------------------------------------------------------------------
// Folder sessions.
//
// The operator rule: "Separate folders use separate syncs. TestSync should NEVER
// be waiting for another folder to finish before it starts or gives information."
// The session slot is keyed by (peer, folder), so each folder gets its own
// session, connection and transfer pipeline, and what stays exclusive is the
// FOLDER (never two rounds on one folder with one peer, which is what kept two
// writers off one .part staging temp).
// ---------------------------------------------------------------------------

// folderSessionsNode builds a Node with SEVERAL folders (id -> root) and an
// optional peer list. SettleScans 1 disables the stability gate, as in the rest
// of the harness: these tests write a file and expect the next scan to index it.
func folderSessionsNode(t *testing.T, id uint64, name string, roots map[string]string, peers []config.Peer) *Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: id, Name: name},
		MetaDir: t.TempDir(),
		Peers:   peers,
	}
	ids := make([]string, 0, len(roots))
	for fid := range roots {
		ids = append(ids, fid)
	}
	sort.Strings(ids)
	for _, fid := range ids {
		cfg.Folders = append(cfg.Folders, config.Folder{ID: fid, Path: roots[fid], SettleScans: 1})
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// scanAll indexes every folder of a node, so they are all advertised (a folder
// still in its first index pass is deliberately invisible to peers).
func scanAll(t *testing.T, n *Node) {
	t.Helper()
	for fid := range n.Folders {
		if _, err := n.ScanFolder(fid); err != nil {
			t.Fatalf("scan %s: %v", fid, err)
		}
	}
}

// waitIdle waits until no sync session is in flight on n. SyncWithPeer returns as
// soon as its sessions have a VERDICT (the peer answered) and lets the transfers
// finish in the background — a folder's long transfer must not hold the next
// scheduling decision — so a test that wants the outcome has to wait for the
// sessions themselves.
func waitIdle(t *testing.T, n *Node) {
	t.Helper()
	waitFor(t, 180*time.Second, "the peer's sync sessions to finish", func() bool {
		return n.SyncSessions() == 0
	})
}

// latencyProxy fronts a listener with a d-delayed link: every accepted
// connection is bridged to target through delayCopy, so a round over it behaves
// like the Tailscale hop between the two live nodes instead of loopback. Each
// session opens its OWN connection, so two folders really do get two pipelines —
// which is the point being measured.
func latencyProxy(t *testing.T, target string, d time.Duration) string {
	t.Helper()
	front, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = front.Close() })
	go func() {
		for {
			cli, err := transfer.Accept(front)
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = cli.Close() }()
				back, err := transfer.Dial(target)
				if err != nil {
					return
				}
				defer func() { _ = back.Close() }()
				var wg sync.WaitGroup
				wg.Add(2)
				go func() { defer wg.Done(); delayCopy(cli, back, d, nil) }()
				go func() { defer wg.Done(); delayCopy(back, cli, d, nil) }()
				wg.Wait()
			}()
		}
	}()
	return front.Addr().String()
}

// serveNode starts a node's accept loop on 127.0.0.1:0 and returns the address a
// peer should dial (the latency proxy in front of it, when d > 0).
func serveNode(t *testing.T, n *Node, d time.Duration) string {
	t.Helper()
	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = n.Run(ln) }()
	if d <= 0 {
		return ln.Addr().String()
	}
	return latencyProxy(t, ln.Addr().String(), d)
}

// TestSeparateFoldersSyncConcurrently is the operator rule, measured: a folder
// whose transfer is slow must not delay another folder's completion AT ALL.
//
// b syncs two folders from a over a link with 25 ms of one-way delay. "bulk"
// holds 500 small files, which on that link is several seconds of round trips;
// "fast" holds one file. The fast folder's file must land while bulk is still
// moving. Under the old one-session-per-peer rule it could not: both folders were
// pulled inside one round, and a folder's turn is bounded at 64 MiB / 30 s, so
// the fast folder's single file waited for bulk's whole transfer to be admitted.
func TestSeparateFoldersSyncConcurrently(t *testing.T) {
	const (
		bulkFiles = 500
		oneWay    = 25 * time.Millisecond
	)
	rootA := map[string]string{"bulk": t.TempDir(), "fast": t.TempDir()}
	rootB := map[string]string{"bulk": t.TempDir(), "fast": t.TempDir()}
	a := folderSessionsNode(t, 1, "a", rootA, nil)
	for i := 0; i < bulkFiles; i++ {
		chunkWrite(t, rootA["bulk"], fmt.Sprintf("b-%03d.txt", i), fmt.Sprintf("bulk %d", i))
	}
	chunkWrite(t, rootA["fast"], "quick.txt", "quick")
	scanAll(t, a)

	b := folderSessionsNode(t, 2, "b", rootB, nil)
	addr := serveNode(t, a, oneWay)
	b.Cfg.Peers = []config.Peer{{ID: 1, Name: "a", Addresses: []string{addr}}}
	scanAll(t, b)

	done := make(chan error, 1)
	go func() {
		_, err := b.SyncWithPeer(1)
		done <- err
	}()

	start := time.Now()
	waitFor(t, 4*time.Second, "the fast folder's file to arrive while the slow folder is still transferring", func() bool {
		return regularFiles(t, rootB["fast"]) == 1
	})
	fastDone := time.Since(start)
	if got := regularFiles(t, rootB["bulk"]); got >= bulkFiles {
		t.Fatalf("bulk had already finished (%d/%d files) when the fast folder completed: the fast folder was never tested against a slow one", got, bulkFiles)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sync with a: %v", err)
		}
	case <-time.After(180 * time.Second):
		t.Fatal("the scheduling call never returned")
	}
	waitIdle(t, b)
	if got := regularFiles(t, rootB["bulk"]); got != bulkFiles {
		t.Fatalf("bulk arrived with %d/%d files", got, bulkFiles)
	}
	if got := readFile(t, rootB["fast"], "quick.txt"); got != "quick" {
		t.Fatalf("fast file content = %q, want %q", got, "quick")
	}
	t.Logf("fast folder completed after %s while bulk was still transferring", fastDone)

	// A finished session frees its slot: both folders are claimable again, and
	// neither was left holding a quota that would block the next round.
	for _, fid := range []string{"bulk", "fast"} {
		release, ok := b.acquireFolderSession(1, fid, 0)
		if !ok {
			t.Fatalf("folder %s: the session slot was not freed when its session ended", fid)
		}
		release()
	}
}

// TestInboundAndOutboundOnOneFolderAreExclusive is the safety half of the change:
// the slot is per folder, so inbound and outbound rounds on the SAME folder still
// cannot overlap (two writers staging one .part temp was the confirmed chtimes
// churn), while a DIFFERENT folder is not blocked by it.
func TestInboundAndOutboundOnOneFolderAreExclusive(t *testing.T) {
	rootA := map[string]string{"data": t.TempDir(), "other": t.TempDir()}
	rootB := map[string]string{"data": t.TempDir(), "other": t.TempDir()}
	a := folderSessionsNode(t, 1, "a", rootA, nil)
	b := folderSessionsNode(t, 2, "b", rootB, nil)
	chunkWrite(t, rootA["data"], "in-data.txt", "data payload")
	chunkWrite(t, rootA["other"], "in-other.txt", "other payload")
	scanAll(t, a)
	scanAll(t, b)

	// Hold the slot exactly as a running outbound round on "data" would.
	release, ok := a.acquireFolderSession(b.ID, "data", 0)
	if !ok {
		t.Fatal("could not claim (b, data) for the test")
	}
	defer release()

	// An inbound session for that folder must be declined with the busy Close,
	// which the initiator classifies as reachable-and-busy — not a failure.
	err := folderPipe(t, a, "data", func(conn *transfer.Conn) error {
		_, err := b.SyncOnceFolder(conn, a.ID, "data")
		return err
	})
	if !errors.Is(err, ErrPeerBusy) {
		t.Fatalf("a session on the held folder = %v, want ErrPeerBusy (the peer is up and busy)", err)
	}

	// A different folder is a different session and must go through untouched.
	err = folderPipe(t, a, "other", func(conn *transfer.Conn) error {
		_, err := b.SyncOnceFolder(conn, a.ID, "other")
		return err
	})
	if err != nil {
		t.Fatalf("a session on a DIFFERENT folder was blocked by the held one: %v", err)
	}
	if got := readFile(t, rootB["other"], "in-other.txt"); got != "other payload" {
		t.Fatalf("other folder did not transfer: %q", got)
	}
	if got := regularFiles(t, rootB["data"]); got != 0 {
		t.Fatalf("the held folder transferred %d file(s) in a declined session", got)
	}
}

// folderPipe runs one session between responder (a) and initiator (b) over a
// fresh pair of connections, with the initiator side supplied by the caller so it
// can be a folder-scoped session.
func folderPipe(t *testing.T, responder *Node, folder string, run func(*transfer.Conn) error) error {
	t.Helper()
	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()
	served := make(chan error, 1)
	go func() {
		conn, err := transfer.Accept(ln)
		if err != nil {
			served <- err
			return
		}
		defer func() { _ = conn.Close() }()
		served <- responder.Serve(conn)
	}()
	conn, err := transfer.Dial(ln.Addr().String())
	if err != nil {
		return err
	}
	runErr := run(conn)
	_ = conn.Close()
	select {
	case err := <-served:
		if runErr != nil {
			return runErr
		}
		return err
	case <-time.After(60 * time.Second):
		return fmt.Errorf("session on %s timed out", folder)
	}
}

// TestFolderSessionCapIsEnforced pins the cap: N live sessions per peer, never
// N+1, and a finished session hands its slot to the next folder.
func TestFolderSessionCapIsEnforced(t *testing.T) {
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())

	// Clamping first: 0 and negatives cannot be honoured from the API (the config
	// file keeps 0 = "default"), so they mean the minimum, 1; anything above the
	// hard cap is the hard cap.
	for _, tc := range []struct {
		in, want int
	}{
		{0, 1}, {-3, 1}, {1, 1}, {4, 4}, {config.MaxFolderSessionsPerPeer, config.MaxFolderSessionsPerPeer},
		{config.MaxFolderSessionsPerPeer + 7, config.MaxFolderSessionsPerPeer},
	} {
		if got := n.SetFolderSessions(tc.in); got != tc.want {
			t.Fatalf("SetFolderSessions(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
	// The default when nothing is set.
	if got := (&Node{Cfg: &config.Config{}}).FolderSessions(); got != config.DefaultFolderSessionsPerPeer {
		t.Fatalf("unset cap = %d, want the default %d", got, config.DefaultFolderSessionsPerPeer)
	}

	// cap = 4 allows four concurrent folder sessions with one peer, not five.
	n.SetFolderSessions(4)
	var held []func()
	for i := 0; i < 4; i++ {
		release, ok := n.acquireFolderSession(9, fmt.Sprintf("f%d", i), 0)
		if !ok {
			t.Fatalf("session %d refused under a cap of 4", i)
		}
		held = append(held, release)
	}
	if _, ok := n.acquireFolderSession(9, "f4", 0); ok {
		t.Fatal("a fifth folder session was allowed under a cap of 4")
	}
	// The cap is per PEER: another peer is unaffected.
	if release, ok := n.acquireFolderSession(10, "f4", 0); !ok {
		t.Fatal("the cap for peer 9 blocked peer 10")
	} else {
		release()
	}
	// Never two sessions on one folder, even with the cap free.
	held[0]()
	if _, ok := n.acquireFolderSession(9, "f1", 0); ok {
		t.Fatal("a second session on the same folder was allowed")
	}
	// The freed slot goes to the next folder instead of being lost.
	release4, ok := n.acquireFolderSession(9, "f4", 0)
	if !ok {
		t.Fatal("a finished session did not free its slot for the next folder")
	}
	release4()
	for _, release := range held[1:] {
		release()
	}

	// A waiter on the cap wakes when a session is released (not on a poll), and
	// gives up when nothing is freed.
	n.SetFolderSessions(1)
	hold, ok := n.acquireFolderSession(9, "w0", 0)
	if !ok {
		t.Fatal("could not claim the single slot for the wait test")
	}
	if _, ok := n.acquireFolderSession(9, "w1", 60*time.Millisecond); ok {
		t.Fatal("a bounded wait took a slot that was beyond the cap")
	}
	waited := make(chan bool, 1)
	go func() {
		release, ok := n.acquireFolderSession(9, "w1", 5*time.Second)
		if ok {
			release()
		}
		waited <- ok
	}()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	hold()
	select {
	case ok := <-waited:
		if !ok {
			t.Fatal("the waiter did not get the slot the finished session released")
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("the waiter took %s to notice a released slot, want an immediate wake", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the waiter never woke after a session was released")
	}
}

// TestFolderSessionCapOneIsSerial is the cap=1 fallback: one folder at a time per
// peer, exactly the old serial shape, and the folder that waited goes first next
// time (least recently synced first) instead of never being picked.
func TestFolderSessionCapOneIsSerial(t *testing.T) {
	rootA := map[string]string{"alpha": t.TempDir(), "beta": t.TempDir()}
	rootB := map[string]string{"alpha": t.TempDir(), "beta": t.TempDir()}
	a := folderSessionsNode(t, 1, "a", rootA, nil)
	chunkWrite(t, rootA["alpha"], "a.txt", "alpha payload")
	chunkWrite(t, rootA["beta"], "b.txt", "beta payload")
	scanAll(t, a)
	b := folderSessionsNode(t, 2, "b", rootB, nil)
	scanAll(t, b)
	addr := serveNode(t, a, 0)
	b.Cfg.Peers = []config.Peer{{ID: 1, Name: "a", Addresses: []string{addr}}}

	b.SetFolderSessions(1)
	if _, err := b.SyncWithPeer(1); err != nil {
		t.Fatalf("first round: %v", err)
	}
	waitIdle(t, b)
	got := []int{regularFiles(t, rootB["alpha"]), regularFiles(t, rootB["beta"])}
	if got[0]+got[1] != 1 {
		t.Fatalf("cap=1 synced %d folders in one round (alpha=%d beta=%d), want exactly one", got[0]+got[1], got[0], got[1])
	}
	// The other folder gets the next turn: a cap of 1 must not starve it.
	if _, err := b.SyncWithPeer(1); err != nil {
		t.Fatalf("second round: %v", err)
	}
	waitIdle(t, b)
	alpha, beta := regularFiles(t, rootB["alpha"]), regularFiles(t, rootB["beta"])
	if alpha != 1 || beta != 1 {
		t.Fatalf("cap=1 ran the same folder twice instead of the one that waited (alpha=%d beta=%d)", alpha, beta)
	}
	// Idle folders still get a (cheap, empty) session, not an error.
	if _, err := b.SyncWithPeer(1); err != nil {
		t.Fatalf("an idle round must not fail: %v", err)
	}
}
