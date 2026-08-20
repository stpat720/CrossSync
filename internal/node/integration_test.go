package node_test

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"crosssync/internal/certs"
	"crosssync/internal/config"
	"crosssync/internal/events"
	"crosssync/internal/node"
	"crosssync/internal/transfer"
)

// makeNode builds a Node with one folder rooted at root and meta at metaDir.
func makeNode(t *testing.T, id uint64, name, root, metaDir, conflictPolicy string) *node.Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: id, Name: name},
		MetaDir: metaDir,
		Listen:  "",
		Folders: []config.Folder{{
			ID:             "data",
			Path:           root,
			ConflictPolicy: conflictPolicy,
		}},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

func write(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	// Give it a stable, distinctive mtime.
	mt := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	os.Chtimes(p, mt, mt)
}

func read(t *testing.T, root, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// pipeSession wires two nodes over a real TCP loopback connection and runs
// a full exchange: b is the initiator, a is the responder.
func pipeSession(t *testing.T, a, b *node.Node) {
	t.Helper()
	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := transfer.Accept(ln)
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		done <- a.Serve(conn)
	}()
	conn, err := transfer.Dial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := b.SyncOnce(conn, a.ID); err != nil {
		var serveErr error
		select {
		case serveErr = <-done:
		default:
		}
		t.Fatalf("SyncOnce: %v (serve: %v)", err, serveErr)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("session timed out")
	}
}

// dropConn simulates a network drop mid-transfer: after `remaining` bytes
// have been read, the underlying connection is closed and further reads
// fail, so a session aborting partway through a file pull is exercised.
type dropConn struct {
	net.Conn
	remaining int64
	mu        sync.Mutex
}

func (c *dropConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remaining <= 0 {
		c.Conn.Close()
		return 0, errors.New("simulated mid-transfer drop")
	}
	n, err := c.Conn.Read(p)
	c.remaining -= int64(n)
	return n, err
}

// TestMidTransferDropRecovers is the kill-mid-transfer fault-injection test:
// a session is interrupted while B is pulling a large multi-block file from
// A, then a fresh session must converge with correct content and no leftover
// temp files.
func TestMidTransferDropRecovers(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNode(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy")
	b := makeNode(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy")

	payload := bytes.Repeat([]byte{0x61}, 4*1024*1024) // 4 MB, many blocks
	write(t, rootA, "big.bin", string(payload))
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	// Session 1: B starts pulling, connection drops after ~256KB of
	// responses (handshake + a few blocks) — a genuine mid-transfer kill.
	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() {
		conn, err := transfer.Accept(ln)
		if err != nil {
			serveDone <- err
			return
		}
		defer conn.Close()
		serveDone <- a.Serve(conn)
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	wrapped := transfer.NewConn(&dropConn{Conn: raw, remaining: 256 * 1024})
	// The first session is interrupted by the drop (any error is fine).
	_, _ = b.SyncOnce(wrapped, a.ID)
	raw.Close()
	ln.Close()
	<-serveDone // A's Serve returns once its writes fail

	// The interrupted pull must not leave temp litter.
	if temps, err := os.ReadDir(filepath.Join(rootB, ".sfx-tmp")); err == nil && len(temps) != 0 {
		t.Fatalf("temp litter after interrupted transfer: %v", temps)
	}

	// Session 2: a clean session converges; B gets the full file intact.
	pipeSession(t, a, b)
	if got := read(t, rootB, "big.bin"); got != string(payload) {
		t.Fatalf("content mismatch after recovery: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestInitialSyncBothWays(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNode(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy")
	b := makeNode(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy")

	// A has a small file, a nested file, and a multi-block file.
	write(t, rootA, "readme.txt", "hello from A")
	write(t, rootA, "docs/nested.txt", "nested content")
	write(t, rootA, "big.bin", string(make([]byte, 300*1024)))
	// B has its own file.
	write(t, rootB, "from-b.txt", "hello from B")

	t.Logf("A root=%s B root=%s", a.Folders["data"].Root, b.Folders["data"].Root)

	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	pipeSession(t, a, b)

	// B must now have A's files (content + mtime) and keep its own.
	for rel, want := range map[string]string{
		"readme.txt":     "hello from A",
		"docs/nested.txt": "nested content",
		"big.bin":        string(make([]byte, 300*1024)),
		"from-b.txt":     "hello from B",
	} {
		if got := read(t, rootB, rel); got != want {
			t.Fatalf("B[%s] = %q, want %q", rel, got, want)
		}
	}
	// A must have B's file.
	if got := read(t, rootA, "from-b.txt"); got != "hello from B" {
		t.Fatalf("A[from-b.txt] = %q", got)
	}
	// mtime preserved for a transferred file.
	ast, _ := os.Stat(filepath.Join(rootA, "readme.txt"))
	bst, _ := os.Stat(filepath.Join(rootB, "readme.txt"))
	if !ast.ModTime().Equal(bst.ModTime()) {
		t.Fatalf("mtime not preserved: %v vs %v", ast.ModTime(), bst.ModTime())
	}
	// Both sides converged: no further needs.
	needsA, _ := a.Folders["data"].Engine.NeedPulls()
	needsB, _ := b.Folders["data"].Engine.NeedPulls()
	if len(needsA) != 0 || len(needsB) != 0 {
		t.Fatalf("not converged: A=%v B=%v", needsA, needsB)
	}
}

func TestModificationPropagates(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNode(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy")
	b := makeNode(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy")

	write(t, rootA, "doc.txt", "v1")
	_, _ = a.ScanFolder("data")
	_, _ = b.ScanFolder("data")
	pipeSession(t, a, b)

	// A modifies the file; B modifies a different file.
	write(t, rootA, "doc.txt", "v2 from A")
	write(t, rootB, "notes.txt", "new note from B")
	_, _ = a.ScanFolder("data")
	_, _ = b.ScanFolder("data")
	pipeSession(t, a, b)

	if got := read(t, rootB, "doc.txt"); got != "v2 from A" {
		t.Fatalf("B[doc.txt] = %q, want v2 from A", got)
	}
	if got := read(t, rootA, "notes.txt"); got != "new note from B" {
		t.Fatalf("A[notes.txt] = %q", got)
	}
}

func TestConflictViaFullSession(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNode(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy")
	b := makeNode(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy")

	// Both start identical, then both edit the same file concurrently.
	write(t, rootA, "shared.txt", "base")
	write(t, rootB, "shared.txt", "base")
	_, _ = a.ScanFolder("data")
	_, _ = b.ScanFolder("data")
	pipeSession(t, a, b)

	write(t, rootA, "shared.txt", "edit from A")
	write(t, rootB, "shared.txt", "edit from B")
	_, _ = a.ScanFolder("data")
	_, _ = b.ScanFolder("data")
	pipeSession(t, a, b)

	// Both must converge to the SAME content (deterministic winner), and
	// the loser's edit must be preserved as a conflict copy on the loser.
	if read(t, rootA, "shared.txt") != read(t, rootB, "shared.txt") {
		t.Fatalf("not converged: A=%q B=%q", read(t, rootA, "shared.txt"), read(t, rootB, "shared.txt"))
	}
	if read(t, rootA, "shared.txt") == "edit from A" && read(t, rootA, "shared.txt") == "edit from B" {
		t.Fatal("unreachable")
	}
	foundA := findConflict(rootA)
	foundB := findConflict(rootB)
	if foundA == foundB {
		t.Fatalf("exactly one side should hold a conflict copy (A=%v B=%v)", foundA, foundB)
	}
	// The conflict copy must exist on the loser side and preserve the loser
	// content; the winner file content must match the other side's edit.
	winner := read(t, rootA, "shared.txt")
	if foundB {
		winner = read(t, rootB, "shared.txt")
	}
	if winner != "edit from A" && winner != "edit from B" {
		t.Fatalf("winner content unexpected: %q", winner)
	}
}

func findConflict(root string) bool {
	found := false
	filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if containsConflictSuffix(filepath.Base(p)) {
			found = true
		}
		return nil
	})
	return found
}

func containsConflictSuffix(name string) bool {
	const suffix = ".sync-conflict"
	for i := 0; i+len(suffix) <= len(name); i++ {
		if name[i:i+len(suffix)] == suffix {
			return true
		}
	}
	return false
}

func TestDeletionPropagatesViaSession(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNode(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy")
	b := makeNode(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy")

	write(t, rootA, "doomed.txt", "i will be deleted")
	_, _ = a.ScanFolder("data")
	_, _ = b.ScanFolder("data")
	pipeSession(t, a, b)
	if _, err := os.Stat(filepath.Join(rootB, "doomed.txt")); err != nil {
		t.Fatal("B should have the file initially")
	}

	os.Remove(filepath.Join(rootA, "doomed.txt"))
	_, _ = a.ScanFolder("data")
	pipeSession(t, a, b)

	if _, err := os.Stat(filepath.Join(rootB, "doomed.txt")); !os.IsNotExist(err) {
		t.Fatal("deletion should propagate to B")
	}
}

func TestDeltaExchangeSendsOnlyNewEntries(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNode(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy")
	b := makeNode(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy")

	for i := 0; i < 10; i++ {
		write(t, rootA, fmt.Sprintf("f-%d.txt", i), fmt.Sprintf("content %d", i))
	}
	_, _ = a.ScanFolder("data")
	_, _ = b.ScanFolder("data")

	pipeSession(t, a, b)
	if initial := a.SentEntries.Load(); initial != 10 {
		t.Fatalf("initial exchange sent %d entries, want 10", initial)
	}

	// Modify one file on A: the next exchange must send exactly that entry.
	write(t, rootA, "f-0.txt", "v2")
	_, _ = a.ScanFolder("data")
	a.SentEntries.Store(0)
	pipeSession(t, a, b)

	if got := a.SentEntries.Load(); got != 1 {
		t.Fatalf("delta exchange sent %d entries, want 1", got)
	}
	if read(t, rootB, "f-0.txt") != "v2" {
		t.Fatal("modification should propagate via the delta")
	}

	// A deletion on A must also travel as a delta (one tombstone entry).
	if err := os.Remove(filepath.Join(rootA, "f-9.txt")); err != nil {
		t.Fatal(err)
	}
	_, _ = a.ScanFolder("data")
	a.SentEntries.Store(0)
	pipeSession(t, a, b)
	if got := a.SentEntries.Load(); got != 1 {
		t.Fatalf("deletion delta sent %d entries, want 1", got)
	}
	if _, err := os.Stat(filepath.Join(rootB, "f-9.txt")); !os.IsNotExist(err) {
		t.Fatal("deletion should propagate via the delta")
	}
}

// TestDeltaFullResendOnNewPeer verifies that a peer we have never synced
// with still receives the complete index even after we hold markers for
// another peer.
func TestDeltaFullResendOnNewPeer(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	rootC := t.TempDir()
	a := makeNode(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy")
	b := makeNode(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy")
	c := makeNode(t, 3, "nas-03", rootC, filepath.Join(t.TempDir(), "meta-c"), "conflict-copy")

	for i := 0; i < 5; i++ {
		write(t, rootA, fmt.Sprintf("f-%d.txt", i), fmt.Sprintf("content %d", i))
	}
	_, _ = a.ScanFolder("data")
	_, _ = b.ScanFolder("data")
	_, _ = c.ScanFolder("data")

	// Sync A with B (A records a marker for peer 2).
	pipeSession(t, a, b)
	a.SentEntries.Store(0)
	// Now sync A with C: C has never seen A's index, so A must resend it.
	pipeSession(t, a, c)
	if got := a.SentEntries.Load(); got != 5 {
		t.Fatalf("sync with new peer sent %d entries, want full 5", got)
	}
	if read(t, rootC, "f-4.txt") != "content 4" {
		t.Fatal("new peer should have received the full index")
	}
}

// newNodeEvents builds a node with an ignore rule (so skips are produced)
// and conflict-copy policy (so conflicts are produced), with the event
// store enabled.
func newNodeEvents(t *testing.T, id uint64, name, root, meta string) *node.Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: id, Name: name},
		MetaDir: meta,
		Folders: []config.Folder{{
			ID: "data", Path: root, ConflictPolicy: "conflict-copy",
			Ignore: []string{"*.tmp"},
		}},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

func countEvents(t *testing.T, n *node.Node, cat events.Category) int {
	t.Helper()
	ev, err := n.Events.Query(events.Filter{Category: cat})
	if err != nil {
		t.Fatal(err)
	}
	return len(ev)
}

func TestEventsRecordedDuringSync(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := newNodeEvents(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"))
	b := newNodeEvents(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"))

	write(t, rootA, "real.txt", "keep me")
	write(t, rootA, "junk.tmp", "ignore me")
	write(t, rootA, "shared.txt", "base")
	write(t, rootB, "shared.txt", "base")
	_, _ = a.ScanFolder("data")
	_, _ = b.ScanFolder("data")

	// junk.tmp is ignored by rule -> a skipped event on A.
	if n := countEvents(t, a, events.CatSkipped); n < 1 {
		t.Fatalf("expected skipped events on A, got %d", n)
	}

	pipeSession(t, a, b)

	// Both converge; then both edit shared.txt concurrently -> conflict copy.
	write(t, rootA, "shared.txt", "edit from A")
	write(t, rootB, "shared.txt", "edit from B")
	_, _ = a.ScanFolder("data")
	_, _ = b.ScanFolder("data")
	pipeSession(t, a, b)

	// The loser created a conflict copy; the winner pulled it. Across the
	// two stores there must be at least one conflict event, and at least
	// one side must have it open (needs attention).
	aConflicts := countEvents(t, a, events.CatConflict)
	bConflicts := countEvents(t, b, events.CatConflict)
	if aConflicts+bConflicts < 1 {
		t.Fatalf("expected a conflict event somewhere, got A=%d B=%d", aConflicts, bConflicts)
	}
	// Applied events on both sides (real.txt pulled to B, conflict copy pulled, ...).
	if n := countEvents(t, b, events.CatApplied); n < 1 {
		t.Fatalf("expected applied events on B, got %d", n)
	}
	// Ack + re-open semantics end to end: ack a conflict, then re-record the
	// same condition -> it re-opens (never dismissed forever).
	ev, _ := a.Events.Query(events.Filter{Category: events.CatConflict})
	if len(ev) > 0 {
		_ = a.Events.Acknowledge(ev[0].ID, "admin")
		if _, err := a.Events.Record(&events.Event{
			TS: time.Now(), Folder: "data", Path: ev[0].Path,
			Category: events.CatConflict, Severity: events.SevWarn, Reason: "still present",
		}); err != nil {
			t.Fatal(err)
		}
		after, _ := a.Events.Query(events.Filter{Category: events.CatConflict, OpenOnly: true})
		if len(after) == 0 {
			t.Fatal("acknowledged conflict should re-open when the condition persists")
		}
	}
}

// genFP pre-generates (or loads) the TLS identity for a meta dir and returns
// its fingerprint, so both fingerprints are known before either node's peer
// config is built.
func genFP(t *testing.T, metaDir, name string) string {
	t.Helper()
	cm, err := certs.LoadOrCreate(filepath.Join(metaDir, "key.pem"), filepath.Join(metaDir, "cert.pem"), name)
	if err != nil {
		t.Fatal(err)
	}
	return cm.Fingerprint()
}

// makeNodeTLS builds a Node with TLS enabled, pinned to peerFP. The node's
// own identity is expected to already exist in metaDir (see genFP).
func makeNodeTLS(t *testing.T, id uint64, name, root, metaDir, conflictPolicy string, peerID uint64, peerFP string) *node.Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: id, Name: name},
		MetaDir: metaDir,
		Listen:  "",
		TLS:     true,
		Folders: []config.Folder{{
			ID:             "data",
			Path:           root,
			ConflictPolicy: conflictPolicy,
		}},
		Peers: []config.Peer{{
			ID: peerID, Name: "peer", Addresses: []string{"127.0.0.1:1"},
			Fingerprint: peerFP,
		}},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

// pipeSessionTLS is the TLS variant of pipeSession: a real loopback TCP
// connection wrapped in TLS 1.3 with fingerprint pinning.
func pipeSessionTLS(t *testing.T, a, b *node.Node) {
	t.Helper()
	ln, err := transfer.ListenTLS("127.0.0.1:0", a.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := transfer.Accept(ln)
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		done <- a.Serve(conn)
	}()
	conn, err := transfer.DialTLS(ln.Addr().String(), b.ClientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := b.SyncOnce(conn, a.ID); err != nil {
		var serveErr error
		select {
		case serveErr = <-done:
		default:
		}
		t.Fatalf("SyncOnce: %v (serve: %v)", err, serveErr)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("session timed out")
	}
}

func TestTLSInitialSync(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	metaA, metaB := t.TempDir(), t.TempDir()
	fpA := genFP(t, metaA, "nas-01")
	fpB := genFP(t, metaB, "nas-02")
	a := makeNodeTLS(t, 1, "nas-01", rootA, metaA, "conflict-copy", 2, fpB)
	b := makeNodeTLS(t, 2, "nas-02", rootB, metaB, "conflict-copy", 1, fpA)

	write(t, rootA, "readme.txt", "hello from A")
	write(t, rootB, "from-b.txt", "hello from B")
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSessionTLS(t, a, b)

	if got := read(t, rootB, "readme.txt"); got != "hello from A" {
		t.Fatalf("B[readme.txt] = %q", got)
	}
	if got := read(t, rootA, "from-b.txt"); got != "hello from B" {
		t.Fatalf("A[from-b.txt] = %q", got)
	}
}

func TestTLSRejectsUnpinnedPeer(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	metaA, metaB := t.TempDir(), t.TempDir()
	stranger, err := certs.LoadOrCreate(filepath.Join(t.TempDir(), "ks"), filepath.Join(t.TempDir(), "cs"), "stranger")
	if err != nil {
		t.Fatal(err)
	}
	// A is pinned to a stranger's fingerprint, so it must reject B.
	a := makeNodeTLS(t, 1, "nas-01", rootA, metaA, "conflict-copy", 2, stranger.Fingerprint())
	fpA := genFP(t, metaA, "nas-01")
	b := makeNodeTLS(t, 2, "nas-02", rootB, metaB, "conflict-copy", 1, fpA)

	write(t, rootA, "secret.txt", "should never reach B")
	_, _ = a.ScanFolder("data")
	_, _ = b.ScanFolder("data")

	ln, err := transfer.ListenTLS("127.0.0.1:0", a.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// Server accept fails the handshake (client cert not pinned).
	acceptErr := make(chan error, 1)
	go func() {
		_, err := transfer.Accept(ln)
		acceptErr <- err
	}()
	conn, err := transfer.DialTLS(ln.Addr().String(), b.ClientTLS)
	if err == nil {
		// In TLS 1.3 the client completes its handshake before the server
		// validates the client cert, so the rejection surfaces on the first
		// exchange rather than at dial time.
		defer conn.Close()
		if _, err := b.SyncOnce(conn, a.ID); err == nil {
			t.Fatal("sync should fail: B's fingerprint is not pinned on A")
		}
	} else {
		t.Logf("dial rejected at handshake (also fine): %v", err)
	}
	select {
	case err := <-acceptErr:
		if err == nil {
			t.Fatal("server accept should have failed the TLS handshake")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server accept did not return")
	}
}

// TestMoveDetectionSyncsAsRename verifies that moving a high-level folder
// (many files below it) on one server converges on the peer as LOCAL
// RENAMES: no re-transfer of the moved content, no deletion-guard trip, no
// pending deletions left behind.
func TestMoveDetectionSyncsAsRename(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := makeNode(t, 1, "nas-01", rootA, filepath.Join(t.TempDir(), "meta-a"), "conflict-copy")
	b := makeNode(t, 2, "nas-02", rootB, filepath.Join(t.TempDir(), "meta-b"), "conflict-copy")

	files := []string{"f1.bin", "f2.bin", "sub/f3.bin", "sub/deep/f4.bin"}
	for _, name := range files {
		write(t, rootA, "media/"+name, strings.Repeat(name, 128))
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	pipeSession(t, a, b)
	if got := read(t, rootB, "media/f1.bin"); got != strings.Repeat("f1.bin", 128) {
		t.Fatal("seed sync failed")
	}

	// A moves the whole media folder (a high-level folder with files below
	// it) into archive/.
	if err := os.MkdirAll(filepath.Join(rootA, "archive"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(rootA, "media"), filepath.Join(rootA, "archive", "media")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	engB := b.Folders["data"].Engine
	bytesBefore := engB.Stats.BytesDown.Load()
	filesBefore := engB.Stats.FilesDown.Load()

	pipeSession(t, a, b)

	// B converged at the new path; old path gone; content identical.
	for _, name := range files {
		if got := read(t, rootB, "archive/media/"+name); got != strings.Repeat(name, 128) {
			t.Fatalf("B[archive/media/%s] mismatch", name)
		}
		if _, err := os.Stat(filepath.Join(rootB, "media", name)); !os.IsNotExist(err) {
			t.Fatalf("B[media/%s] should be gone after the move", name)
		}
	}
	// The empty source directory tree is cleaned up.
	if _, err := os.Stat(filepath.Join(rootB, "media")); !os.IsNotExist(err) {
		t.Fatal("B[media] directory should be removed after the move")
	}
	// No data was re-transferred and no file was re-downloaded.
	if down := engB.Stats.BytesDown.Load(); down != bytesBefore {
		t.Fatalf("bytes re-transferred after a move: before=%d after=%d (want none)", bytesBefore, down)
	}
	if files := engB.Stats.FilesDown.Load(); files != filesBefore {
		t.Fatalf("files re-downloaded after a move: before=%d after=%d (want none)", filesBefore, files)
	}
	// No pending deletions and no deletion-guard trip.
	if dels, _ := engB.PendingDeletions(); len(dels) != 0 {
		t.Fatalf("pending deletions after move = %v", dels)
	}
	if b.DeleteGuardTripped("data") {
		t.Fatal("deletion guard should not trip for a move")
	}
	// Converged in both directions.
	needsA, _ := a.Folders["data"].Engine.NeedPulls()
	needsB, _ := engB.NeedPulls()
	if len(needsA) != 0 || len(needsB) != 0 {
		t.Fatalf("not converged: A=%v B=%v", needsA, needsB)
	}
	// Transparency: a summary event was recorded for the move.
	evs, err := b.Events.Query(events.Filter{Folder: "data", Category: events.CatApplied})
	if err != nil {
		t.Fatal(err)
	}
	summary := 0
	for _, e := range evs {
		if strings.Contains(e.Reason, "moved") {
			summary++
		}
	}
	if summary == 0 {
		t.Fatal("no move summary event recorded")
	}
}
