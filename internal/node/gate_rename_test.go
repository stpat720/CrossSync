package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/transfer"
)

// gateNode builds a node whose single "data" folder carries an include
// (whitelist) gate. SettleScans is 1 so every scan applies immediately.
func gateNode(t *testing.T, id uint64, name, root, metaDir string, include []string) *Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: id, Name: name},
		MetaDir: metaDir,
		Listen:  "",
		Folders: []config.Folder{{
			ID: "data", Path: root, ConflictPolicy: "conflict-copy",
			Include: include, SettleScans: 1,
		}},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// gateSession runs one full exchange with responder serving and initiator
// dialing (the initiator sends its index first, so its peer views are fresh
// by the end of the same round).
func gateSession(t *testing.T, responder, initiator *Node) {
	t.Helper()
	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	done := make(chan error, 1)
	go func() {
		conn, err := transfer.Accept(ln)
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = conn.Close() }()
		done <- responder.Serve(conn)
	}()
	conn, err := transfer.Dial(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := initiator.SyncOnce(conn, responder.ID); err != nil {
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

// gateOfflineRound is "a round with the peer away": the dial to a closed
// listener fails, so no state is exchanged at all.
func gateOfflineRound(t *testing.T, a *Node) {
	t.Helper()
	ln, err := transfer.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	if conn, err := a.Dial(addr); err == nil {
		_ = conn.Close()
		t.Fatal("dial to a closed listener unexpectedly succeeded")
	}
}

func mustScan(t *testing.T, n *Node) int {
	t.Helper()
	got, err := n.ScanFolder("data")
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// liveEntry returns the live (non-tombstone) index entry for name.
func liveEntry(t *testing.T, ix *index.Index, name string) (*index.FileInfo, bool) {
	t.Helper()
	fi, ok, err := ix.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || fi.Deleted {
		return nil, false
	}
	return fi, true
}

// writeAt writes rel under root with a stable mtime, so the (size, mtime)
// rename identity is deterministic.
func writeAt(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	_ = os.Chtimes(p, mt, mt)
}

func exists(t *testing.T, root, rel string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
	return err == nil
}

func mustQuery(t *testing.T, n *Node, folder string) []events.Event {
	t.Helper()
	evs, err := n.Events.Query(events.Filter{Folder: folder})
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

// gatedFolderRenameSetup builds the Feature-1 fixture: A is gated on proxy/**
// and B is ungated, both share folder "data", and A has renamed its local
// proxy/ directory to dev/ (out of its own gate). Rounds are driven with B as
// the initiator.
func gatedFolderRenameSetup(t *testing.T) (a, b *Node, rootA, rootB string) {
	t.Helper()
	rootA = t.TempDir()
	rootB = t.TempDir()
	a = gateNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"), []string{"proxy/"})
	b = gateNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"), nil)

	writeAt(t, rootA, "proxy/a.bin", "alpha content")
	writeAt(t, rootA, "proxy/sub/b.bin", "beta content")
	mustScan(t, a)
	mustScan(t, b)
	gateSession(t, a, b)
	if got := readFile(t, rootB, "proxy/a.bin"); got != "alpha content" {
		t.Fatalf("setup: B proxy/a.bin = %q", got)
	}
	if err := os.Rename(filepath.Join(rootA, "proxy"), filepath.Join(rootA, "dev")); err != nil {
		t.Fatal(err)
	}
	mustScan(t, a)
	return a, b, rootA, rootB
}

// releaseEvents counts events recording a release: A's out-of-gate release
// (system/info) and a receiver's out-of-scope release (applied/info).
func releaseEvents(t *testing.T, n *Node) int {
	t.Helper()
	count := 0
	for _, e := range mustQuery(t, n, "data") {
		if e.Severity == events.SevInfo && strings.Contains(e.Reason, "released") {
			count++
		}
	}
	return count
}

// remoteDeletions counts applied "deleted (remote)" events for one path.
func remoteDeletions(t *testing.T, n *Node, path string) int {
	t.Helper()
	count := 0
	for _, e := range mustQuery(t, n, "data") {
		if e.Category == events.CatApplied && e.Reason == "deleted (remote)" && e.Path == path {
			count++
		}
	}
	return count
}

// TestGateRenameMovesOutOfGateAndReleases is T1: A is gated on proxy/** and
// renames its local proxy/ directory to dev/ (out of its own gate). The
// rename must reach B (ungated, same folder) as a MOVE with no bytes
// transferred, and only once B has confirmed holding the content under the new
// name may A drop its tracking of the paths — leaving its own files on disk
// alone and never broadcasting a deletion for the old names.
func TestGateRenameMovesOutOfGateAndReleases(t *testing.T) {
	a, b, rootA, rootB := gatedFolderRenameSetup(t)
	ixA, ixB := a.Folders["data"].Ix, b.Folders["data"].Ix
	engB := b.Folders["data"].Engine
	bytesBefore := engB.Stats.BytesDown.Load()
	filesBefore := engB.Stats.FilesDown.Load()

	// The rename is advertised as a move straight away; nothing is deleted.
	if _, live := liveEntry(t, ixA, "dev/a.bin"); !live {
		t.Fatal("A must advertise the renamed entry so peers can move it")
	}
	if _, live := liveEntry(t, ixA, "proxy/a.bin"); live {
		t.Fatal("A must not keep the old path live")
	}

	for i := 0; i < 3; i++ {
		gateSession(t, a, b)
		mustScan(t, a)
		mustScan(t, b)
	}

	// B holds the content under the new names, byte-identical, and no longer
	// has the old names.
	for rel, want := range map[string]string{
		"dev/a.bin":     "alpha content",
		"dev/sub/b.bin": "beta content",
	} {
		if got := readFile(t, rootB, rel); got != want {
			t.Fatalf("B %s = %q, want %q", rel, got, want)
		}
	}
	for _, rel := range []string{"proxy/a.bin", "proxy/sub/b.bin"} {
		if exists(t, rootB, rel) {
			t.Fatalf("B still has old path %s", rel)
		}
		if _, live := liveEntry(t, ixB, rel); live {
			t.Fatalf("B index still tracks old path %s", rel)
		}
	}
	if _, live := liveEntry(t, ixB, "dev/a.bin"); !live {
		t.Fatal("B index must hold dev/a.bin")
	}
	// No content was transferred: it was a move on both sides.
	if d := engB.Stats.BytesDown.Load() - bytesBefore; d != 0 {
		t.Fatalf("B re-downloaded %d bytes for a move, want 0", d)
	}
	if d := engB.Stats.FilesDown.Load() - filesBefore; d != 0 {
		t.Fatalf("B re-downloaded %d file(s) for a move, want 0", d)
	}
	// A released: it no longer tracks either path, but its files stay on disk
	// under the new name.
	for _, rel := range []string{"proxy/a.bin", "dev/a.bin", "proxy/sub/b.bin", "dev/sub/b.bin"} {
		if _, live := liveEntry(t, ixA, rel); live {
			t.Fatalf("A still tracks %s after the release", rel)
		}
	}
	for rel, want := range map[string]string{
		"dev/a.bin":     "alpha content",
		"dev/sub/b.bin": "beta content",
	} {
		if got := readFile(t, rootA, rel); got != want {
			t.Fatalf("A must leave its own file alone: %s = %q, want %q", rel, got, want)
		}
	}
	// A never broadcast a deletion of the old names: B's history has no
	// "deleted (remote)" record for them.
	for _, rel := range []string{"proxy/a.bin", "proxy/sub/b.bin"} {
		if n := remoteDeletions(t, b, rel); n != 0 {
			t.Fatalf("B applied %d deletion(s) for %s: A broadcast a delete instead of a move", n, rel)
		}
	}
	// B applied the paths as moves (its move summary event proves it).
	moved := 0
	for _, e := range mustQuery(t, b, "data") {
		if e.Category == events.CatApplied && strings.Contains(e.Reason, "moved") {
			moved++
		}
	}
	if moved == 0 {
		t.Fatal("B recorded no move event")
	}
	if n := releaseEvents(t, a); n != 1 {
		t.Fatalf("A logged %d release event(s), want exactly 1", n)
	}
	if got := a.Folders["data"].Engine.PendingReleases(); got != 0 {
		t.Fatalf("A still has %d pending release(s) after confirmation", got)
	}
}

// TestGateRenameHoldsUntilPeersConfirm is T2 (ordering): while B is away, and
// for the first round after the rename, A must keep tracking the paths and
// broadcast no deletion. Only once B has confirmed the move may A release.
func TestGateRenameHoldsUntilPeersConfirm(t *testing.T) {
	a, b, _, rootB := gatedFolderRenameSetup(t)
	ixA := a.Folders["data"].Ix

	// Round 1: B unreachable. A holds: the new path is still tracked (waiting
	// for confirmation), the old path is only tombstoned as part of the move,
	// and nothing was deleted anywhere.
	gateOfflineRound(t, a)
	if _, live := liveEntry(t, ixA, "dev/a.bin"); !live {
		t.Fatal("A released dev/a.bin without any peer confirmation")
	}
	if got := a.Folders["data"].Engine.PendingReleases(); got != 2 {
		t.Fatalf("A has %d pending release(s), want 2 (nothing confirmed yet)", got)
	}
	if !exists(t, rootB, "proxy/a.bin") {
		t.Fatal("B lost proxy/a.bin before it ever saw the move")
	}
	if n := remoteDeletions(t, b, "proxy/a.bin"); n != 0 {
		t.Fatalf("B applied a deletion of proxy/a.bin before the move")
	}

	// Round 2: B applies the move, but A has not yet seen B's post-move
	// index, so A must still hold (the release waits for CONFIRMATION, not
	// merely for a round).
	gateSession(t, a, b)
	mustScan(t, b)
	if _, live := liveEntry(t, ixA, "dev/a.bin"); !live {
		t.Fatal("A released before B's move was confirmed in its peer view")
	}
	if got := a.Folders["data"].Engine.PendingReleases(); got != 2 {
		t.Fatalf("A has %d pending release(s) before confirmation, want 2", got)
	}
	if got := readFile(t, rootB, "dev/a.bin"); got != "alpha content" {
		t.Fatalf("B dev/a.bin = %q after the move round", got)
	}
	if n := remoteDeletions(t, b, "proxy/a.bin"); n != 0 {
		t.Fatalf("B applied a deletion of proxy/a.bin: an old name was deleted rather than moved")
	}

	// Round 3: B's delta confirms the move, so A releases.
	gateSession(t, a, b)
	mustScan(t, a)
	if got := a.Folders["data"].Engine.PendingReleases(); got != 0 {
		t.Fatalf("A still has %d pending release(s) after confirmation", got)
	}
	for _, rel := range []string{"dev/a.bin", "proxy/a.bin"} {
		if _, live := liveEntry(t, ixA, rel); live {
			t.Fatalf("A still tracks %s after the release", rel)
		}
	}
	if n := releaseEvents(t, a); n != 1 {
		t.Fatalf("release event count = %d, want 1", n)
	}
	// B ends with the files under the new names.
	for rel, want := range map[string]string{
		"dev/a.bin":     "alpha content",
		"dev/sub/b.bin": "beta content",
	} {
		if got := readFile(t, rootB, rel); got != want {
			t.Fatalf("B %s = %q, want %q", rel, got, want)
		}
	}
	for _, rel := range []string{"proxy/a.bin", "proxy/sub/b.bin"} {
		if n := remoteDeletions(t, b, rel); n != 0 {
			t.Fatalf("B applied %d deletion(s) for %s during the handshake", n, rel)
		}
	}
}

// TestGateRenameOutOfScopeReceiverReleases is T3: the RECEIVER's gate excludes
// the new name, so it releases its copy (disk and index) instead of adopting
// it — without re-broadcasting a deletion and without an event loop.
func TestGateRenameOutOfScopeReceiverReleases(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	a := gateNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"), nil)
	b := gateNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"), []string{"proxy/"})

	writeAt(t, rootA, "proxy/a.bin", "alpha content")
	writeAt(t, rootA, "proxy/sub/b.bin", "beta content")
	mustScan(t, a)
	mustScan(t, b)
	gateSession(t, a, b)
	if !exists(t, rootB, "proxy/a.bin") {
		t.Fatal("setup: B should own proxy/a.bin")
	}

	// A is ungated, so its rename is a plain local move.
	if err := os.Rename(filepath.Join(rootA, "proxy"), filepath.Join(rootA, "dev")); err != nil {
		t.Fatal(err)
	}
	mustScan(t, a)

	engB := b.Folders["data"].Engine
	bytesBefore := engB.Stats.BytesDown.Load()
	gateSession(t, a, b)
	mustScan(t, b)

	// B released: nothing on disk, nothing live in its index.
	for _, rel := range []string{"proxy/a.bin", "proxy/sub/b.bin", "dev/a.bin", "dev/sub/b.bin"} {
		if exists(t, rootB, rel) {
			t.Fatalf("B still has %s after releasing it", rel)
		}
		if _, live := liveEntry(t, b.Folders["data"].Ix, rel); live {
			t.Fatalf("B index still tracks %s after the release", rel)
		}
	}
	if d := engB.Stats.BytesDown.Load() - bytesBefore; d != 0 {
		t.Fatalf("B re-downloaded %d bytes releasing an out-of-scope move", d)
	}
	// A is unaffected: it keeps the content under the new name.
	if _, live := liveEntry(t, a.Folders["data"].Ix, "dev/a.bin"); !live {
		t.Fatal("A must keep dev/a.bin live (it is ungated)")
	}
	if got := readFile(t, rootA, "dev/a.bin"); got != "alpha content" {
		t.Fatalf("A dev/a.bin = %q", got)
	}
	if n := releaseEvents(t, b); n != 1 {
		t.Fatalf("B logged %d release event(s), want exactly 1", n)
	}

	// No event loop: another round changes nothing.
	before := len(mustQuery(t, b, "data"))
	gateSession(t, a, b)
	mustScan(t, a)
	mustScan(t, b)
	after := mustQuery(t, b, "data")
	if len(after) != before {
		t.Fatalf("B recorded %d new event(s) in an idle round (loop)", len(after)-before)
	}
	for _, e := range after {
		if e.Severity >= events.SevWarn && e.Resolution == events.ResOpen {
			t.Fatalf("B has an open attention event after the release: %q", e.Reason)
		}
	}
}

// TestGateRenameReincludeRestores is T4: after the release, widening A's gate
// to include the new names pulls the files back from B byte-identically — the
// release cost A nothing on disk and B still holds the content.
func TestGateRenameReincludeRestores(t *testing.T) {
	a, b, rootA, rootB := gatedFolderRenameSetup(t)
	for i := 0; i < 3; i++ {
		gateSession(t, a, b)
		mustScan(t, a)
		mustScan(t, b)
	}
	if got := a.Folders["data"].Engine.PendingReleases(); got != 0 {
		t.Fatalf("setup: A still has %d pending release(s)", got)
	}

	// A loses its local copies (the release left them on disk; a user may
	// clear them, or the folder may live on a volume that is gone).
	if err := os.RemoveAll(filepath.Join(rootA, "dev")); err != nil {
		t.Fatal(err)
	}
	// Widen the gate: dev/ is now in scope.
	fc := a.Folders["data"].Cfg
	fc.Include = []string{"proxy/", "dev/"}
	if _, err := a.UpdateFolder("data", fc); err != nil {
		t.Fatal(err)
	}
	mustScan(t, a)
	gateSession(t, a, b)
	mustScan(t, a)

	for rel, want := range map[string]string{
		"dev/a.bin":     "alpha content",
		"dev/sub/b.bin": "beta content",
	} {
		if got := readFile(t, rootA, rel); got != want {
			t.Fatalf("A %s = %q after re-include, want %q", rel, got, want)
		}
		if _, live := liveEntry(t, a.Folders["data"].Ix, rel); !live {
			t.Fatalf("A index must hold %s after re-include", rel)
		}
		if got := readFile(t, rootB, rel); got != want {
			t.Fatalf("B %s = %q, want %q", rel, got, want)
		}
	}
	// No conflict copies were manufactured on either side.
	for _, n := range []*Node{a, b} {
		for _, e := range mustQuery(t, n, "data") {
			if e.Category == events.CatConflict {
				t.Fatalf("%s: unexpected conflict event %q", n.Name, e.Reason)
			}
		}
	}
	needsA, _ := a.Folders["data"].Engine.NeedPulls()
	needsB, _ := b.Folders["data"].Engine.NeedPulls()
	if len(needsA) != 0 || len(needsB) != 0 {
		t.Fatalf("not converged after re-include: A=%v B=%v", needsA, needsB)
	}
}
