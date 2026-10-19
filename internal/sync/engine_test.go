package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/ignore"
	"crosssync/internal/index"
	"crosssync/internal/scanner"
	"crosssync/internal/staging"
	"crosssync/internal/version"
)

type testNode struct {
	root   string
	ix     *index.Index
	stager *staging.Stager
	engine *Engine
}

func newNode(t *testing.T, id uint64, folderID, policy string, v *Versioning) *testNode {
	t.Helper()
	root := t.TempDir()
	ix, err := index.Open(filepath.Join(root, ".sfx-index", "folder.db"), folderID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	st, err := staging.New(root)
	if err != nil {
		t.Fatal(err)
	}
	ig, _ := ignore.Parse(nil)
	eng := New(id, folderID, root, ParseConflictPolicy(policy), v, ix, st, ig, t.Logf)
	return &testNode{root: root, ix: ix, stager: st, engine: eng}
}

func (n *testNode) writeFile(t *testing.T, rel, content string) {
	t.Helper()
	p := filepath.Join(n.root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	// Ensure mtime is stable and distinct from any prior version.
	future := time.Now().Add(time.Hour)
	_ = os.Chtimes(p, future, future)
}

func (n *testNode) scanAndApply(t *testing.T) {
	t.Helper()
	s := scanner.New(n.root, n.ix, n.engine.Ignore)
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.Kind == scanner.Renamed {
			if _, err := n.engine.ApplyLocalRename(c.From, c.Info); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := n.engine.ApplyLocalChange(c.Kind, c.Info); err != nil {
			t.Fatal(err)
		}
	}
}

func (n *testNode) fileContent(t *testing.T, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(n.root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestPendingDeletions verifies the engine reports live files that are
// globally tombstoned (a peer's authoritative deletion) — the set the
// deletion guard protects and the operator override applies.
func TestPendingDeletions(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	a.writeFile(t, "keep.txt", "keep")
	a.writeFile(t, "gone.txt", "gone")
	a.scanAndApply(t)

	// Peer 2 tombstones gone.txt with a version that dominates our live
	// copy (it adopted our {1:1} then bumped its own counter) and keeps
	// keep.txt as-is.
	keep, ok, _ := a.ix.Get("keep.txt")
	if !ok {
		t.Fatal("keep.txt should be indexed")
	}
	tomb := &index.FileInfo{Name: "gone.txt", Deleted: true, Version: version.New().Bump(1).Bump(2).Bump(2)}
	a.engine.SetPeerIndex(2, []*index.FileInfo{keep, tomb}, false)

	dels, err := a.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) != 1 || dels[0] != "gone.txt" {
		t.Fatalf("PendingDeletions = %v, want [gone.txt]", dels)
	}
	// The deletion is also part of NeedPulls so the session would try to
	// apply it (and the guard would block it).
	needs, err := a.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range needs {
		if n == "gone.txt" {
			found = true
		}
	}
	if !found {
		t.Fatalf("NeedPulls = %v, want it to include gone.txt", needs)
	}
}

func TestApplyLocalChangeVersions(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "one")
	n.scanAndApply(t)

	fi, ok, _ := n.ix.Get("a.txt")
	if !ok {
		t.Fatal("a.txt should be indexed")
	}
	if fi.Version.Get(1) != 1 {
		t.Fatalf("version counter = %d, want 1", fi.Version.Get(1))
	}

	// Modify and re-apply.
	n.writeFile(t, "a.txt", "two")
	n.scanAndApply(t)
	fi, _, _ = n.ix.Get("a.txt")
	if fi.Version.Get(1) != 2 {
		t.Fatalf("version counter = %d, want 2", fi.Version.Get(1))
	}

	// Delete.
	_ = os.Remove(filepath.Join(n.root, "a.txt"))
	n.scanAndApply(t)
	fi, ok, _ = n.ix.Get("a.txt")
	if !ok || !fi.Deleted {
		t.Fatal("a.txt should be a tombstone")
	}
	if fi.Version.Get(1) != 3 {
		t.Fatalf("tombstone counter = %d, want 3", fi.Version.Get(1))
	}
}

func TestApplyLocalRename(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "hello")
	n.scanAndApply(t)

	// Rename a.txt -> b.txt on disk: the scan reports a single Renamed
	// change and the engine applies it as a move (no delete+add pair).
	if err := os.Rename(filepath.Join(n.root, "a.txt"), filepath.Join(n.root, "b.txt")); err != nil {
		t.Fatal(err)
	}
	s := scanner.New(n.root, n.ix, n.engine.Ignore)
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range changes {
		if c.Kind != scanner.Renamed {
			continue
		}
		found = true
		if c.From != "a.txt" || c.Info.Name != "b.txt" {
			t.Fatalf("unexpected rename %+v", c)
		}
		if _, err := n.engine.ApplyLocalRename(c.From, c.Info); err != nil {
			t.Fatal(err)
		}
	}
	if !found {
		t.Fatalf("expected a Renamed change, got %+v", changes)
	}
	// Index: a.txt tombstoned, b.txt live, both on the same bumped version.
	a, ok, _ := n.ix.Get("a.txt")
	if !ok || !a.Deleted {
		t.Fatalf("a.txt should be tombstoned, got %+v", a)
	}
	b, ok, _ := n.ix.Get("b.txt")
	if !ok || b.Deleted {
		t.Fatalf("b.txt should be live, got %+v", b)
	}
	if !a.Version.Equal(b.Version) {
		t.Fatalf("tombstone and live entry should share the bumped version, got %v vs %v", a.Version, b.Version)
	}
	// Nothing left to pull or delete after a local rename.
	needs, err := n.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 0 {
		t.Fatalf("NeedPulls after rename = %v, want empty", needs)
	}
	dels, err := n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) != 0 {
		t.Fatalf("PendingDeletions after rename = %v, want empty", dels)
	}
	// A stale plan (target already exists) is rejected so the caller can
	// fall back to delete+add.
	dup := &index.FileInfo{Name: "a.txt", Type: index.TypeFile}
	if _, err := n.engine.ApplyLocalRename("b.txt", dup); err == nil {
		t.Fatal("rename onto an existing target should fail")
	}
}

func TestPullFlow(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	content := "hello from node a, this is a pull test payload"
	a.writeFile(t, "docs/payload.txt", content)
	a.scanAndApply(t)

	// A's indexed entry.
	fi, ok, _ := a.ix.Get("docs/payload.txt")
	if !ok {
		t.Fatal("a should have indexed the file")
	}

	// B learns A's index; needs to pull.
	b.engine.SetPeerIndex(1, []*index.FileInfo{fi}, false)
	needs, err := b.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 1 || needs[0] != "docs/payload.txt" {
		t.Fatalf("needs = %v", needs)
	}

	// B pulls, serving blocks from A's real file.
	pull, err := b.engine.StartPull("docs/payload.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	af, err := os.Open(filepath.Join(a.root, "docs", "payload.txt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, need := range pull.NeedBlocks() {
		buf := make([]byte, need.Size)
		if _, err := af.ReadAt(buf, need.Offset); err != nil {
			t.Fatal(err)
		}
		if err := pull.ReceiveBlock(need.Index, buf); err != nil {
			t.Fatal(err)
		}
	}
	_ = af.Close()
	if _, err := pull.Finish(); err != nil {
		t.Fatal(err)
	}

	if got := b.fileContent(t, "docs/payload.txt"); got != content {
		t.Fatalf("pulled content = %q, want %q", got, content)
	}
	// mtime must equal the source's (timestamp-before-rename).
	ast, _ := os.Stat(filepath.Join(a.root, "docs", "payload.txt"))
	bst, _ := os.Stat(filepath.Join(b.root, "docs", "payload.txt"))
	if !ast.ModTime().Equal(bst.ModTime()) {
		t.Fatalf("mtime not preserved: %v vs %v", ast.ModTime(), bst.ModTime())
	}
	// Index on B must carry A's version.
	bfi, _, _ := b.ix.Get("docs/payload.txt")
	if !bfi.Version.Equal(fi.Version) {
		t.Fatalf("adopted version = %v, want %v", bfi.Version, fi.Version)
	}
	// No further pulls needed.
	needs, _ = b.engine.NeedPulls()
	if len(needs) != 0 {
		t.Fatalf("expected no more needs, got %v", needs)
	}
}

func TestConflictCreatesCopy(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	// Both edit the same file concurrently with different content.
	a.writeFile(t, "doc.txt", "AAA version")
	a.scanAndApply(t)
	b.writeFile(t, "doc.txt", "BBB version")
	b.scanAndApply(t)
	// Give the files distinct, explicit mtimes: B's is NEWER. Under the
	// default conflict rule the newer file wins, so B must be the winner
	// and A the loser (its edit becomes the conflict copy).
	older := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	newer := older.Add(2 * time.Hour)
	_ = os.Chtimes(filepath.Join(a.root, "doc.txt"), older, older)
	_ = os.Chtimes(filepath.Join(b.root, "doc.txt"), newer, newer)
	a.scanAndApply(t)
	b.scanAndApply(t)

	// Exchange views.
	afi, _, _ := a.ix.Get("doc.txt")
	bfi, _, _ := b.ix.Get("doc.txt")
	a.engine.SetPeerIndex(2, []*index.FileInfo{bfi}, false)
	b.engine.SetPeerIndex(1, []*index.FileInfo{afi}, false)

	// The winner is the newer-mtime file (B), the same on both sides, so
	// exactly ONE side must pull: the loser (A).
	needsA, _ := a.engine.NeedPulls()
	needsB, _ := b.engine.NeedPulls()
	if len(needsA) != 1 || len(needsB) != 0 {
		t.Fatalf("expected only the loser to pull: A=%v B=%v", needsA, needsB)
	}

	// A is the loser: preserve its edit as a conflict copy, then pull.
	plan, err := a.engine.PlanOverwrite("doc.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !plan.MakeConflictCopy {
		t.Fatal("expected conflict copy plan on the loser")
	}
	if _, err := a.engine.Execute(plan); err != nil {
		t.Fatal(err)
	}
	pull, err := a.engine.StartPull("doc.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, need := range pull.NeedBlocks() {
		// Serve from B's file (the winner).
		bf, _ := os.Open(filepath.Join(b.root, "doc.txt"))
		buf := make([]byte, need.Size)
		_, _ = bf.ReadAt(buf, need.Offset)
		_ = bf.Close()
		if err := pull.ReceiveBlock(need.Index, buf); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pull.Finish(); err != nil {
		t.Fatal(err)
	}

	// Both sides must now agree on the winner content.
	if got := a.fileContent(t, "doc.txt"); got != "BBB version" {
		t.Fatalf("a winner = %q, want BBB version", got)
	}
	if got := b.fileContent(t, "doc.txt"); got != "BBB version" {
		t.Fatalf("b winner = %q, want BBB version", got)
	}
	// A's edit is preserved as a conflict copy.
	if !findConflictCopy(t, a.root) {
		t.Fatal("expected a conflict copy on A")
	}
	if findConflictCopy(t, b.root) {
		t.Fatal("no conflict copy expected on the winner side")
	}
}

func findConflictCopy(t *testing.T, root string) bool {
	t.Helper()
	found := false
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
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
	for i := 0; i+len(ConflictSuffix) <= len(name); i++ {
		if name[i:i+len(ConflictSuffix)] == ConflictSuffix {
			return true
		}
	}
	return false
}

func TestNoFalseConflictWhenIdentical(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	// Both write identical content concurrently.
	a.writeFile(t, "same.txt", "identical bytes")
	a.scanAndApply(t)
	b.writeFile(t, "same.txt", "identical bytes")
	b.scanAndApply(t)

	afi, _, _ := a.ix.Get("same.txt")
	bfi, _, _ := b.ix.Get("same.txt")
	a.engine.SetPeerIndex(2, []*index.FileInfo{bfi}, false)
	b.engine.SetPeerIndex(1, []*index.FileInfo{afi}, false)

	needsA, _ := a.engine.NeedPulls()
	needsB, _ := b.engine.NeedPulls()
	if len(needsA) != 0 || len(needsB) != 0 {
		t.Fatalf("identical content must not be a conflict: A=%v B=%v", needsA, needsB)
	}
}

// TestNewerFileWinsConflict verifies the default conflict rule end to end:
// for two concurrent edits with different content, the file with the NEWER
// modification time is the global winner, so the older side must pull and
// preserve its edit as a conflict copy.
func TestNewerFileWinsConflict(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	a.writeFile(t, "doc.txt", "older edit")
	a.scanAndApply(t)
	b.writeFile(t, "doc.txt", "newer edit")
	b.scanAndApply(t)
	older := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	newer := older.Add(5 * time.Hour)
	_ = os.Chtimes(filepath.Join(a.root, "doc.txt"), older, older)
	_ = os.Chtimes(filepath.Join(b.root, "doc.txt"), newer, newer)
	a.scanAndApply(t)
	b.scanAndApply(t)

	afi, _, _ := a.ix.Get("doc.txt")
	bfi, _, _ := b.ix.Get("doc.txt")
	a.engine.SetPeerIndex(2, []*index.FileInfo{bfi}, false)
	b.engine.SetPeerIndex(1, []*index.FileInfo{afi}, false)

	// Global winner must be B's newer entry on BOTH sides.
	ga, _, err := a.engine.GlobalFor("doc.txt")
	if err != nil {
		t.Fatal(err)
	}
	gb, _, err := b.engine.GlobalFor("doc.txt")
	if err != nil {
		t.Fatal(err)
	}
	if ga.Version.Get(2) == 0 || gb.Version.Get(2) == 0 {
		t.Fatalf("newer (B) entry should be the global winner: A=%v B=%v", ga.Version, gb.Version)
	}
	// Only the older side (A) needs to pull.
	needsA, _ := a.engine.NeedPulls()
	needsB, _ := b.engine.NeedPulls()
	if len(needsA) != 1 || len(needsB) != 0 {
		t.Fatalf("expected only the older side to pull: A=%v B=%v", needsA, needsB)
	}
	// A's plan must preserve the loser as a conflict copy.
	plan, err := a.engine.PlanOverwrite("doc.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !plan.MakeConflictCopy {
		t.Fatal("expected conflict copy plan on the older side")
	}
}

// TestNewerOrGreaterTieBreak verifies that equal mtimes fall back to the
// deterministic canonical-version tie-breaker.
func TestNewerOrGreaterTieBreak(t *testing.T) {
	ts := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	mk := func(id uint64) *index.FileInfo {
		return &index.FileInfo{
			Name: "x", ModifiedS: ts.Unix(), ModifiedNs: int32(ts.Nanosecond()),
			Version: version.New().Bump(id).Bump(id), // {id: 2}
		}
	}
	// Same mtime, different vectors -> totalGreater decides (canonical string).
	a := mk(1)
	b := mk(2)
	if newerOrGreater(a, b) == newerOrGreater(b, a) {
		t.Fatal("tie must be broken deterministically (one direction wins)")
	}
	// Newer mtime always wins regardless of the vectors.
	newer := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	old := &index.FileInfo{Name: "x", ModifiedS: ts.Unix(), ModifiedNs: int32(ts.Nanosecond()), Version: version.New().Bump(9)}
	nw := &index.FileInfo{Name: "x", ModifiedS: newer.Unix(), ModifiedNs: int32(newer.Nanosecond()), Version: version.New().Bump(1)}
	if !newerOrGreater(nw, old) || newerOrGreater(old, nw) {
		t.Fatal("newer mtime must win")
	}
}

func TestDeletionPropagates(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	a.writeFile(t, "gone.txt", "bye")
	a.scanAndApply(t)
	afi, _, _ := a.ix.Get("gone.txt")
	b.engine.SetPeerIndex(1, []*index.FileInfo{afi}, false)
	needs, _ := b.engine.NeedPulls()
	if len(needs) != 1 {
		t.Fatalf("b should need the file, got %v", needs)
	}
	// B pulls the file.
	pull, _ := b.engine.StartPull("gone.txt", 0)
	for _, need := range pull.NeedBlocks() {
		f, _ := os.Open(filepath.Join(a.root, "gone.txt"))
		buf := make([]byte, need.Size)
		_, _ = f.ReadAt(buf, need.Offset)
		_ = f.Close()
		_ = pull.ReceiveBlock(need.Index, buf)
	}
	_, _ = pull.Finish()

	// A deletes the file.
	_ = os.Remove(filepath.Join(a.root, "gone.txt"))
	a.scanAndApply(t)
	afi, _, _ = a.ix.Get("gone.txt")
	if !afi.Deleted {
		t.Fatal("a should have a tombstone")
	}
	// B learns the tombstone and applies the deletion.
	b.engine.SetPeerIndex(1, []*index.FileInfo{afi}, true)
	needs, _ = b.engine.NeedPulls()
	if len(needs) != 1 || needs[0] != "gone.txt" {
		t.Fatalf("b should need the deletion, got %v", needs)
	}
	if err := b.engine.ApplyDeletion("gone.txt", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(b.root, "gone.txt")); !os.IsNotExist(err) {
		t.Fatal("file should be deleted on b")
	}
	bfi, _, _ := b.ix.Get("gone.txt")
	if !bfi.Deleted {
		t.Fatal("b should have a tombstone")
	}
}

// movePeerView builds a peer (id 2) view that "moved" local entry `from` to
// `to`: the source tombstoned with a dominating version and the target live
// with identical content.
func movePeerView(t *testing.T, n *testNode, from, to string) (*index.FileInfo, *index.FileInfo) {
	t.Helper()
	local, ok, err := n.ix.Get(from)
	if err != nil || !ok {
		t.Fatalf("get %s: ok=%v err=%v", from, ok, err)
	}
	target := local.Clone()
	target.Name = to
	target.Version = version.New().Bump(1).Bump(2).Bump(2)
	tomb := &index.FileInfo{Name: from, Deleted: true, Version: version.New().Bump(1).Bump(2).Bump(2)}
	n.engine.SetPeerIndex(2, []*index.FileInfo{tomb, target}, false)
	return tomb, target
}

func TestPlanMovesExact(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "dir1/file.txt", "same content")
	n.scanAndApply(t)
	_, target := movePeerView(t, n, "dir1/file.txt", "dir2/file.txt")

	moves, err := n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 1 {
		t.Fatalf("moves = %+v, want 1", moves)
	}
	if moves[0].From != "dir1/file.txt" || moves[0].To != "dir2/file.txt" {
		t.Fatalf("move = %+v", moves[0])
	}
	if !sameContent(moves[0].Global, target) {
		t.Fatal("move should carry the peer's target entry")
	}
}

func TestPlanMovesAmbiguous(t *testing.T) {
	// One source, two targets with identical content → ambiguous, no pairing.
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "dir1/file.txt", "same")
	n.scanAndApply(t)
	local, _, _ := n.ix.Get("dir1/file.txt")
	t1 := local.Clone()
	t1.Name = "a/copy1.txt"
	t1.Version = version.New().Bump(1).Bump(2).Bump(2)
	t2 := local.Clone()
	t2.Name = "a/copy2.txt"
	t2.Version = version.New().Bump(1).Bump(2).Bump(2)
	tomb := &index.FileInfo{Name: "dir1/file.txt", Deleted: true, Version: version.New().Bump(1).Bump(2).Bump(2)}
	n.engine.SetPeerIndex(2, []*index.FileInfo{tomb, t1, t2}, false)

	moves, err := n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 0 {
		t.Fatalf("ambiguous copy should not pair: %+v", moves)
	}
}

func TestPlanMovesContentMismatch(t *testing.T) {
	// Moved AND edited: block hashes differ → no pairing.
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "dir1/file.txt", "aaa")
	n.writeFile(t, "scratch.txt", "bbb") // different content, same size
	n.scanAndApply(t)
	scratch, _, _ := n.ix.Get("scratch.txt")
	target := scratch.Clone()
	target.Name = "dir2/file.txt"
	target.Version = version.New().Bump(1).Bump(2).Bump(2)
	tomb := &index.FileInfo{Name: "dir1/file.txt", Deleted: true, Version: version.New().Bump(1).Bump(2).Bump(2)}
	n.engine.SetPeerIndex(2, []*index.FileInfo{tomb, target}, false)

	moves, err := n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 0 {
		t.Fatalf("moved+edited file should not pair: %+v", moves)
	}
}

func TestPlanMovesTargetExists(t *testing.T) {
	// Target already present locally → not a move (concurrent local change).
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "dir1/file.txt", "same")
	n.writeFile(t, "dir2/file.txt", "same")
	n.scanAndApply(t)
	local, _, _ := n.ix.Get("dir1/file.txt")
	target := local.Clone()
	target.Name = "dir2/file.txt"
	target.Version = version.New().Bump(1).Bump(2).Bump(2)
	tomb := &index.FileInfo{Name: "dir1/file.txt", Deleted: true, Version: version.New().Bump(1).Bump(2).Bump(2)}
	n.engine.SetPeerIndex(2, []*index.FileInfo{tomb, target}, false)

	moves, err := n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 0 {
		t.Fatalf("existing target should not pair: %+v", moves)
	}
}

func TestPlanMovesDirsExcluded(t *testing.T) {
	// Directory moves carry no content — never paired (created/adopted cheaply).
	n := newNode(t, 1, "f", "conflict-copy", nil)
	_ = os.MkdirAll(filepath.Join(n.root, "dir1"), 0o755)
	n.writeFile(t, "dir1/a.txt", "x")
	n.scanAndApply(t)
	dirTomb := &index.FileInfo{Name: "dir1", Type: index.TypeDirectory, Deleted: true, Version: version.New().Bump(1).Bump(2).Bump(2)}
	dirAdd := &index.FileInfo{Name: "dir2", Type: index.TypeDirectory, Version: version.New().Bump(1).Bump(2).Bump(2)}
	n.engine.SetPeerIndex(2, []*index.FileInfo{dirTomb, dirAdd}, false)

	moves, err := n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 0 {
		t.Fatalf("directory moves should not pair: %+v", moves)
	}
}

func TestApplyMove(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "dir1/file.txt", "move me")
	n.scanAndApply(t)
	_, target := movePeerView(t, n, "dir1/file.txt", "dir2/file.txt")

	moves, err := n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 1 {
		t.Fatalf("moves = %d, want 1", len(moves))
	}
	if err := n.engine.ApplyMove(moves[0]); err != nil {
		t.Fatal(err)
	}

	// Disk: content at the new path, old path gone.
	if got := n.fileContent(t, "dir2/file.txt"); got != "move me" {
		t.Fatalf("moved content = %q", got)
	}
	if _, err := os.Stat(filepath.Join(n.root, "dir1", "file.txt")); !os.IsNotExist(err) {
		t.Fatal("old path should be gone after the move")
	}
	// Index: To live with the peer's version; From tombstoned.
	to, ok, _ := n.ix.Get("dir2/file.txt")
	if !ok || to.Deleted {
		t.Fatal("To should be live in the index")
	}
	if !to.Version.Equal(target.Version) {
		t.Fatalf("To version = %v, want %v", to.Version, target.Version)
	}
	from, ok, _ := n.ix.Get("dir1/file.txt")
	if !ok || !from.Deleted {
		t.Fatal("From should be a tombstone in the index")
	}
	// Converged: nothing pending for either path.
	dels, err := n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) != 0 {
		t.Fatalf("pending deletions after move = %v", dels)
	}
	needs, err := n.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 0 {
		t.Fatalf("needs after move = %v", needs)
	}
}

func TestApplyMoveStaleSource(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "dir1/file.txt", "original")
	n.scanAndApply(t)
	movePeerView(t, n, "dir1/file.txt", "dir2/file.txt")
	moves, err := n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 1 {
		t.Fatalf("expected one move, got %d", len(moves))
	}
	// The source changes after planning — the plan is stale.
	n.writeFile(t, "dir1/file.txt", "changed")
	n.scanAndApply(t)
	if err := n.engine.ApplyMove(moves[0]); err == nil {
		t.Fatal("stale move should be rejected")
	}
	if _, err := os.Stat(filepath.Join(n.root, "dir1", "file.txt")); err != nil {
		t.Fatal("source should remain after a rejected move")
	}
	if _, err := os.Stat(filepath.Join(n.root, "dir2", "file.txt")); !os.IsNotExist(err) {
		t.Fatal("target should not exist after a rejected move")
	}
}

func TestApplyMoveTargetExists(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "dir1/file.txt", "same")
	n.scanAndApply(t)
	movePeerView(t, n, "dir1/file.txt", "dir2/file.txt")
	moves, err := n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 1 {
		t.Fatalf("expected one move, got %d", len(moves))
	}
	// A concurrent local file appears at the target before we apply.
	n.writeFile(t, "dir2/file.txt", "mine")
	n.scanAndApply(t)
	if err := n.engine.ApplyMove(moves[0]); err == nil {
		t.Fatal("move onto an existing target should be rejected")
	}
	if _, err := os.Stat(filepath.Join(n.root, "dir1", "file.txt")); err != nil {
		t.Fatal("source should remain after a rejected move")
	}
}

func TestVersioningArchivesOnRemoteReplace(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", &Versioning{Type: "trashcan", CleanDays: 30})
	b := newNode(t, 2, "f", "conflict-copy", nil)

	// A has an old version; B (remote) has a newer one.
	a.writeFile(t, "v.txt", "v1-old")
	a.scanAndApply(t)
	b.writeFile(t, "v.txt", "v2-new")
	b.scanAndApply(t)

	afi, _, _ := a.ix.Get("v.txt")
	bfi, _, _ := b.ix.Get("v.txt")
	a.engine.SetPeerIndex(2, []*index.FileInfo{bfi}, false)
	b.engine.SetPeerIndex(1, []*index.FileInfo{afi}, false)

	plan, err := a.engine.PlanOverwrite("v.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !plan.ArchiveLocal {
		t.Fatal("expected versioning archive")
	}
	if _, err := a.engine.Execute(plan); err != nil {
		t.Fatal(err)
	}
	// Old version must now be in the versions area.
	archived, err := os.ReadFile(filepath.Join(a.root, VersionsDir, "v.txt"))
	if err != nil {
		t.Fatalf("old version not archived: %v", err)
	}
	if string(archived) != "v1-old" {
		t.Fatalf("archived = %q, want v1-old", archived)
	}
}

func TestReservedNamespaceInIndex(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	// Confirm the versions dir name is reserved (never scanned/synced).
	if !staging.IsReserved(VersionsDir) {
		t.Fatalf("%s must be reserved", VersionsDir)
	}
	_ = n
}

// TestApplyLocalChangesBatch verifies a mixed change set (modify + delete +
// add + rename) is applied in one batch with the same result as the
// per-change path, and that a rename's live entry gets a fresh sequence.
func TestApplyLocalChangesBatch(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "aaa")
	n.writeFile(t, "b.txt", "bbbbbb")
	n.writeFile(t, "d.txt", "dddddddd")
	n.scanAndApply(t)

	// Modify a, delete b, add c, and rename d -> e, all before one scan.
	n.writeFile(t, "a.txt", "aaaaa")
	if err := os.Remove(filepath.Join(n.root, "b.txt")); err != nil {
		t.Fatal(err)
	}
	n.writeFile(t, "c.txt", "cccc")
	if err := os.Rename(filepath.Join(n.root, "d.txt"), filepath.Join(n.root, "e.txt")); err != nil {
		t.Fatal(err)
	}
	s := scanner.New(n.root, n.ix, n.engine.Ignore)
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	applied, failed, err := n.engine.ApplyLocalChanges(changes)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 0 {
		t.Fatalf("unexpected failed changes: %+v", failed)
	}
	if applied != len(changes) {
		t.Fatalf("applied = %d, want %d", applied, len(changes))
	}
	// a modified (live, new size)
	fi, ok, _ := n.ix.Get("a.txt")
	if !ok || fi.Deleted || fi.Size != 5 {
		t.Fatalf("a.txt = %+v (ok=%v), want live size 5", fi, ok)
	}
	// b tombstoned
	fi, ok, _ = n.ix.Get("b.txt")
	if !ok || !fi.Deleted {
		t.Fatalf("b.txt = %+v (ok=%v), want tombstone", fi, ok)
	}
	// c added
	fi, ok, _ = n.ix.Get("c.txt")
	if !ok || fi.Deleted || fi.Size != 4 {
		t.Fatalf("c.txt = %+v (ok=%v), want live size 4", fi, ok)
	}
	// e live, d tombstoned, and the live entry got a FRESH sequence (a stale
	// one would fall below a peer's acknowledged max and be missed by a
	// delta exchange).
	eFi, ok, _ := n.ix.Get("e.txt")
	if !ok || eFi.Deleted {
		t.Fatalf("e.txt = %+v (ok=%v), want live", eFi, ok)
	}
	dFi, ok, _ := n.ix.Get("d.txt")
	if !ok || !dFi.Deleted {
		t.Fatalf("d.txt = %+v (ok=%v), want tombstone", dFi, ok)
	}
	if eFi.Sequence == 0 || eFi.Sequence == dFi.Sequence {
		t.Fatalf("e.txt sequence %d must be fresh (tombstone seq %d)", eFi.Sequence, dFi.Sequence)
	}
}

// TestApplyLocalChangesRejectsStaleRename verifies a rename whose target
// already exists is rejected by the batch and left for the per-change
// fallback path.
func TestApplyLocalChangesRejectsStaleRename(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "same")
	n.writeFile(t, "b.txt", "same")
	n.scanAndApply(t)
	fib, ok, _ := n.ix.Get("b.txt")
	if !ok {
		t.Fatal("b.txt should be indexed")
	}
	// Rename a -> b, but b already exists: the batch must reject it.
	stale := scanner.Change{Kind: scanner.Renamed, From: "a.txt", Info: fib.Clone()}
	applied, failed, err := n.engine.ApplyLocalChanges([]scanner.Change{stale})
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 || len(failed) != 1 {
		t.Fatalf("applied=%d failed=%v, want 1 rejected", applied, failed)
	}
	// Nothing changed: both still live.
	if fi, ok, _ := n.ix.Get("a.txt"); !ok || fi.Deleted {
		t.Fatal("a.txt should still be live")
	}
	if fi, ok, _ := n.ix.Get("b.txt"); !ok || fi.Deleted {
		t.Fatal("b.txt should still be live")
	}
}

// TestApplyLocalChangesCollapsesManyRenames verifies that a large local
// rename (a folder move) emits ONE summary event instead of flooding the
// event store with a per-file event, mirroring emitMoves on the remote side.
func TestApplyLocalChangesCollapsesManyRenames(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	const count = 60
	var captured []*events.Event
	n.engine.OnEvent = func(ev *events.Event) { captured = append(captured, ev) }
	for i := 0; i < count; i++ {
		n.writeFile(t, fmt.Sprintf("f%d.txt", i), fmt.Sprintf("content-%d", i))
	}
	n.scanAndApply(t)
	captured = nil // drop the initial "local add" events

	changes := make([]scanner.Change, 0, count)
	for i := 0; i < count; i++ {
		fi, ok, _ := n.ix.Get(fmt.Sprintf("f%d.txt", i))
		if !ok {
			t.Fatalf("f%d.txt missing", i)
		}
		to := fi.Clone()
		to.Name = fmt.Sprintf("g%d.txt", i)
		changes = append(changes, scanner.Change{Kind: scanner.Renamed, From: fi.Name, Info: to})
	}
	applied, failed, err := n.engine.ApplyLocalChanges(changes)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 0 {
		t.Fatalf("unexpected failed: %+v", failed)
	}
	if applied != count {
		t.Fatalf("applied = %d, want %d", applied, count)
	}
	if len(captured) != 1 {
		t.Fatalf("events = %d, want 1 collapsed summary; %+v", len(captured), captured)
	}
	if !strings.Contains(captured[0].Reason, "moved 60 file(s)") {
		t.Fatalf("summary reason = %q", captured[0].Reason)
	}
}

// TestApplyLocalChangesChunked verifies the batch is committed in bounded
// transactions (the chunk loop), not one unbounded transaction, and that
// every rename still lands with a fresh sequence.
func TestApplyLocalChangesChunked(t *testing.T) {
	old := batchChunk
	batchChunk = 2
	t.Cleanup(func() { batchChunk = old })
	n := newNode(t, 1, "f", "conflict-copy", nil)
	for i := 0; i < 5; i++ {
		n.writeFile(t, fmt.Sprintf("f%d.txt", i), fmt.Sprintf("content-%d", i))
	}
	n.scanAndApply(t)
	changes := make([]scanner.Change, 0, 5)
	for i := 0; i < 5; i++ {
		fi, ok, _ := n.ix.Get(fmt.Sprintf("f%d.txt", i))
		if !ok {
			t.Fatalf("f%d.txt missing", i)
		}
		to := fi.Clone()
		to.Name = fmt.Sprintf("g%d.txt", i)
		changes = append(changes, scanner.Change{Kind: scanner.Renamed, From: fi.Name, Info: to})
	}
	applied, failed, err := n.engine.ApplyLocalChanges(changes)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 0 || applied != 5 {
		t.Fatalf("applied=%d failed=%v, want 5 applied", applied, failed)
	}
	seen := map[int64]bool{}
	for i := 0; i < 5; i++ {
		fi, ok, _ := n.ix.Get(fmt.Sprintf("g%d.txt", i))
		if !ok || fi.Deleted {
			t.Fatalf("g%d.txt = %+v (ok=%v), want live", i, fi, ok)
		}
		if fi.Sequence == 0 || seen[fi.Sequence] {
			t.Fatalf("g%d.txt: bad sequence %d", i, fi.Sequence)
		}
		seen[fi.Sequence] = true
	}
}

// TestNeedPullsIncludeFilter verifies the include gate covers only the
// acquire path: a name this device does not own is pulled only when it is in
// scope, while an owned entry keeps following a peer's newer version.
func TestNeedPullsIncludeFilter(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	inc, err := ignore.Parse([]string{"*.mp4"})
	if err != nil {
		t.Fatal(err)
	}
	n.engine.Include = inc

	// y.mov's version is a descendant of the local one below ({1:1}), so the
	// peer causally dominates it — the owned-entry case.
	n.engine.SetPeerIndex(2, []*index.FileInfo{
		{Name: "x.mp4", Size: 3, Type: index.TypeFile, Version: version.New().Bump(2)},
		{Name: "y.mov", Size: 5, Type: index.TypeFile, Version: version.New().Bump(1).Bump(2)},
	}, false)

	needs, err := n.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 1 || needs[0] != "x.mp4" {
		t.Fatalf("NeedPulls = %v, want [x.mp4]", needs)
	}

	// y.mov is already owned locally at an older version: ownership wins over
	// the include gate, so the peer's newer version is still pulled.
	if err := n.ix.Put(&index.FileInfo{
		Name: "y.mov", Size: 3, Type: index.TypeFile, Version: version.New().Bump(1),
	}); err != nil {
		t.Fatal(err)
	}
	needs, err = n.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 2 || needs[0] != "x.mp4" || needs[1] != "y.mov" {
		t.Fatalf("NeedPulls = %v, want [x.mp4 y.mov] (owned entries stay followed)", needs)
	}
}

// TestSendOnlyNeverPulls verifies a send-only engine serves its own changes
// but never acquires from peers: the peer view still merges (the engine needs
// it to know what to serve and to compute globals), yet nothing the peer has
// — file or tombstone — ever becomes a pull.
func TestSendOnlyNeverPulls(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.engine.Direction = "send-only"

	// Local changes are still indexed in send-only: they are what the device
	// serves to peers.
	n.writeFile(t, "local.txt", "mine")
	n.scanAndApply(t)
	if _, ok, err := n.ix.Get("local.txt"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("send-only must still index its own local changes")
	}

	n.engine.SetPeerIndex(2, []*index.FileInfo{
		{Name: "remote.txt", Size: 3, Type: index.TypeFile, Version: version.New().Bump(2)},
	}, false)
	// The peer view is still merged: the engine needs it to serve and to
	// keep the global model coherent.
	if got := n.engine.PeerCount(2); got != 1 {
		t.Fatalf("peer view entries = %d, want 1 (send-only still merges peer state)", got)
	}

	needs, err := n.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 0 {
		t.Fatalf("NeedPulls = %v, want empty in send-only", needs)
	}

	// A peer deletion of a name we do hold is not applied either: remote
	// tombstones never reach the pull list.
	n.engine.SetPeerIndex(2, []*index.FileInfo{
		{Name: "local.txt", Deleted: true, Type: index.TypeFile, Version: version.New().Bump(1).Bump(2).Bump(2)},
	}, false)
	needs, err = n.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 0 {
		t.Fatalf("NeedPulls = %v, want empty for a peer deletion in send-only", needs)
	}
	// Move detection is part of the acquire path too: a plan that would
	// relocate local.txt to moved.txt (peer deleted the one, added the
	// other with identical content) must come back empty in send-only, so
	// nothing is ever renamed locally on a peer's behalf.
	local, ok, err := n.ix.Get("local.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("local.txt should be indexed")
	}
	peerLive := local.Clone()
	peerLive.Name = "moved.txt"
	peerLive.Version = version.New().Bump(2)
	tomb := &index.FileInfo{Name: "local.txt", Type: index.TypeFile, Deleted: true,
		Version: version.New().Bump(2).Bump(2)}
	n.engine.SetPeerIndex(2, []*index.FileInfo{tomb, peerLive}, false)
	moves, err := n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 0 {
		t.Fatalf("PlanMoves = %v, want none in send-only", moves)
	}
}

// TestReceiveOnlyIgnoresLocalChanges verifies a receive-only engine discards
// scanner changes (nothing is indexed, so nothing is advertised) while an
// entry owned before the mode was set keeps following peer state.
func TestReceiveOnlyIgnoresLocalChanges(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	// Indexed before the mode was set: ownership beats the direction gate,
	// exactly like the include filter's owned-entry rule.
	if err := n.ix.Put(&index.FileInfo{
		Name: "owned.txt", Size: 3, Type: index.TypeFile, Version: version.New().Bump(1),
	}); err != nil {
		t.Fatal(err)
	}
	n.engine.Direction = "receive-only"

	n.writeFile(t, "new.txt", "local only")
	s := scanner.New(n.root, n.ix, n.engine.Ignore)
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	applied, failed, err := n.engine.ApplyLocalChanges(changes)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 || failed != nil {
		t.Fatalf("ApplyLocalChanges = (%d, %v), want (0, nil) in receive-only", applied, failed)
	}
	if _, ok, err := n.ix.Get("new.txt"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("receive-only must not index a local change")
	}
	// The file itself is untouched on disk: the change is ignored, not
	// reverted.
	if got := n.fileContent(t, "new.txt"); got != "local only" {
		t.Fatalf("local file content = %q, want %q", got, "local only")
	}

	// The pre-existing entry is still followed: a peer's newer version of it
	// is a pull.
	n.engine.SetPeerIndex(2, []*index.FileInfo{
		{Name: "owned.txt", Size: 3, Type: index.TypeFile, Version: version.New().Bump(1).Bump(2)},
	}, false)
	needs, err := n.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 1 || needs[0] != "owned.txt" {
		t.Fatalf("NeedPulls = %v, want [owned.txt] (owned entries stay followed)", needs)
	}
}

// TestSendOnlyNoPendingDeletions verifies a send-only engine reports no
// pending deletions even when a peer has authoritatively tombstoned a file it
// holds: send-only never applies remote deletions, so neither the session's
// delete pass nor the operator override may see any.
func TestSendOnlyNoPendingDeletions(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "keep.txt", "keep")
	n.writeFile(t, "gone.txt", "gone")
	n.scanAndApply(t)

	keep, ok, err := n.ix.Get("keep.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("keep.txt should be indexed")
	}
	// The peer adopted our {1:1} for gone.txt and bumped its own counter, so
	// its tombstone dominates our live entry.
	tomb := &index.FileInfo{Name: "gone.txt", Deleted: true, Type: index.TypeFile,
		Version: version.New().Bump(1).Bump(2).Bump(2)}
	n.engine.SetPeerIndex(2, []*index.FileInfo{keep, tomb}, false)

	n.engine.Direction = "send-only"
	dels, err := n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) != 0 {
		t.Fatalf("PendingDeletions = %v, want empty in send-only", dels)
	}

	// Contrast: the same peer state IS pending in the default direction, so
	// the empty result above comes from the gate, not from a fixture that
	// never matched.
	n.engine.Direction = ""
	dels, err = n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) != 1 || dels[0] != "gone.txt" {
		t.Fatalf("PendingDeletions = %v, want [gone.txt] in send-receive", dels)
	}
}

// TestReceiveOnlyIgnoresRebuildPath verifies the single-change entry points
// used by the corrupt-index rebuild (rebuildFromFS) honor receive-only: an
// add and a rename both leave no index entry, so a rebuild finishes with an
// empty index instead of indexing the local tree.
func TestReceiveOnlyIgnoresRebuildPath(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.engine.Direction = "receive-only"

	n.writeFile(t, "local.txt", "local only")
	fi := &index.FileInfo{Name: "local.txt", Size: 10, Type: index.TypeFile, Mode: 0o644}
	if got, err := n.engine.ApplyLocalChange(scanner.Added, fi); err != nil {
		t.Fatal(err)
	} else if got != nil {
		t.Fatalf("ApplyLocalChange = %+v, want nil in receive-only", got)
	}
	if _, ok, err := n.ix.Get("local.txt"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("receive-only must not index a local add on the rebuild path")
	}

	// A rename would tombstone the source and adopt the target; both must
	// stay unindexed.
	n.writeFile(t, "renamed.txt", "local only")
	to := &index.FileInfo{Name: "renamed.txt", Size: 10, Type: index.TypeFile, Mode: 0o644}
	if got, err := n.engine.ApplyLocalRename("local.txt", to); err != nil {
		t.Fatal(err)
	} else if got != nil {
		t.Fatalf("ApplyLocalRename = %+v, want nil in receive-only", got)
	}
	for _, name := range []string{"local.txt", "renamed.txt"} {
		if _, ok, err := n.ix.Get(name); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Fatalf("receive-only must not index %q on the rebuild path", name)
		}
	}
	// The files themselves are untouched on disk: ignored, not reverted.
	if got := n.fileContent(t, "renamed.txt"); got != "local only" {
		t.Fatalf("local file content = %q, want %q", got, "local only")
	}
}
