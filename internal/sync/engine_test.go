package sync

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
	t.Cleanup(func() { ix.Close() })
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
	os.Chtimes(p, future, future)
}

func (n *testNode) scanAndApply(t *testing.T) {
	t.Helper()
	s := scanner.New(n.root, n.ix, n.engine.Ignore)
	changes, err := s.Scan()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
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
	os.Remove(filepath.Join(n.root, "a.txt"))
	n.scanAndApply(t)
	fi, ok, _ = n.ix.Get("a.txt")
	if !ok || !fi.Deleted {
		t.Fatal("a.txt should be a tombstone")
	}
	if fi.Version.Get(1) != 3 {
		t.Fatalf("tombstone counter = %d, want 3", fi.Version.Get(1))
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
	af.Close()
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
	os.Chtimes(filepath.Join(a.root, "doc.txt"), older, older)
	os.Chtimes(filepath.Join(b.root, "doc.txt"), newer, newer)
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
		bf.ReadAt(buf, need.Offset)
		bf.Close()
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
	os.Chtimes(filepath.Join(a.root, "doc.txt"), older, older)
	os.Chtimes(filepath.Join(b.root, "doc.txt"), newer, newer)
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
		f.ReadAt(buf, need.Offset)
		f.Close()
		pull.ReceiveBlock(need.Index, buf)
	}
	pull.Finish()

	// A deletes the file.
	os.Remove(filepath.Join(a.root, "gone.txt"))
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
	os.MkdirAll(filepath.Join(n.root, "dir1"), 0o755)
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
