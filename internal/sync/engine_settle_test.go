package sync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crosssync/internal/index"
	"crosssync/internal/scanner"
	"crosssync/internal/version"
)

// holdsInFlight marks one name as held back by the stability gate.
func holdsInFlight(e *Engine, names ...string) {
	list := make([]scanner.InFlight, 0, len(names))
	for _, n := range names {
		list = append(list, scanner.InFlight{Name: n, Size: 1, Scans: 1})
	}
	e.SetInFlight(list)
}

// TestInFlightExcludedFromNeedPulls verifies a file being written locally is
// never pulled over — the gate removes it from the pull set until it settles.
func TestInFlightExcludedFromNeedPulls(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "doc.txt", "v1")
	n.scanAndApply(t)

	cur, ok, _ := n.ix.Get("doc.txt")
	if !ok {
		t.Fatal("doc.txt should be indexed")
	}
	peer := cur.Clone()
	peer.Version = cur.Version.Bump(1).Bump(2) // strictly dominates our copy
	n.engine.SetPeerIndex(2, []*index.FileInfo{peer}, false)

	needs, err := n.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 1 || needs[0] != "doc.txt" {
		t.Fatalf("NeedPulls = %v, want [doc.txt]", needs)
	}

	holdsInFlight(n.engine, "doc.txt")
	needs, err = n.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 0 {
		t.Fatalf("NeedPulls with doc.txt in flight = %v, want empty", needs)
	}

	n.engine.SetInFlight(nil)
	needs, err = n.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 1 || needs[0] != "doc.txt" {
		t.Fatalf("NeedPulls after clearing the in-flight set = %v, want [doc.txt]", needs)
	}
}

// TestInFlightExcludedFromPendingUploads verifies a file being written is not
// reported as pending upload work (it is counted as "held back" instead).
func TestInFlightExcludedFromPendingUploads(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "up.txt", "data")
	n.scanAndApply(t)
	// An empty peer view: everything local is pending upload.
	n.engine.SetPeerIndex(2, nil, false)

	up, err := n.engine.PendingUploads()
	if err != nil {
		t.Fatal(err)
	}
	if len(up) != 1 || up[0] != "up.txt" {
		t.Fatalf("PendingUploads = %v, want [up.txt]", up)
	}

	holdsInFlight(n.engine, "up.txt")
	up, err = n.engine.PendingUploads()
	if err != nil {
		t.Fatal(err)
	}
	if len(up) != 0 {
		t.Fatalf("PendingUploads while in flight = %v, want empty", up)
	}

	n.engine.SetInFlight(nil)
	up, err = n.engine.PendingUploads()
	if err != nil {
		t.Fatal(err)
	}
	if len(up) != 1 || up[0] != "up.txt" {
		t.Fatalf("PendingUploads after clearing = %v, want [up.txt]", up)
	}
}

// TestInFlightExcludedFromPendingDeletions verifies a file being written is
// never deleted on a peer's behalf: the deletion is re-evaluated once the file
// settles.
func TestInFlightExcludedFromPendingDeletions(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "gone.txt", "x")
	n.scanAndApply(t)

	cur, ok, _ := n.ix.Get("gone.txt")
	if !ok {
		t.Fatal("gone.txt should be indexed")
	}
	tomb := &index.FileInfo{Name: "gone.txt", Type: index.TypeFile, Deleted: true,
		Version: cur.Version.Bump(2)}
	n.engine.SetPeerIndex(2, []*index.FileInfo{tomb}, false)

	dels, err := n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) != 1 || dels[0] != "gone.txt" {
		t.Fatalf("PendingDeletions = %v, want [gone.txt]", dels)
	}

	holdsInFlight(n.engine, "gone.txt")
	dels, err = n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) != 0 {
		t.Fatalf("PendingDeletions while in flight = %v, want empty", dels)
	}

	n.engine.SetInFlight(nil)
	dels, err = n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	if len(dels) != 1 || dels[0] != "gone.txt" {
		t.Fatalf("PendingDeletions after clearing = %v, want [gone.txt]", dels)
	}
}

// TestInFlightMoveNotPlanned verifies a same-content move is not planned for a
// file that is being written: PlanMoves would rename the local file out from
// under the writer.
func TestInFlightMoveNotPlanned(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "a.txt", "move-me")
	n.scanAndApply(t)

	cur, ok, _ := n.ix.Get("a.txt")
	if !ok {
		t.Fatal("a.txt should be indexed")
	}
	ver := cur.Version.Bump(2)
	tombA := &index.FileInfo{Name: "a.txt", Type: index.TypeFile, Deleted: true, Version: ver}
	bEntry := cur.Clone()
	bEntry.Name = "b.txt"
	bEntry.Version = ver
	n.engine.SetPeerIndex(2, []*index.FileInfo{tombA, bEntry}, false)

	moves, err := n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 1 || moves[0].From != "a.txt" || moves[0].To != "b.txt" {
		t.Fatalf("PlanMoves = %+v, want one a.txt -> b.txt move", moves)
	}

	holdsInFlight(n.engine, "a.txt")
	moves, err = n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 0 {
		t.Fatalf("PlanMoves while a.txt is in flight = %+v, want none", moves)
	}

	n.engine.SetInFlight(nil)
	moves, err = n.engine.PlanMoves(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(moves) != 1 {
		t.Fatalf("PlanMoves after clearing = %+v, want one move", moves)
	}
}

// fetchAll copies every missing block of a pull out of the source node's tree,
// exactly as the session does over the wire.
func fetchAll(t *testing.T, srcRoot, name string, pull *Pull) {
	t.Helper()
	src, err := os.Open(filepath.Join(srcRoot, filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	for _, need := range pull.NeedBlocks() {
		buf := make([]byte, need.Size)
		if _, err := src.ReadAt(buf, need.Offset); err != nil {
			t.Fatal(err)
		}
		if err := pull.ReceiveBlock(need.Index, buf); err != nil {
			t.Fatal(err)
		}
	}
}

// conflictCopies returns the conflict-copy names under root.
func conflictCopies(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.Contains(d.Name(), ConflictSuffix) {
			out = append(out, d.Name())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestConvergesOverNewerMtimePartial is the case-3 analysis end to end: the
// source holds the FINAL version with an OLD (restored) mtime, the receiver
// holds a PARTIAL with a NEWER mtime. The version vector — not mtime — picks
// the winner, so the partial is replaced with no conflict copy and nothing is
// bounced back to the source.
func TestConvergesOverNewerMtimePartial(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	final := multiBlockContent(300_000)
	partial := final[:220_000]
	a.writeFile(t, "x", final)
	a.scanAndApply(t)

	// The copy tool restored the source's original, older date, and the
	// source has since settled four versions deep.
	aMT := time.Date(2019, 3, 4, 5, 6, 7, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(a.root, "x"), aMT, aMT); err != nil {
		t.Fatal(err)
	}
	aFi, ok, _ := a.ix.Get("x")
	if !ok {
		t.Fatal("a should have indexed x")
	}
	aFi.ModifiedS, aFi.ModifiedNs = aMT.Unix(), 0
	aFi.Version = version.New().Bump(1).Bump(1).Bump(1).Bump(1) // {1:4}
	if err := a.ix.Put(aFi); err != nil {
		t.Fatal(err)
	}

	// B holds a truncated copy with a NEWER mtime, adopted at {1:3}.
	b.writeFile(t, "x", partial)
	bFi := &index.FileInfo{
		Name: "x", Size: int64(len(partial)), Type: index.TypeFile,
		ModifiedS: time.Now().Add(2 * time.Hour).Unix(), ModifiedNs: 0,
		Version: version.New().Bump(1).Bump(1).Bump(1), // {1:3}
	}
	if err := b.ix.Put(bFi); err != nil {
		t.Fatal(err)
	}
	b.engine.SetPeerIndex(1, []*index.FileInfo{aFi}, false)

	needs, err := b.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 1 || needs[0] != "x" {
		t.Fatalf("B NeedPulls = %v, want [x]", needs)
	}

	plan, err := b.engine.PlanOverwrite("x")
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Global.Version.Equal(aFi.Version) || plan.Global.Size != int64(len(final)) {
		t.Fatalf("plan winner = %+v, want the source's final entry", plan.Global)
	}
	if plan.MakeConflictCopy || plan.ArchiveLocal {
		t.Fatalf("plan = (conflict=%v archive=%v), want neither: the partial is causally older",
			plan.MakeConflictCopy, plan.ArchiveLocal)
	}

	if _, err := b.engine.Execute(plan); err != nil {
		t.Fatal(err)
	}
	pull, err := b.engine.StartPull("x", 1)
	if err != nil {
		t.Fatal(err)
	}
	fetchAll(t, a.root, "x", pull)
	if _, err := pull.Finish(); err != nil {
		t.Fatal(err)
	}

	if got := b.fileContent(t, "x"); got != final {
		t.Fatalf("B[x] length %d, want the final %d bytes", len(got), len(final))
	}
	if copies := conflictCopies(t, b.root); len(copies) != 0 {
		t.Fatalf("no conflict copy should be created for a causally-older partial, got %v", copies)
	}
	got, ok, _ := b.ix.Get("x")
	if !ok {
		t.Fatal("B should have indexed x")
	}
	if !got.Version.Equal(aFi.Version) {
		t.Fatalf("B x version = %v, want the source's %v", got.Version, aFi.Version)
	}
	// Nothing bounces back: B has no local change left to advertise.
	if needs, _ := b.engine.NeedPulls(); len(needs) != 0 {
		t.Fatalf("B still needs %v after converging", needs)
	}
}

// TestGenuinelyDivergentPartialPreservesLoser verifies the safety net still
// fires: when the two sides really did diverge (concurrent vectors), the loser
// is preserved as a conflict copy before the winner is written.
func TestGenuinelyDivergentPartialPreservesLoser(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	final := multiBlockContent(200_000)
	divergent := multiBlockContent(90_000)
	a.writeFile(t, "x", final)
	a.scanAndApply(t)

	aMT := time.Date(2019, 3, 4, 5, 6, 7, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(a.root, "x"), aMT, aMT); err != nil {
		t.Fatal(err)
	}
	aFi, ok, _ := a.ix.Get("x")
	if !ok {
		t.Fatal("a should have indexed x")
	}
	aFi.ModifiedS, aFi.ModifiedNs = aMT.Unix(), 0
	aFi.Version = version.New().Bump(1).Bump(1).Bump(1).Bump(1) // {1:4}
	if err := a.ix.Put(aFi); err != nil {
		t.Fatal(err)
	}

	// B edited its own copy from the same base {1:3}: a concurrent version
	// with different content and an older mtime (so A stays the winner).
	b.writeFile(t, "x", divergent)
	bMT := time.Date(2018, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(b.root, "x"), bMT, bMT); err != nil {
		t.Fatal(err)
	}
	bFi := &index.FileInfo{
		Name: "x", Size: int64(len(divergent)), Type: index.TypeFile,
		ModifiedS: bMT.Unix(), ModifiedNs: 0,
		Version: version.New().Bump(1).Bump(1).Bump(1).Bump(2), // {1:3, 2:1}
	}
	if err := b.ix.Put(bFi); err != nil {
		t.Fatal(err)
	}
	b.engine.SetPeerIndex(1, []*index.FileInfo{aFi}, false)

	plan, err := b.engine.PlanOverwrite("x")
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Global.Version.Equal(aFi.Version) {
		t.Fatalf("plan winner = %v, want the source's %v", plan.Global.Version, aFi.Version)
	}
	if !plan.MakeConflictCopy {
		t.Fatal("a genuinely concurrent version must produce a conflict copy")
	}
	if _, err := b.engine.Execute(plan); err != nil {
		t.Fatal(err)
	}
	pull, err := b.engine.StartPull("x", 1)
	if err != nil {
		t.Fatal(err)
	}
	fetchAll(t, a.root, "x", pull)
	if _, err := pull.Finish(); err != nil {
		t.Fatal(err)
	}

	if got := b.fileContent(t, "x"); got != final {
		t.Fatalf("B[x] should hold the winner (%d bytes), got %d", len(final), len(got))
	}
	copies := conflictCopies(t, b.root)
	if len(copies) != 1 {
		t.Fatalf("conflict copies = %v, want exactly one", copies)
	}
	if got := b.fileContent(t, copies[0]); got != divergent {
		t.Fatalf("conflict copy should preserve B's bytes (%d), got %d", len(divergent), len(got))
	}
}

// TestPullFinishRecordsObservedStat verifies truthful adoption: the index
// records the stat the filesystem reports for the file we just committed, not
// the stat the peer claimed — which is what stops the next local scan from
// bumping the vector and manufacturing a version concurrent with the source's.
func TestPullFinishRecordsObservedStat(t *testing.T) {
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)

	content := "measured on the receiver"
	a.writeFile(t, "x", content)
	a.scanAndApply(t)

	aFi, ok, _ := a.ix.Get("x")
	if !ok {
		t.Fatal("a should have indexed x")
	}
	// A nanosecond remainder the receiving filesystem may not be able to
	// represent (coarse-mtime share): the exact hazard from the design.
	aFi.ModifiedS, aFi.ModifiedNs = 1700000000, 999999999
	if err := a.ix.Put(aFi); err != nil {
		t.Fatal(err)
	}
	b.engine.SetPeerIndex(1, []*index.FileInfo{aFi}, false)

	pull, err := b.engine.StartPull("x", 1)
	if err != nil {
		t.Fatal(err)
	}
	fetchAll(t, a.root, "x", pull)
	if _, err := pull.Finish(); err != nil {
		t.Fatal(err)
	}

	st, err := os.Stat(filepath.Join(b.root, "x"))
	if err != nil {
		t.Fatal(err)
	}
	got, ok, _ := b.ix.Get("x")
	if !ok {
		t.Fatal("b should have indexed x")
	}
	if got.Size != st.Size() || got.ModifiedS != st.ModTime().Unix() || got.ModifiedNs != int32(st.ModTime().Nanosecond()) {
		t.Fatalf("indexed stat = (size %d, %d.%d), want the observed (size %d, %d.%d)",
			got.Size, got.ModifiedS, got.ModifiedNs, st.Size(), st.ModTime().Unix(), st.ModTime().Nanosecond())
	}

	// The next scan must find the file unchanged: no bump, so B never creates
	// a version concurrent with the source's.
	before := got.Version.Clone()
	b.scanAndApply(t)
	after, ok, _ := b.ix.Get("x")
	if !ok {
		t.Fatal("b should still have x indexed")
	}
	if !after.Version.Equal(before) {
		t.Fatalf("a completed pull must not bump the version on the next scan: before=%v after=%v",
			before, after.Version)
	}
}
