package sync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/scanner"
	"crosssync/internal/version"
)

// engineSeedStamp / engineEditStamp are the two mtimes the join tests use: a
// shared seed stamp (so two independent copies are byte- AND time-identical,
// exactly as when one corpus is generated on both boxes from the same spec)
// and a strictly newer stamp for a later edit.
var (
	engineSeedStamp = time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	engineEditStamp = engineSeedStamp.Add(48 * time.Hour)
)

// writeStamped writes rel under the node's root with a pinned mtime, so the
// second node can hold an identical (size, mtime) for the same bytes.
func (n *testNode) writeStamped(t *testing.T, rel, body string, mt time.Time) {
	t.Helper()
	p := filepath.Join(n.root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, mt, mt); err != nil {
		t.Fatal(err)
	}
}

// entry returns the index entry for name, failing the test when it is absent.
func (n *testNode) entry(t *testing.T, name string) *index.FileInfo {
	t.Helper()
	fi, ok, err := n.ix.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("%s is not indexed", name)
	}
	return fi
}

// seedIdenticalPair builds the reported state at the engine level: two nodes
// that each independently added the SAME file (identical bytes, identical
// mtime), so both indexes hold a local add and the vectors are CONCURRENT, and
// hands each engine the other's entry as its peer view.
func seedIdenticalPair(t *testing.T, name, body string) (*testNode, *testNode) {
	t.Helper()
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)
	a.writeStamped(t, name, body, engineSeedStamp)
	b.writeStamped(t, name, body, engineSeedStamp)
	a.scanAndApply(t)
	b.scanAndApply(t)
	exchangeViews(t, a, b)
	return a, b
}

// exchangeViews installs each node's current index entry as the other's peer
// view — what the index exchange at the start of every round does.
func exchangeViews(t *testing.T, a, b *testNode) {
	t.Helper()
	a.engine.SetPeerIndex(2, []*index.FileInfo{b.entry(t, "same.bin").Clone()}, false)
	b.engine.SetPeerIndex(1, []*index.FileInfo{a.entry(t, "same.bin").Clone()}, false)
}

// TestJoinIdenticalRepairsConcurrentIdenticalPair is the core of the fix, at
// the exact decision point. Two devices hold the same bytes at the same path
// with concurrent vectors. The join must fold both vectors into one that
// DOMINATES both sides of the pair — on each device, from that device's own
// local entry plus its peer view — without touching content, and afterwards a
// one-sided edit must be an ordinary fast-forward:
//
//   - NeedPulls lists the edit for the peer that is behind, and
//   - PlanOverwrite plans NO conflict copy, because the vectors are no longer
//     concurrent (the edit strictly descends from the joined version).
//
// On main the pair keeps its concurrent vectors forever, so the edit is
// classified concurrent with different content and becomes a conflict copy.
func TestJoinIdenticalRepairsConcurrentIdenticalPair(t *testing.T) {
	const name = "same.bin"
	body := strings.Repeat("seed:", 500)
	a, b := seedIdenticalPair(t, name, body)

	// The fixture really is the bug's precondition: concurrent, identical.
	if cmp := a.entry(t, name).Version.Compare(b.entry(t, name).Version); cmp != 2 {
		t.Fatalf("fixture must start with concurrent vectors, got compare=%d (%v vs %v)",
			cmp, a.entry(t, name).Version, b.entry(t, name).Version)
	}
	if !sameContent(a.entry(t, name), b.entry(t, name)) {
		t.Fatal("fixture must hold identical content")
	}

	// The round's join pass, on both devices independently.
	joinedA, err := a.engine.JoinIdentical()
	if err != nil {
		t.Fatal(err)
	}
	joinedB, err := b.engine.JoinIdentical()
	if err != nil {
		t.Fatal(err)
	}
	if joinedA != 1 || joinedB != 1 {
		t.Fatalf("each device must join the one identical pair: A=%d B=%d", joinedA, joinedB)
	}

	aj, bj := a.entry(t, name), b.entry(t, name)
	if cmp := aj.Version.Compare(bj.Version); cmp != 0 {
		t.Fatalf("after the join the pair must agree: A=%v B=%v (compare=%d)", aj.Version, bj.Version, cmp)
	}
	if aj.Version.Get(1) == 0 || aj.Version.Get(2) == 0 {
		t.Fatalf("joined vector must dominate BOTH seeds, got %v", aj.Version)
	}
	// Content, size, mtime and blocks are untouched: the join is bookkeeping.
	if aj.Size != bj.Size || aj.ModifiedS != bj.ModifiedS || aj.ModifiedNs != bj.ModifiedNs || !sameContent(aj, bj) {
		t.Fatalf("join changed metadata: A=%+v B=%+v", aj, bj)
	}
	if got := a.fileContent(t, name); got != body {
		t.Fatalf("join touched content: %d bytes, want %d", len(got), len(body))
	}
	// Nothing is pending in either direction once joined.
	for label, n := range map[string]*testNode{"A": a, "B": b} {
		if needs, err := n.engine.NeedPulls(); err != nil {
			t.Fatal(err)
		} else if len(needs) != 0 {
			t.Fatalf("%s still wants to pull after the join: %v", label, needs)
		}
	}
	// Idempotent: a second pass over the same state changes nothing.
	if again, err := a.engine.JoinIdentical(); err != nil {
		t.Fatal(err)
	} else if again != 0 {
		t.Fatalf("join is not idempotent: second pass joined %d", again)
	}

	// A edits. This is a strict descendant of the joined vector, so B must see
	// a plain fast-forward: one pull, no conflict copy.
	edited := strings.Repeat("A-edit:", 600)
	a.writeStamped(t, name, edited, engineEditStamp)
	a.scanAndApply(t)
	b.engine.SetPeerIndex(1, []*index.FileInfo{a.entry(t, name).Clone()}, false)

	needs, err := b.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 1 || needs[0] != name {
		t.Fatalf("B must pull the one-sided edit: %v", needs)
	}
	if cmp := a.entry(t, name).Version.Compare(b.entry(t, name).Version); cmp != 1 {
		t.Fatalf("the edit must strictly dominate B's entry, got compare=%d (%v vs %v)",
			cmp, a.entry(t, name).Version, b.entry(t, name).Version)
	}
	plan, err := b.engine.PlanOverwrite(name)
	if err != nil {
		t.Fatal(err)
	}
	if plan.MakeConflictCopy {
		t.Fatalf("one-sided edit after an identical seed must not make a conflict copy (would be %q)", plan.ConflictName)
	}
	if plan.Delete {
		t.Fatal("one-sided edit must not be planned as a deletion")
	}
}

// TestUnjoinedConcurrentIdenticalEditStillConflicts pins the fallback: when the
// join has NOT happened (an index from an older build, or a device that came
// back from offline and edited before its next round), the edit is still
// treated as a genuine conflict and preserved as a conflict copy. The join must
// be the ONLY thing that turns this into a fast-forward — never a blanket
// same-content shortcut.
func TestUnjoinedConcurrentIdenticalEditStillConflicts(t *testing.T) {
	const name = "same.bin"
	a, b := seedIdenticalPair(t, name, strings.Repeat("seed:", 500))

	a.writeStamped(t, name, strings.Repeat("A-edit:", 600), engineEditStamp)
	a.scanAndApply(t)
	b.engine.SetPeerIndex(1, []*index.FileInfo{a.entry(t, name).Clone()}, false)

	plan, err := b.engine.PlanOverwrite(name)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.MakeConflictCopy {
		t.Fatal("an unjoined concurrent pair with DIFFERENT content must still preserve the local edit as a conflict copy")
	}
	if plan.ConflictName == "" {
		t.Fatal("conflict plan without a conflict name")
	}
}

// TestJoinIdenticalLeavesDifferentContentAlone: concurrent entries whose bytes
// differ are a real conflict. The join must not touch them and must not make
// either side adopt the other's vector.
func TestJoinIdenticalLeavesDifferentContentAlone(t *testing.T) {
	const name = "same.bin"
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)
	a.writeStamped(t, name, strings.Repeat("AAA:", 400), engineEditStamp)
	b.writeStamped(t, name, strings.Repeat("BBB:", 400), engineSeedStamp)
	a.scanAndApply(t)
	b.scanAndApply(t)
	a.engine.SetPeerIndex(2, []*index.FileInfo{b.entry(t, name).Clone()}, false)
	b.engine.SetPeerIndex(1, []*index.FileInfo{a.entry(t, name).Clone()}, false)

	for _, n := range []*testNode{a, b} {
		if got, err := n.engine.JoinIdentical(); err != nil {
			t.Fatal(err)
		} else if got != 0 {
			t.Fatalf("join must skip entries with different content, joined %d", got)
		}
	}
	if cmp := a.entry(t, name).Version.Compare(b.entry(t, name).Version); cmp != 2 {
		t.Fatalf("versions were changed by the join: compare=%d", cmp)
	}
	plan, err := b.engine.PlanOverwrite(name)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.MakeConflictCopy {
		t.Fatal("a genuine concurrent edit must still plan a conflict copy")
	}
}

// TestJoinIdenticalAdoptsDominatedVersion covers the asymmetric case the round
// order can produce: one device has already joined, so its version strictly
// dominates the other's while the content is identical. The lagging device must
// adopt the joined vector as bookkeeping — no pull, no re-download of bytes it
// already has — because a single device joining alone would otherwise leave the
// pair's vectors apart until the next edit, and that edit would look concurrent.
func TestJoinIdenticalAdoptsDominatedVersion(t *testing.T) {
	const name = "same.bin"
	body := strings.Repeat("seed:", 500)
	a := newNode(t, 1, "f", "conflict-copy", nil)
	b := newNode(t, 2, "f", "conflict-copy", nil)
	a.writeStamped(t, name, body, engineSeedStamp)
	b.writeStamped(t, name, body, engineSeedStamp)
	a.scanAndApply(t)
	b.scanAndApply(t)

	// A's entry already carries both counters (A joined in an earlier round);
	// B has only ever seen its own counter, and A's joined entry as its view.
	joined := a.entry(t, name).Version.Merge(b.entry(t, name).Version)
	aFi := a.entry(t, name)
	aFi.Version = joined
	if err := a.ix.Put(aFi); err != nil {
		t.Fatal(err)
	}
	b.engine.SetPeerIndex(1, []*index.FileInfo{aFi.Clone()}, false)

	needs, err := b.engine.NeedPulls()
	if err != nil {
		t.Fatal(err)
	}
	if len(needs) != 0 {
		t.Fatalf("identical content must never need a pull: %v", needs)
	}
	if got, err := b.engine.JoinIdentical(); err != nil {
		t.Fatal(err)
	} else if got != 1 {
		t.Fatalf("B must adopt the dominating vector (joined=%d)", got)
	}
	if got := b.entry(t, name).Version; got.Compare(joined) != 0 {
		t.Fatalf("B's vector after adoption = %v, want %v", got, joined)
	}
	if got := b.fileContent(t, name); got != body {
		t.Fatalf("adoption must not touch content: %d bytes", len(got))
	}
}

// TestJoinIdenticalNeverOvertakesATombstone is the deletion-side guard. A path
// one peer has tombstoned is a deletion question with its own rules (does the
// tombstone dominate, does the guard allow it, is it half of a rename). The join
// must not touch such a name at all, and — the invariant that matters — it must
// not move the global model's answer about whether the path exists, nor the set
// of deletions this device still owes.
func TestJoinIdenticalNeverOvertakesATombstone(t *testing.T) {
	const name = "same.bin"
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeStamped(t, name, strings.Repeat("seed:", 400), engineSeedStamp)
	n.scanAndApply(t)
	live, _, _ := n.ix.Get(name)

	// Peer 2 holds the same bytes live (an independent add: concurrent with us).
	peer2 := live.Clone()
	peer2.Version = version.New().Bump(2)
	// Peer 3 tombstoned the path with a version that dominates BOTH live entries,
	// so the global model is a deletion and this device owes it.
	tomb := &index.FileInfo{Name: name, Type: index.TypeFile, Deleted: true,
		Version: live.Version.Merge(peer2.Version).Bump(3)}
	n.engine.SetPeerIndex(2, []*index.FileInfo{peer2}, false)
	n.engine.SetPeerIndex(3, []*index.FileInfo{tomb}, false)

	beforeGlobal, _, err := n.engine.GlobalFor(name)
	if err != nil {
		t.Fatal(err)
	}
	beforeDels, err := n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	beforeVersion := n.entry(t, name).Version

	got, err := n.engine.JoinIdentical()
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("a name with a peer tombstone must not be joined, joined %d", got)
	}
	if after := n.entry(t, name).Version; after.Compare(beforeVersion) != 0 {
		t.Fatalf("the live entry's vector was changed: %v -> %v", beforeVersion, after)
	}
	afterGlobal, _, err := n.engine.GlobalFor(name)
	if err != nil {
		t.Fatal(err)
	}
	if afterGlobal.Deleted != beforeGlobal.Deleted {
		t.Fatalf("the join changed whether the path is deleted: %v -> %v",
			beforeGlobal.Deleted, afterGlobal.Deleted)
	}
	if !afterGlobal.Deleted {
		t.Fatalf("the global model must still be the tombstone, got %+v", afterGlobal)
	}
	afterDels, err := n.engine.PendingDeletions()
	if err != nil {
		t.Fatal(err)
	}
	if len(afterDels) != len(beforeDels) || (len(afterDels) == 1 && afterDels[0] != name) {
		t.Fatalf("pending deletions changed: %v -> %v", beforeDels, afterDels)
	}
}

// TestJoinIdenticalSkipsUnsettledAndDeleted guards the two states the join must
// never touch: a file whose content is still changing (in flight) and a
// tombstone, where "identical bytes" is meaningless (a tombstone carries none).
func TestJoinIdenticalSkipsUnsettledAndDeleted(t *testing.T) {
	const name = "same.bin"
	a := newNode(t, 1, "f", "conflict-copy", nil)
	a.writeStamped(t, name, strings.Repeat("seed:", 100), engineSeedStamp)
	a.scanAndApply(t)

	// A tombstone for the same path, concurrent with the live entry.
	tomb := &index.FileInfo{Name: name, Type: index.TypeFile, Deleted: true,
		Version: a.entry(t, name).Version.Bump(2)}
	a.engine.SetPeerIndex(2, []*index.FileInfo{tomb}, false)
	before := a.entry(t, name).Version
	if got, err := a.engine.JoinIdentical(); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Fatalf("a live/tombstone pair must never be joined, joined %d", got)
	}
	if after := a.entry(t, name).Version; after.Compare(before) != 0 {
		t.Fatalf("tombstone pair changed the live entry's version: %v -> %v", before, after)
	}

	// In flight: the file is still being written, so its content may change
	// under us — no join.
	b := newNode(t, 2, "f", "conflict-copy", nil)
	b.writeStamped(t, name, strings.Repeat("seed:", 100), engineSeedStamp)
	b.scanAndApply(t)
	a.engine.SetPeerIndex(2, []*index.FileInfo{b.entry(t, name).Clone()}, false)
	a.engine.SetInFlight([]scanner.InFlight{{Name: name}})
	b.engine.SetPeerIndex(1, []*index.FileInfo{a.entry(t, name).Clone()}, false)
	if got, err := a.engine.JoinIdentical(); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Fatalf("an in-flight file must not be joined, joined %d", got)
	}
}

// TestJoinIdenticalEmitsNoEvents: the join is bookkeeping on identical bytes,
// not a change. A converged pair adds ZERO events; 500 seeded files must not
// produce 500 "joined" entries in the event log either.
func TestJoinIdenticalEmitsNoEvents(t *testing.T) {
	const name = "same.bin"
	a, b := seedIdenticalPair(t, name, strings.Repeat("seed:", 500))
	var seen []*events.Event
	a.engine.OnEvent = func(ev *events.Event) { seen = append(seen, ev) }
	b.engine.OnEvent = func(ev *events.Event) { seen = append(seen, ev) }
	for _, n := range []*testNode{a, b} {
		if _, err := n.engine.JoinIdentical(); err != nil {
			t.Fatal(err)
		}
	}
	for _, ev := range seen {
		if ev.Category == events.CatConflict || ev.Category == events.CatApplied || ev.Category == events.CatVersioned {
			t.Fatalf("join emitted a %s event: %s", ev.Category, ev.Reason)
		}
	}
	if len(seen) != 0 {
		t.Fatalf("join emitted %d event(s), want none: %+v", len(seen), seen[0])
	}
}
