package node

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/index"
)

// seedStamp is the (size, mtime) every seeded file carries on BOTH nodes. The
// fixture models the way a user seeds two boxes: the same corpus, generated
// independently on each machine, so the bytes are identical AND the mtimes are
// identical — while each box's index records it as its OWN local add, with its
// own version vector. Those vectors are concurrent by construction, and that
// is the state that used to manufacture conflict copies for edits only one
// device made.
var seedStamp = time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)

// editStamp is "+48h": a rewrite on one device, strictly newer than the seed.
var editStamp = seedStamp.Add(48 * time.Hour)

// writeMTime writes rel under root and pins its mtime, so two nodes can hold
// byte- AND timestamp-identical copies.
func writeMTime(t *testing.T, root, rel, body string, mt time.Time) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
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

// seededBody is the corpus content for file i: distinct per index, same on
// both nodes (generated from the same spec, as internal/testgen does).
func seededBody(i int) string { return strings.Repeat(fmt.Sprintf("seed-%03d:", i), 200) }

// editedOnA is what node A writes over file i.
func editedOnA(i int) string { return strings.Repeat(fmt.Sprintf("A-edit-%03d:", i), 240) }

// editedOnB is what node B writes over file i (different bytes from A's).
func editedOnB(i int) string { return strings.Repeat(fmt.Sprintf("B-edit-%03d:", i), 260) }

// conflictCopies lists the conflict copies present anywhere under root.
func conflictCopies(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.Contains(filepath.Base(p), ".sync-conflict") {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// conflictEvents counts the durable conflict-category events recorded by n.
func conflictEvents(t *testing.T, n *Node) int {
	t.Helper()
	c := 0
	for _, e := range mustQuery(t, n, "data") {
		if e.Category == events.CatConflict {
			c++
		}
	}
	return c
}

// liveFiles returns the live (non-tombstone) FILE entries of n's folder,
// keyed by name — directories are excluded because they are indexed too but
// are not part of the corpus the assertions are about.
func liveFiles(t *testing.T, n *Node) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	if err := n.Folders["data"].Ix.List(func(fi *index.FileInfo) error {
		if !fi.Deleted && fi.Type == index.TypeFile {
			out[fi.Name] = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// seedIdenticalCorpus writes the same n files, with the same bytes and the
// same mtime, into both roots independently, then indexes both. The two
// indexes therefore hold concurrent vectors for identical content — the
// normal result of seeding two boxes with the same data.
func seedIdenticalCorpus(t *testing.T, rootA, rootB string, n int) []string {
	t.Helper()
	var rels []string
	for i := 0; i < n; i++ {
		rel := fmt.Sprintf("testdata/versions/f%02d.bnn", i)
		writeMTime(t, rootA, rel, seededBody(i), seedStamp)
		writeMTime(t, rootB, rel, seededBody(i), seedStamp)
		rels = append(rels, rel)
	}
	return rels
}

// TestExistingConcurrentIdenticalIndexRepairedByOneRound is the repair path for
// an index that ALREADY carries the spurious concurrency — the state the
// deployed pair is in, written by a build that never joined. No manual rebuild
// and no content change may be needed: one ordinary round must fold the vectors
// and leave the bytes (and their mtimes) exactly as they were, transferring
// nothing and recording no event.
func TestExistingConcurrentIdenticalIndexRepairedByOneRound(t *testing.T) {
	const n = 6
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"))
	b := chunkTestNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"))

	rels := seedIdenticalCorpus(t, rootA, rootB, n)
	mustScan(t, a)
	mustScan(t, b)

	// The pre-fix state: two independent local adds of the same bytes, never
	// exchanged, so both indexes hold concurrent vectors for identical content.
	for _, rel := range rels {
		fa, ok := liveEntry(t, a.Folders["data"].Ix, rel)
		if !ok {
			t.Fatalf("A: %s not indexed", rel)
		}
		fb, ok := liveEntry(t, b.Folders["data"].Ix, rel)
		if !ok {
			t.Fatalf("B: %s not indexed", rel)
		}
		if cmp := fa.Version.Compare(fb.Version); cmp != 2 {
			t.Fatalf("%s: fixture must start concurrent, got compare=%d", rel, cmp)
		}
	}
	stamps := map[string]time.Time{}
	for _, rel := range rels {
		stamps[rel] = fileMTime(t, rootA, rel)
	}

	downA := a.Folders["data"].Engine.Stats.BytesDown.Load()
	downB := b.Folders["data"].Engine.Stats.BytesDown.Load()
	eventsA := len(mustQuery(t, a, "data"))
	eventsB := len(mustQuery(t, b, "data"))

	// ONE round.
	gateSession(t, a, b)

	for _, rel := range rels {
		fa, _ := liveEntry(t, a.Folders["data"].Ix, rel)
		fb, _ := liveEntry(t, b.Folders["data"].Ix, rel)
		if cmp := fa.Version.Compare(fb.Version); cmp != 0 {
			t.Fatalf("%s: one round must join the vectors, got A=%v B=%v (compare=%d)",
				rel, fa.Version, fb.Version, cmp)
		}
		if fa.Version.Get(1) == 0 || fa.Version.Get(2) == 0 {
			t.Fatalf("%s: joined vector must carry both devices' counters, got %v", rel, fa.Version)
		}
		// Content untouched: nothing was rewritten, not even the mtime.
		if got := fileMTime(t, rootA, rel); !got.Equal(stamps[rel]) {
			t.Fatalf("%s: A's file mtime changed (%v -> %v) during a metadata-only repair", rel, stamps[rel], got)
		}
		if got := fileMTime(t, rootB, rel); !got.Equal(stamps[rel]) {
			t.Fatalf("%s: B's file mtime changed (%v -> %v) during a metadata-only repair", rel, stamps[rel], got)
		}
		if got := readFile(t, rootB, rel); got != seededBody(indexOfRel(rels, rel)) {
			t.Fatalf("%s: B's content changed during the repair", rel)
		}
	}
	if d := a.Folders["data"].Engine.Stats.BytesDown.Load() - downA; d != 0 {
		t.Fatalf("A downloaded %d bytes to repair version vectors", d)
	}
	if d := b.Folders["data"].Engine.Stats.BytesDown.Load() - downB; d != 0 {
		t.Fatalf("B downloaded %d bytes to repair version vectors", d)
	}
	if got := len(mustQuery(t, a, "data")) - eventsA; got != 0 {
		t.Fatalf("A recorded %d event(s) for a metadata-only repair", got)
	}
	if got := len(mustQuery(t, b, "data")) - eventsB; got != 0 {
		t.Fatalf("B recorded %d event(s) for a metadata-only repair", got)
	}
	// Nothing is "pending" either: the pair has nothing left to transfer in
	// either direction, so a convergence check (pending 0/0) passes right away
	// instead of waiting a second round for a phantom upload.
	for name, node := range map[string]*Node{"A": a, "B": b} {
		eng := node.Folders["data"].Engine
		if ups, err := eng.PendingUploads(); err != nil {
			t.Fatal(err)
		} else if len(ups) != 0 {
			t.Fatalf("%s reports %d pending upload(s) after the repair: %v", name, len(ups), ups)
		}
		if downs, err := eng.NeedPulls(); err != nil {
			t.Fatal(err)
		} else if len(downs) != 0 {
			t.Fatalf("%s reports %d pending pull(s) after the repair: %v", name, len(downs), downs)
		}
	}

	// And the repair is what makes the next one-sided edit a fast-forward: no
	// round is spent producing conflict copies.
	for i := 0; i < 2; i++ {
		writeMTime(t, rootA, rels[i], editedOnA(i), editStamp)
	}
	mustScan(t, a)
	gateSession(t, a, b)
	gateSession(t, a, b)
	if got := conflictCopies(t, rootB); len(got) != 0 {
		t.Fatalf("after the repair B still manufactured %d conflict copy(ies): %v", len(got), got)
	}
	for i := 0; i < 2; i++ {
		if got := readFile(t, rootB, rels[i]); got != editedOnA(i) {
			t.Fatalf("B %s = %d bytes, want A's edit", rels[i], len(got))
		}
	}
}

// fileMTime returns the mtime of rel under root.
func fileMTime(t *testing.T, root, rel string) time.Time {
	t.Helper()
	st, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return st.ModTime()
}

// indexOfRel returns the position of rel in rels.
func indexOfRel(rels []string, rel string) int {
	for i, r := range rels {
		if r == rel {
			return i
		}
	}
	return -1
}

// TestSeededIdenticalOneSidedEditIsNoConflict is the reported bug, end to end
// on two nodes: A and B each independently hold the same corpus (identical
// bytes, identical mtime, concurrent version vectors). After they sync, A
// rewrites some files with new content and a newer mtime. Those are edits only
// A made, so B must apply them as a plain fast-forward — no conflict copy, no
// conflict event, no duplicated path — and B's bytes must end up equal to A's.
//
// On main this fails: B's vector for the seeded file was never joined with
// A's, so A's single edit still compares CONCURRENT with B's untouched entry
// and PlanOverwrite turns it into a conflict copy.
func TestSeededIdenticalOneSidedEditIsNoConflict(t *testing.T) {
	const n, edited = 12, 5
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"))
	b := chunkTestNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"))

	rels := seedIdenticalCorpus(t, rootA, rootB, n)
	mustScan(t, a)
	mustScan(t, b)

	// Two rounds, as the daemon runs them: the pair must converge on the
	// identical content without inventing conflict copies.
	gateSession(t, a, b)
	gateSession(t, a, b)
	if got := conflictCopies(t, rootB); len(got) != 0 {
		t.Fatalf("B created %d conflict copy(ies) just from the identical seed: %v", len(got), got)
	}

	// A rewrites the first `edited` files: new bytes, mtime +48h.
	want := map[string]string{}
	for i := 0; i < edited; i++ {
		writeMTime(t, rootA, rels[i], editedOnA(i), editStamp)
		want[rels[i]] = editedOnA(i)
	}
	mustScan(t, a)

	downBefore := b.Folders["data"].Engine.Stats.BytesDown.Load()
	confBefore := conflictEvents(t, b)
	gateSession(t, a, b)
	gateSession(t, a, b)

	if got := conflictEvents(t, b) - confBefore; got != 0 {
		t.Fatalf("B recorded %d conflict event(s) for files only A edited (want 0); copies: %v",
			got, conflictCopies(t, rootB))
	}
	if got := conflictCopies(t, rootB); len(got) != 0 {
		t.Fatalf("B created %d conflict copy(ies) for files only A edited: %v", len(got), got)
	}

	// Content: B must hold exactly A's bytes, edited and unedited alike.
	for i, rel := range rels {
		w, ok := want[rel]
		if !ok {
			w = seededBody(i)
		}
		if got := readFile(t, rootB, rel); got != w {
			t.Fatalf("B %s = %d bytes, want A's %d bytes", rel, len(got), len(w))
		}
		if got := readFile(t, rootA, rel); got != w {
			t.Fatalf("A %s drifted: %d bytes, want %d", rel, len(got), len(w))
		}
	}
	// Exactly one live entry per path on each side: the pull must not leave a
	// duplicate behind (a conflict copy would be a second path).
	for name, node := range map[string]*Node{"A": a, "B": b} {
		live := liveFiles(t, node)
		if len(live) != n {
			t.Fatalf("%s holds %d live file entries, want %d: %v", name, len(live), n, live)
		}
	}
	// The only bytes that may cross the wire are the five edited files.
	var wantBytes int64
	for i := 0; i < edited; i++ {
		wantBytes += int64(len(editedOnA(i)))
	}
	if d := b.Folders["data"].Engine.Stats.BytesDown.Load() - downBefore; d != wantBytes {
		t.Fatalf("B downloaded %d bytes, want exactly the %d edited bytes", d, wantBytes)
	}

	// The seed's concurrent vectors must have been JOINED by the rounds: after
	// the pull, both sides agree on one vector that dominates both seeds, so a
	// later edit anywhere is a strict descendant rather than a third branch.
	aFi, aok := liveEntry(t, a.Folders["data"].Ix, rels[0])
	bFi, bok := liveEntry(t, b.Folders["data"].Ix, rels[0])
	if !aok || !bok {
		t.Fatalf("seed not indexed on both sides (a=%v b=%v)", aok, bok)
	}
	if cmp := aFi.Version.Compare(bFi.Version); cmp != 0 {
		t.Fatalf("after the round the two devices disagree on the version: A=%v B=%v (compare=%d)",
			aFi.Version, bFi.Version, cmp)
	}
	if aFi.Version.Get(1) == 0 || aFi.Version.Get(2) == 0 {
		t.Fatalf("joined version must carry BOTH devices' counters, got %v", aFi.Version)
	}
}

// TestGenuineConcurrentEditStillConflicts is the guard on the other side of the
// same rule: two devices editing the SAME file from the same base, with
// different content and equal mtimes, are a real conflict. Exactly one side
// loses the in-place winner, its edit is preserved as one conflict copy, and
// both contents survive on the pair. The join must never collapse this.
func TestGenuineConcurrentEditStillConflicts(t *testing.T) {
	const n = 4
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"))
	b := chunkTestNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"))

	rels := seedIdenticalCorpus(t, rootA, rootB, n)
	mustScan(t, a)
	mustScan(t, b)
	gateSession(t, a, b)
	gateSession(t, a, b)

	// Both rewrite every file from the same base, with different bytes and the
	// SAME mtime: nothing but the version vectors can order these.
	for i, rel := range rels {
		writeMTime(t, rootA, rel, editedOnA(i), editStamp)
		writeMTime(t, rootB, rel, editedOnB(i), editStamp)
	}
	mustScan(t, a)
	mustScan(t, b)
	gateSession(t, a, b)
	gateSession(t, a, b)
	gateSession(t, a, b)

	copiesA := conflictCopies(t, rootA)
	copiesB := conflictCopies(t, rootB)
	// Exactly one preserved loser per disputed file, and the same single copy
	// on BOTH devices: the losing side creates it in place, and it propagates
	// to the peer as one extra path (never two copies of the same edit).
	if len(copiesA) != n || len(copiesB) != n {
		t.Fatalf("want exactly one conflict copy per disputed file on each side (%d each), got A=%d B=%d: %v %v",
			n, len(copiesA), len(copiesB), copiesA, copiesB)
	}
	for i := range copiesA {
		if filepath.Base(copiesA[i]) != filepath.Base(copiesB[i]) {
			t.Fatalf("the pair disagrees on the conflict copy names: %v vs %v", copiesA, copiesB)
		}
	}
	// Both devices must agree on the in-place winner for each file.
	for i, rel := range rels {
		ga := readFile(t, rootA, rel)
		gb := readFile(t, rootB, rel)
		if ga != gb {
			t.Fatalf("%s: A has %q, B has %q — the winner did not converge", rel, trunc(ga), trunc(gb))
		}
		if ga != editedOnA(i) && ga != editedOnB(i) {
			t.Fatalf("%s holds neither edit: %q", rel, trunc(ga))
		}
	}
	// Nothing was lost, on BOTH devices: the winner is in place (asserted
	// above) and the loser is preserved byte-for-byte in the conflict copy.
	// Every copy must hold REAL bytes — a copy indexed without a block list
	// travels as zeros and silently replaces the edit it was meant to keep.
	for i, rel := range rels {
		winner := readFile(t, rootA, rel)
		var loser string
		switch winner {
		case editedOnA(i):
			loser = editedOnB(i)
		case editedOnB(i):
			loser = editedOnA(i)
		default:
			t.Fatalf("%s: in-place content is neither edit", rel)
		}
		for name, root := range map[string]string{"A": rootA, "B": rootB} {
			for _, cp := range conflictCopiesUnder(t, root, rel) {
				data, err := os.ReadFile(cp)
				if err != nil {
					t.Fatal(err)
				}
				if string(data) != loser {
					t.Fatalf("%s: %s conflict copy holds %d bytes (%q), want the losing edit (%d bytes)",
						rel, name, len(data), trunc(string(data)), len(loser))
				}
			}
		}
	}
}

// conflictCopiesUnder returns the conflict copies that belong to rel (same
// directory, same base name + extension), so the assertion above reads the
// right bytes per file.
func conflictCopiesUnder(t *testing.T, root, rel string) []string {
	t.Helper()
	dir := filepath.Dir(filepath.Join(root, filepath.FromSlash(rel)))
	base := strings.TrimSuffix(filepath.Base(rel), filepath.Ext(rel))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), base+".sync-conflict") && strings.HasSuffix(e.Name(), filepath.Ext(rel)) {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out
}

func trunc(s string) string {
	if len(s) <= 40 {
		return s
	}
	return s[:40] + "…"
}

// TestSeededIdenticalThreeNodesNoConflict: A and B hold the identical seed, C
// never had the file. A edits. C must receive A's bytes as an ordinary add and
// B must converge to them without a conflict copy anywhere — the third vector
// must not resurrect the concurrency the join removed.
func TestSeededIdenticalThreeNodesNoConflict(t *testing.T) {
	const n = 3
	rootA, rootB, rootC := t.TempDir(), t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"))
	b := chunkTestNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"))
	c := chunkTestNode(t, 3, "c", rootC, filepath.Join(t.TempDir(), "meta-c"))

	rels := seedIdenticalCorpus(t, rootA, rootB, n)
	for _, node := range []*Node{a, b, c} {
		mustScan(t, node)
	}
	// A and B converge on the identical seed; C sees the same content (it has
	// no local copy, so it downloads it once and adopts the merged vector).
	gateSession(t, a, b)
	gateSession(t, a, b)
	gateSession(t, a, c)
	gateSession(t, a, b)

	for i, rel := range rels {
		writeMTime(t, rootA, rel, editedOnA(i), editStamp)
	}
	mustScan(t, a)
	gateSession(t, a, c)
	gateSession(t, a, b)
	gateSession(t, a, b)
	gateSession(t, a, c)

	for name, root := range map[string]string{"B": rootB, "C": rootC} {
		if got := conflictCopies(t, root); len(got) != 0 {
			t.Fatalf("%s created %d conflict copy(ies) for a one-sided edit: %v", name, len(got), got)
		}
		for i, rel := range rels {
			if got := readFile(t, root, rel); got != editedOnA(i) {
				t.Fatalf("%s %s = %d bytes, want A's edit (%d bytes)", name, rel, len(got), len(editedOnA(i)))
			}
		}
	}
	if got := conflictCopies(t, rootA); len(got) != 0 {
		t.Fatalf("A created %d conflict copy(ies) while editing its own files: %v", len(got), got)
	}
}

// TestDeployedScenarioDisjointRewrites mirrors the live reproduction that
// started this: a 20-file identical corpus on both devices, then A rewrites
// files 0..19 and B rewrites files 0..4, all with the SAME +48h mtime. The
// expectation is exactly the one the deployed run failed:
//
//   - 5..19 were edited by A only — no conflict copy on either side, both
//     devices end on A's bytes;
//   - 0..4 are genuinely concurrent (two different contents from one base) —
//     exactly one conflict copy per file on each side, and both edits survive.
//
// On main the overlapping prefix is not the problem: files 5..19 also produce
// a conflict copy on B, because B's seed copy still carries a concurrent
// vector that A's single edit never terminates.
func TestDeployedScenarioDisjointRewrites(t *testing.T) {
	const n, overlap = 20, 5
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"))
	b := chunkTestNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"))

	rels := seedIdenticalCorpus(t, rootA, rootB, n)
	mustScan(t, a)
	mustScan(t, b)
	gateSession(t, a, b)
	gateSession(t, a, b)

	// A rewrites every file; B rewrites only the first five. Same mtime on
	// both sides (+48h), different bytes per device.
	for i, rel := range rels {
		writeMTime(t, rootA, rel, editedOnA(i), editStamp)
		if i < overlap {
			writeMTime(t, rootB, rel, editedOnB(i), editStamp)
		}
	}
	mustScan(t, a)
	mustScan(t, b)
	for i := 0; i < 3; i++ {
		gateSession(t, a, b)
	}

	// The disputed prefix: one preserved loser per file on each side.
	for name, root := range map[string]string{"A": rootA, "B": rootB} {
		copies := conflictCopies(t, root)
		if len(copies) != overlap {
			t.Fatalf("%s holds %d conflict copy(ies), want exactly the %d genuinely concurrent files: %v",
				name, len(copies), overlap, copies)
		}
		for i := 0; i < overlap; i++ {
			found := false
			for _, cp := range copies {
				if strings.Contains(filepath.Base(cp), fmt.Sprintf("f%02d.", i)) {
					found = true
				}
			}
			if !found {
				t.Fatalf("%s has no conflict copy for the disputed f%02d: %v", name, i, copies)
			}
		}
		for i := overlap; i < n; i++ {
			for _, cp := range copies {
				if strings.Contains(filepath.Base(cp), fmt.Sprintf("f%02d.", i)) {
					t.Fatalf("%s created a conflict copy for f%02d, which only A edited: %s", name, i, cp)
				}
			}
		}
	}

	// One-sided files: both devices converge on A's bytes, byte for byte.
	for i := overlap; i < n; i++ {
		rel := rels[i]
		for name, root := range map[string]string{"A": rootA, "B": rootB} {
			if got := readFile(t, root, rel); got != editedOnA(i) {
				t.Fatalf("%s %s = %d bytes, want A's edit (%d bytes)", name, rel, len(got), len(editedOnA(i)))
			}
		}
	}
	// Disputed files: the pair agrees on one winner and both edits survive.
	for i := 0; i < overlap; i++ {
		rel := rels[i]
		ga, gb := readFile(t, rootA, rel), readFile(t, rootB, rel)
		if ga != gb {
			t.Fatalf("%s: A holds %q, B holds %q — no agreed winner", rel, trunc(ga), trunc(gb))
		}
		if ga != editedOnA(i) && ga != editedOnB(i) {
			t.Fatalf("%s holds neither edit: %q", rel, trunc(ga))
		}
		preserved := 0
		for _, root := range []string{rootA, rootB} {
			for _, cp := range conflictCopiesUnder(t, root, rel) {
				data, err := os.ReadFile(cp)
				if err != nil {
					t.Fatal(err)
				}
				if string(data) == editedOnA(i) || string(data) == editedOnB(i) {
					preserved++
				}
			}
		}
		if preserved < 2 {
			detail := ""
			for _, root := range []string{rootA, rootB} {
				for _, cp := range conflictCopiesUnder(t, root, rel) {
					data, _ := os.ReadFile(cp)
					detail += fmt.Sprintf("\n  %s -> %d bytes prefix=%q wantA=%d wantB=%d",
						cp, len(data), trunc(string(data)), len(editedOnA(i)), len(editedOnB(i)))
				}
			}
			t.Fatalf("%s: only %d of the two edits survive in a conflict copy (A=%q B=%q)%s",
				rel, preserved, trunc(ga), trunc(gb), detail)
		}
	}
}
