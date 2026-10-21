package node

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/hold"
	"crosssync/internal/index"
	"crosssync/internal/staging"
)

// holdNode builds a node with one ungated "data" folder whose deletion guard
// trips easily (a low percentage cap, and tests lower minGuardDeletes too).
func holdNode(t *testing.T, id uint64, name, root, metaDir string, maxDeletePct int) *Node {
	t.Helper()
	n := gateNode(t, id, name, root, metaDir, nil)
	n.Cfg.Folders[0].MaxDeletePct = maxDeletePct
	n.Folders["data"].Cfg.MaxDeletePct = maxDeletePct
	return n
}

// lowerGuardFloor makes the absolute deletion floor small enough that a
// 50-file folder can trip the guard, restoring it when the test ends.
func lowerGuardFloor(t *testing.T, floor int) {
	t.Helper()
	old := minGuardDeletes
	minGuardDeletes = floor
	t.Cleanup(func() { minGuardDeletes = old })
}

// syncFully runs rounds until neither side needs anything (bounded).
func syncFully(t *testing.T, a, b *Node) {
	t.Helper()
	for i := 0; i < 4; i++ {
		gateSession(t, a, b)
		mustScan(t, a)
		mustScan(t, b)
	}
}

// writeMany creates n small files in two directories under root and returns
// their relative paths.
func writeMany(t *testing.T, root string, n int) []string {
	t.Helper()
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		rel := fmt.Sprintf("d%d/f%02d.txt", i%2, i)
		writeAt(t, root, rel, fmt.Sprintf("content %02d payload", i))
		names = append(names, rel)
	}
	return names
}

// TestSoftDeleteOverThresholdHoldsOnReceiver is T5: A deletes 40 of 50 shared
// files locally (the bytes are gone from A — nothing to hold there). B receives
// an over-threshold delete pass, the guard no longer blocks, the deletions GO
// THROUGH, and B keeps its copies in .sfx-hold instead of unlinking them. No
// interaction is required.
func TestSoftDeleteOverThresholdHoldsOnReceiver(t *testing.T) {
	lowerGuardFloor(t, 10)
	rootA, rootB := t.TempDir(), t.TempDir()
	a := holdNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"), 25)
	b := holdNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"), 25)
	names := writeMany(t, rootA, 50)
	mustScan(t, a)
	mustScan(t, b)
	syncFully(t, a, b)
	for _, rel := range names {
		if !exists(t, rootB, rel) {
			t.Fatalf("setup: B missing %s", rel)
		}
	}

	// A deletes 40 files (80% of the folder) from its own disk.
	for i := 0; i < 40; i++ {
		if err := os.Remove(filepath.Join(rootA, filepath.FromSlash(names[i]))); err != nil {
			t.Fatal(err)
		}
	}
	mustScan(t, a)
	gateSession(t, a, b)
	mustScan(t, b)

	// B's copies are held, with the original structure preserved...
	for i := 0; i < 40; i++ {
		rel := names[i]
		if exists(t, rootB, rel) {
			t.Fatalf("B still has %s: the deletion did not go through", rel)
		}
		if !exists(t, rootB, ".sfx-hold/"+rel) {
			t.Fatalf("B did not hold %s (want .sfx-hold/%s)", rel, rel)
		}
		if got, want := readFile(t, rootB, ".sfx-hold/"+rel), fmt.Sprintf("content %02d payload", i); got != want {
			t.Fatalf("held %s = %q, want %q", rel, got, want)
		}
		if _, live := liveEntry(t, b.Folders["data"].Ix, rel); live {
			t.Fatalf("B still tracks deleted path %s", rel)
		}
	}
	// ...and the untouched files are still live on both sides.
	for i := 40; i < 50; i++ {
		if !exists(t, rootB, names[i]) || !exists(t, rootA, names[i]) {
			t.Fatalf("%s must be untouched", names[i])
		}
	}
	// A is the originator: it holds nothing, its bytes are simply gone.
	if exists(t, rootA, ".sfx-hold") {
		t.Fatal("A must not hold anything: the bytes were already gone from its disk")
	}
	// The hold section of the folder status lists every held path.
	held, err := b.HeldFiles("data")
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 40 {
		t.Fatalf("B hold count = %d, want 40", len(held))
	}
	var total int64
	for _, e := range held {
		if e.HeldAt.IsZero() || e.HeldAt.After(time.Now()) {
			t.Fatalf("held entry %s has no usable hold time: %v", e.Path, e.HeldAt)
		}
		if e.ExpiresAt().Sub(e.HeldAt) != hold.MaxAge {
			t.Fatalf("held entry %s expiry = %v after hold, want %v", e.Path, e.ExpiresAt().Sub(e.HeldAt), hold.MaxAge)
		}
		total += e.Size
	}
	if total <= 0 {
		t.Fatal("held bytes must be reported")
	}
	// No blocked state, nothing pending: the pass completed without
	// interaction.
	if b.DeleteGuardTripped("data") {
		t.Fatal("the guard must not block anymore: the deletions went through")
	}
	if pd, err := b.PendingDeletionsCount("data"); err != nil || pd != 0 {
		t.Fatalf("pending deletions = %d (%v), want 0", pd, err)
	}
	// The hold area stays invisible to scanning, indexing and events.
	before := len(mustQuery(t, b, "data"))
	if n := mustScan(t, b); n != 0 {
		t.Fatalf("scanning a folder with held deletions reported %d change(s)", n)
	}
	_ = b.Folders["data"].Ix.ListAll(func(fi *index.FileInfo) error {
		if strings.Contains(fi.Name, staging.HoldDir) {
			t.Fatalf("hold area leaked into the index: %s", fi.Name)
		}
		return nil
	})
	for _, e := range mustQuery(t, b, "data") {
		if strings.Contains(e.Path, staging.HoldDir) || strings.Contains(e.Linked, staging.HoldDir) {
			t.Fatalf("hold area leaked into events: %+v", e)
		}
	}
	if after := len(mustQuery(t, b, "data")); after != before {
		t.Fatalf("an idle scan recorded %d new event(s)", after-before)
	}
	// One durable event says what happened.
	heldEvents := 0
	for _, e := range mustQuery(t, b, "data") {
		if e.Category == events.CatSystem && strings.Contains(e.Reason, "held") {
			heldEvents++
		}
	}
	if heldEvents != 1 {
		t.Fatalf("hold events = %d, want exactly 1", heldEvents)
	}
}

// TestSoftDeleteRestoreReturnsBytesToOriginator is T6: restoring a held file on
// B puts it back on A (the device that deleted it) after a round, byte for
// byte.
func TestSoftDeleteRestoreReturnsBytesToOriginator(t *testing.T) {
	lowerGuardFloor(t, 10)
	rootA, rootB := t.TempDir(), t.TempDir()
	a := holdNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"), 25)
	b := holdNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"), 25)
	names := writeMany(t, rootA, 50)
	mustScan(t, a)
	mustScan(t, b)
	syncFully(t, a, b)
	for i := 0; i < 40; i++ {
		if err := os.Remove(filepath.Join(rootA, filepath.FromSlash(names[i]))); err != nil {
			t.Fatal(err)
		}
	}
	mustScan(t, a)
	gateSession(t, a, b)
	mustScan(t, b)
	restore := names[7]
	if !exists(t, rootB, ".sfx-hold/"+restore) {
		t.Fatalf("setup: %s should be held on B", restore)
	}

	// Undo on B: the file returns to its original path and becomes a normal
	// local change.
	if err := b.RestoreHeld("data", restore); err != nil {
		t.Fatal(err)
	}
	if !exists(t, rootB, restore) {
		t.Fatalf("B: %s was not restored", restore)
	}
	if exists(t, rootB, ".sfx-hold/"+restore) {
		t.Fatalf("B: %s is still in the hold area", restore)
	}
	if _, live := liveEntry(t, b.Folders["data"].Ix, restore); !live {
		t.Fatalf("B index must hold the restored %s", restore)
	}

	syncFully(t, a, b)

	// The originator gets its bytes back.
	if !exists(t, rootA, restore) {
		t.Fatalf("A did not get %s back from B's hold", restore)
	}
	want := readFile(t, rootB, restore)
	if got := readFile(t, rootA, restore); got != want {
		t.Fatalf("A %s = %q, want %q (byte-identical)", restore, got, want)
	}
	// The other 39 are still only in B's hold area.
	if exists(t, rootA, names[8]) {
		t.Fatalf("A %s must stay deleted", names[8])
	}
	if held, err := b.HeldFiles("data"); err != nil || len(held) != 39 {
		t.Fatalf("B hold count = %d (%v), want 39", len(held), err)
	}
}

// TestSoftDeleteDeleteNowPurges is T7: "delete now" drops the held copy.
func TestSoftDeleteDeleteNowPurges(t *testing.T) {
	lowerGuardFloor(t, 10)
	rootA, rootB := t.TempDir(), t.TempDir()
	a := holdNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"), 25)
	b := holdNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"), 25)
	names := writeMany(t, rootA, 50)
	mustScan(t, a)
	mustScan(t, b)
	syncFully(t, a, b)
	for i := 0; i < 40; i++ {
		if err := os.Remove(filepath.Join(rootA, filepath.FromSlash(names[i]))); err != nil {
			t.Fatal(err)
		}
	}
	mustScan(t, a)
	gateSession(t, a, b)
	mustScan(t, b)
	victim := names[3]
	if !exists(t, rootB, ".sfx-hold/"+victim) {
		t.Fatalf("setup: %s should be held", victim)
	}
	n, err := b.DeleteHeld("data", victim)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("purged %d, want 1", n)
	}
	if exists(t, rootB, ".sfx-hold/"+victim) {
		t.Fatalf("%s must be gone from the hold area", victim)
	}
	if exists(t, rootB, victim) {
		t.Fatalf("%s must not reappear at its original path", victim)
	}
	if held, err := b.HeldFiles("data"); err != nil || len(held) != 39 {
		t.Fatalf("B hold count = %d (%v), want 39", len(held), err)
	}
}

// TestSoftDeleteHoldExpires is T8: maintenance purges held entries past the undo
// window and logs it.
func TestSoftDeleteHoldExpires(t *testing.T) {
	lowerGuardFloor(t, 10)
	rootA, rootB := t.TempDir(), t.TempDir()
	a := holdNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"), 25)
	b := holdNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"), 25)
	names := writeMany(t, rootA, 50)
	mustScan(t, a)
	mustScan(t, b)
	syncFully(t, a, b)
	for i := 0; i < 40; i++ {
		if err := os.Remove(filepath.Join(rootA, filepath.FromSlash(names[i]))); err != nil {
			t.Fatal(err)
		}
	}
	mustScan(t, a)
	gateSession(t, a, b)
	mustScan(t, b)
	if held, err := b.HeldFiles("data"); err != nil || len(held) != 40 {
		t.Fatalf("setup: hold count = %d (%v), want 40", len(held), err)
	}

	// Shorten the undo window: everything already held is now too old.
	oldAge := hold.MaxAge
	hold.MaxAge = -time.Second
	defer func() { hold.MaxAge = oldAge }()
	if _, err := b.RunMaintenance(); err != nil {
		t.Fatal(err)
	}

	if held, err := b.HeldFiles("data"); err != nil || len(held) != 0 {
		t.Fatalf("hold count after expiry = %d (%v), want 0", len(held), err)
	}
	if exists(t, rootB, ".sfx-hold/"+names[0]) {
		t.Fatalf("%s must be purged from disk", names[0])
	}
	purged := 0
	for _, e := range mustQuery(t, b, "data") {
		if e.Category == events.CatSystem && strings.Contains(e.Reason, "purged") &&
			strings.Contains(e.Reason, "held deletion") {
			purged++
		}
	}
	if purged != 1 {
		t.Fatalf("purge events = %d, want 1", purged)
	}
}

// TestBelowThresholdDeleteUnchanged is T9: a small delete pass is still a plain
// delete on the receiver — no hold area, anywhere.
func TestBelowThresholdDeleteUnchanged(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	a := holdNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"), 25)
	b := holdNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"), 25)
	names := writeMany(t, rootA, 12)
	mustScan(t, a)
	mustScan(t, b)
	syncFully(t, a, b)

	for i := 0; i < 3; i++ {
		if err := os.Remove(filepath.Join(rootA, filepath.FromSlash(names[i]))); err != nil {
			t.Fatal(err)
		}
	}
	mustScan(t, a)
	gateSession(t, a, b)
	mustScan(t, b)

	for i := 0; i < 3; i++ {
		if exists(t, rootB, names[i]) {
			t.Fatalf("B still has %s after a plain deletion", names[i])
		}
		if exists(t, rootB, ".sfx-hold/"+names[i]) {
			t.Fatalf("%s must not be held: the pass is below the threshold", names[i])
		}
	}
	if exists(t, rootB, ".sfx-hold") {
		t.Fatal("no hold area should exist for a below-threshold deletion")
	}
	if held, err := b.HeldFiles("data"); err != nil || len(held) != 0 {
		t.Fatalf("hold count = %d (%v), want 0", len(held), err)
	}
	// The deletion is recorded as a plain remote deletion, not a held one.
	for _, e := range mustQuery(t, b, "data") {
		if strings.Contains(e.Reason, "held for undo") {
			t.Fatalf("unexpected soft deletion event: %+v", e)
		}
	}
}

// TestGatedRenameMassDeletionHoldsInGateDevice is T10: B renames a folder that
// A gates. From A's point of view that is a mass deletion of its in-gate paths
// — the guard engages, and A's copies are HELD (not unlinked) while B keeps the
// content under the new name. No interaction required.
func TestGatedRenameMassDeletionHoldsInGateDevice(t *testing.T) {
	lowerGuardFloor(t, 5)
	rootA, rootB := t.TempDir(), t.TempDir()
	a := gateNode(t, 1, "a", rootA, filepath.Join(t.TempDir(), "meta-a"), []string{"proxy/"})
	b := gateNode(t, 2, "b", rootB, filepath.Join(t.TempDir(), "meta-b"), nil)
	a.Cfg.Folders[0].MaxDeletePct = 25
	a.Folders["data"].Cfg.MaxDeletePct = 25

	var names []string
	for i := 0; i < 12; i++ {
		rel := fmt.Sprintf("proxy/p%02d.bin", i)
		writeAt(t, rootA, rel, fmt.Sprintf("proxy payload %02d", i))
		names = append(names, rel)
	}
	mustScan(t, a)
	mustScan(t, b)
	syncFully(t, a, b)
	for _, rel := range names {
		if !exists(t, rootB, rel) {
			t.Fatalf("setup: B missing %s", rel)
		}
	}

	// B (ungated) renames proxy/ -> dev/. A's gate means it sees only a mass
	// deletion of its in-gate paths plus an out-of-scope move to release.
	if err := os.Rename(filepath.Join(rootB, "proxy"), filepath.Join(rootB, "dev")); err != nil {
		t.Fatal(err)
	}
	mustScan(t, b)
	gateSession(t, a, b)
	mustScan(t, a)

	// A holds its copies instead of unlinking them.
	for i, rel := range names {
		if exists(t, rootA, rel) {
			t.Fatalf("A still has %s at its old path", rel)
		}
		heldRel := ".sfx-hold/" + rel
		if !exists(t, rootA, heldRel) {
			t.Fatalf("A did not hold %s (want %s)", rel, heldRel)
		}
		if got, want := readFile(t, rootA, heldRel), fmt.Sprintf("proxy payload %02d", i); got != want {
			t.Fatalf("held %s = %q, want %q", rel, got, want)
		}
	}
	if held, err := a.HeldFiles("data"); err != nil || len(held) != len(names) {
		t.Fatalf("A hold count = %d (%v), want %d", len(held), err, len(names))
	}
	if a.DeleteGuardTripped("data") {
		t.Fatal("the guard must not block: the deletions went through and were held")
	}
	// B keeps the content under the new name, and A released the new paths.
	for rel, want := range map[string]string{
		"dev/p00.bin": "proxy payload 00",
		"dev/p11.bin": "proxy payload 11",
	} {
		if got := readFile(t, rootB, rel); got != want {
			t.Fatalf("B %s = %q, want %q", rel, got, want)
		}
		if _, live := liveEntry(t, a.Folders["data"].Ix, rel); live {
			t.Fatalf("A must not track the out-of-gate %s", rel)
		}
	}
	// A full round later, nothing is lost and no interaction was needed.
	syncFully(t, a, b)
	for _, rel := range names {
		if exists(t, rootA, renamedPath(rel)) {
			t.Fatalf("A must not resurrect %s (out of its gate)", rel)
		}
		if !exists(t, rootA, ".sfx-hold/"+rel) {
			t.Fatalf("A lost its held copy of %s", rel)
		}
	}
}

// renamedPath maps proxy/x to the renamed dev/x path.
func renamedPath(rel string) string {
	return "dev/" + strings.TrimPrefix(rel, "proxy/")
}
