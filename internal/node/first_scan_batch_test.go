package node

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crosssync/internal/index"
	"crosssync/internal/scanner"
)

// scanFingerprint renders the whole index (live and tombstoned) as
// name -> (size, mtime, type, blocks, version). Two passes over the same tree
// must produce the SAME fingerprint: a resumed pass may not re-version content
// it had already committed, and it may not leave an entry behind.
func scanFingerprint(t *testing.T, n *Node, id string) map[string]string {
	t.Helper()
	f, ok := n.Folders[id]
	if !ok {
		t.Fatalf("fixture folder %q is missing", id)
	}
	out := map[string]string{}
	if err := f.Ix.ListAll(func(fi *index.FileInfo) error {
		out[fi.Name] = fmt.Sprintf("%d/%d.%09d/type=%d/del=%v/ver=%v/block=%d/%x",
			fi.Size, fi.ModifiedS, fi.ModifiedNs, int(fi.Type), fi.Deleted, fi.Version,
			fi.BlockSize, fi.Blocks)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// diffFingerprints reports the first difference between two snapshots.
func diffFingerprints(a, b map[string]string) string {
	names := make([]string, 0, len(a)+len(b))
	seen := map[string]bool{}
	for name := range a {
		names = append(names, name)
		seen[name] = true
	}
	for name := range b {
		if !seen[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		av, aok := a[name]
		bv, bok := b[name]
		switch {
		case !aok:
			return fmt.Sprintf("%s appeared in the second index (%s)", name, bv)
		case !bok:
			return fmt.Sprintf("%s is missing from the second index (%s)", name, av)
		case av != bv:
			return fmt.Sprintf("%s: %s -> %s", name, av, bv)
		}
	}
	return ""
}

// countLiveFiles counts the folder's live (non-tombstone) file entries. Safe to
// call from a scanner worker (no *testing.T): the hash hook samples it.
func countLiveFiles(f *Folder) (int, error) {
	n := 0
	err := f.Ix.ListAll(func(fi *index.FileInfo) error {
		if !fi.Deleted && fi.Type == index.TypeFile {
			n++
		}
		return nil
	})
	return n, err
}

// liveFileCount is countLiveFiles for the test goroutine.
func liveFileCount(f *Folder) int {
	n, err := countLiveFiles(f)
	if err != nil {
		panic(err)
	}
	return n
}

// firstScanFixture builds a "data" folder holding n small files whose (size,
// mtime) are fixed, so two fixtures are byte-identical to the index.
func firstScanFixture(t *testing.T, files int) (*Node, *Folder) {
	t.Helper()
	n := chunkTestNode(t, 1, "a", t.TempDir(), t.TempDir())
	f := n.Folders["data"]
	for i := 0; i < files; i++ {
		writeAt(t, f.Root, fmt.Sprintf("src/f%03d.bin", i), fmt.Sprintf("%04d-%s", i, strings.Repeat("p", 2048)))
	}
	return n, f
}

// TestFirstScanCommitsInBatches is the resumability contract: a first pass over a
// share must not hold every hashed entry in memory and commit once at the very
// end. With the batch size shrunk, the index must GROW during the pass, the
// number of index transactions must stay O(batches) instead of O(files), and the
// folder must still count as "first pass pending" while that happens — a
// partially indexed folder is never advertised.
func TestFirstScanCommitsInBatches(t *testing.T) {
	const batch = 5
	const files = 40
	restore := scanner.SetBatchLimits(batch, 0)
	t.Cleanup(restore)

	n, f := firstScanFixture(t, files)
	// A daemon start arms every folder's first pass: the folder is withheld from
	// peers until that pass COMPLETES, no matter how much of the index it has
	// already banked.
	n.SetStartupScanning(true)

	writeBefore := f.Ix.WriteSeq()

	// Sample the folder WHILE it is hashing. The hook runs on a scanner worker,
	// so it records raw numbers and the test goroutine asserts on them.
	var (
		midOnce        sync.Once
		mu             sync.Mutex
		midFiles       int
		midStatusFiles int
		midCommitted   int64
		midBatches     int
		midPending     bool
		midAdvertised  []string
		midErr         error
		hashed         atomic.Int32
	)
	f.Scanner.SetHashHook(func(string) {
		if hashed.Add(1) != 3*batch {
			return
		}
		midOnce.Do(func() {
			// A commit lands from another worker, so the index and the status
			// counter can be observed a hair apart: wait until a batch is
			// visible as committed (the pass is still running: the producer has
			// 40 files to hand out).
			deadline := time.Now().Add(5 * time.Second)
			for n.scanSnapshot("data").Committed == 0 {
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			cnt, err := countLiveFiles(f)
			st := n.scanSnapshot("data")
			adv := n.advertisedFolders(7)
			// What /api/status would publish right now: the cached aggregate,
			// refreshed the way the background ticker does.
			n.refreshFolderStatusCounts(f)
			stc, _ := f.StatusCounts()
			mu.Lock()
			midFiles, midErr = cnt, err
			midStatusFiles = stc.Files
			midCommitted, midBatches = st.Committed, st.Batches
			midPending = n.FolderIndexing("data")
			midAdvertised = adv
			mu.Unlock()
		})
	})

	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	f.Scanner.SetHashHook(nil)

	mu.Lock()
	gotFiles, gotCommitted, gotBatches := midFiles, midCommitted, midBatches
	gotPending, gotAdv, sampleErr := midPending, midAdvertised, midErr
	gotStatusFiles := midStatusFiles
	mu.Unlock()
	if sampleErr != nil {
		t.Fatalf("sampling mid-pass: %v", sampleErr)
	}
	if gotFiles == 0 || gotCommitted == 0 || gotBatches == 0 {
		t.Fatalf("mid-pass the index held %d file(s) with committed=%d batches=%d: the pass saved everything for the end instead of committing in batches",
			gotFiles, gotCommitted, gotBatches)
	}
	if gotFiles >= files {
		t.Fatalf("index already held all %d files mid-pass — the sample did not land inside the pass", gotFiles)
	}
	if !gotPending {
		t.Fatal("the folder stopped counting as 'first pass pending' while the pass was still running: a partial index would be advertised")
	}
	if len(gotAdv) != 0 {
		t.Fatalf("advertised %v mid-pass, want nothing: a partially indexed folder must not be published", gotAdv)
	}
	// The status page reads the cached aggregate: it must be able to see the
	// banked entries rather than report an empty folder.
	//
	// The sampler runs on a scanner worker while another worker's batch commit
	// is in flight, and a commit writes the index and THEN adds what it wrote
	// to the cached counters (commitScanBatch). The two are therefore observed a
	// hair apart — the sampling code above says so — and exactly one batch can
	// legitimately be missing from the count. The defect this test is about is a
	// status figure that is frozen (or wholesale behind), so a one-batch lag is
	// tolerated and anything more is not.
	if gotStatusFiles == 0 && gotFiles > batch {
		t.Fatalf("status files = 0 mid-pass while %d entries are banked: the status aggregate "+
			"does not see the committed batches at all", gotFiles)
	}
	if lag := gotFiles - gotStatusFiles; lag < 0 || lag > batch {
		t.Fatalf("status files = %d mid-pass, want the %d entries the index already held "+
			"(at most one %d-entry batch may be mid-publish: lag=%d)",
			gotStatusFiles, gotFiles, batch, lag)
	}

	// Finished: everything is indexed once, and the folder is published.
	if got := liveFileCount(f); got != files {
		t.Fatalf("index holds %d live files after the pass, want %d", got, files)
	}
	if n.FolderIndexing("data") {
		t.Fatal("first pass still pending after the pass finished")
	}
	if got := n.advertisedFolders(7); len(got) != 1 || got[0] != "data" {
		t.Fatalf("advertised %v after the pass finished, want [data]", got)
	}
	done := scanStatusOf(t, n, "data")
	if done.Committed != files {
		t.Fatalf("committed = %d after the pass, want %d", done.Committed, files)
	}
	if want := files / batch; done.Batches != want {
		t.Fatalf("batches = %d, want %d (one commit per %d files)", done.Batches, want, batch)
	}

	// Task count, not file count: one transaction per batch (plus the directory
	// entry applied at the end), never one per file.
	if writes := f.Ix.WriteSeq() - writeBefore; writes > int64(files/batch)+2 {
		t.Fatalf("%d index transactions for %d files in batches of %d: want O(batches), not O(files)", writes, files, batch)
	}
}

// TestFirstScanResumesAfterCancellation is the operator's requirement, measured:
// "the hashing should persist after restart so it picks up from where it left
// off". A pass is cancelled mid-hash; the next pass over the same tree must hash
// ONLY the files the first one did not bank, and its end state must be identical
// (entry for entry, hashes and versions) to an uninterrupted pass.
func TestFirstScanResumesAfterCancellation(t *testing.T) {
	const batch = 4
	const files = 40
	restore := scanner.SetBatchLimits(batch, 0)
	t.Cleanup(restore)

	n, f := firstScanFixture(t, files)
	n.SetStartupScanning(true)

	var (
		hashed1 atomic.Int32
		mu      sync.Mutex
		first   []string
	)
	f.Scanner.SetHashHook(func(name string) {
		mu.Lock()
		first = append(first, name)
		mu.Unlock()
		if hashed1.Add(1) == 3*batch {
			if err := n.SetFolderPaused("data", true); err != nil {
				t.Errorf("pause: %v", err)
			}
		}
	})

	if applied, err := n.ScanFolder("data"); !errors.Is(err, ErrScanCancelled) {
		t.Fatalf("pass 1 = (%d, %v), want ErrScanCancelled", applied, err)
	}
	f.Scanner.SetHashHook(nil)

	banked := liveFileCount(f)
	if banked == 0 {
		t.Fatal("the cancelled pass banked nothing: batching is not in effect")
	}
	if banked >= files {
		t.Fatalf("the cancelled pass banked all %d files, so resume cannot be observed", files)
	}
	if banked%batch != 0 {
		t.Fatalf("%d entries banked by a cancelled pass, want a whole number of %d-entry batches (a partial batch must never be applied)", banked, batch)
	}
	if !n.FolderIndexing("data") {
		t.Fatal("a cancelled pass cleared the first-pass mark: a partially indexed folder must stay withheld")
	}
	if st := scanStatusOf(t, n, "data"); st.Phase != "cancelled" {
		t.Fatalf("phase = %q after a cancelled pass, want \"cancelled\"", st.Phase)
	}
	// The snapshot the resumed pass must preserve for everything it banked.
	bankedFP := scanFingerprint(t, n, "data")

	// Restart: a fresh process unpauses the folder and arms its first pass again.
	// The index (and only the index) carries the hashing already done.
	if err := n.SetFolderPaused("data", false); err != nil {
		t.Fatal(err)
	}
	n.SetStartupScanning(true)

	var (
		mu2    sync.Mutex
		second = map[string]int{}
	)
	f.Scanner.SetHashHook(func(name string) {
		mu2.Lock()
		second[name]++
		mu2.Unlock()
	})
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatalf("resumed pass: %v", err)
	}
	f.Scanner.SetHashHook(nil)

	// Everything pass 1 banked is in the index; everything it merely HASHED
	// (but never committed) is not. The resumed pass must read exactly the
	// second group: never a banked file, never nothing.
	mu.Lock()
	firstNames := append([]string(nil), first...)
	mu.Unlock()
	if len(firstNames) == 0 {
		t.Fatal("pass 1 hashed nothing: the hook never fired")
	}
	bankedSet := map[string]bool{}
	for name := range bankedFP {
		bankedSet[name] = true
	}
	if len(bankedSet) != banked {
		t.Fatalf("fingerprint has %d entries, want the %d files the index holds", len(bankedSet), banked)
	}
	mu2.Lock()
	for name := range bankedSet {
		if got := second[name]; got != 0 {
			mu2.Unlock()
			t.Fatalf("%s was hashed again by the resumed pass (%d read(s)): it was already in the index and must be skipped", name, got)
		}
	}
	rehashed := 0
	for _, c := range second {
		rehashed += c
	}
	mu2.Unlock()
	// Everything not banked must be read (and only that): 20 of 40 in the usual
	// case, since the pause stops the hash phase part way.
	if want := files - banked; rehashed != want {
		t.Fatalf("the resumed pass hashed %d file(s), want exactly the %d that were still unindexed", rehashed, want)
	}

	if got := liveFileCount(f); got != files {
		t.Fatalf("after the resumed pass the index holds %d live files, want %d", got, files)
	}
	// The banked entries must be untouched by the resumed pass: same hashes,
	// same version (a double apply would have bumped them).
	after := scanFingerprint(t, n, "data")
	for name, want := range bankedFP {
		if got, ok := after[name]; !ok || got != want {
			t.Fatalf("banked entry %s changed by the resumed pass: %s -> %s", name, want, got)
		}
	}
	// And the end state equals an uninterrupted pass over the same tree.
	ref, _ := firstScanFixture(t, files)
	if _, err := ref.ScanFolder("data"); err != nil {
		t.Fatalf("reference pass: %v", err)
	}
	if diff := diffFingerprints(scanFingerprint(t, ref, "data"), after); diff != "" {
		t.Fatalf("resumed pass end state differs from an uninterrupted pass: %s", diff)
	}
}

// TestPauseAtBatchBoundaryAppliesOnlyWholeBatches: a pause that lands between
// batch commits must leave the index holding whole batches only, report
// "cancelled", and keep the folder unadvertised.
func TestPauseAtBatchBoundaryAppliesOnlyWholeBatches(t *testing.T) {
	const batch = 4
	const files = 40
	restore := scanner.SetBatchLimits(batch, 0)
	t.Cleanup(restore)

	n, f := firstScanFixture(t, files)
	n.SetStartupScanning(true)

	var hashed atomic.Int32
	f.Scanner.SetHashHook(func(string) {
		// Pause once the first batch is certainly banked and while another is
		// still being built.
		if hashed.Add(1) == batch+2 {
			if err := n.SetFolderPaused("data", true); err != nil {
				t.Errorf("pause: %v", err)
			}
		}
	})
	if _, err := n.ScanFolder("data"); !errors.Is(err, ErrScanCancelled) {
		t.Fatalf("scan = %v, want ErrScanCancelled", err)
	}
	f.Scanner.SetHashHook(nil)

	got := liveFileCount(f)
	if got == 0 {
		t.Fatal("no batch was committed before the pause: the test proves nothing")
	}
	if got%batch != 0 {
		t.Fatalf("%d entries in the index, want a whole number of %d-entry batches: the uncommitted remainder was applied", got, batch)
	}
	if got >= files {
		t.Fatalf("all %d files are indexed, want the pass to have been cut short", files)
	}
	st := scanStatusOf(t, n, "data")
	if st.Phase != "cancelled" {
		t.Fatalf("phase = %q, want \"cancelled\"", st.Phase)
	}
	if st.Committed != int64(got) {
		t.Fatalf("status committed = %d, want %d (the entries actually banked)", st.Committed, got)
	}
	if !n.FolderIndexing("data") {
		t.Fatal("a cancelled pass cleared the first-pass mark")
	}
	if adv := n.advertisedFolders(7); len(adv) != 0 {
		t.Fatalf("advertised %v after a partial pass, want nothing", adv)
	}
}

// TestBatchCommitsWithholdTheFolderUntilThePassCompletes pins the publication
// gate (e010659) against batching: the folder stays unadvertised while it is
// hashing even though its index already holds entries, and becomes visible the
// moment the pass finishes.
func TestBatchCommitsWithholdTheFolderUntilThePassCompletes(t *testing.T) {
	const batch = 3
	const files = 20
	restore := scanner.SetBatchLimits(batch, 0)
	t.Cleanup(restore)

	n, f := firstScanFixture(t, files)
	n.SetStartupScanning(true)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var hashed atomic.Int32
	f.Scanner.SetHashHook(func(string) {
		if hashed.Add(1) == 3*batch {
			once.Do(func() { close(entered) })
			<-release
		}
	})
	done := make(chan error, 1)
	go func() {
		_, err := n.ScanFolder("data")
		done <- err
	}()

	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("the pass ended before the barrier (err=%v)", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the pass never reached the barrier")
	}

	// The barrier fires on ONE worker the moment it finishes a hash; the batch
	// that worker belongs to is flushed by whichever worker crosses the entry
	// limit, which can be a hair later. Wait (bounded) for the first batch to
	// land instead of assuming the barrier implies it — the pass is still
	// running and the other workers keep hashing.
	waitUntil := time.Now().Add(5 * time.Second)
	for {
		if banked := liveFileCount(f); banked > 0 {
			break
		}
		if time.Now().After(waitUntil) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	banked := liveFileCount(f)
	if banked == 0 {
		t.Fatalf("nothing banked at the barrier: batching is not in effect")
	}
	if adv := n.advertisedFolders(7); len(adv) != 0 {
		t.Fatalf("advertised %v while the first pass is still running, want nothing (partial index published)", adv)
	}
	if got := n.sharedFolders(map[string]bool{"data": true}, 7); len(got) != 0 {
		t.Fatalf("shared %v mid-pass, want nothing: a round would exchange a partial index", got)
	}
	if !n.StartupScanning() {
		t.Fatal("startup_scanning reports false while the folder is still hashing")
	}
	// Wait (bounded) for the first batch's own publish to land: the commit
	// writes the index and THEN adds what it wrote to the cached counters, so a
	// sample taken inside that window legitimately sees the index one batch
	// ahead. What this test is about — a status figure frozen at zero for the
	// whole pass — still fails below.
	statusDeadline := time.Now().Add(5 * time.Second)
	var stc index.Stats
	for {
		n.refreshFolderStatusCounts(f)
		stc, _ = f.StatusCounts()
		if stc.Files > 0 || time.Now().After(statusDeadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := stc.Files; got == 0 {
		t.Fatalf("status files = 0 while %d entries are banked: the status aggregate never sees "+
			"the committed batches", banked)
	} else if banked-got > batch {
		t.Fatalf("status files = %d while %d entries are banked: the count trails by more than "+
			"the one %d-entry batch a commit may have in flight", got, banked, batch)
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pass: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the pass did not finish after the barrier was released")
	}
	if adv := n.advertisedFolders(7); len(adv) != 1 || adv[0] != "data" {
		t.Fatalf("advertised %v after the pass completed, want [data]", adv)
	}
	if n.StartupScanning() {
		t.Fatal("startup_scanning still true after the only folder's pass finished")
	}
}

// TestBatchCommitsCoverAnEmptyRuntimeIndex pins the second half of the rule that
// decides when a pass batches: an index that holds NO files yet (a folder added
// at runtime, a re-add after the index was wiped) is building an index just as
// much as a first pass is, and gets the same batching. Once the index is
// populated, the next pass goes back to the single end-of-pass apply, where
// rename pairing needs the whole change set in hand.
func TestBatchCommitsCoverAnEmptyRuntimeIndex(t *testing.T) {
	const batch = 5
	const files = 20
	restore := scanner.SetBatchLimits(batch, 0)
	t.Cleanup(restore)

	n, f := firstScanFixture(t, files)
	// No SetStartupScanning: this is the runtime add-folder path.
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatalf("pass over the empty index: %v", err)
	}
	if got := liveFileCount(f); got != files {
		t.Fatalf("index holds %d files, want %d", got, files)
	}
	if st := scanStatusOf(t, n, "data"); st.Batches != files/batch {
		t.Fatalf("batches = %d for an empty-index pass over %d files in batches of %d, want %d (one end-of-pass apply is what makes the pass unresumable)",
			st.Batches, files, batch, files/batch)
	}

	// A file changes on an index that is now populated: one end-of-pass apply.
	writeAt(t, f.Root, "src/f000.bin", "changed content, different length")
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatalf("pass over the populated index: %v", err)
	}
	if st := scanStatusOf(t, n, "data"); st.Batches != 0 {
		t.Fatalf("batches = %d for a pass over an already populated index, want 0 (the single end-of-pass apply keeps rename pairing intact)", st.Batches)
	}
}
