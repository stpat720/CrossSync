package node

import (
	"bytes"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/events"
)

// Duplicate corpora.
//
// A texture/asset tree routinely holds the same bytes under several names, and
// the live 57.8 GB pull that wedged for 74 minutes did so on a single file with
// zero bytes transferred, no error and no event. The tests here build that
// corpus — three files of IDENTICAL 10 MiB content in one directory, none of
// them present on the receiver — and require the round to finish, commit all
// three byte-exactly and leave nothing behind.
//
// 10 MiB is deliberate: with the 2000-block rule it is 80 blocks of 128 KiB,
// and with the default 4 MiB run cap that is three requests per file, so three
// files want 30 MiB of response bytes against the default 16 MiB
// pull_inflight_mb budget. The budget has to hold several files at once
// WITHOUT letting a file that has not been asked for yet fall out of the batch.
//
// The distinct-content control below shares that budget arithmetic and differs
// only in block hashes: it wedged exactly like the duplicate corpus did, so the
// trigger is "more response bytes wanted than the budget holds", not duplicate
// content. The duplicate corpus is kept because it is the live shape, and
// because a twin must never be pulled twice, conflicted or billowed into copies.

// twinStandInTime is the mtime every fixture file gets, so a re-scan sees no
// drift and the round's outcome is about content, never timing.
var twinStandInTime = time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)

// writeTwin writes body at rel under root with the fixed fixture mtime.
func writeTwin(t *testing.T, root, rel string, body []byte) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, body, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, twinStandInTime, twinStandInTime); err != nil {
		t.Fatal(err)
	}
}

// waitRoundWithin waits for the initiator's round and returns its error,
// cutting the link if the round does not come back inside d.
//
// A wedged round must fail THIS test, not the suite: the stall watchdog is
// shortened by the caller, so a round that is merely waiting on the peer
// aborts itself in seconds, and a round that is stuck with nothing outstanding
// (the bug these tests are about) has no timer at all and would hang forever.
func waitRoundWithin(t *testing.T, r *roundRun, d time.Duration, what string) error {
	t.Helper()
	var roundErr error
	select {
	case roundErr = <-r.done:
	case <-time.After(d):
		r.Cut() // unsticks the serving side so the node can be closed
		t.Fatalf("%s did not finish within %s (a round with no request outstanding and no timer never returns)", what, d)
	}
	_ = r.client.Close()
	select {
	case <-r.bridge:
	case <-time.After(20 * time.Second):
		t.Fatalf("%s: the link bridge did not finish", what)
	}
	select {
	case err := <-r.served:
		if roundErr == nil && err != nil {
			return fmt.Errorf("server side: %w", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("%s: the serving side did not finish", what)
	}
	return roundErr
}

// twinRound runs one round from b (puller) against a (server) over the latency
// link used by the pull tests, with the stall window shortened so a real stall
// is reported quickly instead of after the production 120 s.
func twinRound(t *testing.T, a, b *Node) error {
	t.Helper()
	stalledPullTimeout(t, 5*time.Second)
	r := startLatencyRound(t, a, b, 2*time.Millisecond, nil)
	return waitRoundWithin(t, r, 30*time.Second, "the pull round")
}

// checkTwinsArrived verifies every name on the receiver: content byte-exact,
// indexed at the right size, no conflict copy, no staged temp left, no open
// unsynced event.
func checkTwinsArrived(t *testing.T, b *Node, root string, names []string, body []byte) {
	t.Helper()
	for _, name := range names {
		got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil {
			t.Fatalf("%s did not arrive: %v", name, err)
		}
		if !bytes.Equal(got, body) {
			diff := -1
			for i := 0; i < len(got) && i < len(body); i++ {
				if got[i] != body[i] {
					diff = i
					break
				}
			}
			t.Fatalf("%s: %d bytes, want %d, first difference at %d", name, len(got), len(body), diff)
		}
		fi, ok, err := b.Folders["data"].Ix.Get(name)
		if err != nil || !ok {
			t.Fatalf("%s not indexed on the receiver: ok=%v err=%v", name, ok, err)
		}
		if fi.Size != int64(len(body)) {
			t.Fatalf("%s indexed with size %d, want %d", name, fi.Size, len(body))
		}
	}
	if conflicts := findConflictCopies(t, root); len(conflicts) > 0 {
		t.Fatalf("duplicate content produced conflict copies: %v", conflicts)
	}
	if leftovers, _ := filepath.Glob(filepath.Join(root, ".sfx-tmp", "*.part")); len(leftovers) > 0 {
		t.Fatalf("staged temps left after the round: %v", leftovers)
	}
	evs, err := b.Events.Query(events.Filter{Folder: "data", Category: events.CatUnsynced, OpenOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 0 {
		t.Fatalf("a healthy duplicate pull left %d open unsynced event(s), first: %q", len(evs), evs[0].Reason)
	}
}

// findConflictCopies lists every conflict copy anywhere under root.
func findConflictCopies(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.Contains(d.Name(), ".sync-conflict-") {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	return out
}

// TestPullIdenticalTwinsConverge is the wedge test: a directory of files with
// IDENTICAL content must converge in ONE round, exactly like a directory of
// distinct files does.
func TestPullIdenticalTwinsConverge(t *testing.T) {
	const size = 10 << 20
	body := pattern(31, size, rand.New(rand.NewSource(31)))

	t.Run("same-directory-twins", func(t *testing.T) {
		rootA, rootB := t.TempDir(), t.TempDir()
		a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
		b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
		b.Cfg.PullConcurrency = config.DefaultPullConcurrency

		names := []string{"tex/a.dds", "tex/b.dds", "tex/c.dds"}
		for _, name := range names {
			writeTwin(t, rootA, name, body)
		}
		if _, err := a.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}

		if err := twinRound(t, a, b); err != nil {
			t.Fatalf("round failed: %v", err)
		}
		checkTwinsArrived(t, b, rootB, names, body)
		if got := b.Folders["data"].Engine.Stats.FilesDown.Load(); got != int64(len(names)) {
			t.Fatalf("%d file(s) transferred, want %d (the receiver held no copy to reuse)", got, len(names))
		}

		// Convergence: a second round is a no-op — the twins are ONE value per
		// name with joined version vectors, so nothing is pulled again and the
		// two indexes agree.
		down := b.Folders["data"].Engine.Stats.FilesDown.Load()
		if err := twinRound(t, a, b); err != nil {
			t.Fatalf("second round failed: %v", err)
		}
		if got := b.Folders["data"].Engine.Stats.FilesDown.Load(); got != down {
			t.Fatalf("a converged corpus pulled %d more file(s) on the next round", got-down)
		}
		for _, name := range names {
			fa, oka, err := a.Folders["data"].Ix.Get(name)
			if err != nil || !oka {
				t.Fatalf("%s missing on the source: ok=%v err=%v", name, oka, err)
			}
			fb, okb, err := b.Folders["data"].Ix.Get(name)
			if err != nil || !okb {
				t.Fatalf("%s missing on the receiver: ok=%v err=%v", name, okb, err)
			}
			if fa.Size != fb.Size {
				t.Fatalf("%s: sizes differ between the nodes (%d vs %d)", name, fa.Size, fb.Size)
			}
			if !fa.Version.Equal(fb.Version) {
				t.Fatalf("%s: version vectors did not join (%v vs %v)", name, fa.Version, fb.Version)
			}
		}
	})

	t.Run("distinct-content-control", func(t *testing.T) {
		rootA, rootB := t.TempDir(), t.TempDir()
		a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
		b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
		b.Cfg.PullConcurrency = config.DefaultPullConcurrency

		rng := rand.New(rand.NewSource(32))
		names := []string{"tex/a.dds", "tex/b.dds", "tex/c.dds"}
		for i, name := range names {
			writeTwin(t, rootA, name, pattern(byte(40+i), size, rng))
		}
		if _, err := a.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if err := twinRound(t, a, b); err != nil {
			t.Fatalf("round failed: %v", err)
		}
		for _, name := range names {
			if _, err := os.Stat(filepath.Join(rootB, filepath.FromSlash(name))); err != nil {
				t.Fatalf("%s did not arrive: %v", name, err)
			}
		}
	})

	// The donor path while the batch is FULL: the receiver already holds one
	// copy (from a seed round), so the twins added in the second round are
	// satisfied from that local donor while distinct files are still crossing
	// the wire. A copy and a transfer therefore share one batch, which is the
	// shape that puts a local-copy commit between two network commits.
	t.Run("twins-satisfied-from-a-local-donor", func(t *testing.T) {
		rootA, rootB := t.TempDir(), t.TempDir()
		a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
		b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
		// Two slots: the network files hold both, so the twins are admitted
		// while requests are outstanding.
		b.Cfg.PullConcurrency = 2

		rng := rand.New(rand.NewSource(33))
		writeTwin(t, rootA, "tex/donor.dds", body)
		for _, name := range []string{"tex/net-1.dds", "tex/net-2.dds"} {
			writeTwin(t, rootA, name, pattern(byte(len(name)+50), size, rng))
		}
		if _, err := a.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if err := twinRound(t, a, b); err != nil {
			t.Fatalf("seed round failed: %v", err)
		}

		// Second round: three twins of the donor plus two new distinct files.
		twins := []string{"tex/twin-1.dds", "tex/twin-2.dds", "tex/twin-3.dds"}
		for _, name := range twins {
			writeTwin(t, rootA, name, body)
		}
		for _, name := range []string{"tex/net-3.dds", "tex/net-4.dds"} {
			writeTwin(t, rootA, name, pattern(byte(len(name)+60), size, rng))
		}
		if _, err := a.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}

		before := b.Folders["data"].Engine.Stats.FilesLocalCopy.Load()
		if err := twinRound(t, a, b); err != nil {
			t.Fatalf("round failed: %v", err)
		}
		checkTwinsArrived(t, b, rootB, twins, body)
		for _, name := range []string{"tex/net-3.dds", "tex/net-4.dds"} {
			if _, err := os.Stat(filepath.Join(rootB, filepath.FromSlash(name))); err != nil {
				t.Fatalf("%s did not arrive: %v", name, err)
			}
		}
		if got := b.Folders["data"].Engine.Stats.FilesLocalCopy.Load() - before; got != int64(len(twins)) {
			t.Fatalf("%d twin(s) were served from the local donor, want %d", got, len(twins))
		}
		// The donor is copied, never consumed: a receiver-side local copy is
		// additive and the donor stays live (only the origin's deletion half
		// may remove it).
		if _, err := os.Stat(filepath.Join(rootB, "tex", "donor.dds")); err != nil {
			t.Fatalf("the donor was removed by the local copies: %v", err)
		}
	})
}
