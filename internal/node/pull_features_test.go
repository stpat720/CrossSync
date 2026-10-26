package node

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLocalBlockReuseSavesTheWire pins the per-block reuse toggle.
//
// B already holds the first megabyte of a file as a DIFFERENT name (the shape of
// an edited copy, a shared prefix, or a .venv tree). The whole-file reuse path
// cannot help — the content keys differ — so every block used to be pulled. With
// local block reuse the matching prefix is copied on disk and only the tail
// crosses the wire.
func TestLocalBlockReuseSavesTheWire(t *testing.T) {
	newPair := func(t *testing.T) (a, b *Node, rootA, rootB string) {
		rootA, rootB = t.TempDir(), t.TempDir()
		a = chunkTestNode(t, 1, "a", rootA, t.TempDir())
		b = chunkTestNode(t, 2, "b", rootB, t.TempDir())
		return
	}

	// 1 MiB prefix shared, 1 MiB tail new: two 128 KiB-block runs differ.
	prefix := pattern(51, 1<<20, rand.New(rand.NewSource(51)))
	tail := pattern(52, 1<<20, rand.New(rand.NewSource(52)))
	full := append(append([]byte{}, prefix...), tail...)

	t.Run("enabled", func(t *testing.T) {
		a, b, rootA, rootB := newPair(t)
		// reuse is ON by default (only pull_disable_block_reuse turns it off)
		writePattern(t, rootA, "full.bin", full)
		writePattern(t, rootB, "donor.bin", prefix)
		if _, err := a.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		before := b.Folders["data"].Engine.Stats.BytesDown.Load()
		if err := chunkPipe(t, a, b); err != nil {
			t.Fatalf("round failed: %v", err)
		}
		got, err := os.ReadFile(filepath.Join(rootB, "full.bin"))
		if err != nil || !bytes.Equal(got, full) {
			t.Fatalf("content mismatch after local block reuse (err=%v)", err)
		}
		wire := b.Folders["data"].Engine.Stats.BytesDown.Load() - before
		reused := b.Folders["data"].Engine.Stats.BlocksReused.Load()
		t.Logf("wire %d bytes, %d block(s) reused locally", wire, reused)
		if reused == 0 {
			t.Fatalf("no block was reused from the local donor")
		}
		if wire >= int64(len(full)) {
			t.Fatalf("%d wire bytes for a %d-byte file: nothing was saved", wire, len(full))
		}
	})

	t.Run("disabled", func(t *testing.T) {
		a, b, rootA, rootB := newPair(t)
		b.Cfg.PullDisableBlockReuse = true
		writePattern(t, rootA, "full.bin", full)
		writePattern(t, rootB, "donor.bin", prefix)
		if _, err := a.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ScanFolder("data"); err != nil {
			t.Fatal(err)
		}
		before := b.Folders["data"].Engine.Stats.BytesDown.Load()
		if err := chunkPipe(t, a, b); err != nil {
			t.Fatalf("round failed: %v", err)
		}
		got, err := os.ReadFile(filepath.Join(rootB, "full.bin"))
		if err != nil || !bytes.Equal(got, full) {
			t.Fatalf("content mismatch with reuse off (err=%v)", err)
		}
		wire := b.Folders["data"].Engine.Stats.BytesDown.Load() - before
		if reused := b.Folders["data"].Engine.Stats.BlocksReused.Load(); reused != 0 {
			t.Fatalf("reuse is off but %d block(s) were reused", reused)
		}
		if wire != int64(len(full)) {
			t.Fatalf("wire %d bytes, want the whole %d-byte file", wire, len(full))
		}
	})
}

// TestPullOrderSmallFilesFirst pins the ordering toggle: with "smallest" the
// small files are requested before the big one, and with "index" the walk order
// is untouched.
func TestPullOrderSmallFilesFirst(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
	b.Cfg.PullOrder = "smallest"

	rng := rand.New(rand.NewSource(61))
	writePattern(t, rootA, "a-big.bin", pattern(62, 6<<20, rng))   // first by name
	writePattern(t, rootA, "m-small.bin", pattern(63, 2<<10, rng)) // tiny
	writePattern(t, rootA, "z-mid.bin", pattern(64, 1<<20, rng))
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	log := &wireLog{}
	if err := latencyRound(t, a, b, time.Millisecond, log); err != nil {
		t.Fatal(err)
	}
	first := ""
	log.mu.Lock()
	if len(log.names) > 0 {
		first = log.names[0]
	}
	log.mu.Unlock()
	if first != "m-small.bin" {
		t.Fatalf("first request was for %q, want the smallest file (m-small.bin)", first)
	}
	if got := readAllT(t, rootB, "a-big.bin"); len(got) != 6<<20 {
		t.Fatalf("big file arrived with %d bytes", len(got))
	}
}

// TestPullOrderIndexKeepsWalkOrder: the escape hatch must not reorder anything.
func TestPullOrderIndexKeepsWalkOrder(t *testing.T) {
	b := chunkTestNode(t, 2, "b", t.TempDir(), t.TempDir())
	b.Cfg.PullOrder = "index"
	names := []string{"a.bin", "b.bin", "c.bin"}
	got := b.orderPulls(b.Folders["data"], 1, names)
	for i := range names {
		if got[i] != names[i] {
			t.Fatalf("index order changed: %v", got)
		}
	}
}

// TestPullOrderUnknownSizesSortLast: a name the peer's view has no size for must
// not jump ahead of files whose cost is known.
func TestPullOrderUnknownSizesSortLast(t *testing.T) {
	b := chunkTestNode(t, 2, "b", t.TempDir(), t.TempDir())
	b.Cfg.PullOrder = "smallest"
	names := []string{"unknown.bin", "small.bin"}
	// orderPulls reads the peer view; with no peer view every size is unknown,
	// so ordering falls back to name order (stable, deterministic).
	got := b.orderPulls(b.Folders["data"], 99, names)
	if len(got) != 2 || got[0] != "small.bin" || got[1] != "unknown.bin" {
		t.Fatalf("unexpected order: %v", got)
	}
}

// readAllT reads a file under root, failing the test on error.
func readAllT(t *testing.T, root, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestLocalBlockReuseIsSameDirectory pins the scope: a donor in a DIFFERENT
// directory is not consulted. Cross-directory partial overlap would need a
// folder-wide map, which costs an index walk per round and still missed donors
// on a big folder; whole-file twins are still free anywhere (fillFromLocalCopy).
func TestLocalBlockReuseIsSameDirectory(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	prefix := pattern(71, 1<<20, rand.New(rand.NewSource(71)))
	tail := pattern(72, 1<<20, rand.New(rand.NewSource(72)))
	full := append(append([]byte{}, prefix...), tail...)

	writePattern(t, rootA, "sub/full.bin", full)
	writePattern(t, rootB, "other/donor.bin", prefix) // same blocks, other directory
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	before := b.Folders["data"].Engine.Stats.BytesDown.Load()
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatalf("round failed: %v", err)
	}
	if got := readAllT(t, rootB, "sub/full.bin"); !bytes.Equal(got, full) {
		t.Fatalf("content mismatch")
	}
	if reused := b.Folders["data"].Engine.Stats.BlocksReused.Load(); reused != 0 {
		t.Fatalf("%d block(s) reused across directories, want 0 (same-directory scope)", reused)
	}
	if wire := b.Folders["data"].Engine.Stats.BytesDown.Load() - before; wire != int64(len(full)) {
		t.Fatalf("wire %d bytes, want the whole %d-byte file", wire, len(full))
	}
}

// TestLocalBlockReuseWhenTheTargetOwnsTheHash pins the bug the live probe found:
// the file being replaced is itself an owner of the blocks it needs, and the
// index is walked name-DESCENDING, so the target is always visited first. With a
// single owner per hash the donor map handed every hash to the target, the pull
// skipped itself, and a sibling copy sitting right beside it was never used
// (0 blocks reused on the live pair). One extra owner per hash fixes it.
func TestLocalBlockReuseWhenTheTargetOwnsTheHash(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	prefix := pattern(91, 1<<20, rand.New(rand.NewSource(91)))
	tail := pattern(92, 1<<20, rand.New(rand.NewSource(92)))
	v2 := append(append([]byte{}, prefix...), tail...)

	// Both live in ONE subdirectory: the folder root walks via ListAll (rowid
	// order), while a real directory uses ListPrefix's name-DESC walk, which is
	// what hands the target the hashes before its copy (the live failure).
	writePattern(t, rootA, "sub/zz-target.bin", v2)
	writePattern(t, rootB, "sub/zz-target.bin", prefix) // version 1: owns those hashes
	writePattern(t, rootB, "sub/aa-copy.bin", prefix)   // the sibling that must be used
	newer := time.Date(2026, 8, 18, 13, 0, 0, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(rootA, "sub", "zz-target.bin"), newer, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	before := b.Folders["data"].Engine.Stats.BytesDown.Load()
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatalf("round failed: %v", err)
	}
	if got := readAllT(t, rootB, "sub/zz-target.bin"); !bytes.Equal(got, v2) {
		t.Fatalf("content mismatch")
	}
	reused := b.Folders["data"].Engine.Stats.BlocksReused.Load()
	wire := b.Folders["data"].Engine.Stats.BytesDown.Load() - before
	t.Logf("target owns the hash: %d block(s) reused, wire %d of %d", reused, wire, len(v2))
	if reused == 0 {
		t.Fatalf("no block reused: the sibling copy was shadowed by the target")
	}
	if wire >= int64(len(v2)) {
		t.Fatalf("wire %d bytes, want the prefix to come from the local copy", wire)
	}
}

// TestLocalBlockReuseAcrossRounds mirrors the live probe row for row: the donor
// arrives in an EARLIER round and settles, then the peer rewrites the target
// larger so only its tail is new. This is the shape that failed live (0 blocks
// reused, full 2 MiB on the wire) while a same-round unit test passed, so the
// freshness of the donor is exactly what the test has to control.
func TestLocalBlockReuseAcrossRounds(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	prefix := pattern(93, 1<<20, rand.New(rand.NewSource(93)))
	tail := pattern(94, 1<<20, rand.New(rand.NewSource(94)))
	v2 := append(append([]byte{}, prefix...), tail...)

	writePattern(t, rootA, "sub/aa-copy.bin", prefix)
	writePattern(t, rootA, "sub/zz-target.bin", prefix)
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	if err := chunkPipe(t, a, b); err != nil { // round 1: both files land on b
		t.Fatalf("first round: %v", err)
	}
	if got := readAllT(t, rootB, "sub/aa-copy.bin"); !bytes.Equal(got, prefix) {
		t.Fatalf("donor did not land")
	}

	// Round 2: the target grows; its first megabyte is already here as aa-copy.
	newer := time.Date(2026, 8, 18, 15, 0, 0, 0, time.UTC)
	writePattern(t, rootA, "sub/zz-target.bin", v2)
	if err := os.Chtimes(filepath.Join(rootA, "sub", "zz-target.bin"), newer, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	before := b.Folders["data"].Engine.Stats.BytesDown.Load()
	if err := chunkPipe(t, a, b); err != nil {
		t.Fatalf("second round: %v", err)
	}
	if got := readAllT(t, rootB, "sub/zz-target.bin"); !bytes.Equal(got, v2) {
		t.Fatalf("content mismatch after the rewrite")
	}
	reused := b.Folders["data"].Engine.Stats.BlocksReused.Load()
	wire := b.Folders["data"].Engine.Stats.BytesDown.Load() - before
	t.Logf("across rounds: %d block(s) reused, wire %d of %d", reused, wire, len(v2))
	if reused == 0 {
		t.Fatalf("no block reused across rounds")
	}
}
