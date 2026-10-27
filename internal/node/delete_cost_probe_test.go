package node

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TestApplyDeletionHeldCost measures the per-file cost of applying a held
// deletion with a production-sized index (13k entries), which is what the peer
// pays 34k times for a wiped folder. It exists to answer "is the ~2s per file on
// the array in the code or in that box's disk", and is a measurement rather than
// an assertion so it can run anywhere.
func TestApplyDeletionHeldCost(t *testing.T) {
	root := t.TempDir()
	n := chunkTestNode(t, 1, "a", root, t.TempDir())
	f := n.Folders["data"]
	f.Scanner.SetSettleScans(1)

	const (
		files = 13000
		probe = 200
	)
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < files; i++ {
		writePattern(t, root, fmt.Sprintf("many/f%05d.bin", i), pattern(byte(i), 1024, rng))
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	held := 0
	for i := 0; i < probe; i++ {
		ok, err := f.Engine.ApplyDeletionHeld(fmt.Sprintf("many/f%05d.bin", i), 1)
		if err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
		if ok {
			held++
		}
	}
	per := time.Since(start) / probe
	t.Logf("%d held deletions over %d indexed files: %v per file (held %d)", probe, files, per, held)
	if per > 50*time.Millisecond {
		t.Errorf("per-deletion cost %v is far above the in-process budget; the cost is in the code, not the disk", per)
	}
}
