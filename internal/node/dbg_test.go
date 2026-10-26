package node

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDbgReuse(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	a := chunkTestNode(t, 1, "a", rootA, t.TempDir())
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())
	prefix := pattern(81, 1<<20, rand.New(rand.NewSource(81)))
	tail := pattern(82, 1<<20, rand.New(rand.NewSource(82)))
	v2 := append(append([]byte{}, prefix...), tail...)
	writePattern(t, rootA, "doc.bin", v2)
	writePattern(t, rootB, "doc.bin", prefix)
	newer := time.Date(2026, 8, 18, 13, 0, 0, 0, time.UTC)
	_ = os.Chtimes(filepath.Join(rootA, "doc.bin"), newer, newer)
	if _, err := a.ScanFolder("data"); err != nil { t.Fatal(err) }
	if _, err := b.ScanFolder("data"); err != nil { t.Fatal(err) }
	eng := b.Folders["data"].Engine
	fi, ok, err := eng.Ix.Get("doc.bin")
	t.Logf("local index: ok=%v err=%v blocks=%d size=%d bs=%d inflight=%v", ok, err, len(fi.Blocks), fi.Size, fi.BlockSize, eng.InFlight("doc.bin"))
}
