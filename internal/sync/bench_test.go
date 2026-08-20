package sync

import (
	"fmt"
	"path/filepath"
	"testing"

	"crosssync/internal/ignore"
	"crosssync/internal/index"
	"crosssync/internal/version"
)

// benchEngine builds an engine with n live entries in its index.
func benchEngine(b *testing.B, n int) *Engine {
	b.Helper()
	root := b.TempDir()
	ix, err := index.Open(filepath.Join(root, "ix.db"), "f")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { ix.Close() })
	for i := 0; i < n; i++ {
		if err := ix.Put(&index.FileInfo{
			Name: fmt.Sprintf("dir%d/f%d.txt", i%200, i), Size: 1,
			ModifiedS: int64(i), Type: index.TypeFile, Version: version.New().Bump(1),
		}); err != nil {
			b.Fatal(err)
		}
	}
	ig, _ := ignore.Parse(nil)
	return New(1, "f", root, ConflictCopy, nil, ix, nil, ig, b.Logf)
}

// BenchmarkNeedPulls measures the whole-index scan that every sync session
// runs. The 20k-entry case should be well under a second (a regression to
// per-name SQLite queries makes this many orders of magnitude worse).
func BenchmarkNeedPulls(b *testing.B) {
	for _, n := range []int{1000, 20000} {
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			eng := benchEngine(b, n)
			// A peer with a small delta: one tombstone + one new file.
			eng.SetPeerIndex(2, []*index.FileInfo{
				{Name: "dir0/f0.txt", Deleted: true, Version: version.New().Bump(1).Bump(2)},
				{Name: "new.txt", Size: 1, ModifiedS: 1, Type: index.TypeFile, Version: version.New().Bump(2)},
			}, false)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := eng.NeedPulls(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkIndexStats measures the single-pass aggregate used by the status
// poll. This must stay cheap even at large sizes (COUNT/SUM in SQLite, not
// row-by-row Go iteration).
func BenchmarkIndexStats(b *testing.B) {
	for _, n := range []int{1000, 20000} {
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			eng := benchEngine(b, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := eng.Ix.Stats(ConflictSuffix); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
