package index

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"crosssync/internal/version"
)

// TestScaleFullExchangePipeline simulates the cost of a full index exchange
// at scale: seed N entries, stream them through JSON frames the way
// sendIndexes does, and merge the frames into a peer view the way
// mergeIndex does. This is the hot path a million-file folder pays once
// per (re)connection, so it must stay near-linear in N.
func TestScaleFullExchangePipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	const chunk = 4096
	for _, n := range []int{100000, 200000} {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
			ix := openTestIndex(t)
			files := make([]*FileInfo, 0, n)
			for i := 0; i < n; i++ {
				files = append(files, &FileInfo{
					Name: fmt.Sprintf("d%03d/f%06d.bin", i/10000, i),
					Size: 2048, Type: TypeFile, Version: version.New(),
				})
			}
			if err := ix.SeedBulk(files); err != nil {
				t.Fatal(err)
			}

			// Sender side: ListAll -> JSON frames (what sendIndexes sends).
			sent := make([]*FileInfo, 0, n)
			if err := ix.ListAll(func(fi *FileInfo) error {
				sent = append(sent, fi)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := json.Marshal(sent); err != nil {
				t.Fatal(err)
			}

			// Receiver side: parse frames into a peer view (what
			// mergeIndex does, minus the engine lock — the map merge is
			// the same shape).
			view := map[string]*FileInfo{}
			for start := 0; start < n; {
				end := start + chunk
				if end > n {
					end = n
				}
				for _, fi := range sent[start:end] {
					view[fi.Name] = fi
				}
				start = end
			}
			if len(view) != n {
				t.Fatalf("peer view = %d entries, want %d", len(view), n)
			}
		})
	}
}

// TestScaleLinearGrowth checks that the per-entry cost of the database
// paths does not grow superlinearly: a 10x larger folder must take at most
// ~20x the time (linear + slack; a quadratic regression would be ~100x).
func TestScaleLinearGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	measure := func(n int) time.Duration {
		ix := openTestIndex(t)
		files := make([]*FileInfo, 0, n)
		for i := 0; i < n; i++ {
			files = append(files, &FileInfo{
				Name: fmt.Sprintf("d%03d/f%06d.bin", i/10000, i),
				Size: 2048, Type: TypeFile, Version: version.New(),
			})
		}
		start := time.Now()
		if err := ix.SeedBulk(files); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := ix.ListAll(func(fi *FileInfo) error { count++; return nil }); err != nil {
			t.Fatal(err)
		}
		if count != n {
			t.Fatalf("count = %d, want %d", count, n)
		}
		return time.Since(start)
	}
	base := measure(50000)
	ten := measure(500000)
	ratio := float64(ten) / float64(base)
	if ratio > 20 {
		t.Fatalf("superlinear growth: 50k=%v 500k=%v (ratio %.1f, want < 20x)", base, ten, ratio)
	}
	t.Logf("50k=%v 500k=%v ratio=%.1f (limit 20x)", base, ten, ratio)
}

// BenchmarkSeedAndListAll times the database bulk insert and full-table
// read at production-ish folder sizes. Run with:
//
//	go test ./internal/index/ -run TestScaleFullExchangePipeline -bench BenchmarkSeedAndListAll -benchtime 1x
func BenchmarkSeedAndListAll(b *testing.B) {
	b.ReportAllocs()
	for _, n := range []int{100000, 1000000} {
		b.Run(fmt.Sprintf("n%d", n), func(b *testing.B) {
			files := make([]*FileInfo, 0, n)
			for j := 0; j < n; j++ {
				files = append(files, &FileInfo{
					Name: fmt.Sprintf("d%03d/f%06d.bin", j/10000, j),
					Size: 2048, Type: TypeFile, Version: version.New(),
				})
			}
			for i := 0; i < b.N; i++ {
				ix, err := Open(filepath.Join(b.TempDir(), "bench.db"), "bench")
				if err != nil {
					b.Fatal(err)
				}
				if err := ix.SeedBulk(files); err != nil {
					b.Fatal(err)
				}
				if err := ix.ListAll(func(fi *FileInfo) error { return nil }); err != nil {
					b.Fatal(err)
				}
				_ = ix.Close()
			}
		})
	}
}
