package scanner

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"crosssync/internal/ignore"
)

// grow rewrites every file with a longer body, so the next pass sees a size
// change and has to hash (a pass with nothing pending never builds a pool).
func grow(t *testing.T, root string, n int, pad string) {
	t.Helper()
	for i := 0; i < n; i++ {
		writeFile(t, root, fmt.Sprintf("f%d.bin", i), strings.Repeat("x", i+1)+pad)
	}
}

// scanPool runs one scan and reports how many hash workers it actually
// launched, which is the only place the pool size is observable from outside
// (hashPending decides it after clamping to the number of pending files).
func scanPool(t *testing.T, s *Scanner) int {
	t.Helper()
	launched := -1
	s.SetWorkerHook(func(n int) { launched = n })
	defer s.SetWorkerHook(nil)
	if _, err := s.Scan(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return launched
}

// TestHashWorkersReachTheHasher is the "the knob is honoured" test: the pool the
// hash phase starts is the configured one, not one per CPU. On a single spinning
// disk this is the difference between 37 interleaved readers (864 r/s, 126 kB,
// 27.8 ms r_await, ~55-70 MB/s delivered) and a pool that can actually stream.
func TestHashWorkersReachTheHasher(t *testing.T) {
	root, ix := setup(t)
	const files = 8
	grow(t, root, files, "")
	s := newTestScanner(root, ix, &ignore.Matcher{})

	s.SetWorkers(3)
	if got := scanPool(t, s); got != 3 {
		t.Fatalf("%d files hashed with %d workers, want the configured 3: the pool size never reached hashPending", files, got)
	}

	// A pool larger than NumCPU is clamped DOWN: extra readers only block in the
	// read syscall, they cannot add throughput. (0 = "one per CPU" is resolved
	// one layer up, by config.ScanTuning.HashWorkersOrDefault / Node.HashWorkers;
	// this type only ever sees a concrete pool.)
	s.SetWorkers(runtime.NumCPU() + 10)
	grow(t, root, files, "y")
	want := runtime.NumCPU()
	if want > files {
		want = files
	}
	if got := scanPool(t, s); got != want {
		t.Fatalf("a pool of %d launched %d workers, want the CPU clamp %d (pending files: %d)",
			runtime.NumCPU()+10, got, want, files)
	}
}

// TestWorkerSourceIsSampledOncePerPass proves the live-knob contract that the
// API relies on: the pool is read ONCE when the hash phase starts, so a change
// applies to the NEXT scan and a pass in flight keeps the readers it launched.
func TestWorkerSourceIsSampledOncePerPass(t *testing.T) {
	root, ix := setup(t)
	const files = 6
	grow(t, root, files, "")
	s := newTestScanner(root, ix, &ignore.Matcher{})

	pool := 2
	reads := 0
	s.SetWorkerSource(func() int { reads++; return pool })
	// A static value must not win over the live source.
	s.SetWorkers(5)

	if got := scanPool(t, s); got != 2 {
		t.Fatalf("first pass launched %d workers, want the source's 2 (not the static 5)", got)
	}
	if reads != 1 {
		t.Fatalf("the pool source was read %d times in one pass, want exactly 1 (once, at the start of the hash phase)", reads)
	}

	// Change it at runtime: the next pass must use the new value, with no
	// rebuild of the Scanner (this is what PUT /api/settings/sync does).
	pool = 4
	grow(t, root, files, "more")
	if got := scanPool(t, s); got != 4 {
		t.Fatalf("after a runtime change to 4 the next pass launched %d workers, want 4", got)
	}
	if reads != 2 {
		t.Fatalf("the pool source was read %d times over two passes, want 2", reads)
	}
}

// TestWorkersFloorIsOne: 0 and negative pools must never launch zero readers —
// that would leave the pass waiting on a pool that cannot drain its queue.
func TestWorkersFloorIsOne(t *testing.T) {
	root, ix := setup(t)
	grow(t, root, 2, "")
	s := newTestScanner(root, ix, &ignore.Matcher{})
	s.SetWorkers(0) // clamps to 1 at set time
	if got := scanPool(t, s); got != 1 {
		t.Fatalf("SetWorkers(0) launched %d workers, want the floor 1", got)
	}
	s.SetWorkerSource(func() int { return -7 })
	grow(t, root, 2, "again")
	if got := scanPool(t, s); got != 1 {
		t.Fatalf("a negative pool from the source launched %d workers, want the floor 1", got)
	}
}
