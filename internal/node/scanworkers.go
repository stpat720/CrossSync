package node

import (
	"runtime"

	"crosssync/internal/config"
)

// Hash reader pool at runtime.
//
// The index hasher reads and hashes N files at the same time, N = one per CPU
// by default (scanner.New). That default is wrong on a single spinning disk:
// measured on Tower, a 234 GiB folder living on ONE platter was read by 37
// goroutines and delivered only ~55-70 MB/s at 100 % utilisation
// (`iostat -x`: 864 r/s, 126 kB rareq-sz, 27.8 ms r_await, aqu-sz 24), while
// the same corpus hashed at ~156 MB/s on the uncontended node. Interleaving
// readers on one platter costs seeks, so a SMALLER pool can deliver more MB/s.
//
// The pool is a knob because the right value is a property of the disk, not of
// the CPU count: PUT /api/settings/sync changes it on a running daemon (the
// production boxes have no reachable shell), and the value persists to
// config.yaml as scan.hash_workers.
//
// The scanner samples it ONCE, at the start of a pass (scanner.SetWorkerSource),
// so a change applies to the NEXT scan and can never disturb one in flight.

// clampHashWorkers maps a requested pool onto the supported range. 0 and
// negatives cannot be honoured from the API (the config file keeps 0 =
// "one per CPU"), so an explicit request of 0 means "the minimum", 1 — the same
// rule pull_concurrency uses.
//
// The ceiling is NumCPU but never above config.MaxHashWorkers: whatever this
// returns may be WRITTEN to config.yaml, and a persisted value the file's own
// Validate rejects would leave a daemon that cannot start.
func clampHashWorkers(n int) int {
	max := runtime.NumCPU()
	if max > config.MaxHashWorkers {
		max = config.MaxHashWorkers
	}
	if max < 1 {
		max = 1
	}
	if n < 1 {
		return 1
	}
	if n > max {
		return max
	}
	return n
}

// HashWorkers is the pool the NEXT scan will use: the value set at runtime when
// one has been set, otherwise the configured one (0 there means one per CPU,
// clamped to [1, NumCPU]).
func (n *Node) HashWorkers() int {
	if k := n.hashW.Load(); k > 0 {
		return int(k)
	}
	if n.Cfg == nil {
		return clampHashWorkers(runtime.NumCPU())
	}
	return n.Cfg.Scan.HashWorkersOrDefault()
}

// SetHashWorkers sets the reader pool for the next scan, clamped to
// [1, NumCPU], and reports the value applied. Scans already running keep the
// workers they launched.
func (n *Node) SetHashWorkers(k int) int {
	k = clampHashWorkers(k)
	n.hashW.Store(int64(k))
	return k
}
