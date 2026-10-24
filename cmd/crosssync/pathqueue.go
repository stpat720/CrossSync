package main

import (
	"sort"
	"strings"
	"sync"
)

// pathQueue coalesces filesystem-change batches into scoped scans for ONE
// folder.
//
// The watcher emits debounced batches, but a busy copy can outpace a scan, so
// batches are merged into a pending set and a single worker drains it: no scan
// pile-up, no dropped paths (a batch that arrives while a scan runs is picked up
// by the next iteration), and each scan walks only the paths that changed.
type pathQueue struct {
	mu      sync.Mutex
	pending map[string]struct{}
	running bool
	scan    func([]string) error // scans one batch
	logf    func(string, ...any)
}

func newPathQueue(scan func([]string) error, logf func(string, ...any)) *pathQueue {
	return &pathQueue{pending: map[string]struct{}{}, scan: scan, logf: logf}
}

// Add merges a batch and starts the worker if it is idle.
func (q *pathQueue) Add(rels []string) {
	if len(rels) == 0 {
		return
	}
	q.mu.Lock()
	for _, r := range rels {
		if r == "" || r == "." {
			continue
		}
		q.pending[r] = struct{}{}
	}
	if q.running || len(q.pending) == 0 {
		q.mu.Unlock()
		return
	}
	q.running = true
	q.mu.Unlock()
	go q.worker()
}

// worker drains pending batches until the queue is empty.
func (q *pathQueue) worker() {
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.running = false
			q.mu.Unlock()
			return
		}
		batch := make([]string, 0, len(q.pending))
		for r := range q.pending {
			batch = append(batch, r)
		}
		q.pending = map[string]struct{}{}
		q.mu.Unlock()

		// Drop paths already covered by a shorter one in the same batch (a
		// directory event plus its children would otherwise be walked twice).
		batch = collapsePaths(batch)
		if err := q.scan(batch); err != nil && q.logf != nil {
			q.logf("watcher scan: %v", err)
		}
	}
}

// collapsePaths sorts and drops entries whose ancestor is also present.
func collapsePaths(paths []string) []string {
	sort.Strings(paths)
	out := paths[:0]
	var lastKept string
	for _, p := range paths {
		if lastKept != "" && (p == lastKept || strings.HasPrefix(p, lastKept+"/")) {
			continue
		}
		out = append(out, p)
		lastKept = p
	}
	return out
}
