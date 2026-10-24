package main

import "sync"

// syncKicker turns "a change was just applied" into at most one sync round at a
// time.
//
// ConnManager.SyncAll is synchronous and a round can run for minutes (a big pull),
// so calling it directly from the watcher or the held-back tick would block the
// scanner behind it. A kick runs in its own goroutine; kicks that arrive while a
// round is running collapse into exactly one follow-up round, which is what a
// burst of writes needs (the follow-up sees the settled state of all of them).
type syncKicker struct {
	mu      sync.Mutex
	running bool
	again   bool
	sync    func()
}

func newSyncKicker(fn func()) *syncKicker { return &syncKicker{sync: fn} }

// Kick requests a sync round. It never blocks.
func (k *syncKicker) Kick() {
	k.mu.Lock()
	if k.running {
		k.again = true
		k.mu.Unlock()
		return
	}
	k.running = true
	k.mu.Unlock()

	go func() {
		for {
			k.sync()
			k.mu.Lock()
			if !k.again {
				k.running = false
				k.mu.Unlock()
				return
			}
			k.again = false
			k.mu.Unlock()
		}
	}()
}
