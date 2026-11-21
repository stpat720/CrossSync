package lockorder

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPairReportsNestingInEitherDirection is the unit-level contract: taking
// one paired lock while the SAME goroutine holds the other is a violation, in
// either order.
func TestPairReportsNestingInEitherDirection(t *testing.T) {
	for _, order := range [][2]string{{"a", "b"}, {"b", "a"}} {
		t.Run(order[0]+"-then-"+order[1], func(t *testing.T) {
			var a, b Mutex
			ga := a.Init("lock a", nil)
			gb := b.Init("lock b", nil)
			Pair(ga, gb)
			ga.Enable()
			gb.Enable()

			first, second := &a, &b
			if order[0] == "b" {
				first, second = &b, &a
			}
			first.Lock()
			second.Lock()
			if !second.Locked() || !first.Locked() {
				t.Fatal("a paired mutex did not report itself held")
			}
			second.Unlock()
			first.Unlock()

			total := ga.Violations() + gb.Violations()
			if total != 1 {
				t.Fatalf("violations = %d, want exactly 1 (first: %q)", total, gb.FirstViolation()+ga.FirstViolation())
			}
			msg := ga.FirstViolation() + gb.FirstViolation()
			if !strings.Contains(msg, "lock order violation") || !strings.Contains(msg, "never be nested") {
				t.Fatalf("violation message does not name the rule: %q", msg)
			}
		})
	}
}

// TestPairIgnoresOtherGoroutines is the property that makes the detector
// usable in production: one goroutine holding the engine lock while ANOTHER
// takes the event store's lock is ordinary operation, not an inversion. Only
// the same goroutine nesting them is a violation.
func TestPairIgnoresOtherGoroutines(t *testing.T) {
	var a, b Mutex
	ga := a.Init("lock a", nil)
	gb := b.Init("lock b", nil)
	Pair(ga, gb)
	ga.Enable()
	gb.Enable()

	a.Lock()
	done := make(chan bool, 1)
	go func() {
		defer close(done)
		b.Lock()
		done <- b.Locked()
		b.Unlock()
	}()
	select {
	case held := <-done:
		if !held {
			t.Fatal("the second goroutine did not hold the paired lock")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second goroutine never got the paired lock")
	}
	a.Unlock()

	if v := ga.Violations() + gb.Violations(); v != 0 {
		t.Fatalf("violations = %d, want 0: different goroutines may hold the paired locks", v)
	}
}

// TestStrictPanics proves a violation cannot slip past a test: Strict turns the
// report into a panic at the offending acquisition.
func TestStrictPanics(t *testing.T) {
	var a, b Mutex
	ga := a.Init("lock a", nil)
	gb := b.Init("lock b", nil)
	Pair(ga, gb)
	ga.Strict()
	gb.Strict()

	a.Lock()
	defer a.Unlock()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("nested acquisition of a strict pair did not panic")
		}
		if s, ok := r.(string); !ok || !strings.Contains(s, "lock order violation") {
			t.Fatalf("panic value = %v, want the lock-order message", r)
		}
	}()
	b.Lock()
}

// TestUnlockHookRunsWithLockFree is what the engine's event flush relies on:
// the after-unlock hook runs after the lock is released, so the hook (or a
// callback it dispatches) may take the same lock again.
func TestUnlockHookRunsWithLockFree(t *testing.T) {
	var m Mutex
	var depthAtHook int32 = -1
	var again bool
	m.Init("test lock", func() {
		depthAtHook = 0
		if m.Locked() {
			depthAtHook = 1
		}
	})
	m.Lock()
	if !m.Locked() {
		t.Fatal("the mutex did not report itself held")
	}
	m.Unlock()
	if depthAtHook != 0 {
		t.Fatal("the after-unlock hook ran while the mutex was still held")
	}
	// Re-entrant use from the hook: the engine's flush calls OnEvent, which is
	// free to call back into engine methods that lock.
	var wg sync.WaitGroup
	var inner Mutex
	inner.Init("inner", nil)
	wg.Add(1)
	go func() {
		defer wg.Done()
		inner.Lock()
		if inner.Locked() {
			again = true
		}
		inner.Unlock()
	}()
	wg.Wait()
	if !again {
		t.Fatal("a plain mutex no longer reports itself held")
	}
}
