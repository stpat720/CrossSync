// Package lockorder keeps two locks that must NEVER be nested from being
// nested — in either direction — and names the pair when they are.
//
// CrossSync has one rule for this: the per-folder engine lock
// (sync.Engine.mu) and the durable event store's lock (events.Store.mu) may
// never be held together by one goroutine. Taking the events lock under the
// engine lock makes the whole engine wait on a SQLite write (and on any long
// attention query the control plane is decoding); taking the engine lock
// under the events lock closes the cycle. A live first-index pass wedged on
// exactly that inversion: 8 hash workers parked on the scan commit, one
// goroutine holding the engine lock inside the event callback
// (engine.go emit -> Node.recordEvent -> Store.Record), a query holding the
// events lock, and every other goroutine — control reads, the connection
// manager, the ack path — queued behind them. hash_done froze for 180 s with
// no disk I/O and idle CPU, and only a restart cleared it.
//
// The structural fix is in the callers (the engine queues the events it wants
// to record and emits them once its lock is released). This package is how
// that stays true: each lock is a Mutex that knows its Guard, two guards are
// Paired when the locks must not nest, and acquiring one while the calling
// goroutine holds the other is reported as a violation — logged once in
// production, a test failure under Strict.
//
// Detection costs one goroutine-id read (~0.5 µs) per lock acquisition and
// only while a guard is enabled; the zero value is inert.
package lockorder

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

// Mutex is a sync.Mutex that counts its holders and, when a Guard is
// attached, reports a lock-order violation the moment the calling goroutine
// takes it while holding a lock it must not be nested with.
//
// The zero Mutex is a plain mutex (no order tracking). Call Init once, before
// the mutex is used, to attach a named guard and an optional after-unlock
// hook.
type Mutex struct {
	mu    sync.Mutex
	depth atomic.Int32 // holders; always 0 or 1 for a mutex, but a counter is what unlock needs
	g     *Guard
	after func()
}

// Init attaches a guard (named for violation messages) and an optional hook
// that runs on the unlocking goroutine AFTER the lock is released. The hook is
// the engine's way to deliver the events it queued while the lock was held
// without ever calling out under it. It must be safe to call with the lock
// free and must not assume it is the only caller (each unlock runs it).
func (m *Mutex) Init(name string, afterUnlock func()) *Guard {
	g := &Guard{name: name}
	m.g = g
	m.after = afterUnlock
	return g
}

// Guard returns the guard attached by Init, or nil for a plain mutex.
func (m *Mutex) Guard() *Guard { return m.g }

// Lock acquires the mutex, checking the lock order first when a guard is
// enabled.
func (m *Mutex) Lock() {
	g := m.g
	var id uint64
	if g.on() {
		id = goid()
		g.beforeAcquire(id)
	}
	m.mu.Lock()
	m.depth.Add(1)
	if g.on() {
		g.holder.Store(id)
	}
}

// TryLock is Lock without waiting. It reports whether the mutex was taken; a
// failed attempt reports no violation state change (the check that matters is
// about the caller's other lock, which it still holds either way).
func (m *Mutex) TryLock() bool {
	g := m.g
	var id uint64
	if g.on() {
		id = goid()
		g.beforeAcquire(id)
	}
	if !m.mu.TryLock() {
		return false
	}
	m.depth.Add(1)
	if g.on() {
		g.holder.Store(id)
	}
	return true
}

// Unlock releases the mutex and then runs the after-unlock hook, if any. The
// depth is dropped BEFORE the mutex is released so a producer that checks
// "is the lock held" after appending an event cannot both miss the hook and
// see the lock as still held (see Engine.emit).
func (m *Mutex) Unlock() {
	if g := m.g; g.on() {
		g.holder.Store(0)
	}
	m.depth.Add(-1)
	m.mu.Unlock()
	if m.after != nil {
		m.after()
	}
}

// Locked reports whether the mutex is held by ANY goroutine. It is the cheap
// question a producer asks to decide whether it must defer its callback; it
// is not a substitute for the per-goroutine order check.
func (m *Mutex) Locked() bool { return m.depth.Load() > 0 }

// Guard tracks which goroutine holds one mutex and reports an inversion with
// the guards it has been Paired with. A nil Guard is inert.
type Guard struct {
	name string

	mu     sync.Mutex
	others []*Guard

	enabled    atomic.Bool
	strict     atomic.Bool
	holder     atomic.Uint64 // goroutine id holding the mutex; 0 when free
	violations atomic.Int64
	first      atomic.Pointer[string]
	reporter   atomic.Pointer[func(string)]
}

// Pair declares that the two locks must never be held at the same time by one
// goroutine, in either order. Nil guards are ignored (a store or engine with
// no guard simply is not tracked).
func Pair(a, b *Guard) {
	if a == nil || b == nil || a == b {
		return
	}
	a.mu.Lock()
	if !a.has(b) {
		a.others = append(a.others, b)
	}
	a.mu.Unlock()
	b.mu.Lock()
	if !b.has(a) {
		b.others = append(b.others, a)
	}
	b.mu.Unlock()
}

func (g *Guard) has(o *Guard) bool {
	for _, x := range g.others {
		if x == o {
			return true
		}
	}
	return false
}

// Enable starts order checking on this guard. It stays non-strict: a violation
// is counted and reported (once) and the lock is still taken, because a
// detector must never turn a working daemon into a crash.
func (g *Guard) Enable() {
	if g == nil {
		return
	}
	g.enabled.Store(true)
}

// Strict turns order checking on AND panics on the first violation. It exists
// for tests: a violation there is a bug in the code under test, not a
// condition to ride through.
func (g *Guard) Strict() {
	if g == nil {
		return
	}
	g.strict.Store(true)
	g.enabled.Store(true)
}

// OnViolation installs a reporter (typically the node's Logf) and enables
// checking. The reporter fires once per guard pair: the first violation is the
// diagnostic and repeating it every event would bury the log.
func (g *Guard) OnViolation(fn func(string)) {
	if g == nil || fn == nil {
		return
	}
	g.reporter.Store(&fn)
	g.enabled.Store(true)
}

// Violations reports how many inversions this guard has observed.
func (g *Guard) Violations() int64 {
	if g == nil {
		return 0
	}
	return g.violations.Load()
}

// FirstViolation returns the first violation's message, or "".
func (g *Guard) FirstViolation() string {
	if g == nil {
		return ""
	}
	if s := g.first.Load(); s != nil {
		return *s
	}
	return ""
}

// on reports whether order checking is active. Nil-safe so callers can hold a
// nil guard (an un-Init'ed mutex) without lining every site with checks.
func (g *Guard) on() bool { return g != nil && g.enabled.Load() }

// beforeAcquire is called by Mutex.Lock/TryLock BEFORE the mutex is taken, with
// the caller's goroutine id. It reports a violation if that same goroutine
// already holds a paired lock: that is the inversion, and it is per-goroutine —
// one goroutine holding the engine lock while another takes the events lock is
// ordinary and must never be flagged.
func (g *Guard) beforeAcquire(id uint64) {
	g.mu.Lock()
	others := g.others
	g.mu.Unlock()
	for _, o := range others {
		if h := o.holder.Load(); h != 0 && h == id {
			g.violate(fmt.Sprintf(
				"lock order violation: goroutine %d took the %s while holding the %s; "+
					"the engine lock and the event-store lock must never be nested "+
					"(emit events after releasing the engine lock, and never call into "+
					"the engine from inside the event store)",
				id, g.name, o.name))
		}
	}
}

func (g *Guard) violate(msg string) {
	n := g.violations.Add(1)
	if n == 1 {
		g.first.Store(&msg)
	}
	if g.strict.Load() {
		panic(msg)
	}
	if n > 1 {
		return
	}
	if fn := g.reporter.Load(); fn != nil {
		(*fn)(msg)
	}
}

// goid returns the calling goroutine's id. It is read from the stack header
// because the runtime exposes no cheaper accessor; only the opt-in detector
// pays for it.
func goid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// "goroutine 123 [running]:" — digits start at byte 10.
	s := buf[10:n]
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	id, err := strconv.ParseUint(string(s[:i]), 10, 64)
	if err != nil || id == 0 {
		return 1 // never look "free" (0) and never break the comparison
	}
	return id
}
