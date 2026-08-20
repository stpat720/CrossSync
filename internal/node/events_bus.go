package node

import (
	"sync"

	"crosssync/internal/events"
)

// eventSub is one live subscriber to the event stream (used by the control
// plane's SSE endpoint).
type eventSub struct {
	ch     chan *events.Event
	mu     sync.Mutex
	closed bool
}

func (s *eventSub) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
}

func (s *eventSub) send(e *events.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.ch <- e:
	default:
		// Subscriber is slow; drop rather than block the daemon.
	}
}

// Subscribe returns a channel of new events plus a cancel function. The
// channel is buffered and drops events if the consumer cannot keep up, so
// a slow subscriber never blocks the daemon.
func (n *Node) Subscribe() (<-chan *events.Event, func()) {
	sub := &eventSub{ch: make(chan *events.Event, 64)}
	n.subMu.Lock()
	if n.subs == nil {
		n.subs = map[*eventSub]struct{}{}
	}
	n.subs[sub] = struct{}{}
	n.subMu.Unlock()
	return sub.ch, func() {
		sub.close()
		n.subMu.Lock()
		delete(n.subs, sub)
		n.subMu.Unlock()
	}
}

// broadcast fans an event out to all live subscribers.
func (n *Node) broadcast(e *events.Event) {
	n.subMu.Lock()
	defer n.subMu.Unlock()
	for sub := range n.subs {
		sub.send(e)
	}
}
