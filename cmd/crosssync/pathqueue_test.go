package main

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCollapsePathsDropsAncestorDuplicates(t *testing.T) {
	got := collapsePaths([]string{"a/b/c.txt", "a", "a/b", "z/y", "z/w", "file.txt"})
	want := []string{"a", "file.txt", "z/w", "z/y"} // sorted order
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("collapsePaths = %v, want %v", got, want)
	}
	// A single directory event plus its children must collapse to the directory.
	if got := collapsePaths([]string{"dir/x", "dir"}); !reflect.DeepEqual(got, []string{"dir"}) {
		t.Fatalf("dir + child = %v, want [dir]", got)
	}
}

// TestPathQueueCoalescesWithoutLosingPaths: batches that arrive while a scan is
// running are merged into the next scan, and every path is scanned exactly once.
func TestPathQueueCoalescesWithoutLosingPaths(t *testing.T) {
	var mu sync.Mutex
	var calls [][]string
	firstStarted := make(chan struct{})
	release := make(chan struct{})

	q := newPathQueue(func(rels []string) error {
		mu.Lock()
		calls = append(calls, append([]string(nil), rels...))
		n := len(calls)
		mu.Unlock()
		if n == 1 {
			close(firstStarted)
			<-release // hold the worker so later batches pile up
		}
		return nil
	}, t.Logf)

	q.Add([]string{"a.txt"})
	<-firstStarted
	q.Add([]string{"b.txt", "c.txt"})
	q.Add([]string{"c.txt", "d"}) // duplicate c.txt, plus a directory
	close(release)

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		done := len(calls) >= 2
		mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("scan calls = %d (%v), want 2 (first batch, then the merged rest)", len(calls), calls)
	}
	if !reflect.DeepEqual(calls[0], []string{"a.txt"}) {
		t.Fatalf("first scan = %v, want [a.txt]", calls[0])
	}
	if !reflect.DeepEqual(calls[1], []string{"b.txt", "c.txt", "d"}) {
		t.Fatalf("merged scan = %v, want [b.txt c.txt d] (deduped, sorted)", calls[1])
	}
}
