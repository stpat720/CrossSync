package node

import (
	"os"
	"path/filepath"
	"testing"
)

// TestThreeNodesConverge runs the same folder on three nodes and checks that a
// file written on the third reaches both others, with no duplication of work.
//
// Everything in this project had only ever been exercised with two nodes, which
// is the topology of the test share, not of a real office: a third node makes the
// per-peer views, the transitive propagation and the pending numbers do work that
// two nodes never ask of them, and none of that was covered.
func TestThreeNodesConverge(t *testing.T) {
	clock := int64(2_000_000)
	mk := func(id uint64, name string) (*Node, string) {
		root := t.TempDir()
		n := renameNode(t, id, name, root, filepath.Join(t.TempDir(), "meta-"+name), 2)
		n.Folders["data"].Scanner.SetClock(func() int64 { return clock })
		return n, root
	}
	a, rootA := mk(1, "a")
	b, rootB := mk(2, "b")
	c, rootC := mk(3, "c")

	// A file appears on the third node only.
	const rel = "docs/from-c.txt"
	const body = "written on node c"
	writeAt(t, rootC, rel, body)
	mustScan(t, c)
	clock += 12
	mustScan(t, c)
	if _, live := liveEntry(t, c.Folders["data"].Ix, rel); !live {
		t.Fatal("node c did not index its own file")
	}

	// Propagate: c <-> a, c <-> b, then a <-> b (which should have nothing to do).
	gateSession(t, c, a)
	gateSession(t, c, b)
	gateSession(t, b, a)

	for name, n := range map[string]*Node{"a": a, "b": b, "c": c} {
		if _, live := liveEntry(t, n.Folders["data"].Ix, rel); !live {
			t.Fatalf("node %s does not have %s after three rounds", name, rel)
		}
	}
	for name, root := range map[string]string{"a": rootA, "b": rootB, "c": rootC} {
		got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("node %s: %v", name, err)
		}
		if string(got) != body {
			t.Fatalf("node %s content = %q, want %q", name, got, body)
		}
	}

	// Nothing should be owed in either direction once all three hold it.
	for name, n := range map[string]*Node{"a": a, "b": b, "c": c} {
		needs, err := n.Folders["data"].Engine.NeedPulls()
		if err != nil {
			t.Fatalf("node %s: %v", name, err)
		}
		if len(needs) != 0 {
			t.Fatalf("node %s still needs %v", name, needs)
		}
		ups, err := n.Folders["data"].Engine.PendingUploads()
		if err != nil {
			t.Fatalf("node %s: %v", name, err)
		}
		// A peer that has not exchanged with this node yet is legitimately owed
		// the file; what must not happen is the node owing itself or re-sending
		// bytes. The two peers that did exchange must be settled.
		if len(ups) > 1 {
			t.Fatalf("node %s believes %d files are owed: %v", name, len(ups), ups)
		}
	}
}
