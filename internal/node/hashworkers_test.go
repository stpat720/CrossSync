package node_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/node"
)

// hashNode builds a single-folder node whose config carries scan.hash_workers,
// with the stability gate off (one scan per change, like makeNode).
func hashNode(t *testing.T, workers int) (*node.Node, string) {
	t.Helper()
	root, meta := t.TempDir(), t.TempDir()
	for i := 0; i < 6; i++ {
		p := filepath.Join(root, fmt.Sprintf("f%d.bin", i))
		if err := os.WriteFile(p, []byte(strings.Repeat("x", i+1)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "a"},
		MetaDir: meta,
		Scan:    config.ScanTuning{HashWorkers: workers},
		Folders: []config.Folder{{ID: "data", Path: root, SettleScans: 1, SettleSeconds: -1}},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n, root
}

// poolOf runs one scan of folder "data" and reports the number of hash workers
// it launched (installed through the scanner's worker hook, so it observes the
// goroutines that actually started, not a config field).
func poolOf(t *testing.T, n *node.Node) int {
	t.Helper()
	f, ok := n.Folders["data"]
	if !ok {
		t.Fatal("folder data missing")
	}
	launched := -1
	f.Scanner.SetWorkerHook(func(k int) { launched = k })
	defer f.Scanner.SetWorkerHook(nil)
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	return launched
}

// dirty makes every file a hash candidate again (a pass with nothing pending
// never builds a pool).
func dirty(t *testing.T, root, pad string) {
	t.Helper()
	for i := 0; i < 6; i++ {
		p := filepath.Join(root, fmt.Sprintf("f%d.bin", i))
		if err := os.WriteFile(p, []byte(strings.Repeat("x", i+1)+pad), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestFolderScannerUsesConfiguredHashWorkers closes the loop from config.yaml to
// the goroutines: the scanner the node builds for a folder must hash with
// scan.hash_workers readers, a runtime change (PUT /api/settings/sync) must
// apply to the next scan without rebuilding anything, and UpdateFolder — which
// re-creates the Scanner — must not silently drop the wiring back to one per
// CPU.
func TestFolderScannerUsesConfiguredHashWorkers(t *testing.T) {
	n, root := hashNode(t, 3)
	if got := n.HashWorkers(); got != 3 {
		t.Fatalf("Node.HashWorkers() = %d, want the configured 3", got)
	}
	if got := poolOf(t, n); got != 3 {
		t.Fatalf("the folder's scan launched %d hash workers, want the configured 3", got)
	}

	// Runtime change: the next scan, same Scanner.
	n.SetHashWorkers(2)
	dirty(t, root, "!")
	if got := poolOf(t, n); got != 2 {
		t.Fatalf("after SetHashWorkers(2) the next scan launched %d workers, want 2", got)
	}

	// UpdateFolder re-creates the Scanner: the live pool source must be rewired.
	if _, err := n.UpdateFolder("data", config.Folder{ID: "data", Path: root, SettleScans: 1, SettleSeconds: -1}); err != nil {
		t.Fatalf("UpdateFolder: %v", err)
	}
	dirty(t, root, "!!")
	if got := poolOf(t, n); got != 2 {
		t.Fatalf("after UpdateFolder the scan launched %d workers, want the live pool 2 (the re-created Scanner lost its pool source)", got)
	}
}

// TestHashWorkersClamping pins the API's range rule, which is the same one
// pull_concurrency uses: 0 and negatives mean "the minimum" (1), and anything
// above the CPU count is pulled down to it — more readers than CPUs cannot add
// throughput because they block in the read syscall.
func TestHashWorkersClamping(t *testing.T) {
	n, _ := hashNode(t, 0)
	cpu := runtime.NumCPU()
	if got := n.HashWorkers(); got != cpu {
		t.Fatalf("unset scan.hash_workers = %d, want one per CPU (%d)", got, cpu)
	}
	for _, in := range []int{0, -1, -99} {
		if got := n.SetHashWorkers(in); got != 1 {
			t.Fatalf("SetHashWorkers(%d) = %d, want the floor 1", in, got)
		}
	}
	if got := n.SetHashWorkers(cpu + 32); got != cpu {
		t.Fatalf("SetHashWorkers(NumCPU+32) = %d, want the clamp %d", got, cpu)
	}
	if got := n.SetHashWorkers(4); got != 4 {
		t.Fatalf("SetHashWorkers(4) = %d, want 4", got)
	}
	if got := n.HashWorkers(); got != 4 {
		t.Fatalf("HashWorkers() = %d after setting 4", got)
	}
}
