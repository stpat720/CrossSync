package node

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/events"
)

// Fair turns for every folder inside one peer's round.
//
// The operator's rule is that separate folders are separate syncs and that one
// folder must never be waiting for another to finish before it starts or gives
// information. Folders share one session with a peer (one round per peer at a
// time, by design and separately handled), so the fairness has to live INSIDE
// the round: each folder gets a bounded turn per visit and the round revisits
// every folder that still has work.
//
// What this file pins, over the same latency harness as the concurrency tests:
//   - a small folder's files arrive while the big folder is still transferring
//     (they used to wait for the big folder's batch to drain completely),
//   - both folders still converge, byte for byte,
//   - no file is pulled twice (a turn must not abandon work it started — the
//     6783050 shape is a live job with nothing on the wire),
//   - every turn closes with nothing in flight.

// twoFolderNode builds a node with the two folders this file needs.
func twoFolderNode(t *testing.T, id uint64, name, bigRoot, smallRoot, metaDir string) *Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: id, Name: name},
		MetaDir: metaDir,
		Listen:  "",
		Folders: []config.Folder{
			{ID: "big", Path: bigRoot, SettleScans: 1},
			{ID: "small", Path: smallRoot, SettleScans: 1},
		},
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// writeCorpus writes count files of size bytes into root, all distinguishable.
func writeCorpus(t *testing.T, root, prefix string, count, size int) []string {
	t.Helper()
	names := make([]string, 0, count)
	for i := 0; i < count; i++ {
		name := prefix + "-" + strconv.Itoa(i) + ".bin"
		data := bytes.Repeat([]byte{byte('a' + i%26)}, size)
		p := filepath.Join(root, name)
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	return names
}

// pullEventsIn counts the applied "pulled from peer" events of one folder: one
// per file actually transferred, so a file pulled twice shows up as two.
func pullEventsIn(t *testing.T, n *Node, folder string) int {
	t.Helper()
	evs, err := n.Events.Query(events.Filter{Folder: folder, Category: events.CatApplied})
	if err != nil {
		t.Fatal(err)
	}
	c := 0
	for _, e := range evs {
		if strings.Contains(e.Reason, "pulled from peer") {
			c++
		}
	}
	return c
}

// TestSmallFolderPullsWhileBigFolderStillTransferring is the measured failure:
// pullAll walked the folders in order and ran each folder's batch to completion,
// so on a peer-wide round a three-file folder's files moved only after the big
// folder had drained — a wait that can be an hour on a real share.
//
// A turn is set to one file here so the interleaving is deterministic; the
// production default (64 MiB / 30 s) is the same mechanism with a bigger bite.
func TestSmallFolderPullsWhileBigFolderStillTransferring(t *testing.T) {
	oldBytes, oldSecs := pullFolderTurnBytes, pullFolderTurnSeconds
	pullFolderTurnBytes, pullFolderTurnSeconds = 1<<10, 600
	defer func() { pullFolderTurnBytes, pullFolderTurnSeconds = oldBytes, oldSecs }()

	var mu sync.Mutex
	var order []string
	turns := map[string]int{}
	inFlightAtClose := 0
	pullTurnEndHook = func(folder string, consumed, inFlight int) {
		mu.Lock()
		defer mu.Unlock()
		if consumed > 0 {
			order = append(order, folder)
			turns[folder] += consumed
		}
		inFlightAtClose += inFlight
	}
	defer func() { pullTurnEndHook = nil }()

	const (
		bigFiles, smallFiles = 24, 3
		size                 = 64 << 10
	)
	bigRootA, smallRootA := t.TempDir(), t.TempDir()
	bigRootB, smallRootB := t.TempDir(), t.TempDir()
	bigNames := writeCorpus(t, bigRootA, "big", bigFiles, size)
	smallNames := writeCorpus(t, smallRootA, "small", smallFiles, size)

	a := twoFolderNode(t, 1, "a", bigRootA, smallRootA, t.TempDir())
	b := twoFolderNode(t, 2, "b", bigRootB, smallRootB, t.TempDir())
	for _, id := range []string{"big", "small"} {
		if _, err := a.ScanFolder(id); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ScanFolder(id); err != nil {
			t.Fatal(err)
		}
	}

	// 10 ms each way: slow enough that a turn is a visible slice of the round.
	r := startLatencyRound(t, a, b, 10*time.Millisecond, nil)

	// The small folder's files must all land while the big folder still has
	// work: that is the whole point of a bounded turn.
	waitFor(t, 30*time.Second, "the small folder's files to arrive while 'big' is still transferring", func() bool {
		return regularFiles(t, smallRootB) == len(smallNames)
	})
	bigDone := b.Folders["big"].Engine.Stats.BytesDown.Load()
	if want := int64(bigFiles * size); bigDone >= want {
		t.Fatalf("'big' had already received %d of %d bytes when the small folder finished: the folders were not interleaved", bigDone, want)
	}
	if err := r.Wait(t); err != nil {
		t.Fatalf("round failed: %v", err)
	}

	// Both folders converged, byte for byte, and nothing was pulled twice.
	for i, name := range bigNames {
		want := bytes.Repeat([]byte{byte('a' + i%26)}, size)
		got, err := os.ReadFile(filepath.Join(bigRootB, name))
		if err != nil {
			t.Fatalf("%s missing from the big folder: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s has the wrong content after the round (%d bytes, want %d)", name, len(got), len(want))
		}
	}
	for i, name := range smallNames {
		want := bytes.Repeat([]byte{byte('a' + i%26)}, size)
		got, err := os.ReadFile(filepath.Join(smallRootB, name))
		if err != nil {
			t.Fatalf("%s missing from the small folder: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s has the wrong content after the round", name)
		}
	}
	if got := pullEventsIn(t, b, "big"); got != bigFiles {
		t.Fatalf("%d pull events for %d files in 'big': a file was pulled more than once", got, bigFiles)
	}
	if got := pullEventsIn(t, b, "small"); got != smallFiles {
		t.Fatalf("%d pull events for %d files in 'small': a file was pulled more than once", got, smallFiles)
	}

	// One file per turn, so the turn counts are the file counts.
	mu.Lock()
	defer mu.Unlock()
	if turns["big"] != bigFiles || turns["small"] != smallFiles {
		t.Fatalf("turn file counts: big=%d small=%d, want %d and %d", turns["big"], turns["small"], bigFiles, smallFiles)
	}
	// The small folder was visited BEFORE the big folder's last turn: the
	// interleaving, not just eventual convergence.
	firstSmall, lastBig := -1, -1
	for i, id := range order {
		if id == "small" && firstSmall < 0 {
			firstSmall = i
		}
		if id == "big" {
			lastBig = i
		}
	}
	if firstSmall < 0 || lastBig < 0 || firstSmall > lastBig {
		t.Fatalf("folder visit order %v: the small folder's first turn did not come before the big folder's last turn", order)
	}
	// A closed turn leaves nothing on the wire (6783050): the batch drains what
	// it started instead of abandoning a job between requests.
	if inFlightAtClose != 0 {
		t.Fatalf("%d request(s) were still in flight when turns closed, want 0", inFlightAtClose)
	}
}
