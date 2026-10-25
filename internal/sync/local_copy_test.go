package sync

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"crosssync/internal/events"
	"crosssync/internal/hash"
	"crosssync/internal/index"
	"crosssync/internal/scanner"
	"crosssync/internal/version"
)

// addOnlyPeerView publishes a peer's live entry for `to` — a copy of the local
// entry for `from` under a new name — WITHOUT a tombstone for `from`. That is
// the add-first ordering a rename reaches when the two halves are advertised by
// different scans (or different scan scopes): the receiver is told about the new
// path while the peer still has the old one live, so PlanMoves has no source to
// pair and nothing proves the old path is going away.
func addOnlyPeerView(t *testing.T, n *testNode, from, to string) *index.FileInfo {
	t.Helper()
	local, ok, err := n.ix.Get(from)
	if err != nil || !ok {
		t.Fatalf("get %s: ok=%v err=%v", from, ok, err)
	}
	target := local.Clone()
	target.Name = to
	target.Version = version.New().Bump(1).Bump(2).Bump(2)
	n.engine.SetPeerIndex(2, []*index.FileInfo{target}, false)
	return target
}

// drainPull finishes a pull that needed no blocks: that is what the session
// does when a pull's block needs are empty.
func drainPull(t *testing.T, p *Pull) *index.FileInfo {
	t.Helper()
	if needs := p.NeedBlocks(); len(needs) != 0 {
		t.Fatalf("pull still needs %d block(s): the local copy was not used", len(needs))
	}
	out, err := p.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// captureEvents records the engine's durable events for a test.
func captureEvents(t *testing.T, n *testNode) *[]*events.Event {
	t.Helper()
	var mu sync.Mutex
	var got []*events.Event
	n.engine.OnEvent = func(e *events.Event) {
		mu.Lock()
		got = append(got, e)
		mu.Unlock()
	}
	t.Cleanup(func() { n.engine.OnEvent = nil })
	return &got
}

// TestPullUsesIdenticalLocalCopy is the engine half of the add-first rename: the
// peer offers content this device already holds byte-identically under another
// live path, so the pull must be committed from that copy — no block requested,
// no byte counted as downloaded — while the source path stays live and untouched
// (whether it is really going away is the peer's deletion half to say, and it
// arrives later through the normal deletion path).
func TestPullUsesIdenticalLocalCopy(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	body := strings.Repeat("payload-", 1000)
	n.writeFile(t, "src/a.bin", body)
	n.scanAndApply(t)
	srcBefore := n.fileContent(t, "src/a.bin")
	addOnlyPeerView(t, n, "src/a.bin", "dst/a.bin")
	evs := captureEvents(t, n)

	p, err := n.engine.StartPull("dst/a.bin", 2)
	if err != nil {
		t.Fatal(err)
	}
	out := drainPull(t, p)

	if got := n.engine.Stats.BytesDown.Load(); got != 0 {
		t.Fatalf("BytesDown = %d, want 0 (the content was already on disk)", got)
	}
	if got := n.engine.Stats.BytesLocalCopy.Load(); got != int64(len(body)) {
		t.Fatalf("BytesLocalCopy = %d, want %d", got, len(body))
	}
	if got := n.engine.Stats.FilesLocalCopy.Load(); got != 1 {
		t.Fatalf("FilesLocalCopy = %d, want 1", got)
	}
	if got := n.engine.Stats.FilesDown.Load(); got != 0 {
		t.Fatalf("FilesDown = %d, want 0: a local copy is not a download", got)
	}
	// The new path is byte-identical, indexed with the adopted version.
	if got := n.fileContent(t, "dst/a.bin"); got != srcBefore {
		t.Fatalf("dst/a.bin is not byte-identical (%d bytes, want %d)", len(got), len(srcBefore))
	}
	if got := n.fileContent(t, "src/a.bin"); got != srcBefore {
		t.Fatalf("the source path was modified: %d bytes, want %d", len(got), len(srcBefore))
	}
	live, ok := liveEntryOf(t, n, "dst/a.bin")
	if !ok {
		t.Fatal("dst/a.bin must be live in the index")
	}
	if out == nil || !out.Version.Equal(live.Version) {
		t.Fatalf("returned entry version = %v, index = %v", out, live.Version)
	}
	if _, ok := liveEntryOf(t, n, "src/a.bin"); !ok {
		t.Fatal("the source must stay live until the peer says it is deleted")
	}
	// Reported, not silent: the event names the path the bytes came from.
	found := 0
	for _, e := range *evs {
		if e.Category == events.CatApplied && strings.HasPrefix(e.Reason, "copied from local") {
			found++
			if e.Linked != "src/a.bin" {
				t.Fatalf("event linked = %q, want src/a.bin", e.Linked)
			}
		}
	}
	if found != 1 {
		t.Fatalf("recorded %d local-copy event(s), want 1", found)
	}
}

// TestPullFallsBackWhenLocalCopyDrifted: the index claims identical content but
// the file on disk no longer hashes to it. Reusing it would corrupt the
// destination, so every block must be reported as still needed.
func TestPullFallsBackWhenLocalCopyDrifted(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	body := strings.Repeat("payload-", 1000)
	n.writeFile(t, "src/a.bin", body)
	n.scanAndApply(t)
	addOnlyPeerView(t, n, "src/a.bin", "dst/a.bin")

	// Same size, different bytes, no re-scan: the index still claims the old
	// content, exactly like a file rewritten behind the scanner's back.
	drift := strings.Repeat("X", len(body))
	if err := os.WriteFile(filepath.Join(n.root, "src", "a.bin"), []byte(drift), 0o644); err != nil {
		t.Fatal(err)
	}

	p, err := n.engine.StartPull("dst/a.bin", 2)
	if err != nil {
		t.Fatal(err)
	}
	p.Abort()
	if needs := p.NeedBlocks(); len(needs) != len(p.info.Blocks) {
		t.Fatalf("needs = %d block(s), want all %d fetched from the peer", len(needs), len(p.info.Blocks))
	}
	if got := n.engine.Stats.BytesLocalCopy.Load(); got != 0 {
		t.Fatalf("BytesLocalCopy = %d, want 0: a drifted donor must not be reused", got)
	}
}

// TestPullFallsBackWhenDonorInFlight: a file the stability gate is still
// holding may be half written, so it is never used as a source.
func TestPullFallsBackWhenDonorInFlight(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "src/a.bin", strings.Repeat("payload-", 1000))
	n.scanAndApply(t)
	addOnlyPeerView(t, n, "src/a.bin", "dst/a.bin")
	n.engine.SetInFlight([]scanner.InFlight{{Name: "src/a.bin", Size: 8000}})

	p, err := n.engine.StartPull("dst/a.bin", 2)
	if err != nil {
		t.Fatal(err)
	}
	p.Abort()
	if needs := p.NeedBlocks(); len(needs) == 0 {
		t.Fatal("an in-flight file must not be used as a local copy")
	}
}

// TestPullFallsBackWhenContentDiffers: the peer offers a file that no local
// file matches byte for byte (same size, different block hashes). Nothing may
// be reused: every block has to come from the peer.
func TestPullFallsBackWhenContentDiffers(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	n.writeFile(t, "src/a.bin", strings.Repeat("aaaa", 2000))
	n.scanAndApply(t)

	body := strings.Repeat("cccc", 2000)
	probe := filepath.Join(t.TempDir(), "peer.bin")
	if err := os.WriteFile(probe, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	bs := hash.BlockSizeForSize(int64(len(body)))
	_, blocks, err := hash.FileHashes(probe, bs)
	if err != nil {
		t.Fatal(err)
	}
	target := &index.FileInfo{
		Name: "dst/a.bin", Type: index.TypeFile,
		Size: int64(len(body)), BlockSize: int32(bs), Blocks: blocks,
		ModifiedS: 1900000000, Version: version.New().Bump(1).Bump(2).Bump(2),
	}
	n.engine.SetPeerIndex(2, []*index.FileInfo{target}, false)

	if donor := n.engine.localCopyDonor(target); donor != "" {
		t.Fatalf("donor = %q, want none (no local file holds that content)", donor)
	}
	p, err := n.engine.StartPull("dst/a.bin", 2)
	if err != nil {
		t.Fatal(err)
	}
	p.Abort()
	if needs := p.NeedBlocks(); len(needs) != len(target.Blocks) {
		t.Fatalf("needs = %d block(s), want all %d from the peer", len(needs), len(target.Blocks))
	}
	if got := n.engine.Stats.BytesLocalCopy.Load(); got != 0 {
		t.Fatalf("BytesLocalCopy = %d, want 0", got)
	}
}

// TestPullUsesVerifiedPrefixOfADriftedCopy: a donor whose LATER block no longer
// matches its index entry may still supply the blocks that do match — each one
// is verified against the peer's hash — and the rest is fetched. The assembled
// file is never a mix of two versions: every reused block is the peer's content.
func TestPullUsesVerifiedPrefixOfADriftedCopy(t *testing.T) {
	n := newNode(t, 1, "f", "conflict-copy", nil)
	// Three blocks at the minimum block size (128 KiB): 128 + 128 + 44 KiB.
	bs := hash.BlockSizeForSize(300 * 1024)
	body := strings.Repeat("A", 300*1024)
	n.writeFile(t, "src/big.bin", body)
	n.scanAndApply(t)
	local, _, _ := n.ix.Get("src/big.bin")
	if got := len(local.Blocks); got != 3 {
		t.Fatalf("setup: %d block(s), want 3 (block size %d)", got, bs)
	}
	addOnlyPeerView(t, n, "src/big.bin", "dst/big.bin")

	// Corrupt only the LAST block on disk, behind the scanner's back.
	p := filepath.Join(n.root, "src", "big.bin")
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte(strings.Repeat("Z", 44*1024)), 256*1024); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	pull, err := n.engine.StartPull("dst/big.bin", 2)
	if err != nil {
		t.Fatal(err)
	}
	pull.Abort()
	needs := pull.NeedBlocks()
	if len(needs) != 1 || needs[0].Index != 2 {
		t.Fatalf("needs = %+v, want only the drifted last block", needs)
	}
	if got := n.engine.Stats.BytesLocalCopy.Load(); got != int64(256*1024) {
		t.Fatalf("BytesLocalCopy = %d, want the two verified blocks (%d)", got, 256*1024)
	}
	if pull.donor != "" {
		t.Fatalf("donor = %q, want none: the pull still needs the peer", pull.donor)
	}
}

// liveEntryOf returns the live index entry for name.
func liveEntryOf(t *testing.T, n *testNode, name string) (*index.FileInfo, bool) {
	t.Helper()
	fi, ok, err := n.ix.Get(name)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || fi.Deleted {
		return nil, false
	}
	return fi, true
}
