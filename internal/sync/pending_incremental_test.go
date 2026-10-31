package sync

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"crosssync/internal/ignore"
	"crosssync/internal/index"
	"crosssync/internal/scanner"
	"crosssync/internal/version"
)

// incEngine builds an engine over a real (temp) index for the equivalence test.
func incEngine(t *testing.T) *Engine {
	t.Helper()
	root := t.TempDir()
	ix, err := index.Open(filepath.Join(root, "ix.db"), "f")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	ig, _ := ignore.Parse(nil)
	return New(1, "f", root, ConflictCopy, nil, ix, nil, ig, t.Logf)
}

// TestIncrementalPendingMatchesAFullWalk is the whole safety argument for the
// incremental pending lists: drive the engine through random local writes, peer
// view merges, in-flight churn and views going mid-exchange, and assert that the
// names it reports are exactly the names a full walk reports.
//
// Two things are checked. Whether the answers agree, on every third step (the
// other steps run the incremental path for real, with no reference walk to
// resynchronise it). And that those steps did NOT walk the index: a rebuild moves
// builtAt, so a version of this test where every call rebuilt would pass the first
// check and prove nothing.
func TestIncrementalPendingMatchesAFullWalk(t *testing.T) {
	e := incEngine(t)
	rng := rand.New(rand.NewSource(7))
	peers := []uint64{2, 3}
	live := map[string]*index.FileInfo{}

	put := func(name string, ver version.Vector, del bool) {
		fi := &index.FileInfo{Name: name, Size: int64(len(name)), Type: index.TypeFile, Version: ver, Deleted: del}
		if err := e.Ix.Put(fi); err != nil {
			t.Fatal(err)
		}
		live[name] = fi
	}
	peerViews := map[uint64]map[string]*index.FileInfo{}

	pushView := func(pid uint64) {
		view := peerViews[pid]
		if view == nil {
			view = map[string]*index.FileInfo{}
			peerViews[pid] = view
		}
		// Derive a view from the local world, mutated at random: the point is to
		// exercise the classifier, not to model a real peer.
		for name, fi := range live {
			switch rng.Intn(6) {
			case 0:
				continue // the peer does not have it
			case 1:
				view[name] = &index.FileInfo{Name: name, Size: fi.Size, Type: fi.Type, Deleted: true, Version: fi.Version.Clone()}
			case 2:
				view[name] = &index.FileInfo{Name: name, Size: fi.Size, Type: fi.Type, Version: fi.Version.Clone().Bump(pid)}
			default:
				view[name] = &index.FileInfo{Name: name, Size: fi.Size, Type: fi.Type, Version: fi.Version.Clone()}
			}
		}
		list := make([]*index.FileInfo, 0, len(view))
		for _, fi := range view {
			list = append(list, fi.Clone())
		}
		e.SetPeerIndex(pid, list, true)
	}

	compare := func(step int, pulls, ups []string) {
		t.Helper()
		e.SetIncrementalPending(false)
		wantPulls, err := e.NeedPulls()
		if err != nil {
			t.Fatalf("step %d: reference pulls: %v", step, err)
		}
		wantUps, err := e.PendingUploads()
		if err != nil {
			t.Fatalf("step %d: reference ups: %v", step, err)
		}
		e.SetIncrementalPending(true)
		if !sameNames(pulls, wantPulls) {
			t.Fatalf("step %d: incremental pulls %v != full walk %v", step, pulls, wantPulls)
		}
		if !sameNames(ups, wantUps) {
			for _, n := range wantUps {
				local, ok, _ := e.Ix.Get(n)
				var where []string
				for pid, view := range e.peers {
					pf, has := view[n]
					where = append(where, fmt.Sprintf("peer%d has=%v del=%v pend=%v", pid, has, pf != nil && pf.Deleted, e.viewPending[pid]))
				}
				t.Logf("UPDIAG %q local ok=%v del=%v inflight=%v %v", n, ok, local != nil && local.Deleted, e.InFlight(n), where)
			}
			t.Fatalf("step %d: incremental uploads %v != full walk %v", step, ups, wantUps)
		}
	}

	// Prime both lists.
	if _, err := e.NeedPulls(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.PendingUploads(); err != nil {
		t.Fatal(err)
	}

	incrementalRounds := 0
	for step := 0; step < 400; step++ {
		name := fmt.Sprintf("d%d/f%d.bin", rng.Intn(5), rng.Intn(40))
		switch rng.Intn(10) {
		case 0, 1, 2: // local write
			v := version.New().Bump(1)
			if old, ok := live[name]; ok {
				v = old.Version.Clone().Bump(1)
			}
			put(name, v, false)
		case 3: // local delete, the tombstone a scan would write
			if old, ok := live[name]; ok {
				put(name, old.Version.Clone().Bump(1), true)
			}
		case 4: // hard delete from the index
			if err := e.Ix.Delete(name); err != nil {
				t.Fatal(err)
			}
			delete(live, name)
		case 5, 6: // peer view replaced
			pushView(peers[rng.Intn(len(peers))])
		case 7: // one entry merged into a view
			e.SetPeerIndex(peers[rng.Intn(len(peers))], []*index.FileInfo{{
				Name: name, Size: int64(len(name)), Type: index.TypeFile, Version: version.New().Bump(9),
			}}, true)
		case 8: // a file starts or stops being written
			if rng.Intn(2) == 0 {
				e.SetInFlight([]scanner.InFlight{{Name: name}})
			} else {
				e.SetInFlight(nil)
			}
		case 9: // an index exchange opens or closes
			who := peers[rng.Intn(len(peers))]
			if rng.Intn(2) == 0 {
				e.BeginPeerViewExchange(who)
			} else {
				e.EndPeerViewExchange(who)
			}
		}

		before := e.pendingCache.builtAt
		pulls, err := e.NeedPulls()
		if err != nil {
			t.Fatalf("step %d: NeedPulls: %v", step, err)
		}
		ups, err := e.PendingUploads()
		if err != nil {
			t.Fatalf("step %d: PendingUploads: %v", step, err)
		}
		if !before.IsZero() && e.pendingCache.builtAt.Equal(before) {
			incrementalRounds++
		}
		if step%3 == 0 || step == 399 {
			compare(step, pulls, ups)
		}
	}

	// The point of the exercise: most rounds must have been served without a walk.
	if incrementalRounds < 100 {
		t.Fatalf("only %d of 400 rounds avoided a full walk: the incremental path is not engaging", incrementalRounds)
	}
}

func sameNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]string(nil), a...)
	y := append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// TestPendingRebuildIsCheaperThanManyLookups pins the switch between the two
// strategies: a dirty set that covers a quarter of the index must cost one walk,
// not one query per changed name.
func TestPendingRebuildIsCheaperThanManyLookups(t *testing.T) {
	e := incEngine(t)
	for i := 0; i < 40; i++ {
		if err := e.Ix.Put(&index.FileInfo{
			Name: fmt.Sprintf("f%03d.bin", i), Size: 1, Type: index.TypeFile, Version: version.New().Bump(1),
		}); err != nil {
			t.Fatal(err)
		}
	}
	e.SetPeerIndex(2, nil, true)
	if _, err := e.NeedPulls(); err != nil {
		t.Fatal(err)
	}

	// Touch 20 of 40 names: over the threshold, so one walk.
	before := e.pendingCache.builtAt
	for i := 0; i < 20; i++ {
		if err := e.Ix.Put(&index.FileInfo{
			Name: fmt.Sprintf("f%03d.bin", i), Size: 2, Type: index.TypeFile, Version: version.New().Bump(2),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.NeedPulls(); err != nil {
		t.Fatal(err)
	}
	if e.pendingCache.builtAt.Equal(before) {
		t.Fatal("a dirty set covering half the index was resolved per-name instead of by a walk")
	}

	// One name: no walk.
	before = e.pendingCache.builtAt
	if err := e.Ix.Put(&index.FileInfo{
		Name: "f000.bin", Size: 3, Type: index.TypeFile, Version: version.New().Bump(3),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.NeedPulls(); err != nil {
		t.Fatal(err)
	}
	if !e.pendingCache.builtAt.Equal(before) {
		t.Fatal("a single changed name triggered a full walk")
	}
}
