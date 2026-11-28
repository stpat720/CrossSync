package sync

import "testing"

// TestRoundProgressTracksWork pins the live progress a status reader gets while
// a round is in flight. The pending counters are a snapshot taken when the round
// planned (recomputing them reads the whole index, so the refresher stays out of
// a live round's way), and on a million-file folder a round can run for hours:
// without this, both nodes report "0 pending" while files move.
func TestRoundProgressTracksWork(t *testing.T) {
	e := &Engine{}
	e.BeginRound(7)
	e.BeginRoundWork(7, 10, 5000)

	if _, _, _, _, _, active := e.RoundProgress(); !active {
		t.Fatal("a round is in flight but RoundProgress says otherwise")
	}
	e.RoundFileDone(7, 1000)
	e.RoundFileDone(7, 0)  // metadata-only work still counts as a file
	e.RoundFileDone(99, 1) // another peer's round is not this folder's progress

	ft, fd, bt, bd, _, active := e.RoundProgress()
	if !active || ft != 10 || fd != 2 || bt != 5000 || bd != 1000 {
		t.Fatalf("progress = (%d,%d,%d,%d) active=%v, want (10,2,5000,1000) active=true", ft, fd, bt, bd, active)
	}

	e.EndRound(7)
	ft, fd, bt, bd, _, active = e.RoundProgress()
	if active || ft != 0 || fd != 0 || bt != 0 || bd != 0 {
		t.Fatalf("after the round: (%d,%d,%d,%d) active=%v, want all zero and inactive", ft, fd, bt, bd, active)
	}
}
