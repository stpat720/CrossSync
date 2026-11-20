package sync

// Pull-stage instrumentation.
//
// A 57.8 GB transfer measured t_file ~ 9-10 s fixed + size / ~8.6 MB/s per
// stream, so for a small file the fixed part is 75-85 % of its whole lifetime.
// "Fixed" was a guess: it could be the peer's settle-gate re-check, the queue
// behind K other files, the fsync on the receiving array, or the commit. These
// four counters turn it into an answer, measured on the live pair, without
// logging anything per file (the pull path is the hot path).
//
// The split, per COMMITTED pull, in nanoseconds:
//
//	wait_first_block  batch admitted the file -> its first block was written
//	transfer          first block -> last block written (the wire + disk time)
//	fsync             last block -> Pull.Finish's fsync returned
//	commit            fsync -> the atomic rename AND the index write are done
//
// Totals are summed atomics on the folder's EngineStats, so they cost one
// assignment each per file; the averages are derived by the /metrics renderer,
// never on a request path.

// PullStageTiming is the four-stage split of one committed pull, in
// nanoseconds. A stage can legitimately be zero (a pull that needed no blocks
// has no wait and no transfer; it was satisfied from a local copy or a resumed
// temp).
type PullStageTiming struct {
	WaitFirstBlock int64
	Transfer       int64
	Fsync          int64
	Commit         int64
}

// AddPullStageTiming folds one file's timing into the folder's totals and counts
// the file. Negative values are clamped to zero: the samples come from one
// monotonic clock, so a negative can only mean a stage boundary was not observed
// and reporting it as zero is the honest answer.
func (s *EngineStats) AddPullStageTiming(t PullStageTiming) {
	s.PullWaitFirstNs.Add(nonNegative(t.WaitFirstBlock))
	s.PullTransferNs.Add(nonNegative(t.Transfer))
	s.PullFsyncNs.Add(nonNegative(t.Fsync))
	s.PullCommitNs.Add(nonNegative(t.Commit))
	s.PullFiles.Add(1)
}

func nonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
