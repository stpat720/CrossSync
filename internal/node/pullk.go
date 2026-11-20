package node

import "crosssync/internal/config"

// Pull concurrency at runtime.
//
// K (how many FILES a round keeps in flight over one peer connection) used to be
// read straight out of the config struct, which made tuning it a file edit plus a
// daemon restart — impossible on the production boxes, which have no reachable
// shell for us. PUT /api/settings/sync now changes it on a running daemon.
//
// The batch samples the value ONCE, when it is built (newPullBatch), so a change
// applies to the NEXT batch and can never disturb one already in flight: the
// jobs already admitted keep their slots and the admitted count is what bounds
// memory (K x window block buffers).

// clampPullConcurrency maps a requested K onto the supported range. 0 and
// negatives cannot be honoured from the API (the config file keeps 0 =
// "default"), so an explicit request of 0 means "the minimum", 1.
func clampPullConcurrency(k int) int {
	if k < 1 {
		return 1
	}
	if k > config.MaxPullConcurrency {
		return config.MaxPullConcurrency
	}
	return k
}

// PullConcurrency is the K the next pull batch will use: the value set at runtime
// when one has been set, otherwise the configured one (which itself defaults to
// config.DefaultPullConcurrency and is clamped to the hard cap).
func (n *Node) PullConcurrency() int {
	if k := n.pullK.Load(); k > 0 {
		return int(k)
	}
	return n.Cfg.PullFiles()
}

// SetPullConcurrency sets K for the next pull batch, clamped to
// [1, config.MaxPullConcurrency], and reports the value applied. Batches already
// in flight are unaffected.
func (n *Node) SetPullConcurrency(k int) int {
	k = clampPullConcurrency(k)
	n.pullK.Store(int64(k))
	return k
}
