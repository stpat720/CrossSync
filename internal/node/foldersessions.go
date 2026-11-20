package node

import "crosssync/internal/config"

// Concurrent folder sessions per peer, at runtime.
//
// A sync session carries exactly ONE folder, so the session slot is keyed by
// (peer, folder): two folders of the same peer each get their own session, their
// own connection and their own transfer pipeline, and neither waits for the
// other to finish before it starts or reports ("separate folders are separate
// syncs"). What the slot still guarantees is unchanged — no two rounds on the
// SAME folder — which is what kept two writers off one deterministic .part
// staging temp (the live "chtimes …/.sfx-*.part: no such file or directory"
// churn).
//
// The knob bounds how many of those sessions run with one peer at once: each is
// a connection with its own pull pipeline, so N > 1 multiplies the per-session
// memory (pull_inflight_mb, plus K block windows) by N. It is an atomic so
// PUT /api/settings/sync can change it on a running daemon without a restart —
// the production boxes have no reachable shell — and each scheduling decision
// samples it ONCE, so a change applies to the NEXT decision and never disturbs a
// session already in flight (the same contract as K and the hash pool).

// clampFolderSessions maps a requested cap onto the supported range. 0 and
// negatives cannot be honoured from the API (the config file keeps 0 =
// "default"), so an explicit request of 0 means "the minimum", 1 — the rule
// pull_concurrency uses. The ceiling is config.MaxFolderSessionsPerPeer so
// whatever this returns may be written to config.yaml and still load.
func clampFolderSessions(k int) int {
	if k < 1 {
		return 1
	}
	if k > config.MaxFolderSessionsPerPeer {
		return config.MaxFolderSessionsPerPeer
	}
	return k
}

// FolderSessions is the cap the NEXT scheduling decision will use: the value set
// at runtime when one has been set, otherwise the configured one (which itself
// defaults to config.DefaultFolderSessionsPerPeer and is clamped to the hard
// cap). Sessions already running keep their slots.
func (n *Node) FolderSessions() int {
	if k := n.folderK.Load(); k > 0 {
		return int(k)
	}
	return n.Cfg.FolderSessionsPerPeer()
}

// SetFolderSessions sets the cap for the next scheduling decision, clamped to
// [1, config.MaxFolderSessionsPerPeer], and reports the value applied. Sessions
// already in flight are unaffected: lowering the cap below the number of live
// sessions simply starts no new ones until the live count drops under it.
func (n *Node) SetFolderSessions(k int) int {
	k = clampFolderSessions(k)
	n.folderK.Store(int64(k))
	return k
}
