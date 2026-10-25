package control

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/sync"
)

// metricsContentType is the Prometheus text exposition format.
const metricsContentType = "text/plain; version=0.0.4; charset=utf-8"

// handleMetrics exposes the daemon's operational numbers in Prometheus text
// format, so a homelab can alert on "nothing has synced for an hour" or "the
// pending queue is growing" without scraping /api/status and re-parsing JSON.
//
// Cost is bounded and comparable to one /api/status poll: one aggregate query
// per folder plus in-memory atomics. It deliberately does NOT walk the
// versioning/conflict directories (that is what the UI's storage line is for),
// because a metrics endpoint gets scraped forever and must stay cheap.
func (s *Service) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", metricsContentType)
	var b strings.Builder

	fmt.Fprintf(&b, "# HELP crosssync_build_info Build identity of this daemon.\n")
	fmt.Fprintf(&b, "# TYPE crosssync_build_info gauge\n")
	fmt.Fprintf(&b, "crosssync_build_info{version=%q,device=%q,device_id=\"%d\"} 1\n",
		esc(s.Version), esc(s.n.Name), s.n.ID)

	fmt.Fprintf(&b, "# HELP crosssync_open_events Open warn+ events across all folders.\n")
	fmt.Fprintf(&b, "# TYPE crosssync_open_events gauge\n")
	if open, err := s.n.Events.CountOpen(); err == nil {
		fmt.Fprintf(&b, "crosssync_open_events %d\n", open)
	}

	running := 0
	if s.n.SyncRunning() {
		running = 1
	}
	// Process memory: the number that decides how far this design scales. Read
	// from the kernel where possible (RSS is what the container is billed for),
	// falling back to the Go heap so the metric always exists.
	fmt.Fprintf(&b, "# HELP crosssync_process_resident_memory_bytes Resident set size of the daemon.\n")
	fmt.Fprintf(&b, "# TYPE crosssync_process_resident_memory_bytes gauge\n")
	fmt.Fprintf(&b, "crosssync_process_resident_memory_bytes %d\n", residentBytes())

	fmt.Fprintf(&b, "# HELP crosssync_sync_running 1 while a peer sync round is in flight.\n")
	fmt.Fprintf(&b, "# TYPE crosssync_sync_running gauge\n")
	fmt.Fprintf(&b, "crosssync_sync_running %d\n", running)

	// Deterministic order: Prometheus does not care, but diffs and tests do.
	ids := make([]string, 0, len(s.n.Folders))
	for id := range s.n.Folders {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	folderHelp := []struct{ name, help, typ string }{
		{"crosssync_folder_files", "Live indexed files (tombstones excluded).", "gauge"},
		{"crosssync_folder_directories", "Live indexed directories.", "gauge"},
		{"crosssync_folder_tombstones", "Deleted entries still held in the index for delta propagation.", "gauge"},
		{"crosssync_folder_bytes", "Bytes of live files.", "gauge"},
		{"crosssync_folder_pending_files", "Files this node still has to download from peers.", "gauge"},
		{"crosssync_folder_pending_up", "Files peers still need from this node.", "gauge"},
		{"crosssync_folder_conflict_files", "Conflict copies on disk.", "gauge"},
		{"crosssync_folder_paused", "1 when scanning and syncing are paused for this folder on this device.", "gauge"},
		{"crosssync_folder_last_sync_timestamp_seconds", "Unix time of the last completed sync round for the folder (0 = never).", "gauge"},
		{"crosssync_folder_bytes_up_total", "Bytes served to peers.", "counter"},
		{"crosssync_folder_bytes_down_total", "Bytes received from peers.", "counter"},
		{"crosssync_folder_files_down_total", "Files pulled from peers.", "counter"},
		{"crosssync_folder_sessions_total", "Completed sync sessions touching the folder.", "counter"},
		{"crosssync_folder_index_bytes", "Size of the per-folder index database on disk.", "gauge"},
		{"crosssync_folder_last_scan_entries", "Entries visited by the last completed scan of the folder.", "gauge"},
		{"crosssync_folder_last_scan_duration_seconds", "Duration of the last completed scan of the folder.", "gauge"},
		{"crosssync_folder_pending_compute_ms", "Time the last sync round spent computing what to pull and what to delete (the O(index) part).", "gauge"},
	}
	byName := map[string]*strings.Builder{}
	for _, h := range folderHelp {
		byName[h.name] = &strings.Builder{}
	}
	for _, id := range ids {
		f, ok := s.n.Folders[id]
		if !ok {
			continue
		}
		lbl := fmt.Sprintf("{folder=%q}", esc(id))
		set := func(name string, v int64) {
			fmt.Fprintf(byName[name], "%s%s %d\n", name, lbl, v)
		}
		if st, err := f.Ix.Stats(sync.ConflictSuffix); err == nil {
			set("crosssync_folder_files", int64(st.Files))
			set("crosssync_folder_directories", int64(st.Directories))
			set("crosssync_folder_tombstones", int64(st.Tombstones))
			set("crosssync_folder_bytes", st.Size)
			set("crosssync_folder_conflict_files", int64(st.ConflictFiles))
		}
		pending, _ := s.pending(id)
		set("crosssync_folder_pending_files", int64(pending))
		if up, err := f.Engine.PendingUploads(); err == nil {
			set("crosssync_folder_pending_up", int64(len(up)))
		}
		paused := 0
		if f.Paused() {
			paused = 1
		}
		set("crosssync_folder_paused", int64(paused))
		set("crosssync_folder_last_sync_timestamp_seconds", f.Engine.Stats.LastSync.Load()/1e9)
		set("crosssync_folder_bytes_up_total", f.Engine.Stats.BytesUp.Load())
		set("crosssync_folder_bytes_down_total", f.Engine.Stats.BytesDown.Load())
		set("crosssync_folder_files_down_total", f.Engine.Stats.FilesDown.Load())
		set("crosssync_folder_sessions_total", f.Engine.Stats.Syncs.Load())
		if fi, err := os.Stat(f.Ix.Path()); err == nil {
			set("crosssync_folder_index_bytes", fi.Size())
		}
		entries, scanSecs := s.n.FolderScanInfo(id)
		set("crosssync_folder_last_scan_entries", int64(entries))
		set("crosssync_folder_last_scan_duration_seconds", int64(scanSecs))
		set("crosssync_folder_pending_compute_ms", f.Engine.Stats.PendingComputeMs.Load())
	}
	for _, h := range folderHelp {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", h.name, h.help, h.name, h.typ)
		b.WriteString(byName[h.name].String())
	}

	// Peers, sorted by name so the exposition order is stable.
	peers := append([]config.Peer(nil), s.n.Cfg.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i].Name < peers[j].Name })
	fmt.Fprintf(&b, "# HELP crosssync_peer_connected 1 when the peer was reachable in the last 2 minutes.\n")
	fmt.Fprintf(&b, "# TYPE crosssync_peer_connected gauge\n")
	for _, p := range peers {
		up := 0
		if s.n.ConnMgr != nil {
			if lo := s.n.ConnMgr.LastOnline(p.ID); !lo.IsZero() && time.Since(lo) < 2*time.Minute {
				up = 1
			}
		}
		fmt.Fprintf(&b, "crosssync_peer_connected{peer=%q,peer_id=\"%d\"} %d\n", esc(p.Name), p.ID, up)
	}
	fmt.Fprintf(&b, "# HELP crosssync_peer_last_sync_timestamp_seconds Unix time of the last successful round with a peer (assumed 1970 when never).\n")
	fmt.Fprintf(&b, "# TYPE crosssync_peer_last_sync_timestamp_seconds gauge\n")
	for _, p := range peers {
		var ts int64
		if s.n.ConnMgr != nil {
			if ls := s.n.ConnMgr.LastSync(p.ID); !ls.IsZero() {
				ts = ls.Unix()
			}
		}
		fmt.Fprintf(&b, "crosssync_peer_last_sync_timestamp_seconds{peer=%q} %d\n", esc(p.Name), ts)
	}

	_, _ = w.Write([]byte(b.String()))
}

// residentBytes returns the process RSS, falling back to the Go heap size on
// platforms without /proc (Windows dev builds).
func residentBytes() int64 {
	if b, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(b))
		if len(fields) >= 2 {
			if pages, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				return pages * int64(os.Getpagesize())
			}
		}
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

// esc escapes a label value for the text format (backslash, quote, newline).
func esc(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}
