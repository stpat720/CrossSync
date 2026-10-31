package control

import (
	"crypto/subtle"
	"errors"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/sync"
	"crosssync/internal/ui"
)

// Handler returns the REST API HTTP handler (stdlib ServeMux).
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /api/backup", s.handleBackup)
	mux.HandleFunc("GET /api/folders", s.handleFolders)
	mux.HandleFunc("GET /api/browse", s.handleBrowse)
	mux.HandleFunc("POST /api/folders", s.handleAddFolder)
	mux.HandleFunc("PUT /api/folders/{id}", s.handleUpdateFolder)
	mux.HandleFunc("PUT /api/folders/{id}/peers", s.handleSetFolderPeers)
	mux.HandleFunc("DELETE /api/folders/{id}", s.handleRemoveFolder)
	mux.HandleFunc("GET /api/folders/{id}/files", s.handleFiles)
	mux.HandleFunc("GET /api/folders/{id}/pending", s.handlePending)
	mux.HandleFunc("GET /api/folders/{id}/conflicts", s.handleConflicts)
	mux.HandleFunc("GET /api/folders/{id}/attention", s.handleAttention)
	mux.HandleFunc("GET /api/folders/{id}/versions", s.handleVersions)
	mux.HandleFunc("POST /api/folders/{id}/restore", s.handleRestore)
	mux.HandleFunc("POST /api/folders/{id}/conflicts/restore", s.handleRestoreConflict)
	mux.HandleFunc("POST /api/folders/{id}/conflicts/compare", s.handleCompareConflict)
	mux.HandleFunc("POST /api/folders/{id}/conflicts/resolve_all", s.handleResolveAllConflicts)
	mux.HandleFunc("POST /api/folders/{id}/deletions/apply", s.handleApplyDeletions)
	// Soft deletions: the hold area of an over-threshold delete pass.
	mux.HandleFunc("POST /api/folders/{id}/hold/restore", s.handleRestoreHeld)
	mux.HandleFunc("POST /api/folders/{id}/hold/delete", s.handleDeleteHeld)
	mux.HandleFunc("GET /api/folders/{id}/hold", s.handleFolderHold)
	mux.HandleFunc("POST /api/folders/{id}/pause", s.handlePauseFolder)
	mux.HandleFunc("PUT /api/settings/auto_archive", s.handleSetAutoArchive)
	mux.HandleFunc("PUT /api/settings/alerts", s.handleSetAlerts)
	mux.HandleFunc("POST /api/alerts/test", s.handleTestAlert)
	mux.HandleFunc("DELETE /api/folders/{id}/files/{name...}", s.handleDeleteFile)
	mux.HandleFunc("POST /api/folders/{id}/rescan", s.handleFolderRescan)
	// Convergence-test data generator (testdata/ subtree only, capped per call).
	mux.HandleFunc("POST /api/folders/{id}/testdata", s.handleTestData)
	mux.HandleFunc("POST /api/folders/{id}/sync", s.handleFolderSync)
	mux.HandleFunc("POST /api/folders/{id}/clean_versions", s.handleCleanVersions)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("GET /api/events/archive_preview", s.handleArchivePreview)
	mux.HandleFunc("POST /api/events/archive", s.handleArchive)
	mux.HandleFunc("POST /api/events/ack_condition", s.handleAckCondition)
	mux.HandleFunc("POST /api/events/{id}/ack", s.handleAck)
	mux.HandleFunc("POST /api/rescan", s.handleRescan)
	mux.HandleFunc("POST /api/sync", s.handleSync)
	mux.HandleFunc("POST /api/restart", s.handleRestart)
	mux.HandleFunc("GET /api/peers", s.handlePeers)
	mux.HandleFunc("POST /api/peers", s.handleAddPeer)
	mux.HandleFunc("PUT /api/peers/{id}", s.handleUpdatePeer)
	mux.HandleFunc("DELETE /api/peers/{id}", s.handleRemovePeer)
	mux.HandleFunc("GET /api/ui/check", s.handleUICheck)
	mux.HandleFunc("POST /api/ui/update", s.handleUIUpdate)
	mux.HandleFunc("GET /api/update/check", s.handleDaemonUpdateCheck)
	mux.HandleFunc("POST /api/update/apply", s.handleDaemonUpdateApply)
	mux.HandleFunc("GET /api/events/stream", s.handleStream)
	// The embedded web UI is served at the root; more specific /api/*
	// patterns take precedence. When ui_dir is configured and contains an
	// index.html, that file overrides the embedded UI.
	mux.Handle("GET /", ui.Handler(s.UIDir))
	return withCORS(withToken(s.ControlToken, mux))
}

// withToken demands the configured control token on every request when one is
// set. The header form is for API clients and the web UI; the query form exists
// because EventSource (SSE) and plain download links cannot set headers.
// A 401 is answered with a JSON body the UI can recognise and prompt on.
func withToken(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := ""
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			got = strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
		}
		if got == "" {
			got = r.URL.Query().Get("token")
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{
				"error": "control token required",
				"code":  "unauthorized",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Status())
}

func (s *Service) handleFolders(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Folders())
}

func (s *Service) handleBrowse(w http.ResponseWriter, r *http.Request) {
	res, err := s.Browse(r.URL.Query().Get("path"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Service) handleAddFolder(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID        string   `json:"id"`   // optional; generated when empty
		Name      string   `json:"name"` // human label shown in the UI
		Path      string   `json:"path"`
		Policy    string   `json:"conflict"`
		Peers     []string `json:"peers"`     // peer names or numeric ids (empty = all peers)
		Direction string   `json:"direction"` // "" | send-receive | send-only | receive-only
		Include   []string `json:"include"`   // whitelist patterns (empty = everything)
		Ignore    []string `json:"ignore"`    // exclude patterns
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	peers, err := s.resolvePeers(body.Peers)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	fs, err := s.AddFolder(body.ID, body.Name, body.Path, body.Policy, peers, body.Direction, body.Include, body.Ignore)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, fs)
}

func (s *Service) handleUpdateFolder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Name      string   `json:"name"`
		Path      string   `json:"path"`
		Direction string   `json:"direction"` // "" | send-receive | send-only | receive-only
		Include   []string `json:"include"`   // whitelist patterns (empty = everything)
		Ignore    []string `json:"ignore"`    // exclude patterns
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	fs, err := s.UpdateFolder(id, body.Name, body.Path, body.Direction, body.Include, body.Ignore)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, fs)
}

func (s *Service) handleSetFolderPeers(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Peers []string `json:"peers"` // peer names or numeric ids (empty = all peers)
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	peers, err := s.resolvePeers(body.Peers)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.SetFolderPeers(id, peers); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, s.folderStatus(id))
}

func (s *Service) handleRemoveFolder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.RemoveFolder(id); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"removed": id})
}

func (s *Service) handleFiles(w http.ResponseWriter, r *http.Request) {
	files, err := s.Files(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if files == nil {
		files = []FileStatus{}
	}
	writeJSON(w, http.StatusOK, files)
}

func (s *Service) handlePending(w http.ResponseWriter, r *http.Request) {
	pending, err := s.Pending(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if pending == nil {
		pending = []PendingFile{}
	}
	writeJSON(w, http.StatusOK, pending)
}

func (s *Service) handleConflicts(w http.ResponseWriter, r *http.Request) {
	conflicts, err := s.Conflicts(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if conflicts == nil {
		conflicts = []FileStatus{}
	}
	writeJSON(w, http.StatusOK, conflicts)
}

func (s *Service) handleAttention(w http.ResponseWriter, r *http.Request) {
	files, err := s.Attention(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if files == nil {
		files = []AttentionFile{}
	}
	writeJSON(w, http.StatusOK, files)
}

func (s *Service) handleVersions(w http.ResponseWriter, r *http.Request) {
	vers, err := s.Versions(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if vers == nil {
		vers = []sync.VersionEntry{}
	}
	writeJSON(w, http.StatusOK, vers)
}

func (s *Service) handleRestore(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path        string `json:"path"`
		ArchivePath string `json:"archive_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	if err := s.RestoreVersion(r.PathValue("id"), body.Path, body.ArchivePath); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"restored": body.Path})
}

func (s *Service) handleRestoreConflict(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	if err := s.RestoreConflict(r.PathValue("id"), body.Name); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"restored": body.Name})
}

func (s *Service) handleCompareConflict(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	cc, err := s.CompareConflict(r.PathValue("id"), body.Name)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, cc)
}

func (s *Service) handleResolveAllConflicts(w http.ResponseWriter, r *http.Request) {
	n, err := s.ResolveAllConflicts(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"resolved": n})
}

func (s *Service) handleApplyDeletions(w http.ResponseWriter, r *http.Request) {
	n, err := s.ApplyDeletions(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"applied": n})
}

// healthzStaleAfter is how long a folder may go without a completed sync
// before /healthz reports degraded. A variable so tests can shorten it.
var healthzStaleAfter = time.Hour

// handleHealthz is the cheap container probe: 200 when every folder has
// completed a sync round recently, 503 when one has not. It exists because the
// failure that matters most in production is the SILENT one — a share that
// stops converging while the process stays happily alive — and nothing else
// surfaces that to a container runtime. Deliberately unauthenticated and
// read-only: it reports a status word, a version and ages, never file data.
func (s *Service) handleHealthz(w http.ResponseWriter, r *http.Request) {
	newest, oldest, folders := s.n.SyncHealth()
	peers := len(s.n.Cfg.Peers)
	now := time.Now()

	// A folder with a scan in flight is BUSY, not stalled; SyncHealth excludes
	// those (a verified scan of a large share legitimately holds its folder
	// while it re-hashes every byte).

	body := map[string]any{
		"version":                 s.Version,
		"folders":                 folders,
		"peers":                   peers,
		"newest_sync_age_seconds": int64(-1),
		"oldest_sync_age_seconds": int64(-1),
	}
	if newest > 0 {
		body["newest_sync_age_seconds"] = int64(now.Sub(time.Unix(0, newest)).Seconds())
	}
	if oldest > 0 {
		body["oldest_sync_age_seconds"] = int64(now.Sub(time.Unix(0, oldest)).Seconds())
	}

	// Nothing to sync: a single-node box (or an empty config) is healthy by
	// definition — staleness is only meaningful when a peer should be feeding
	// this folder.
	if folders == 0 || peers == 0 {
		body["status"] = "ok"
		writeJSON(w, http.StatusOK, body)
		return
	}
	code := http.StatusOK
	body["status"] = "ok"
	switch {
	case oldest == 0:
		code = http.StatusServiceUnavailable
		body["status"] = "degraded"
		body["reason"] = "no sync round has completed yet"
	case now.Sub(time.Unix(0, oldest)) > healthzStaleAfter:
		code = http.StatusServiceUnavailable
		body["status"] = "degraded"
		body["reason"] = fmt.Sprintf("no completed sync round in over %s — check peers and the last open events", healthzStaleAfter)
	}
	writeJSON(w, code, body)
}

// handlePauseFolder pauses or resumes scanning and syncing for one folder on
// this device. Body: {"paused": true|false}. The folder keeps its index; while
// paused it is not advertised to peers, so they see it as not shared and stop
// expected transfers instead of erroring.
//
// The answer is the minimal {id, paused, transfer_paused} record, deliberately
// not a full FolderStatus: this is the request that must stop disk I/O, and
// building a status costs SQL aggregates plus a statfs (seconds on a large
// Unraid share). The UI already polls /api/status for everything else.
func (s *Service) handlePauseFolder(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Paused *bool `json:"paused"`
		// Transfers pauses data movement only: scanning keeps running and the
		// folder stays advertised. Two distinct levels, because "stop using my
		// link" and "stop touching my disk" are different operational needs.
		Transfers *bool `json:"transfers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	if body.Paused == nil && body.Transfers == nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf(`"paused" (everything) or "transfers" (data movement only) is required`))
		return
	}
	if body.Transfers != nil {
		fs, err := s.SetFolderTransfersPaused(r.PathValue("id"), *body.Transfers)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if body.Paused == nil {
			writeJSON(w, http.StatusOK, fs)
			return
		}
	}
	fs, err := s.SetFolderPaused(r.PathValue("id"), *body.Paused)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, fs)
}

// holdRequest is the body of the soft-deletion actions. An empty path means
// "every held entry" (delete only).
type holdRequest struct {
	Path string `json:"path"`
}

func (s *Service) handleRestoreHeld(w http.ResponseWriter, r *http.Request) {
	var body holdRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	if err := s.RestoreHeld(r.PathValue("id"), body.Path); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"restored": body.Path})
}

func (s *Service) handleDeleteHeld(w http.ResponseWriter, r *http.Request) {
	var body holdRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	n, err := s.DeleteHeld(r.PathValue("id"), body.Path)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": n})
}

// handleFolderHold lists one folder's held (soft-deleted) copies in full.
// /api/status deliberately carries only the count/bytes summary: this list is
// unbounded and status is polled every couple of seconds.
func (s *Service) handleFolderHold(w http.ResponseWriter, r *http.Request) {
	h, err := s.Hold(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if h == nil {
		h = &FolderHold{Entries: []HeldFile{}}
	}
	writeJSON(w, http.StatusOK, h)
}

func (s *Service) handleSetAutoArchive(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Enabled   bool  `json:"enabled"`
		OlderThan int64 `json:"older_than"` // seconds
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	if err := s.SetAutoArchive(body.Enabled, body.OlderThan); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"enabled": body.Enabled, "older_than": body.OlderThan})
}

func (s *Service) handleSetAlerts(w http.ResponseWriter, r *http.Request) {
	var body struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	if err := s.SetAlertURL(body.URL); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"url": body.URL})
}

func (s *Service) handleTestAlert(w http.ResponseWriter, r *http.Request) {
	if err := s.SendTestAlert(); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sent": true})
}

func (s *Service) handleDeleteFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	name := r.PathValue("name")
	if err := s.DeleteConflict(id, name); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": name})
}

func (s *Service) handleFolderRescan(w http.ResponseWriter, r *http.Request) {
	// Optional body: {"verify": true} runs a VERIFIED scan (re-hash every file).
	var body struct {
		Verify bool `json:"verify"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
			return
		}
	}
	id := r.PathValue("id")
	if _, ok := s.n.Folders[id]; !ok {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("unknown folder %q", id))
		return
	}
	// A paused folder is not scanned now, and the request is not thrown away:
	// node.RequestRescan remembers it and the resume runs it exactly once.
	// Answering 202 here (for both kinds) is what makes that visible instead of
	// looking like "the scan ran and found nothing".
	if s.n.Paused(id) {
		if _, _, err := s.n.RequestRescan(id, body.Verify); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"queued": true, "folder": id, "verify": body.Verify,
			"note": "folder is paused — the rescan will run once when it is resumed",
		})
		return
	}
	if body.Verify {
		// A verified scan reads EVERY byte: on a large or busy share that is
		// minutes to hours, so it cannot live inside one HTTP request (the
		// browser and any proxy in between would time out and leave the
		// operator staring at a spinner). Kick it off and answer immediately;
		// progress is visible in /api/status scans[] and the folder row.
		if s.n.ScanRunning(id) {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("scan already in progress for folder %q", id))
			return
		}
		go func() {
			if _, err := s.RescanVerify(id, true); err != nil {
				s.n.Logf("verified rescan %s: %v", id, err)
			}
		}()
		writeJSON(w, http.StatusAccepted, map[string]any{"started": true, "folder": id, "verify": true})
		return
	}
	applied, err := s.RescanVerify(id, false)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	// The folder was paused (before or during the pass): the scan is owed, not
	// done. Answering "0 changes" here would be the silent consumption this
	// endpoint must never do.
	if applied[id] < 0 {
		writeJSON(w, http.StatusAccepted, map[string]any{
			"queued": true, "folder": id, "verify": false,
			"note": "folder is paused — the rescan will run once when it is resumed",
		})
		return
	}
	writeJSON(w, http.StatusOK, applied)
}

// statusFor maps a service error to the HTTP status that describes it. A sync
// that cannot start because one is already running, a scan is running, or the
// folder is paused is 409; an unreachable peer is 503 with the dial error in
// the body; everything else is a plain 400. Before this, ALL of them were 500
// with the message buried in the body, so a routine overlap looked like a
// crash and the reason was easy to miss.
func statusFor(err error) int {
	switch {
	case errors.Is(err, ErrBusy):
		return http.StatusConflict
	case errors.Is(err, ErrUnavailable):
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadRequest
	}
}

func (s *Service) handleFolderSync(w http.ResponseWriter, r *http.Request) {
	if err := s.FolderSync(r.PathValue("id")); err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"synced": r.PathValue("id")})
}

func (s *Service) handleCleanVersions(w http.ResponseWriter, r *http.Request) {
	res, err := s.CleanVersions(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// archiveParams reads the shared older_than/include_open parameters.
func archiveParams(r *http.Request) (olderThan int64, includeOpen bool, err error) {
	q := r.URL.Query()
	if v := q.Get("older_than"); v != "" {
		olderThan, err = strconv.ParseInt(v, 10, 64)
		if err != nil || olderThan < 0 {
			return 0, false, fmt.Errorf("invalid older_than %q", v)
		}
	}
	includeOpen = q.Get("include_open") == "1" || q.Get("include_open") == "true"
	return olderThan, includeOpen, nil
}

func (s *Service) handleArchivePreview(w http.ResponseWriter, r *http.Request) {
	olderThan, includeOpen, err := archiveParams(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	res, err := s.ArchivePreview(olderThan, includeOpen)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Service) handleArchive(w http.ResponseWriter, r *http.Request) {
	var body struct {
		OlderThan   int64 `json:"older_than"`   // 0 = older than the oldest open attention
		IncludeOpen bool  `json:"include_open"` // also delete unviewed attention events
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	if body.OlderThan < 0 {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("older_than must be >= 0"))
		return
	}
	res, err := s.ArchiveEvents(body.OlderThan, body.IncludeOpen)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Service) handleEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := events.Filter{
		Folder:   q.Get("folder"),
		Category: events.Category(q.Get("category")),
		Path:     q.Get("path"),
	}
	if v := q.Get("open"); v == "1" || v == "true" {
		f.OpenOnly = true
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid limit %q", v))
			return
		}
		f.Limit = n
	}
	if v := q.Get("after"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid after %q", v))
			return
		}
		f.AfterID = n
	}
	if v := q.Get("since"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid since %q", v))
			return
		}
		f.Since = n
	}
	if v := q.Get("peer"); v != "" {
		if s.isSelf(v) {
			f.Self = true
		} else {
			id := s.resolvePeer(v)
			if id == 0 {
				writeErr(w, http.StatusBadRequest, fmt.Errorf("unknown peer %q", v))
				return
			}
			f.PeerID = id
			f.PeerName = s.peerName(id)
		}
	}
	evs, err := s.Events(f)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if evs == nil {
		evs = []events.Event{}
	}
	writeJSON(w, http.StatusOK, evs)
}

func (s *Service) handleAck(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid event id"))
		return
	}
	var body struct {
		By string `json:"by"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.Ack(id, body.By); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "acknowledged": true})
}

func (s *Service) handleAckCondition(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Folder   string `json:"folder"`
		Path     string `json:"path"`
		Category string `json:"category"`
		By       string `json:"by"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	if body.Category == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("category is required"))
		return
	}
	if err := s.AckCondition(body.Folder, body.Path, events.Category(body.Category), body.By); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true})
}

func (s *Service) handleRescan(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Folder string `json:"folder"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	applied, err := s.Rescan(body.Folder)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	// A folder that is paused is queued rather than scanned (RescanVerify
	// reports it as -1 in the applied map). Say so instead of answering
	// "0 changes", which reads as "your tree is clean".
	var queued []string
	for id, n := range applied {
		if n < 0 {
			queued = append(queued, id)
		}
	}
	if len(queued) > 0 {
		sort.Strings(queued)
		writeJSON(w, http.StatusAccepted, map[string]any{
			"applied": applied, "queued": queued,
			"note": "a paused folder is not scanned now — its rescan runs once when it is resumed",
		})
		return
	}
	writeJSON(w, http.StatusOK, applied)
}

// handleRestart re-executes the daemon so an operator can clear a wedged link or
// pick up a config change without reinstalling anything (which was the only
// restart path before this).
func (s *Service) handleRestart(w http.ResponseWriter, r *http.Request) {
	s.Restart()
	writeJSON(w, http.StatusAccepted, map[string]any{"restarting": true})
}

func (s *Service) handleSync(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PeerID uint64 `json:"peer_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.SyncNow(body.PeerID); err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"synced": true})
}

func (s *Service) handlePeers(w http.ResponseWriter, r *http.Request) {
	st := s.Status()
	writeJSON(w, http.StatusOK, st.Peers)
}

func (s *Service) handleAddPeer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID          uint64   `json:"id"` // 0 = generate
		Name        string   `json:"name"`
		Addresses   []string `json:"addresses"`
		Fingerprint string   `json:"fingerprint"` // optional; empty = not pinned yet
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	p, err := s.AddPeer(body.ID, body.Name, body.Addresses, body.Fingerprint)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, p)
}

func (s *Service) handleUpdatePeer(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("peer id must be a number"))
		return
	}
	var body struct {
		Name        string   `json:"name"`
		Addresses   []string `json:"addresses"`
		Fingerprint string   `json:"fingerprint"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid body: %w", err))
		return
	}
	p, err := s.UpdatePeer(id, body.Name, body.Addresses, body.Fingerprint)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Service) handleRemovePeer(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("peer id must be a number"))
		return
	}
	if err := s.RemovePeer(id); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"removed": id})
}

func (s *Service) handleUICheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.CheckUIUpdate())
}

func (s *Service) handleUIUpdate(w http.ResponseWriter, r *http.Request) {
	version, err := s.UpdateUI()
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"updated": true, "version": version})
}

// handleDaemonUpdateCheck reports whether a newer daemon release is
// available at the configured update_url. ?refresh=1 forces a fresh fetch
// instead of the throttled cache.
func (s *Service) handleDaemonUpdateCheck(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("refresh") == "1" {
		s.ForceUpdateCheck()
	}
	writeJSON(w, http.StatusOK, s.CheckDaemonUpdate())
}

// handleDaemonUpdateApply downloads, verifies and swaps in a newer daemon
// binary, then restarts the process. The response is sent BEFORE the swap
// so the client sees the result; config.yaml and meta_dir are untouched.
func (s *Service) handleDaemonUpdateApply(w http.ResponseWriter, r *http.Request) {
	newVersion, err := s.StageUpdate()
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"updated": newVersion, "restarting": true})
	// Give the response a moment to flush, then swap + re-exec.
	go func() {
		time.Sleep(800 * time.Millisecond)
		s.CommitUpdate()
	}()
}

// handleStream serves new events as Server-Sent Events. Each event is sent
// as `event: event` with a JSON data payload. A slow client never blocks
// the daemon (the subscription drops rather than buffers forever).
func (s *Service) handleStream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("streaming not supported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ch, cancel := s.Subscribe()
	defer cancel()
	// Heartbeat so proxies do not time out the connection.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	enc := json.NewEncoder(w)
	writeEvent := func(name string, v any) {
		_, _ = fmt.Fprintf(w, "event: %s\ndata: ", name)
		_ = enc.Encode(v)
		_, _ = fmt.Fprint(w, "\n")
		fl.Flush()
	}
	writeEvent("hello", s.Status())
	for {
		select {
		case e := <-ch:
			writeEvent("event", e)
		case <-ticker.C:
			writeEvent("ping", map[string]string{"ts": time.Now().UTC().Format(time.RFC3339)})
		case <-r.Context().Done():
			return
		}
	}
}
