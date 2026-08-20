package control

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/sync"
	"crosssync/internal/ui"
)

// Handler returns the REST API HTTP handler (stdlib ServeMux).
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /api/folders", s.handleFolders)
	mux.HandleFunc("GET /api/browse", s.handleBrowse)
	mux.HandleFunc("POST /api/folders", s.handleAddFolder)
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
	mux.HandleFunc("PUT /api/settings/auto_archive", s.handleSetAutoArchive)
	mux.HandleFunc("PUT /api/settings/alerts", s.handleSetAlerts)
	mux.HandleFunc("POST /api/alerts/test", s.handleTestAlert)
	mux.HandleFunc("DELETE /api/folders/{id}/files/{name...}", s.handleDeleteFile)
	mux.HandleFunc("POST /api/folders/{id}/rescan", s.handleFolderRescan)
	mux.HandleFunc("POST /api/folders/{id}/sync", s.handleFolderSync)
	mux.HandleFunc("POST /api/folders/{id}/clean_versions", s.handleCleanVersions)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("GET /api/events/archive_preview", s.handleArchivePreview)
	mux.HandleFunc("POST /api/events/archive", s.handleArchive)
	mux.HandleFunc("POST /api/events/ack_condition", s.handleAckCondition)
	mux.HandleFunc("POST /api/events/{id}/ack", s.handleAck)
	mux.HandleFunc("POST /api/rescan", s.handleRescan)
	mux.HandleFunc("POST /api/sync", s.handleSync)
	mux.HandleFunc("GET /api/ui/check", s.handleUICheck)
	mux.HandleFunc("POST /api/ui/update", s.handleUIUpdate)
	mux.HandleFunc("GET /api/events/stream", s.handleStream)
	// The embedded web UI is served at the root; more specific /api/*
	// patterns take precedence. When ui_dir is configured and contains an
	// index.html, that file overrides the embedded UI.
	mux.Handle("GET /", ui.Handler(s.UIDir))
	return withCORS(mux)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
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
		ID     string   `json:"id"`   // optional; generated when empty
		Name   string   `json:"name"` // human label shown in the UI
		Path   string   `json:"path"`
		Policy string   `json:"conflict"`
		Peers  []string `json:"peers"` // peer names or numeric ids (empty = all peers)
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
	fs, err := s.AddFolder(body.ID, body.Name, body.Path, body.Policy, peers)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, fs)
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
	applied, err := s.Rescan(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, applied)
}

func (s *Service) handleFolderSync(w http.ResponseWriter, r *http.Request) {
	if err := s.FolderSync(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusBadRequest, err)
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
	writeJSON(w, http.StatusOK, applied)
}

func (s *Service) handleSync(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PeerID uint64 `json:"peer_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := s.SyncNow(body.PeerID); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"synced": true})
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
		fmt.Fprintf(w, "event: %s\ndata: ", name)
		_ = enc.Encode(v)
		fmt.Fprint(w, "\n")
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
