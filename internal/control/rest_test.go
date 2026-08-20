package control_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"crosssync/internal/config"
	"crosssync/internal/control"
	"crosssync/internal/events"
	"crosssync/internal/node"
)

func testNode(t *testing.T) (*node.Node, string) {
	t.Helper()
	root := t.TempDir()
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{{ID: "data", Path: root, ConflictPolicy: "conflict-copy"}},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n, root
}

func getJSON(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body
}

func TestRESTStatus(t *testing.T) {
	n, root := testNode(t)
	// A file + scan produces an applied event; also verify open count.
	if err := os.WriteFile(filepath.Join(root, "x.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}

	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	code, body := getJSON(t, srv.URL+"/api/status")
	if code != 200 {
		t.Fatalf("status code %d: %s", code, body)
	}
	var st struct {
		Device     string `json:"device"`
		Version    string `json:"version"`
		OpenEvents int64  `json:"open_events"`
		Folders    []struct {
			ID    string `json:"id"`
			Files int    `json:"files"`
		} `json:"folders"`
	}
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatal(err)
	}
	if st.Device != "nas-01" || st.Version != "test" {
		t.Fatalf("unexpected status: %+v", st)
	}
	if len(st.Folders) != 1 || st.Folders[0].ID != "data" || st.Folders[0].Files != 1 {
		t.Fatalf("unexpected folders: %+v", st.Folders)
	}
}

// TestFolderPeersSmartAll verifies the smart "all peers" rule: a folder
// shows as all-peers when it has no peer list OR its list covers every
// currently configured peer (so adding a new peer later flips it back to
// showing its explicit scoped peers).
func TestFolderPeersSmartAll(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{
			{ID: "both", Path: t.TempDir(), ConflictPolicy: "conflict-copy", Peers: []uint64{2, 3}},
			{ID: "one", Path: t.TempDir(), ConflictPolicy: "conflict-copy", Peers: []uint64{2}},
			{ID: "open", Path: t.TempDir(), ConflictPolicy: "conflict-copy"},
		},
		Peers: []config.Peer{
			{ID: 2, Name: "nas-02", Addresses: []string{"h:1"}},
			{ID: 3, Name: "nas-03", Addresses: []string{"h:1"}},
		},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	svc := control.New(n, "test")

	byID := map[string]control.FolderStatus{}
	for _, f := range svc.Folders() {
		byID[f.ID] = f
	}

	// Covers all current peers -> shown as all peers, but the explicit
	// names are still reported so the UI can preselect them.
	both := byID["both"]
	if !both.PeersAll {
		t.Fatalf("'both' should be all-peers (covers every configured peer): %+v", both)
	}
	if len(both.PeerNames) != 2 || both.PeerNames[0] != "nas-02" || both.PeerNames[1] != "nas-03" {
		t.Fatalf("'both' peer names = %v", both.PeerNames)
	}
	// Partially scoped -> NOT all peers.
	if one := byID["one"]; one.PeersAll {
		t.Fatalf("'one' must not be all-peers: %+v", one)
	}
	// No list -> all peers.
	if open := byID["open"]; !open.PeersAll || len(open.PeerNames) != 0 {
		t.Fatalf("'open' should be all-peers with no names: %+v", open)
	}
}

// TestAttentionIncludesUploadsAndResolvesStaleConflicts verifies that the
// attention list includes pending uploads (so the ↑ upload filter can show
// them) and auto-resolves stale open conflict events whose conflict copy no
// longer exists.
func TestAttentionIncludesUploadsAndResolvesStaleConflicts(t *testing.T) {
	n, root := testNode(t)
	svc := control.New(n, "test")

	// A local file, plus a peer view that lacks every file -> everything
	// becomes a pending upload.
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	n.Folders["data"].Engine.SetPeerIndex(99, nil, false)

	// An open conflict event for a path with NO conflict copy on disk (stale).
	if _, err := n.Events.Record(&events.Event{TS: time.Now(), Folder: "data", Path: "ghost.txt",
		Category: events.CatConflict, Severity: events.SevWarn, Reason: "conflict copy created"}); err != nil {
		t.Fatal(err)
	}

	att, err := svc.Attention("data")
	if err != nil {
		t.Fatal(err)
	}
	// a.txt must be listed as a pending upload.
	var found *control.AttentionFile
	for i := range att {
		if att[i].Name == "a.txt" {
			found = &att[i]
			break
		}
	}
	if found == nil || !found.PendingUp {
		t.Fatalf("a.txt should be a pending upload: %+v", att)
	}
	// ghost.txt must not appear, and its conflict event must be resolved.
	for _, a := range att {
		if a.Name == "ghost.txt" {
			t.Fatalf("stale conflict should not appear in attention: %+v", a)
		}
	}
	evs, _ := n.Events.Query(events.Filter{Folder: "data", Path: "ghost.txt",
		Category: events.CatConflict, OpenOnly: true})
	if len(evs) != 0 {
		t.Fatalf("stale conflict event should be auto-resolved: %+v", evs)
	}
}

// TestFolderNotShared verifies the not-shared indicator: a folder is
// flagged when configured peers exist, all have been seen, and none of the
// allowed ones advertises the folder (e.g. id changed elsewhere or scoped
// to a peer that never added it).
func TestFolderNotShared(t *testing.T) {
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{
			{ID: "shared", Path: t.TempDir(), ConflictPolicy: "conflict-copy"},
			{ID: "orphan", Path: t.TempDir(), ConflictPolicy: "conflict-copy"},
		},
		Peers: []config.Peer{
			{ID: 2, Name: "nas-02", Addresses: []string{"h:1"}},
			{ID: 3, Name: "nas-03", Addresses: []string{"h:1"}},
		},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	// Both peers advertise "shared" only; neither has "orphan".
	n.RecordPeerFolders(2, map[string]bool{"shared": true})
	n.RecordPeerFolders(3, map[string]bool{"shared": true})

	svc := control.New(n, "test")
	byID := map[string]control.FolderStatus{}
	for _, f := range svc.Folders() {
		byID[f.ID] = f
	}
	if s := byID["shared"]; s.NotShared || s.SharedPeers != 2 {
		t.Fatalf("'shared' should be shared with 2 peers: %+v", s)
	}
	if o := byID["orphan"]; !o.NotShared || o.SharedPeers != 0 {
		t.Fatalf("'orphan' should be flagged not-shared: %+v", o)
	}
}

// TestRESTBrowse verifies the folder-picker endpoint lists directories
// (directories first, then files), returns the parent for navigation, and
// rejects invalid paths.
func TestRESTBrowse(t *testing.T) {
	n, _ := testNode(t)
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "sub-a", "deep"), 0o755)
	os.MkdirAll(filepath.Join(root, "sub-b"), 0o755)
	os.WriteFile(filepath.Join(root, "note.txt"), []byte("x"), 0o644)

	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	code, body := getJSON(t, srv.URL+"/api/browse?path="+url.QueryEscape(root))
	if code != 200 {
		t.Fatalf("status code %d: %s", code, body)
	}
	var res struct {
		Path    string `json:"path"`
		Parent  string `json:"parent"`
		Entries []struct {
			Name  string `json:"name"`
			IsDir bool   `json:"is_dir"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	if res.Path != filepath.Clean(root) {
		t.Fatalf("path = %q", res.Path)
	}
	// Dirs first: sub-a, sub-b, then the file.
	if len(res.Entries) != 3 || !res.Entries[0].IsDir || !res.Entries[1].IsDir || res.Entries[2].IsDir {
		t.Fatalf("expected dirs-first listing: %+v", res.Entries)
	}
	if res.Entries[0].Name != "sub-a" || res.Entries[1].Name != "sub-b" || res.Entries[2].Name != "note.txt" {
		t.Fatalf("unexpected entry order: %+v", res.Entries)
	}
	if res.Parent == "" {
		t.Fatal("parent should be non-empty for a non-root directory")
	}

	// Relative paths are rejected.
	if code, _ := getJSON(t, srv.URL+"/api/browse?path=relative"); code != 400 {
		t.Fatalf("relative path should be rejected, got %d", code)
	}
}

func TestRESTEventsAndAck(t *testing.T) {
	n, root := testNode(t)
	if err := os.WriteFile(filepath.Join(root, "y.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ScanFolder("data"); err != nil {
		t.Fatal(err)
	}
	// Force a warn-level conflict condition so the open count is non-zero.
	_, err := n.Events.Record(&events.Event{
		TS: time.Now(), Folder: "data", Path: "y.txt",
		Category: events.CatConflict, Severity: events.SevWarn, Reason: "test conflict",
	})
	if err != nil {
		t.Fatal(err)
	}

	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	code, body := getJSON(t, srv.URL+"/api/events?category=conflict")
	if code != 200 {
		t.Fatalf("events code %d: %s", code, body)
	}
	var evs []events.Event
	if err := json.Unmarshal(body, &evs); err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Category != events.CatConflict {
		t.Fatalf("unexpected events: %+v", evs)
	}
	id := evs[0].ID

	// Ack via POST.
	resp, err := http.Post(srv.URL+"/api/events/"+strconv.FormatInt(id, 10)+"/ack", "application/json",
		bytes.NewReader([]byte(`{"by":"tester"}`)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("ack status %d", resp.StatusCode)
	}
	// Open count should drop to 0.
	_, body = getJSON(t, srv.URL+"/api/status")
	var st struct {
		OpenEvents int64 `json:"open_events"`
	}
	_ = json.Unmarshal(body, &st)
	if st.OpenEvents != 0 {
		t.Fatalf("open events after ack = %d, want 0", st.OpenEvents)
	}
}

func TestRESTRescan(t *testing.T) {
	n, root := testNode(t)
	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	if err := os.WriteFile(filepath.Join(root, "new.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/api/rescan", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("rescan status %d: %s", resp.StatusCode, body)
	}
	var applied map[string]int
	if err := json.Unmarshal(body, &applied); err != nil {
		t.Fatal(err)
	}
	if applied["data"] != 1 {
		t.Fatalf("rescan applied = %+v, want data:1", applied)
	}
}

func TestRESTEventStream(t *testing.T) {
	n, root := testNode(t)
	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/events/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}

	// Trigger an event after subscribing.
	go func() {
		time.Sleep(200 * time.Millisecond)
		os.WriteFile(filepath.Join(root, "streamed.txt"), []byte("x"), 0o644)
		n.ScanFolder("data")
	}()

	br := bufio.NewReader(resp.Body)
	deadline := time.After(5 * time.Second)
	for {
		line, err := br.ReadString('\n')
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if strings.Contains(line, "event: event") {
			// Next line is data: <json>
			dataLine, err := br.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			data := strings.TrimPrefix(strings.TrimSpace(dataLine), "data:")
			var e events.Event
			if err := json.Unmarshal([]byte(data), &e); err != nil {
				t.Fatal(err)
			}
			if e.Path == "streamed.txt" {
				return // got the streamed event
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for streamed event")
		default:
		}
	}
}

func TestRESTUiServedAtRoot(t *testing.T) {
	n, _ := testNode(t)
	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("root status = %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "CrossSync") || !strings.Contains(string(body), "/api/events/stream") {
		t.Fatal("root should serve the embedded web UI")
	}
}

// TestRESTArchivePreviewAndArchive verifies the archive endpoints end to end,
// including the safety warning semantics: with include_open=false the
// preview reports kept_open > 0 and the actual archive preserves the
// unviewed attention event; with include_open=true it is deleted.
func TestRESTArchivePreviewAndArchive(t *testing.T) {
	n, _ := testNode(t)
	// Seed one old info event (deletable) and one old open warn attention
	// event (the "not dismissed" note the user asked us to warn about).
	old := time.Now().Add(-400 * 24 * time.Hour) // well beyond the 1-year cutoff
	if _, err := n.Events.Record(&events.Event{TS: old, Folder: "data", Path: "old.txt",
		Category: events.CatApplied, Severity: events.SevInfo, Reason: "old applied"}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Events.Record(&events.Event{TS: old, Folder: "data", Path: "old-conflict.txt",
		Category: events.CatConflict, Severity: events.SevWarn, Reason: "old conflict"}); err != nil {
		t.Fatal(err)
	}
	// Recent event must never be touched by a "1 year old" cutoff.
	if _, err := n.Events.Record(&events.Event{TS: time.Now(), Folder: "data", Path: "new.txt",
		Category: events.CatApplied, Severity: events.SevInfo, Reason: "new"}); err != nil {
		t.Fatal(err)
	}

	svc := control.New(n, "test")
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	// Preview: older_than = 1 year (31536000s), include_open=false.
	pre := srv.URL + "/api/events/archive_preview?older_than=31536000"
	resp, err := http.Get(pre)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("preview status %d: %s", resp.StatusCode, body)
	}
	var pv control.ArchiveResult
	if err := json.Unmarshal(body, &pv); err != nil {
		t.Fatal(err)
	}
	if pv.Deleted != 1 || pv.KeptOpen != 1 {
		t.Fatalf("preview = %+v, want Deleted=1 KeptOpen=1 (warning case)", pv)
	}
	if !strings.Contains(pv.CutoffText, "older than") {
		t.Fatalf("unexpected cutoff text: %q", pv.CutoffText)
	}

	// Smart mode (older_than=0): cutoff = oldest open warn+ event, which is
	// old-conflict.txt itself -> nothing older than it is deletable, and the
	// open warn event is preserved anyway.
	pre0 := srv.URL + "/api/events/archive_preview?older_than=0"
	resp, err = http.Get(pre0)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("smart preview status %d: %s", resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, &pv); err != nil {
		t.Fatal(err)
	}
	if pv.Deleted != 0 {
		t.Fatalf("smart preview should delete nothing: %+v", pv)
	}
	if !strings.Contains(pv.CutoffText, "oldest open attention") {
		t.Fatalf("smart preview cutoff text: %q", pv.CutoffText)
	}

	// Perform the archive with include_open=false: deletes only old.txt,
	// keeps the old open conflict event.
	payload, _ := json.Marshal(map[string]any{"older_than": 31536000, "include_open": false})
	resp, err = http.Post(srv.URL+"/api/events/archive", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("archive status %d: %s", resp.StatusCode, body)
	}
	var ar control.ArchiveResult
	if err := json.Unmarshal(body, &ar); err != nil {
		t.Fatal(err)
	}
	if ar.Deleted != 1 || ar.KeptOpen != 1 {
		t.Fatalf("archive = %+v, want Deleted=1 KeptOpen=1", ar)
	}

	// Confirm the attention event survived; then force-archive it.
	evs, err := n.Events.Query(events.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, e := range evs {
		found[e.Path] = true
	}
	if !found["old-conflict.txt"] || !found["new.txt"] || found["old.txt"] {
		t.Fatalf("unexpected survivors after safe archive: %v", found)
	}

	payload, _ = json.Marshal(map[string]any{"older_than": 31536000, "include_open": true})
	resp, err = http.Post(srv.URL+"/api/events/archive", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("forced archive status %d: %s", resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, &ar); err != nil {
		t.Fatal(err)
	}
	// DeletedOpen=1: the "not dismissed" attention note was permanently lost.
	if ar.Deleted != 1 || ar.DeletedOpen != 1 || ar.KeptOpen != 0 {
		t.Fatalf("forced archive = %+v, want Deleted=1 DeletedOpen=1 KeptOpen=0", ar)
	}
	evs, err = n.Events.Query(events.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 || evs[0].Path != "new.txt" {
		t.Fatalf("after forced archive: %+v", evs)
	}

	// Bad input is rejected.
	resp, err = http.Get(srv.URL + "/api/events/archive_preview?older_than=-5")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("negative older_than should be 400, got %d", resp.StatusCode)
	}
}

// TestRESTSetAutoArchive verifies the event-history auto-archive setting:
// it updates the live daemon config, is reflected in /api/status, is
// persisted to the config file, and rejects bad input.
func TestRESTSetAutoArchive(t *testing.T) {
	meta := t.TempDir()
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfgText := "device:\n    id: 1\n    name: nas-01\nmeta_dir: " + filepath.ToSlash(meta) +
		"\nfolders: []\npeers: []\ncontrol_listen: \"\"\n"
	if err := os.WriteFile(cfgPath, []byte(cfgText), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	// Enable with a 1-year age.
	payload, _ := json.Marshal(map[string]any{"enabled": true, "older_than": 31536000})
	req, _ := http.NewRequest("PUT", srv.URL+"/api/settings/auto_archive", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("set auto_archive status %d: %s", resp.StatusCode, body)
	}

	// /api/status reflects the effective setting.
	code, body := getJSON(t, srv.URL+"/api/status")
	if code != 200 {
		t.Fatalf("status code %d", code)
	}
	var st struct {
		AutoArchiveEnabled  bool  `json:"auto_archive_enabled"`
		AutoArchiveOlderThan int64 `json:"auto_archive_older_than"`
	}
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatal(err)
	}
	if !st.AutoArchiveEnabled || st.AutoArchiveOlderThan != 31536000 {
		t.Fatalf("status auto-archive = (%v, %d), want (true, 31536000)", st.AutoArchiveEnabled, st.AutoArchiveOlderThan)
	}

	// The config file was persisted (comments preserved is covered by the
	// folder persistence test; here we check the keys landed).
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "auto_archive_events: true") ||
		!strings.Contains(string(data), "auto_archive_older_than: 31536000") {
		t.Fatalf("config not persisted:\n%s", data)
	}

	// Negative age is rejected.
	bad, _ := json.Marshal(map[string]any{"enabled": true, "older_than": -5})
	req, _ = http.NewRequest("PUT", srv.URL+"/api/settings/auto_archive", bytes.NewReader(bad))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("negative older_than should be 400, got %d", resp.StatusCode)
	}
}

// TestRESTSetAlerts verifies the alert endpoint setting: it is persisted to
// the config, reflected in status, and the test-alert route posts through
// it.
func TestRESTSetAlerts(t *testing.T) {
	var got struct {
		Title   string `json:"title"`
		Message string `json:"message"`
	}
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer hook.Close()

	meta := t.TempDir()
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	cfgText := "device:\n    id: 1\n    name: nas-01\nmeta_dir: " + filepath.ToSlash(meta) +
		"\nfolders: []\npeers: []\ncontrol_listen: \"\"\n"
	if err := os.WriteFile(cfgPath, []byte(cfgText), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	svc := control.New(n, "test")
	svc.SetConfigPath(cfgPath)
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	// Set the alert URL.
	payload, _ := json.Marshal(map[string]any{"url": hook.URL})
	req, _ := http.NewRequest("PUT", srv.URL+"/api/settings/alerts", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("set alerts status %d", resp.StatusCode)
	}

	// Status reflects it.
	code, body := getJSON(t, srv.URL+"/api/status")
	if code != 200 {
		t.Fatalf("status code %d", code)
	}
	var st struct {
		AlertURL string `json:"alert_url"`
	}
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatal(err)
	}
	if st.AlertURL != hook.URL {
		t.Fatalf("status alert_url = %q, want %q", st.AlertURL, hook.URL)
	}

	// Config persisted.
	data, _ := os.ReadFile(cfgPath)
	if !strings.Contains(string(data), "alert_url: "+hook.URL) {
		t.Fatalf("alert_url not persisted:\n%s", data)
	}

	// Test alert posts through the endpoint.
	resp, err = http.Post(srv.URL+"/api/alerts/test", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("test alert status %d", resp.StatusCode)
	}
	if !strings.Contains(got.Message, "Test alert") {
		t.Fatalf("hook received %+v, want a test alert", got)
	}
}

