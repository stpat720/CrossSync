package ui

import (
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestServesIndex(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	Handler("").ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	s := string(body)
	for _, want := range []string{"CrossSync", "/api/status", "/api/events/stream", "Add folder", "Event history", "settingsModal"} {
		if !strings.Contains(s, want) {
			t.Fatalf("index.html missing %q", want)
		}
	}
}

func TestOverrideDirTakesPrecedence(t *testing.T) {
	dir := t.TempDir()
	override := `<!DOCTYPE html><html><head><meta name="crosssync-ui-version" content="9.9.9"></head><body>OVERRIDE-UI</body></html>`
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(override), 0o644); err != nil {
		t.Fatal(err)
	}
	if v := CurrentVersion(dir); v != "9.9.9" {
		t.Fatalf("CurrentVersion = %q, want 9.9.9", v)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	Handler(dir).ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "OVERRIDE-UI") {
		t.Fatalf("override UI not served: %s", body)
	}
	// Dropping the override file falls back to the embedded UI.
	if err := os.Remove(filepath.Join(dir, "index.html")); err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	Handler(dir).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	body, _ = io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "CrossSync") {
		t.Fatalf("embedded fallback failed")
	}
}

func TestWriteOverrideValidates(t *testing.T) {
	dir := t.TempDir()
	good := `<!DOCTYPE html><html><head><meta name="crosssync-ui-version" content="1.2.3"></head><body>x</body></html>`
	v, err := WriteOverride(dir, good)
	if err != nil || v != "1.2.3" {
		t.Fatalf("WriteOverride = %q, %v", v, err)
	}
	if _, err := WriteOverride(dir, "<html>not a ui</html>"); err == nil {
		t.Fatal("WriteOverride accepted a non-CrossSync payload")
	}
	if v := Version(); v == "" {
		t.Fatal("embedded UI should report a version")
	}
}

// TestEventTabsShowOnlyOpenCounts pins the events-tab rule: every category tab
// (including "all") displays the number of OPEN warn+ events in the current
// range — a lifetime total only grows as history accumulates — and the number
// is omitted entirely when nothing needs attention. The attention tab keeps its
// all-time deduped condition count, and the header still reports the total in
// range.
func TestEventTabsShowOnlyOpenCounts(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler("").ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	body, _ := io.ReadAll(rec.Body)
	s := string(body)

	for _, want := range []string{
		// the displayed number comes from the open counts, never the totals
		`const n = c === "__attention" ? attn.size : (c === "all" ? openAll : (openByCat[c]||0));`,
		// ... and 0 is not displayed at all
		"if (n > 0) {",
		// openAll is the in-range sum over categories
		"openAll++;",
		// the in-range total stays in the header line
		`eventsCache.length + " events · "`,
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("index.html missing %q", want)
		}
	}
	// The old lifetime-total wording must be gone from the tab builder.
	if strings.Contains(s, `(c === "all" ? eventsCache.length : counts[c])`) {
		t.Fatal("event tabs still display the in-range total instead of the open count")
	}
}

// TestUISelectorsAreAnchored pins the rule that every static $("...") selector
// starts with #, . or [ . `$` is document.querySelector, so a bare tag name
// matches nothing and the .style access that follows throws "Cannot read
// properties of null (reading 'style')". That is not a cosmetic failure: the
// render aborts mid-function and the dashboard reports the TypeError as
// "Cannot reach CrossSync API", i.e. a typo looks like a dead daemon.
func TestUISelectorsAreAnchored(t *testing.T) {
	src, err := os.ReadFile("index.html")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`\$\("([^"]*)"\)`)
	for _, m := range re.FindAllStringSubmatch(string(src), -1) {
		sel := m[1]
		if sel == "" {
			t.Fatalf("empty selector %q", m[0])
		}
		switch sel[0] {
		case '#', '.', '[':
			continue
		}
		t.Fatalf("selector %q must be anchored with #, . or [: querySelector returns null, and the next .style/.textContent access throws", m[0])
	}
}

// readIndex returns index.html as a string, for the source-level UI pins below.
func readIndex(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("index.html")
	if err != nil {
		t.Fatal(err)
	}
	return string(src)
}

// funcBody returns the source of the function whose declaration starts with sig,
// up to its closing brace at column 0.
func funcBody(t *testing.T, src, sig string) string {
	t.Helper()
	i := strings.Index(src, sig)
	if i < 0 {
		t.Fatalf("index.html has no %q", sig)
	}
	j := strings.Index(src[i:], "\n}")
	if j < 0 {
		t.Fatalf("%q is unterminated", sig)
	}
	return src[i : i+j]
}

// TestUIPendingLinePublishesWhatIsKnowable pins the pending-line rule.
//
// Measured failure it fixes: during a 74-minute sync round EVERY folder on BOTH
// nodes rendered "awaiting sync" — including folders that were idle and in sync —
// because the daemon gated the download figure on the same flat rule as the
// upload figure. A row must now show the download count with its age
// ("↓ 146 · as of 12m ago") whenever that count is knowable, keep the upload
// count out when only that one is gated, and reserve "awaiting sync" for the
// cases where the download number really cannot be known.
func TestUIPendingLinePublishesWhatIsKnowable(t *testing.T) {
	src := readIndex(t)
	body := funcBody(t, src, "function pendingCell(f) {")
	for _, want := range []string{
		"f.pending_verified !== false",        // download figure: the one that is published
		"f.pending_up_verified !== undefined", // upload figure, gated separately
		"as of ${fmtTime(f.pending_as_of)}",   // the age, in the existing "· as of …" idiom
		"Pending: <b>↓ ${f.pending_files||0}",
		"Pending: <b>awaiting sync</b>",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("pendingCell is missing %q:\n%s", want, body)
		}
	}
	// "awaiting sync" must be reachable only from the genuinely-unknown branch:
	// it is inside `if (!downKnown)`, and the number rendering follows it.
	iGuard := strings.Index(body, "if (!downKnown)")
	iAwait := strings.Index(body, "Pending: <b>awaiting sync</b>")
	iNum := strings.Index(body, "Pending: <b>↓ ")
	if iGuard < 0 || iAwait < iGuard || iNum < iAwait {
		t.Fatalf("awaiting sync is not confined to the unknown-download branch (guard=%d await=%d number=%d)", iGuard, iAwait, iNum)
	}
	// The tooltip must name the reason the daemon withheld the figure.
	if !strings.Contains(body, "${esc(f.pending_reason||'unknown')}") {
		t.Fatal("the awaiting-sync tooltip does not name pending_reason, so it cannot say which case it is")
	}
	// The upload count is rendered only when it is verified.
	if !strings.Contains(body, `upKnown ? " / ↑ " + (f.pending_up||0) : ""`) {
		t.Fatal("the upload count is not gated on pending_up_verified")
	}
	// The row signature must cover both flags, or a change that only affects one
	// of them would not re-render the row.
	sig := funcBody(t, src, "function foldersSig() {")
	for _, want := range []string{"f.pending_verified", "f.pending_up_verified", "pendingAgeMin(f)"} {
		if !strings.Contains(sig, want) {
			t.Fatalf("foldersSig is missing %q, so a pending-flag change would not re-render the row", want)
		}
	}
}

// TestUIStatusCallsDoNotStack pins the anti-stacking guards. /api/status took
// 5.8-8.3 s on Tower (0.28 s on NAS-02 for the identical call) against a 2 s
// poll interval, so an unguarded async body queued a new request every 2 s and
// the dashboard painted whichever response landed last.
func TestUIStatusCallsDoNotStack(t *testing.T) {
	src := readIndex(t)
	if !strings.Contains(src, "let pollBusy = false;") || !strings.Contains(src, "let statusBusy = false;") {
		t.Fatal("the poll/status in-flight guards are gone")
	}
	poll := funcBody(t, src, "async function poll() {")
	if !strings.Contains(poll, "if (pollBusy) return;") || !strings.Contains(poll, "finally { pollBusy = false; }") {
		t.Fatal("poll() no longer skips a tick while a status call is in flight")
	}
	if !strings.Contains(poll, "pollBusy = true;") {
		t.Fatal("poll() never sets pollBusy")
	}
	load := funcBody(t, src, "async function loadStatus() {")
	if !strings.Contains(load, "if (statusBusy) return statusPending;") {
		t.Fatal("loadStatus() no longer coalesces onto the call already in flight")
	}
	if !strings.Contains(load, "finally { statusBusy = false; statusPending = null; }") {
		t.Fatal("loadStatus() leaves the guard set, which would disable it forever")
	}
}

// TestUIEODismissIsOptimistic pins that a dismiss click changes the row and the
// badge immediately instead of after a status round-trip (measured: ~7.5 s).
func TestUIDismissIsOptimistic(t *testing.T) {
	src := readIndex(t)
	for _, sig := range []string{"async function ack(id) {", "async function ackCondition(folder, path, category) {"} {
		body := funcBody(t, src, sig)
		if !strings.Contains(body, "dismissLocally(") {
			t.Fatalf("%s does not apply the dismissal locally", sig)
		}
		if strings.Contains(body, "await loadStatus()") || strings.Contains(body, "await loadEvents()") {
			t.Fatalf("%s still blocks the click on a status/events refresh", sig)
		}
		if !strings.Contains(body, "refreshSoon()") {
			t.Fatalf("%s never reconciles with the server in the background", sig)
		}
		if !strings.Contains(body, "undo()") {
			t.Fatalf("%s does not put the rows back when the request fails", sig)
		}
		// The optimistic write must land before the request is awaited.
		if strings.Index(body, "dismissLocally(") > strings.Index(body, "await api(") {
			t.Fatalf("%s dismisses only after the round-trip", sig)
		}
	}
	dismiss := funcBody(t, src, "function dismissLocally(match) {")
	for _, want := range []string{"openCache", "eventsCache", "renderTabs()", "renderTiles()", "syncOpenBadges()", "return function undo()"} {
		if !strings.Contains(dismiss, want) {
			t.Fatalf("dismissLocally is missing %q", want)
		}
	}
}

// TestUITransferRateIsWindowed pins the 5 s window / EMA / peak / hold rule.
// The old sampler divided a byte delta by the latency of the previous status
// call, so the same transfer read differently after every poll.
func TestUITransferRateIsWindowed(t *testing.T) {
	src := readIndex(t)
	for _, want := range []string{
		"const RATE_WINDOW_MS = 12000;",
		"const RATE_MIN_SPAN_MS = 5000;", // the "at least 5 s old" baseline
		"const RATE_HOLD_MS = 10000;",
		"const RATE_EMA = 0.3;",
		"performance.now()",  // monotonic time, not Date.now()
		"peakDown", "peakUp", // the window peak for the tooltip
	} {
		if !strings.Contains(src, want) {
			t.Fatalf("index.html is missing %q", want)
		}
	}
	// The counter-decrease reset (a daemon restart) must be there.
	sample := funcBody(t, src, "function sampleRate(key, up, down) {")
	if !strings.Contains(sample, "up < last.up || down < last.down") {
		t.Fatal("sampleRate does not reset the window when the counters go backwards (daemon restart)")
	}
	if !strings.Contains(sample, "if (t - s.t >= RATE_MIN_SPAN_MS) base = s; else break;") {
		t.Fatal("sampleRate does not measure the slope from the oldest sample at least 5 s old")
	}
	// The old instant-rate sampler must be gone entirely.
	for _, gone := range []string{"prevBytes", "prevFolders", "if (dt < 0.8) return;"} {
		if strings.Contains(src, gone) {
			t.Fatalf("the old instant-rate sampler is still present (%q)", gone)
		}
	}
	// The DOM ids/classes the rest of the page depends on are unchanged.
	for _, keep := range []string{`id="dDown"`, `id="dUp"`, `[data-rate="${CSS.escape(id)}"]`} {
		if !strings.Contains(src, keep) {
			t.Fatalf("index.html lost %q", keep)
		}
	}
}
