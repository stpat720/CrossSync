// Package ui embeds the CrossSync web UI (a self-contained single-page
// app — no build step, no CDN) and serves it from the control plane so the
// daemon binary is the only artifact needed on Unraid.
//
// The UI can be overridden at runtime by dropping an index.html into a
// configured ui_dir (e.g. a bind-mounted appdata share in Docker). The
// embedded copy is used whenever no override file exists, so updating the
// UI never requires rebuilding the binary.
package ui

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

//go:embed index.html
var content embed.FS

// The version marker is emitted as a <meta> tag in the HTML head. The
// version is parsed at runtime so the embedded and any fetched UI report
// their own version without a second source of truth.
const versionMeta = `name="crosssync-ui-version"`

var versionRe = regexp.MustCompile(`crosssync-ui-version"\s+content="([0-9]+\.[0-9]+\.[0-9]+)"`)

// Version returns the version of the embedded UI.
func Version() string {
	b, err := content.ReadFile("index.html")
	if err != nil {
		return "0.0.0"
	}
	return VersionFrom(b)
}

// VersionFrom extracts the UI version from an index.html payload. It
// returns "" when the payload is not a CrossSync UI.
func VersionFrom(b []byte) string {
	if !strings.Contains(string(b), versionMeta) {
		return ""
	}
	m := versionRe.FindSubmatch(b)
	if len(m) != 2 {
		return ""
	}
	return string(m[1])
}

// OverridePath returns the on-disk override file (dir/index.html), or ""
// when dir is empty.
func OverridePath(dir string) string {
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "index.html")
}

// ReadOverride returns the override UI file if present.
func ReadOverride(dir string) ([]byte, bool, error) {
	p := OverridePath(dir)
	if p == "" {
		return nil, false, nil
	}
	b, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return b, true, nil
}

// CurrentVersion returns the version of the UI that would currently be
// served (override file wins over the embedded copy).
func CurrentVersion(dir string) string {
	if b, ok, err := ReadOverride(dir); err == nil && ok {
		if v := VersionFrom(b); v != "" {
			return v
		}
	}
	return Version()
}

// WriteOverride atomically writes a new UI file into dir, validating that
// it looks like a CrossSync UI (carries the version marker).
func WriteOverride(dir, payload string) (string, error) {
	v := VersionFrom([]byte(payload))
	if v == "" {
		return "", fmt.Errorf("refusing UI update: payload is not a CrossSync UI (missing version marker)")
	}
	if dir == "" {
		return "", fmt.Errorf("ui_dir is not configured; cannot persist an override UI")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("ui_dir: %w", err)
	}
	p := OverridePath(dir)
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, []byte(payload), 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return v, nil
}

// Handler serves the UI: the override file from disk when present,
// otherwise the embedded copy. The override file is read per request so
// dropping a new index.html into ui_dir takes effect immediately.
func Handler(dir string) http.Handler {
	embedded := func(w http.ResponseWriter, r *http.Request) {
		sub, err := fs.Sub(content, ".")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.FileServer(http.FS(sub)).ServeHTTP(w, r)
	}
	if dir == "" {
		return http.HandlerFunc(embedded)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "" || r.URL.Path == "/index.html" {
			p := OverridePath(dir)
			if b, err := os.ReadFile(p); err == nil && len(b) > 0 {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.Header().Set("Cache-Control", "no-cache")
				_, _ = w.Write(b)
				return
			}
		}
		embedded(w, r)
	})
}

// ParseVersion compares dotted numeric versions like "0.1.0". It returns
// a > 0 when a is newer, < 0 when older, 0 when equal (any unparsable
// component is treated as 0).
func ParseVersion(s string) (major, minor, patch int) {
	parts := strings.SplitN(s, ".", 3)
	for i, p := range parts {
		n, _ := strconv.Atoi(strings.TrimSpace(p))
		switch i {
		case 0:
			major = n
		case 1:
			minor = n
		case 2:
			patch = n
		}
	}
	return
}
