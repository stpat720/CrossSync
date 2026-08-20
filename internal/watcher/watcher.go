// Package watcher implements filesystem change detection via fsnotify
// (inotify on Linux, ReadDirectoryChangesW on Windows). It watches every
// directory under a folder root, filters out our own reserved namespaces
// (staging temps, version archives — self-trigger suppression), and emits
// debounced, deduplicated batches of changed relative paths.
package watcher

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Debounce is how long events are accumulated before a batch is emitted.
const Debounce = 250 * time.Millisecond

// Watcher watches one folder root.
type Watcher struct {
	Root string
	Fs   *fsnotify.Watcher
	logf func(format string, args ...any)

	changes chan []string

	mu      sync.Mutex
	pending map[string]bool
	timer   *time.Timer
	watches int

	stop chan struct{}
	done chan struct{}
}

// New creates a watcher for root. Call Start to begin watching.
func New(root string, logf func(format string, args ...any)) (*Watcher, error) {
	fs, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Watcher{
		Root:    root,
		Fs:      fs,
		logf:    logf,
		changes: make(chan []string, 8),
		pending: map[string]bool{},
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}, nil
}

// Start walks the tree (skipping reserved namespaces) and adds a watch to
// every directory, then begins pumping events.
func (w *Watcher) Start() error {
	if err := w.addTree(w.Root); err != nil {
		return err
	}
	go w.pump()
	return nil
}

// addTree recursively adds a watch to every directory under dir, skipping
// reserved namespaces (our own staging/versioning areas).
func (w *Watcher) addTree(dir string) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(w.Root, p)
		if rerr == nil && isReserved(rel) {
			return filepath.SkipDir
		}
		if rerr := w.Fs.Add(p); rerr != nil {
			w.logf("watcher: add %s: %v", p, rerr)
			return nil
		}
		w.mu.Lock()
		w.watches++
		w.mu.Unlock()
		return nil
	})
}

// Changes returns a channel of debounced, deduplicated batches of changed
// relative paths (slash-separated). The consumer should trigger a scan per
// batch.
func (w *Watcher) Changes() <-chan []string { return w.changes }

// WatchCount returns the number of directories currently watched.
func (w *Watcher) WatchCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.watches
}

// Close stops the watcher.
func (w *Watcher) Close() error {
	close(w.stop)
	<-w.done
	return w.Fs.Close()
}

func (w *Watcher) pump() {
	defer close(w.done)
	for {
		select {
		case ev, ok := <-w.Fs.Events:
			if !ok {
				return
			}
			w.handle(ev)
		case err, ok := <-w.Fs.Errors:
			if !ok {
				return
			}
			w.logf("watcher: %v", err)
		case <-w.stop:
			return
		}
	}
}

func (w *Watcher) handle(ev fsnotify.Event) {
	rel, err := filepath.Rel(w.Root, ev.Name)
	if err != nil {
		return
	}
	rel = filepath.ToSlash(rel)
	if isReserved(rel) {
		// Self-trigger suppression: our own staging/version writes never
		// schedule a rescan.
		return
	}
	if ev.Op&(fsnotify.Create) != 0 {
		if st, err := os.Stat(ev.Name); err == nil && st.IsDir() && !isReserved(rel) {
			_ = w.Fs.Add(ev.Name)
			w.mu.Lock()
			w.watches++
			w.mu.Unlock()
		}
	}
	if ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		// fsnotify removes watches automatically on delete; on rename the
		// watch survives and must be dropped, otherwise stale.
		_ = w.Fs.Remove(ev.Name)
	}
	w.mu.Lock()
	w.pending[rel] = true
	if w.timer == nil {
		w.timer = time.AfterFunc(Debounce, w.flush)
	}
	w.mu.Unlock()
}

func (w *Watcher) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	if len(w.pending) == 0 {
		return
	}
	keys := make([]string, 0, len(w.pending))
	for k := range w.pending {
		keys = append(keys, k)
	}
	w.pending = map[string]bool{}
	sort.Strings(keys)
	select {
	case w.changes <- keys:
	default:
		// Consumer busy; drop rather than grow unbounded. The periodic
		// rescan is the safety net that covers dropped batches.
		w.logf("watcher: dropped change batch (consumer busy)")
	}
}

// isReserved reports whether a relative path is in our reserved namespace:
// any `.sfx-*` entry (staging dir, version archives, temp files). These are
// produced by our own writes and must never trigger a rescan.
func isReserved(rel string) bool {
	parts := strings.Split(rel, "/")
	for _, p := range parts {
		if strings.HasPrefix(p, ".sfx-") {
			return true
		}
	}
	return false
}
