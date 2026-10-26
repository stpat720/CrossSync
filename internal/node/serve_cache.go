package node

import (
	"os"
	"sync"
)

// Serve-side handle cache.
//
// serveBlock used to do os.Open + ReadAt + Close for EVERY block request. That
// is invisible on a local SSD and ruinous on Unraid's shfs: an open is a FUSE
// round trip and a pull requests one block at a time, so a 64 MiB file (512
// blocks of 128 KiB) paid 512 opens. Measured on the live pair: ~66 ms per
// block = 1.97 MB/s, while the documented 43 MB/s for a 1 GiB file was the same
// per-open cost amortised over 1 MiB blocks. The disk was never the limit — the
// open call was.
//
// A pull hammers ONE file with consecutive block requests (up to
// pull_concurrency files interleaved), so a small cache of open handles turns
// those 512 opens into a handful. The cache is per node (serveBlock can run on
// two peer sessions at once), keyed by absolute path, and every hit is
// revalidated:
//
//   - os.Stat(abs) must still succeed — a deleted or renamed path must not be
//     served from a stale handle (the puller reads RespNoSuchFile as a
//     converging delete),
//   - the handle's own fstat must describe the SAME file (os.SameFile), so a
//     rewrite-over (editor save, rsync) is never served from the old inode,
//   - (size, mtime) must be unchanged since the handle was opened.
//
// Two stats are far cheaper than one open on shfs, and the kernel caches the
// path lookup, so a sequential pull becomes: stat, fstat, pread.
const maxServeHandles = 32

// serveOpenFile is a seam for tests: it is what the cache calls to open a path.
var serveOpenFile = os.Open

// serveCacheEnabled is the platform default, restated as a variable so tests
// can exercise the caching path on a platform where it is off (Windows).
var serveCacheEnabled = serveCacheEnabledByBuild

type serveHandle struct {
	f  *os.File
	st os.FileInfo // stat at open time, for the revalidation above
}

type serveHandleCache struct {
	mu     sync.Mutex
	byPath map[string]*serveHandle
}

func newServeHandleCache() *serveHandleCache {
	return &serveHandleCache{byPath: map[string]*serveHandle{}}
}

// readBlock fills buf from abs at off, reusing a cached handle when one is
// provably the same file at the same version. The handle's lifetime stays
// inside the cache: where caching is disabled (serve_cache_windows.go) it is
// opened and closed per call, exactly as before.
func (c *serveHandleCache) readBlock(abs string, buf []byte, off int64) (int, error) {
	if !serveCacheEnabled {
		f, err := serveOpenFile(abs)
		if err != nil {
			return 0, err
		}
		defer func() { _ = f.Close() }()
		return f.ReadAt(buf, off)
	}
	f, err := c.open(abs)
	if err != nil {
		return 0, err
	}
	return f.ReadAt(buf, off)
}

// open returns a handle for abs, reusing a cached one when it is provably the
// same file at the same version.
func (c *serveHandleCache) open(abs string) (*os.File, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if h, ok := c.byPath[abs]; ok {
		if c.validLocked(abs, h) {
			return h.f, nil
		}
		// Stale in any way: drop it and reopen below.
		_ = h.f.Close()
		delete(c.byPath, abs)
	}
	f, err := serveOpenFile(abs)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if len(c.byPath) >= maxServeHandles {
		c.evictOneLocked()
	}
	c.byPath[abs] = &serveHandle{f: f, st: st}
	return f, nil
}

// validLocked reports whether the cached handle is still the right file at the
// right version. Called with the lock held.
func (c *serveHandleCache) validLocked(abs string, h *serveHandle) bool {
	pst, err := os.Stat(abs)
	if err != nil {
		return false
	}
	hst, err := h.f.Stat()
	if err != nil {
		return false
	}
	if !os.SameFile(pst, hst) {
		return false // the path now names a different file
	}
	if pst.Size() != h.st.Size() || !pst.ModTime().Equal(h.st.ModTime()) {
		return false // rewritten in place
	}
	return true
}

// evictOneLocked drops and closes one handle to stay under the cap. The map is
// unordered, so "oldest" is approximated by whatever the runtime hands back
// first: the cap is 32 against a handful of live pulls, so a real LRU would buy
// nothing.
func (c *serveHandleCache) evictOneLocked() {
	for path, h := range c.byPath {
		_ = h.f.Close()
		delete(c.byPath, path)
		return
	}
}

// forget closes every cached handle whose path no longer resolves, or all of
// them when all is true (node shutdown).
func (c *serveHandleCache) forget(all bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for path, h := range c.byPath {
		if all {
			_ = h.f.Close()
			delete(c.byPath, path)
			continue
		}
		if _, err := os.Stat(path); err != nil {
			_ = h.f.Close()
			delete(c.byPath, path)
		}
	}
}

// len reports how many handles are cached (tests and diagnostics).
func (c *serveHandleCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.byPath)
}

// closeServeHandles releases every cached handle so a shutdown never leaves the
// source files open.
func (n *Node) closeServeHandles() {
	if n.serveCache != nil {
		n.serveCache.forget(true)
	}
}
