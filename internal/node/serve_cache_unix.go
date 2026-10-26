//go:build !windows

package node

// serveCacheEnabled is false on Windows: a handle held open there blocks
// os.Remove and os.Rename of the same path (Go opens without FILE_SHARE_DELETE),
// so caching source files would break the daemon's own deletes, renames and
// archives. The per-open cost this cache removes is a shfs (Linux) problem —
// production runs on Unraid — so Windows keeps the open-per-block path.
const serveCacheEnabledByBuild = true
