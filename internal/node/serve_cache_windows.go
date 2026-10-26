//go:build windows

package node

// serveCacheEnabled is off on Windows: holding a file open there blocks
// os.Remove/os.Rename of the same path (Go opens without FILE_SHARE_DELETE), so
// a cache would break the daemon's own deletes, renames and archives — worse
// than the per-block open it saves. The cost it removes is a shfs (Linux)
// problem; production is Unraid. See serve_cache.go.
const serveCacheEnabledByBuild = false
