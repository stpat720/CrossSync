//go:build !linux && !windows

package node

// freeBytes is unavailable on this platform; the status field reports 0 rather
// than a number nobody should trust.
func freeBytes(string) int64 { return 0 }
