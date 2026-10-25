//go:build linux

package node

import "syscall"

// freeBytes reports the free space of the filesystem holding path. It is used
// for the folders[].free_bytes field so an operator (and a load test) can see
// whether a corpus will fit BEFORE writing it — filling an Unraid array breaks
// unrelated services, so this is a safety number, not decoration.
func freeBytes(path string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0
	}
	// Bavail is what an unprivileged writer may use; Bsize is the block size.
	return int64(st.Bavail) * int64(st.Bsize)
}
