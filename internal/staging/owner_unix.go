//go:build !windows

package staging

import (
	"os"
	"syscall"
)

// OwnerOf reports the uid/gid that owns path. ok is false when the platform or
// filesystem does not expose POSIX ownership (Windows, some FUSE mounts).
func OwnerOf(path string) (uid, gid int, ok bool) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, 0, false
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	return int(sys.Uid), int(sys.Gid), true
}
