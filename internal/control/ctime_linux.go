//go:build linux

package control

import (
	"os"
	"syscall"
	"time"
)

// birthTime returns a creation-like timestamp on Linux (stat ctime — the
// time the inode was last changed, which for files written by atomic rename
// is when this server wrote them).
func birthTime(info os.FileInfo) time.Time {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return time.Unix(st.Ctim.Sec, st.Ctim.Nsec)
	}
	return info.ModTime()
}
