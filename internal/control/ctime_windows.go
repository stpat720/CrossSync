//go:build windows

package control

import (
	"os"
	"syscall"
	"time"
)

// birthTime returns the file's creation time on Windows.
func birthTime(info os.FileInfo) time.Time {
	if st, ok := info.Sys().(*syscall.Win32FileAttributeData); ok {
		return time.Unix(0, st.CreationTime.Nanoseconds())
	}
	return info.ModTime()
}
