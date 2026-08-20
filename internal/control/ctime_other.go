//go:build !windows && !linux

package control

import (
	"os"
	"time"
)

// birthTime falls back to the modification time on platforms without an
// easily accessible creation timestamp.
func birthTime(info os.FileInfo) time.Time {
	return info.ModTime()
}
