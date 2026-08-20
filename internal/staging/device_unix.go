//go:build !windows

package staging

import (
	"os"
	"syscall"
)

// sameDevice reports whether the two paths live on the same filesystem.
// On Unix we compare st_dev; the EXDEV fallback covers any mismatch.
func sameDevice(a, b string) bool {
	ai, err := os.Stat(a)
	if err != nil {
		return false
	}
	bi, err := os.Stat(b)
	if err != nil {
		return false
	}
	as, ok1 := ai.Sys().(*syscall.Stat_t)
	bs, ok2 := bi.Sys().(*syscall.Stat_t)
	if !ok1 || !ok2 {
		return false
	}
	return as.Dev == bs.Dev
}
