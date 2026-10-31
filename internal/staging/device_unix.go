//go:build !windows

package staging

import (
	"os"
	"path/filepath"
	"syscall"
)

// existingStat stats p or, when p does not exist yet, its nearest existing
// ancestor. The staging temp dir is created lazily, so on a fresh folder it does
// not exist when TempPathFor asks whether the target and the temp share a device:
// a plain Stat failed, the answer was "different device", and every first pull
// silently staged into the target directory instead of the central temp area
// (which is where orphaned temps are swept from).
func existingStat(p string) (os.FileInfo, error) {
	for {
		fi, err := os.Stat(p)
		if err == nil {
			return fi, nil
		}
		parent := filepath.Dir(p)
		if parent == p {
			return nil, err
		}
		p = parent
	}
}

// sameDevice reports whether two paths live on the same filesystem, resolving
// through not-yet-created paths.
func sameDevice(a, b string) bool {
	ai, err := existingStat(a)
	if err != nil {
		return false
	}
	bi, err := existingStat(b)
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
