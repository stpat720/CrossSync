//go:build !windows

package control

import (
	"os"
	"syscall"
)

// restartExec replaces the current process with the freshly installed
// binary at path, preserving PID, arguments and environment. On unix,
// renaming over a running binary is allowed, so the swap in CommitUpdate
// already happened before this is called. If exec fails, the error is
// returned and the (old) daemon keeps running — the next restart uses the
// new binary.
func restartExec(path string, args []string) error {
	if err := os.Chmod(path, 0o755); err != nil {
		return err
	}
	argv := append([]string{path}, args[1:]...)
	return syscall.Exec(path, argv, os.Environ())
}
