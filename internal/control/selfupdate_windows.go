//go:build windows

package control

import (
	"os"
	"syscall"
)

// restartExec spawns the freshly installed binary and exits the current
// process. Windows cannot rename over a running executable, so the target
// path (from update_dir or the config dir) normally differs from the
// running binary; if it does not, the rename in CommitUpdate fails first
// and the running daemon is left untouched.
func restartExec(path string, args []string) error {
	argv := append([]string{path}, args[1:]...)
	proc, err := os.StartProcess(path, argv, &os.ProcAttr{
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		Env:   os.Environ(),
		Sys:   &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP},
	})
	if err != nil {
		return err
	}
	_ = proc.Release()
	os.Exit(0)
	return nil
}
