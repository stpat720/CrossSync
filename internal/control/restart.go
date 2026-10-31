package control

import (
	"os"
	"time"
)

// Restart replaces the running daemon with itself, without touching the binary.
//
// It exists because the only way to restart used to be /api/update/apply — i.e.
// downloading and reinstalling the same version. Twice in one debugging session
// the fix for a wedged link was "reinstall 0.1.x", which is a ridiculous thing to
// ask of an operator (and needs the update server to be reachable exactly when
// the daemon is misbehaving).
//
// The response is written first: the caller gets 202 and the re-exec happens a
// moment later, so the HTTP reply is not cut off by the exec. On unix the exec
// replaces the process (same PID, same container); on Windows a new process is
// spawned and the old one exits.
func (s *Service) Restart() {
	go func() {
		time.Sleep(300 * time.Millisecond)
		exe, err := os.Executable()
		if err != nil {
			s.n.Logf("restart: cannot locate own binary: %v", err)
			return
		}
		s.n.Logf("restart: requested by the control API; re-executing %s", exe)
		// restartExec only returns on failure: on unix exec replaces this process,
		// on windows a new one is spawned and this one exits. Anything that reaches
		// the next line is a failed restart, and the daemon keeps running.
		err = restartExec(exe, os.Args)
		s.n.Logf("restart: re-exec did not take over, still running: %v", err)
	}()
}
