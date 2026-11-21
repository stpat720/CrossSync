package control

import (
	"net/http"
	"runtime"
)

// handleGoroutines dumps every goroutine's stack as plain text.
//
// It exists because of the one failure this daemon keeps producing: a box that
// looks busy while the disks are idle, a /api/status that never answers, and no
// way from outside the process to see WHICH lock everything is queued behind.
// The alternative is SIGQUIT, which kills the daemon and takes a resumable scan
// with it.
//
// It touches runtime state only: no locks, no SQL, no filesystem, no index
// walk, so it answers in the exact situation it exists for. That is also why it
// is a GET: it reads and returns, and changes nothing.
func (s *Service) handleGoroutines(w http.ResponseWriter, r *http.Request) {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		// Truncated: the dump itself filled the buffer, so grow it and retry.
		buf = make([]byte, 2*len(buf))
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf)
}
