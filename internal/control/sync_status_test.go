package control

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

// TestStatusForClassifiesSyncFailures: a sync that cannot start because one is
// already running, a scan is in flight, or the folder is paused is a CONFLICT,
// not a server fault; an unreachable peer is 503 carrying the dial error. The
// handler used to answer 500 to all of them, so a routine overlap looked like a
// crash and the reason was buried in the body.
func TestStatusForClassifiesSyncFailures(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{fmt.Errorf("%w: a sync is already running; retry when it finishes", ErrBusy), http.StatusConflict},
		{fmt.Errorf("%w: scan already in progress for folder %q", ErrBusy, "x"), http.StatusConflict},
		{fmt.Errorf("%w: dial tcp 100.87.122.41:55557: connect: connection refused", ErrUnavailable), http.StatusServiceUnavailable},
		{errors.New(`unknown folder "nope"`), http.StatusBadRequest},
	}
	for _, c := range cases {
		if got := statusFor(c.err); got != c.want {
			t.Errorf("statusFor(%v) = %d, want %d", c.err, got, c.want)
		}
	}
}
