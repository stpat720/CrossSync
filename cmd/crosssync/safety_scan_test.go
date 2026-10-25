package main

import (
	"testing"
	"time"

	"crosssync/internal/config"
)

// TestSafetyIntervalBounds pins the full-walk schedule: it adapts to the cost of
// the walk (so a slow share is never walked back-to-back) and is bounded by the
// configured floor and ceiling, which is what keeps the safety net predictable
// for an operator.
func TestSafetyIntervalBounds(t *testing.T) {
	cfg := &config.Config{} // defaults: 3m floor, 15m ceiling

	cases := []struct {
		walk time.Duration
		want time.Duration
	}{
		{time.Second, 3 * time.Minute},      // 12s -> floor
		{20 * time.Second, 4 * time.Minute}, // 12x the walk
		{2 * time.Minute, 15 * time.Minute}, // 24m -> capped
	}
	for _, c := range cases {
		if got := safetyInterval(c.walk, cfg); got != c.want {
			t.Fatalf("safetyInterval(%s) = %s, want %s", c.walk, got, c.want)
		}
	}

	tuned := &config.Config{Scan: config.ScanTuning{FullInterval: "30s", FullMax: "5m"}}
	if got := safetyInterval(time.Second, tuned); got != 30*time.Second {
		t.Fatalf("tuned floor = %s, want 30s", got)
	}
	if got := safetyInterval(2*time.Minute, tuned); got != 5*time.Minute {
		t.Fatalf("tuned ceiling = %s, want 5m", got)
	}
}

// TestScanTuningDefaults: absent or unparsable values fall back to the defaults
// rather than disabling the safety net.
func TestScanTuningDefaults(t *testing.T) {
	var s config.ScanTuning
	if got := s.FullIntervalOrDefault(); got != 3*time.Minute {
		t.Fatalf("default floor = %s, want 3m", got)
	}
	if got := s.FullMaxOrDefault(); got != 15*time.Minute {
		t.Fatalf("default ceiling = %s, want 15m", got)
	}
	bad := config.ScanTuning{FullInterval: "nonsense", FullMax: "-5m"}
	if got := bad.FullIntervalOrDefault(); got != 3*time.Minute {
		t.Fatalf("unparsable floor = %s, want the 3m default", got)
	}
	if got := bad.FullMaxOrDefault(); got != 15*time.Minute {
		t.Fatalf("negative ceiling = %s, want the 15m default", got)
	}
}
