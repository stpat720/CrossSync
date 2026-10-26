package sync

import "testing"

// TestConflictOriginRoundTrip pins the inverse of conflictName. Conflict copies
// move the extension past the timestamp and device id, so cutting the name at
// the ".sync-conflict" marker loses it ("dir/a" instead of "dir/a.dng") and every
// consumer — the attention row, compare, resolve — then points at a path that
// does not exist.
func TestConflictOriginRoundTrip(t *testing.T) {
	cases := []struct{ live, copy string }{
		{"dir/a.dng", conflictName("dir/a.dng", 1793000000, 0, 42)},
		{"noext", conflictName("noext", 1793000000, 0, 42)},
		{"deep/path/My Report.final.docx", conflictName("deep/path/My Report.final.docx", 1793000000, 0, 7)},
		{"archive.tar.gz", conflictName("archive.tar.gz", 1793000000, 0, 9)},
	}
	for _, c := range cases {
		got, ok := ConflictOrigin(c.copy)
		if !ok {
			t.Fatalf("ConflictOrigin(%q) reported no origin", c.copy)
		}
		if got != c.live {
			t.Fatalf("ConflictOrigin(%q) = %q, want %q", c.copy, got, c.live)
		}
	}
	if _, ok := ConflictOrigin("dir/a.dng"); ok {
		t.Fatalf("a name without the marker must not yield an origin")
	}
	if _, ok := ConflictOrigin(ConflictSuffix + "-20261026-101500-1.dng"); ok {
		t.Fatalf("a marker at offset 0 has no original path")
	}
}
