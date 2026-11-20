package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// pull_fsync is a DURABILITY knob and it is a pointer for exactly one reason: an
// absent key must mean ON. A plain bool would make every config written before
// the knob existed — and every config that simply omits it — silently stop
// fsyncing pulled files, which is the one failure this setting must never have.
//
// These tests pin all three shapes: absent (nil) = on, explicit false = off,
// explicit true = on, and a YAML round trip that never turns it off by omission.
func TestPullFsyncDefaultsOnWhenAbsent(t *testing.T) {
	if got := (Folder{ID: "a", Path: "/x"}).PullFsyncOn(); !got {
		t.Fatal("an absent pull_fsync must mean ON: a zero value must never disable durability")
	}
	off, on := false, true
	for _, tc := range []struct {
		row  *bool
		want bool
	}{
		{nil, true},
		{&off, false},
		{&on, true},
	} {
		f := Folder{ID: "a", Path: "/x", PullFsync: tc.row}
		if got := f.PullFsyncOn(); got != tc.want {
			t.Fatalf("pull_fsync %v = %v, want %v", tc.row, got, tc.want)
		}
	}
}

func TestLoadFolderPullFsync(t *testing.T) {
	p := writeTemp(t, `
device:
  id: 1
  name: nas-01
meta_dir: /tmp/m
folders:
  - id: legacy
    path: /l
  - id: fast
    path: /f
    pull_fsync: false
  - id: explicit-on
    path: /e
    pull_fsync: true
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Folders[0].PullFsync != nil {
		t.Fatalf("omitted pull_fsync parsed to %v, want nil (absent)", *cfg.Folders[0].PullFsync)
	}
	if !cfg.Folders[0].PullFsyncOn() {
		t.Fatal("an omitted pull_fsync must sync durably: the config predates the knob")
	}
	if cfg.Folders[1].PullFsync == nil || cfg.Folders[1].PullFsyncOn() {
		t.Fatal("pull_fsync: false must turn fsync off")
	}
	if !cfg.Folders[2].PullFsyncOn() {
		t.Fatal("pull_fsync: true must keep fsync on")
	}
}

// The default must not be written back into the file: `pull_fsync: true` on
// every folder would be noise, and (worse) a persisted `true` cannot be told
// apart from a key the operator set on purpose.
func TestFolderPullFsyncOmitsTheDefault(t *testing.T) {
	off := false
	raw, err := yaml.Marshal([]Folder{
		{ID: "legacy", Path: "/l"},
		{ID: "fast", Path: "/f", PullFsync: &off},
	})
	if err != nil {
		t.Fatal(err)
	}
	out := string(raw)
	if !strings.Contains(out, "- id: legacy") {
		t.Fatalf("unexpected marshal output:\n%s", out)
	}
	if strings.Contains(out, "pull_fsync: true") {
		t.Fatalf("the default must not be written:\n%s", out)
	}
	if !strings.Contains(out, "pull_fsync: false") {
		t.Fatalf("an explicit false must be persisted:\n%s", out)
	}
	// And it survives the round trip in both directions.
	var back []Folder
	if err := yaml.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if !back[0].PullFsyncOn() {
		t.Fatal("a round-tripped default must still be ON")
	}
	if back[1].PullFsyncOn() {
		t.Fatal("a round-tripped false must still be OFF")
	}
}
