package cfgcheck

import (
	"os"
	"testing"

	"crosssync/internal/config"
	"gopkg.in/yaml.v3"
)

func TestParseLiveConfig(t *testing.T) {
	raw, err := os.ReadFile(os.Getenv("CFG"))
	if err != nil {
		t.Fatal(err)
	}
	var c config.Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		t.Fatalf("parse: %v", err)
	}
	t.Logf("PullFileMode=%d (%04o) PullDirMode=%d (%04o) uid=%d gid=%d => FileModeForPull=%04o DirModeForPull=%04o owner=%v",
		c.PullFileMode, c.PullFileMode, c.PullDirMode, c.PullDirMode, c.PullUID, c.PullGID,
		uint32(c.FileModeForPull()), uint32(c.DirModeForPull()), func() [2]int { a, b := c.PullOwner(); return [2]int{a, b} }())
}
