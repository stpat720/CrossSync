package index

import (
	"fmt"
	"os"
	"strings"
)

// SnapshotTo writes a CONSISTENT copy of this index to dst, using SQLite's
// VACUUM INTO: it reads through a live connection, so the file is a coherent
// snapshot even while the daemon is writing (which plain file copying is not —
// that is how a "backup" restores corrupt).
//
// The index is worth backing up even though it can be rebuilt from disk: a
// rebuild re-derives every entry as a fresh local add, and the version vectors
// that let peers recognise identical content go with it. Restoring the DB keeps
// those vectors, so a restored node does not manufacture conflicts against peers
// that still hold the same files.
func (ix *Index) SnapshotTo(dst string) error {
	if dst == "" {
		return fmt.Errorf("index: empty snapshot path")
	}
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("index: snapshot target: %w", err)
	}
	// VACUUM INTO takes a string literal, not a bound parameter.
	quoted := "'" + strings.ReplaceAll(dst, "'", "''") + "'"
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if _, err := ix.db.Exec(`VACUUM INTO ` + quoted); err != nil {
		return fmt.Errorf("index: snapshot: %w", err)
	}
	return nil
}
