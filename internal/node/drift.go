package node

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/staging"
)

// DriftReport is what a drift check found: names that exist in only one of the two
// worlds the daemon keeps in step. It is a READ-ONLY comparison — unlike a scan it
// neither applies changes nor advances the settling/deletion tables, so it can be
// run at any time without consequences.
type DriftReport struct {
	Folder string `json:"folder"`
	// OnDisk is the number of entries the walk saw (files and directories,
	// excluding reserved and bookkeeping names).
	OnDisk int `json:"on_disk"`
	// Indexed is the number of live entries in the index.
	Indexed int `json:"indexed"`
	// Missing lists index entries whose file is not on disk (truncated sample).
	Missing []string `json:"missing,omitempty"`
	// Unknown lists files on disk with no index entry (truncated sample).
	Unknown []string `json:"unknown,omitempty"`
	// MissingCount/UnknownCount are the full counts, even when the samples are
	// truncated.
	MissingCount int       `json:"missing_count"`
	UnknownCount int       `json:"unknown_count"`
	Checked      time.Time `json:"checked"`
}

// driftSampleSize bounds the sample returned to a caller; the counts are exact.
const driftSampleSize = 50

// CheckDrift walks a folder and reports where the index and the disk disagree,
// without changing either.
//
// The scan reconciles the two, so in principle they never disagree — but they can,
// and silently: a name that the walk deliberately ignores (a FUSE ghost), an index
// entry whose file vanished while nothing was scanning, a partially applied pass.
// Until now the only way to notice was for an operator to spot a number that looked
// wrong. This turns that into a check with an answer.
func (n *Node) CheckDrift(id string) (DriftReport, error) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return DriftReport{}, fmt.Errorf("unknown folder %q", id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	rep := DriftReport{Folder: id, Checked: time.Now()}
	seen := make(map[string]struct{}, 1024)
	root := f.Root
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if p == root {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		relSlash := filepath.ToSlash(rel)
		// The same exclusions the scanner uses: CrossSync's own areas and the
		// filesystem's bookkeeping names are not data and are not expected in
		// the index.
		if staging.IsReserved(relSlash) || staging.IsTransient(relSlash) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if f.Engine != nil && f.Engine.Ignore != nil {
			if ignored, _ := f.Engine.Ignore.Match(relSlash, d.IsDir()); ignored {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}
		seen[relSlash] = struct{}{}
		return nil
	})
	if err != nil {
		return rep, err
	}
	rep.OnDisk = len(seen)

	// One pass over the index: every live entry must be on disk, and every name on
	// disk must have an entry.
	live := make(map[string]struct{}, len(seen))
	if err := f.Ix.List(func(fi *index.FileInfo) error {
		if fi.Deleted {
			return nil
		}
		live[fi.Name] = struct{}{}
		return nil
	}); err != nil {
		return rep, err
	}
	rep.Indexed = len(live)

	for name := range live {
		if _, ok := seen[name]; !ok {
			rep.MissingCount++
			if len(rep.Missing) < driftSampleSize {
				rep.Missing = append(rep.Missing, name)
			}
		}
	}
	for name := range seen {
		if _, ok := live[name]; !ok {
			rep.UnknownCount++
			if len(rep.Unknown) < driftSampleSize {
				rep.Unknown = append(rep.Unknown, name)
			}
		}
	}
	sort.Strings(rep.Missing)
	sort.Strings(rep.Unknown)
	f.driftMissing.Store(int64(rep.MissingCount))
	f.driftUnknown.Store(int64(rep.UnknownCount))
	return rep, nil
}

// FolderDrift returns the counts from the last drift check for a folder (0,0
// before the first one).
func (n *Node) FolderDrift(id string) (missing, unknown int) {
	f, ok := n.lookupFolder(id)
	if !ok {
		return 0, 0
	}
	return int(f.driftMissing.Load()), int(f.driftUnknown.Load())
}

// recordDrift raises one warning per folder when a drift check finds a
// disagreement, and clears it when the next check is clean. The message names the
// sample so the operator has somewhere to start looking.
func (n *Node) recordDrift(rep DriftReport) {
	if n.Events == nil {
		return
	}
	total := rep.MissingCount + rep.UnknownCount
	if total == 0 {
		if _, rerr := n.Events.ResolveAutoReasonPrefix(rep.Folder, events.CatSystem, "index and disk disagree", "drift check"); rerr != nil {
			n.Logf("folder %s: clear drift warning: %v", rep.Folder, rerr)
		}
		return
	}
	detail := ""
	if len(rep.Missing) > 0 {
		detail += fmt.Sprintf("; missing from disk: %s", strings.Join(rep.Missing[:min(3, len(rep.Missing))], ", "))
	}
	if len(rep.Unknown) > 0 {
		detail += fmt.Sprintf("; not in the index: %s", strings.Join(rep.Unknown[:min(3, len(rep.Unknown))], ", "))
	}
	n.recordEvent(&events.Event{
		TS: time.Now(), Folder: rep.Folder, Category: events.CatSystem, Severity: events.SevWarn,
		Reason: fmt.Sprintf("index and disk disagree: %d entr(y/ies) missing from disk, %d file(s) not indexed%s",
			rep.MissingCount, rep.UnknownCount, detail),
	})
}

