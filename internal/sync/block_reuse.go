package sync

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"crosssync/internal/hash"
	"crosssync/internal/index"
)

// Local block reuse.
//
// Syncthing calls this "local block reuse" and it is what makes a folder full of
// near-duplicates cheap: a template copied twenty times, a .venv tree, a document
// whose edit touched one region, a ZIP whose member also exists on disk. Our
// whole-file reuse (fillFromLocalCopy) only covers a byte-identical twin — the
// same content key — and everything else crossed the wire.
//
// This fills a pull's missing blocks from other local files in the SAME
// directory: for every block the pull still needs, if a sibling file holds those
// exact bytes, they are read locally and verified against the PEER's recorded
// hash before being written. A donor whose disk content drifted stops being used
// at the first mismatch, exactly like the whole-file path. Nothing about the
// index, the block list or the protocol changes, so reusing a block can only make
// a round faster — never less correct.
//
// Why the same directory and not the whole folder: copies, edits and version
// piles live beside each other, and a directory is the only scope that can be
// scanned cheaply. A folder-wide map was the first design and it failed twice
// over — building it per pulled file turned one pull into an index walk (three
// times slower test suite, which a benchmark would have hidden), and capping it
// to keep memory bounded meant the donor was simply absent from the map whenever
// the folder was big (0 blocks reused on a live 84k-file pair while the unit test
// passed). A per-directory map, cached for the round, is O(files in that
// directory) once per round instead of O(folder) per file.
//
// Cross-directory partial overlap is deliberately out of scope: whole-file twins
// are still free anywhere in the folder, and a shifted-content matcher needs
// content-defined chunking, which would change the block format.

const (
	// maxDonorFilesPerDir bounds one directory's donor scan: enough for a
	// version pile or a template set, never a folder walk.
	maxDonorFilesPerDir = 500
	// maxDonorBlocksPerDir bounds the map one directory can build.
	maxDonorBlocksPerDir = 20000
	// maxOwnersPerBlock is how many files per directory may offer the same block.
	// Two covers the real shapes (the file being replaced plus its copy); more
	// only costs memory, since a pull stops at the first usable owner.
	maxOwnersPerBlock = 2
	// maxDonorDirs caches this many directories per round (LRU).
	maxDonorDirs = 64
)

// donorLoc is where a block's bytes live locally.
type donorLoc struct {
	name  string
	index int // block index within the donor file
	size  int32
}

// donorState holds the per-round directory maps, built lazily on first pull in
// that directory and dropped with the round.
type donorState struct {
	mu    sync.Mutex
	dirs  map[string]*donorDir
	order []string // LRU keys, oldest first
}

// donorDir is one directory's block-hash -> local location map, plus how full it
// was (a truncated map means "we stopped looking").
type donorDir struct {
	locs map[string][]donorLoc
}

// donorMaps returns this round's directory cache, creating it if needed.
func (e *Engine) donorMaps() *donorState {
	if e.donors == nil {
		return nil
	}
	return e.donors
}

// dirDonors returns the block map for dir, building it on first use this round.
func (e *Engine) dirDonors(dir string) map[string][]donorLoc {
	st := e.donorMaps()
	if st == nil {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.dirs == nil {
		st.dirs = make(map[string]*donorDir, 8)
	}
	if d, ok := st.dirs[dir]; ok {
		// Move to the young end of the LRU.
		for i, k := range st.order {
			if k == dir {
				st.order = append(append(st.order[:i:i], st.order[i+1:]...), dir)
				break
			}
		}
		return d.locs
	}

	locs := make(map[string][]donorLoc, 64)
	files, blocks := 0, 0
	walk := func(fn func(*index.FileInfo) error) error {
		if dir == "" {
			// The folder root: ListPrefix needs a prefix, so this is the one
			// scope that costs a full walk — once per round, not per file.
			return e.Ix.ListAll(fn)
		}
		return e.Ix.ListPrefix(dir+"/", fn)
	}
	_ = walk(func(fi *index.FileInfo) error {
		// Only direct children: a subdirectory's blocks are a different scope
		// and would blow the budget on a deep tree.
		if fi.Deleted || fi.Type != index.TypeFile || fi.Size <= 0 || len(fi.Blocks) == 0 {
			return nil
		}
		if parentDir(fi.Name) != dir {
			return nil
		}
		if files >= maxDonorFilesPerDir || blocks >= maxDonorBlocksPerDir {
			return errStopDonorWalk
		}
		// A file the stability gate still considers in flight may change under
		// us; its blocks are not trustworthy donors.
		if e.InFlight(fi.Name) {
			return nil
		}
		files++
		for i := range fi.Blocks {
			key := string(fi.Blocks[i])
			owners := locs[key]
			if len(owners) >= maxOwnersPerBlock {
				continue // two candidates is enough: a third never gets reached
			}
			dup := false
			for _, o := range owners {
				if o.name == fi.Name {
					dup = true
					break
				}
			}
			if dup {
				continue
			}
			locs[key] = append(owners, donorLoc{name: fi.Name, index: i, size: int32(blockSizeAt(fi, i))})
			blocks++
			if blocks >= maxDonorBlocksPerDir {
				return errStopDonorWalk
			}
		}
		return nil
	})

	st.dirs[dir] = &donorDir{locs: locs}
	st.order = append(st.order, dir)
	for len(st.order) > maxDonorDirs {
		oldest := st.order[0]
		st.order = st.order[1:]
		delete(st.dirs, oldest)
	}
	return locs
}

// resetDonorIndex drops every directory map so the next round rebuilds them (the
// index has moved on: files were added, edited and deleted).
func (e *Engine) resetDonorIndex() {
	if e.donors == nil {
		return
	}
	e.donors.mu.Lock()
	e.donors.dirs = nil
	e.donors.order = nil
	e.donors.mu.Unlock()
}

// FillBlocksFromLocal satisfies as many of p's missing blocks as possible from
// other local files in the same directory, returning how many blocks and bytes
// were reused.
func (e *Engine) FillBlocksFromLocal(p *Pull) (int, int64, error) {
	needs := p.NeedBlocks()
	if len(needs) == 0 {
		return 0, 0, nil
	}
	locs := e.dirDonors(parentDir(p.name))
	if len(locs) == 0 {
		return 0, 0, nil
	}

	reused, bytesOut := 0, int64(0)
	openDonor := ""
	var donorFile *os.File
	defer func() {
		if donorFile != nil {
			_ = donorFile.Close()
		}
	}()
	for _, need := range needs {
		owners := locs[string(need.Hash)]
		if len(owners) == 0 {
			continue
		}
		// A pull cannot be its own donor: skip the target and take the copy
		// beside it (the index is walked name-DESCENDING, so the file being
		// replaced is usually the first owner and would shadow the copy).
		var loc donorLoc
		found := false
		for _, cand := range owners {
			if cand.name != p.name {
				loc, found = cand, true
				break
			}
		}
		if !found {
			continue
		}
		if loc.name != openDonor {
			if donorFile != nil {
				_ = donorFile.Close()
				donorFile = nil
			}
			f, err := os.Open(filepath.Join(e.Root, filepath.FromSlash(loc.name)))
			if err != nil {
				continue
			}
			donorFile, openDonor = f, loc.name
		}
		bs := int64(p.info.BlockSize)
		if bs <= 0 {
			bs = int64(hash.MinBlockSize)
		}
		buf := make([]byte, int(loc.size))
		if _, err := donorFile.ReadAt(buf, int64(loc.index)*bs); err != nil {
			continue
		}
		// writeLocalBlock verifies the bytes against the PEER's hash and writes
		// them without counting a byte as downloaded.
		if err := p.writeLocalBlock(need.Index, buf); err != nil {
			continue
		}
		reused++
		bytesOut += int64(len(buf))
	}
	if reused > 0 {
		e.Stats.BlocksReused.Add(int64(reused))
		e.Stats.BytesReused.Add(bytesOut)
	}
	return reused, bytesOut, nil
}

// writeLocalBlock writes a block that came from a LOCAL donor: it is verified
// against the peer's recorded hash first (the donor's disk may have drifted from
// its index entry), and it deliberately does not touch BytesDown — no byte
// crossed the wire, so counting it would make the transfer metric lie.
func (p *Pull) writeLocalBlock(idx int, data []byte) error {
	if idx < 0 || idx >= len(p.info.Blocks) {
		return fmt.Errorf("block %d out of range", idx)
	}
	if !bytes.Equal(hash.HashBytes(data), p.info.Blocks[idx]) {
		return fmt.Errorf("local block %d does not match the peer's hash", idx)
	}
	bs := int64(p.info.BlockSize)
	if bs <= 0 {
		bs = int64(hash.MinBlockSize)
	}
	if _, err := p.f.WriteAt(data, int64(idx)*bs); err != nil {
		return err
	}
	p.written[idx] = true
	return nil
}

// errStopDonorWalk ends a capped directory scan. It is not a failure: the map is
// simply full.
var errStopDonorWalk = fmt.Errorf("donor index is full")

// parentDir returns the directory part of a slash-separated relative name.
func parentDir(name string) string {
	if i := lastSlash(name); i > 0 {
		return name[:i]
	}
	return ""
}

func lastSlash(name string) int {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' {
			return i
		}
	}
	return -1
}

// PeerSizes returns the peer's recorded size for each name (0 when the peer's
// view has no entry). Used to order a pull list; it reads the in-memory view, so
// it costs one map lookup per name and never touches the index.
func (e *Engine) PeerSizes(peerID uint64, names []string) map[string]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]int64, len(names))
	view := e.peers[peerID]
	for _, name := range names {
		if fi, ok := view[name]; ok {
			out[name] = fi.Size
		}
	}
	return out
}

// BlocksReusedText is a small helper for log lines/tests.
func (e *Engine) BlocksReusedText() string {
	return fmt.Sprintf("%d block(s), %d byte(s) reused locally",
		e.Stats.BlocksReused.Load(), e.Stats.BytesReused.Load())
}
