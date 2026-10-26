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
// whose edit touched one region, an archive whose member appears elsewhere. Our
// whole-file reuse (fillFromLocalCopy) only covers a byte-identical twin — the
// same content key — and everything else crossed the wire.
//
// This fills a pull's missing blocks from OTHER local files: for every block the
// pull still needs, if some local file holds those exact bytes (at the same
// offset or any other), they are read locally and verified against the PEER's
// recorded hash before being written. A donor whose disk content drifted stops
// being used at the first mismatch, exactly like the whole-file path. The index,
// the block list and the protocol are untouched, so reusing a block can only
// make a round faster — never less correct.
//
// Cost is what makes or breaks this. The donor map is built ONCE per sync round
// (one index walk, capped) and every pull after that is O(blocks in that file)
// map lookups. Walking the index per pulled file instead would cost about a
// second per file on an 84k-entry folder — more than the transfer it saves.

const (
	// maxDonorBlocks caps the donor map so a huge folder cannot turn reuse into
	// a memory problem (200k blocks is ~15 MB of map).
	maxDonorBlocks = 200000
	// maxDonorFiles caps the walk that builds it.
	maxDonorFiles = 20000
)

// errStopDonorWalk ends the capped index walk that builds the donor map. It is
// not a failure: the map is simply full.
var errStopDonorWalk = fmt.Errorf("donor index is full")

// donorLoc is where a block's bytes live locally.
type donorLoc struct {
	name  string
	index int // block index within the donor file
	size  int32
}

// donorState is the per-round donor map, built lazily on first use.
type donorState struct {
	mu    sync.Mutex
	built bool
	locs  map[string]donorLoc
}

// ensureDonorIndex builds (once per round) the block-hash -> local location map.
func (e *Engine) ensureDonorIndex() map[string]donorLoc {
	if e.donors == nil {
		return nil
	}
	e.donors.mu.Lock()
	defer e.donors.mu.Unlock()
	if e.donors.built {
		return e.donors.locs
	}
	locs := make(map[string]donorLoc, 4096)
	files, blocks := 0, 0
	_ = e.Ix.ListAll(func(fi *index.FileInfo) error {
		if fi.Deleted || fi.Type != index.TypeFile || fi.Size <= 0 || len(fi.Blocks) == 0 {
			return nil
		}
		if files >= maxDonorFiles || blocks >= maxDonorBlocks {
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
			if _, seen := locs[key]; seen {
				continue // first owner wins: deterministic, and enough to reuse
			}
			locs[key] = donorLoc{name: fi.Name, index: i, size: int32(blockSizeAt(fi, i))}
			blocks++
			if blocks >= maxDonorBlocks {
				break
			}
		}
		return nil
	})
	e.donors.locs = locs
	e.donors.built = true
	return locs
}

// resetDonorIndex drops the map so the next round rebuilds it (the index has
// moved on: files were added, edited and deleted).
func (e *Engine) resetDonorIndex() {
	if e.donors == nil {
		return
	}
	e.donors.mu.Lock()
	e.donors.locs = nil
	e.donors.built = false
	e.donors.mu.Unlock()
}

// FillBlocksFromLocal satisfies as many of p's missing blocks as possible from
// other local files, returning how many blocks and bytes were reused.
func (e *Engine) FillBlocksFromLocal(p *Pull) (int, int64, error) {
	needs := p.NeedBlocks()
	if len(needs) == 0 {
		return 0, 0, nil
	}
	locs := e.ensureDonorIndex()
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
		loc, ok := locs[string(need.Hash)]
		if !ok || loc.name == p.name {
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
