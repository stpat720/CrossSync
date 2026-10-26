package sync

import (
	"bytes"
	"fmt"

	"crosssync/internal/hash"
)

// ReceiveBlocks verifies and writes a RUN of consecutive blocks from one buffer.
//
// The wire can answer several consecutive blocks in a single response (the pull
// batch asks for a run — see node.pullBatch/planPulls). Committing that run with
// ONE WriteAt instead of one per block is the half that matters on a FUSE share:
// measured on the live pair, a 128 KiB write to the receiving array costs ~40 ms
// whether it arrives alone or inside a larger response, so writing block by
// block kept a 64 MiB pull at ~2-3 MB/s while 1 MiB blocks (1024 writes for 1 GiB)
// ran at ~20 MB/s. Asking for bigger runs without writing them as one range made
// things WORSE, because the first block of a run could not be written until the
// whole run had arrived.
//
// Integrity is unchanged: every block is verified against its own recorded hash
// before anything is written, and a run whose sizes do not line up with the block
// list is refused outright. Nothing is written unless the whole run verifies, so
// a partly-bad response cannot leave a half-updated temp.
func (p *Pull) ReceiveBlocks(first int, data []byte) error {
	if first < 0 || first >= len(p.info.Blocks) {
		return fmt.Errorf("run start %d out of range", first)
	}
	bs := int64(p.info.BlockSize)
	if bs <= 0 {
		bs = int64(hash.MinBlockSize)
	}
	pos, idx := 0, first
	for pos < len(data) {
		if idx >= len(p.info.Blocks) {
			return fmt.Errorf("run of %d bytes overruns the block list at %d", len(data), idx)
		}
		size := blockSizeAt(p.info, idx)
		if pos+size > len(data) {
			return fmt.Errorf("run is %d bytes short of block %d", pos+size-len(data), idx)
		}
		if !bytes.Equal(hash.HashBytes(data[pos:pos+size]), p.info.Blocks[idx]) {
			return fmt.Errorf("block %d hash mismatch (bad transfer)", idx)
		}
		pos += size
		idx++
	}
	if pos != len(data) {
		return fmt.Errorf("run has %d trailing byte(s)", pos-len(data))
	}
	// Consecutive blocks are contiguous in the file, so this is one write.
	if _, err := p.f.WriteAt(data, int64(first)*bs); err != nil {
		return err
	}
	for i := first; i < idx; i++ {
		p.written[i] = true
	}
	p.engine.Stats.BytesDown.Add(int64(len(data)))
	return nil
}
