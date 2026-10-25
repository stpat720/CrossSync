// Package hash implements block hashing with the BEP block-size rule.
package hash

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
)

const (
	// MinBlockSize is the smallest allowed block size.
	MinBlockSize = 128 * 1024
	// MaxBlockSize is the largest allowed block size.
	MaxBlockSize = 16 * 1024 * 1024
)

// BlockSizeForSize returns the block size for a file of the given size,
// following the BEP rule: the smallest power of two between 128 KiB and
// 16 MiB that keeps the file at or under 2000 blocks.
func BlockSizeForSize(size int64) int {
	if size <= 0 {
		return MinBlockSize
	}
	bs := MinBlockSize
	for bs < MaxBlockSize {
		blocks := (size + int64(bs) - 1) / int64(bs)
		if blocks <= 2000 {
			break
		}
		bs *= 2
	}
	return bs
}

// readChunk is how many bytes are pulled from the reader per call. It is
// deliberately larger than the common block size: on a FUSE-backed share
// (Unraid's shfs) and on network filesystems every read is a round trip, and the
// old code issued one read per 128-256 KiB block. Reading a megabyte at a time
// and slicing it into blocks cuts the syscall count by an order of magnitude,
// which is the difference between a full verification pass crawling and
// saturating the array.
const readChunk = 1024 * 1024

// BlockHashes reads r in fixed-size blocks and returns the SHA-256 hash of
// each block (the last block may be smaller).
//
// Blocks are hashed through a streaming hasher rather than by allocating one
// buffer per block, so a 16 MiB block size costs no more memory than a 128 KiB
// one: the buffer stays at readChunk regardless. Block boundaries are exact —
// the writer's block list must match byte for byte or every transfer would fail
// verification.
func BlockHashes(r io.Reader, blockSize int) ([][]byte, error) {
	if blockSize <= 0 {
		blockSize = MinBlockSize
	}
	buf := make([]byte, readChunk)
	h := sha256.New()
	remaining := blockSize
	var hashes [][]byte
	for {
		n, err := r.Read(buf)
		if n > 0 {
			for off := 0; off < n; {
				take := n - off
				if take > remaining {
					take = remaining
				}
				h.Write(buf[off : off+take])
				off += take
				remaining -= take
				if remaining == 0 {
					hashes = append(hashes, h.Sum(nil))
					h.Reset()
					remaining = blockSize
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	if remaining != blockSize {
		// Trailing partial block: the file is not a whole multiple of the
		// block size, and that short final block is its own hash.
		hashes = append(hashes, h.Sum(nil))
	}
	return hashes, nil
}

// HashBytes returns the SHA-256 hash of b.
func HashBytes(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}

// FileHashes computes block hashes for the file at path using the given
// block size. If blockSize <= 0 the BEP rule is applied from the file size.
func FileHashes(path string, blockSize int) (int, [][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return 0, nil, err
	}
	if blockSize <= 0 {
		blockSize = BlockSizeForSize(st.Size())
	}
	hashes, err := BlockHashes(f, blockSize)
	return blockSize, hashes, err
}

// EqualHashes reports whether two block-hash lists are identical.
func EqualHashes(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// Flatten packs block hashes into a single byte slice for storage.
// Returns a non-nil empty slice when there are no hashes (so callers can
// bind it to NOT NULL columns).
func Flatten(hashes [][]byte) []byte {
	if len(hashes) == 0 {
		return []byte{}
	}
	n := 0
	for _, h := range hashes {
		n += len(h)
	}
	out := make([]byte, 0, n)
	for _, h := range hashes {
		out = append(out, h...)
	}
	return out
}

// Unflatten unpacks block hashes from a flattened byte slice (32 bytes each).
func Unflatten(b []byte) [][]byte {
	if len(b) == 0 {
		return nil
	}
	var out [][]byte
	for i := 0; i+sha256.Size <= len(b); i += sha256.Size {
		h := make([]byte, sha256.Size)
		copy(h, b[i:i+sha256.Size])
		out = append(out, h)
	}
	return out
}
