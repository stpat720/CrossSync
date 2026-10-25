package hash

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math/rand"
	"testing"
)

// naiveBlockHashes is the original one-read-per-block implementation, kept as the
// reference the streaming version must match exactly: block boundaries decide
// what a peer verifies, so a different split would reject every transfer.
func naiveBlockHashes(data []byte, blockSize int) [][]byte {
	var out [][]byte
	for off := 0; off < len(data); off += blockSize {
		end := off + blockSize
		if end > len(data) {
			end = len(data)
		}
		h := sha256.Sum256(data[off:end])
		out = append(out, h[:])
	}
	return out
}

func TestBlockHashesMatchesPerBlockHashing(t *testing.T) {
	// Sizes chosen around the interesting boundaries: empty, shorter than a
	// block, exactly one block, one block plus a byte, and several megabytes so
	// reads and blocks do not line up.
	sizes := []int{0, 1, MinBlockSize - 1, MinBlockSize, MinBlockSize + 1, 3*MinBlockSize + 7, 5 << 20}
	blockSizes := []int{MinBlockSize, 256 * 1024, 1 << 20, 3 << 20, MaxBlockSize}
	rng := rand.New(rand.NewSource(7))
	for _, size := range sizes {
		data := make([]byte, size)
		if _, err := rng.Read(data); err != nil {
			t.Fatal(err)
		}
		for _, bs := range blockSizes {
			got, err := BlockHashes(bytes.NewReader(data), bs)
			if err != nil {
				t.Fatalf("size %d block %d: %v", size, bs, err)
			}
			want := naiveBlockHashes(data, bs)
			if len(got) != len(want) {
				t.Fatalf("size %d block %d: got %d hashes, want %d", size, bs, len(got), len(want))
			}
			for i := range want {
				if !bytes.Equal(got[i], want[i]) {
					t.Fatalf("size %d block %d: hash %d differs", size, bs, i)
				}
			}
		}
	}
}

// TestBlockHashesLargeBlocksStayBounded: a 16 MiB block must not allocate a 16 MiB
// buffer per file — the streaming path keeps one readChunk buffer no matter how
// large the block is.
func TestBlockHashesLargeBlocksStayBounded(t *testing.T) {
	r := &countingReader{r: bytes.NewReader(make([]byte, 3*MaxBlockSize+123))}
	got, err := BlockHashes(r, MaxBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d blocks, want 4 (three full + one short)", len(got))
	}
	// Each read must be at most readChunk, i.e. blocks are assembled in memory
	// rather than by one oversized read.
	if r.maxRead > readChunk {
		t.Fatalf("largest read was %d bytes, want <= %d", r.maxRead, readChunk)
	}
}

// TestBlockHashesShortReads: a reader that dribbles out small chunks must produce
// the same blocks as one that returns everything at once.
func TestBlockHashesShortReads(t *testing.T) {
	data := make([]byte, 4*MinBlockSize+11)
	for i := range data {
		data[i] = byte(i * 31)
	}
	want := naiveBlockHashes(data, MinBlockSize)
	got, err := BlockHashes(&dribbleReader{data: data, step: 1000}, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d blocks, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("block %d differs under short reads", i)
		}
	}
}

type countingReader struct {
	r       io.Reader
	maxRead int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > c.maxRead {
		c.maxRead = n
	}
	return n, err
}

type dribbleReader struct {
	data []byte
	step int
	off  int
}

func (d *dribbleReader) Read(p []byte) (int, error) {
	if d.off >= len(d.data) {
		return 0, io.EOF
	}
	n := d.step
	if n > len(p) {
		n = len(p)
	}
	if d.off+n > len(d.data) {
		n = len(d.data) - d.off
	}
	copy(p, d.data[d.off:d.off+n])
	d.off += n
	return n, nil
}
