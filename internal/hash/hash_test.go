package hash

import (
	"bytes"
	"testing"
)

func TestBlockSizeForSize(t *testing.T) {
	cases := []struct {
		size int64
		want int
	}{
		{0, MinBlockSize},
		{100, MinBlockSize},
		{250 * 1024 * 1024, 128 * 1024},  // 250 MiB -> 2000 blocks at 128K
		{500 * 1024 * 1024, 256 * 1024},  // 500 MiB -> 2000 blocks at 256K
		{1 << 30, 1024 * 1024},           // 1 GiB -> 1 MiB
		{2 << 30, 2 * 1024 * 1024},       // 2 GiB -> 2 MiB
		{4 << 30, 4 * 1024 * 1024},       // 4 GiB -> 4 MiB
		{8 << 30, 8 * 1024 * 1024},       // 8 GiB -> 8 MiB
		{16 << 30, 16 * 1024 * 1024},     // 16 GiB -> 16 MiB
		{64 << 30, 16 * 1024 * 1024},     // 64 GiB -> capped at max
	}
	for _, c := range cases {
		if got := BlockSizeForSize(c.size); got != c.want {
			t.Errorf("BlockSizeForSize(%d) = %d, want %d", c.size, got, c.want)
		}
	}
}

func TestBlockHashes(t *testing.T) {
	// A payload of exactly one block plus a partial block.
	payload := bytes.Repeat([]byte{0xAB}, MinBlockSize+17)
	bs := BlockSizeForSize(int64(len(payload)))
	if bs != MinBlockSize {
		t.Fatalf("block size = %d, want %d", bs, MinBlockSize)
	}
	hashes, err := BlockHashes(bytes.NewReader(payload), bs)
	if err != nil {
		t.Fatal(err)
	}
	if len(hashes) != 2 {
		t.Fatalf("got %d hashes, want 2", len(hashes))
	}
	if !bytes.Equal(hashes[0], HashBytes(payload[:MinBlockSize])) {
		t.Fatalf("first block hash mismatch")
	}
	if !bytes.Equal(hashes[1], HashBytes(payload[MinBlockSize:])) {
		t.Fatalf("second block hash mismatch")
	}
}

func TestFlattenUnflatten(t *testing.T) {
	in := [][]byte{
		HashBytes([]byte("one")),
		HashBytes([]byte("two")),
		HashBytes([]byte("three")),
	}
	flat := Flatten(in)
	if len(flat) != len(in)*32 {
		t.Fatalf("flatten length = %d", len(flat))
	}
	out := Unflatten(flat)
	if !EqualHashes(in, out) {
		t.Fatalf("round trip mismatch")
	}
	if Unflatten(nil) != nil {
		t.Fatalf("unflatten of nil should be nil")
	}
}

func TestEmptyReader(t *testing.T) {
	hashes, err := BlockHashes(bytes.NewReader(nil), MinBlockSize)
	if err != nil {
		t.Fatalf("empty reader should succeed, got %v", err)
	}
	if len(hashes) != 0 {
		t.Fatalf("empty reader produced %d hashes", len(hashes))
	}
}
