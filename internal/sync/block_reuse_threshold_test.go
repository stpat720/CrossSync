package sync

import (
	"testing"

	"crosssync/internal/index"
)

// TestBlockDonorScanSkipsTinyPulls pins the threshold that keeps a metadata pull
// from walking the whole index. Block-donor lookup builds a directory map (a
// full index walk at the folder root); on Tower's 1.06M-file folder that took
// minutes per round, and the 1,629 pending files were almost all .DS_Store /
// .sync metadata worth a few KB each. Below one hash block the content is
// cheaper to fetch than to look up.
//
// The engine here has NO index: any attempt to scan would panic, so a clean
// return proves the pull never touched it.
func TestBlockDonorScanSkipsTinyPulls(t *testing.T) {
	e := &Engine{}
	p := &Pull{
		engine:  e,
		name:    ".sync/ID",
		info:    &index.FileInfo{Name: ".sync/ID", Type: index.TypeFile, Size: 40, Blocks: [][]byte{{1, 2, 3}}},
		written: []bool{false},
	}
	blocks, bytes, err := e.FillBlocksFromLocal(p)
	if err != nil || blocks != 0 || bytes != 0 {
		t.Fatalf("FillBlocksFromLocal(tiny) = (%d, %d, %v), want (0, 0, nil)", blocks, bytes, err)
	}
	if localReuseMinSize < 1 {
		t.Fatalf("localReuseMinSize = %d; the threshold must keep small files out of the donor walk", localReuseMinSize)
	}
}
