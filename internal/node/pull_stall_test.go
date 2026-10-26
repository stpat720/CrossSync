package node

import (
	"bytes"
	"io"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	"crosssync/internal/hash"
	"crosssync/internal/index"
	"crosssync/internal/transfer"
	"crosssync/internal/version"
)

// TestPullStallGuardFires pins the no-progress deadline on the pull loop. A peer
// that restarts leaves a half-open socket: no FIN arrives, so Recv blocks forever
// and the round hangs with files still pending and nothing to report. Measured
// live: a 31 GiB folder stopped dead with 3 files left, 0 bytes for minutes, no
// error on either node. The guard must turn that silence into a transport error
// the round can retry.
func TestPullStallGuardFires(t *testing.T) {
	rootB := t.TempDir()
	b := chunkTestNode(t, 2, "b", rootB, t.TempDir())

	// A 256 KiB file in the peer's view: one block, so the round asks for it.
	data := pattern(101, 256<<10, rand.New(rand.NewSource(101)))
	blocks, err := hash.BlockHashes(bytes.NewReader(data), hash.MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	const peerID = 9
	b.Folders["data"].Engine.SetPeerIndex(peerID, []*index.FileInfo{{
		Name:      "stalled.bin",
		Size:      int64(len(data)),
		Type:      index.TypeFile,
		Version:   version.New().Bump(peerID),
		BlockSize: hash.MinBlockSize,
		Blocks:    blocks,
	}}, false)

	// A peer that accepts, reads everything and NEVER answers: the socket stays
	// open (no EOF), which is exactly the half-open case.
	srv, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Close() }()
	go func() {
		c, err := srv.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_, _ = io.Copy(io.Discard, c)
	}()

	c, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	conn := transfer.NewConn(c)
	conn.PeerID = peerID

	old := pullStallTimeout
	pullStallTimeout = 400 * time.Millisecond
	defer func() { pullStallTimeout = old }()

	start := time.Now()
	err = b.pullFiles(conn, b.Folders["data"], []string{"stalled.bin"})
	waited := time.Since(start)
	if err == nil {
		t.Fatalf("pulling from a silent peer returned no error after %s", waited)
	}
	if !strings.Contains(err.Error(), "sent nothing") {
		t.Fatalf("error = %v, want the no-progress timeout", err)
	}
	if waited > 5*time.Second {
		t.Fatalf("guard took %s, the deadline was %s", waited, pullStallTimeout)
	}
}
