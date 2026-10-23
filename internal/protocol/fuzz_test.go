package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// FuzzReadMessage feeds arbitrary framed bytes at the wire decoder. Peers are
// authenticated, but a decoder that panics on a malformed frame is a remotely
// triggerable crash — and corrupted bytes on disk/in a backlog produce the same
// input shape. Invariant: never panic; either an error or a decoded message.
func FuzzReadMessage(f *testing.F) {
	// A valid frame as the seed, so the fuzzer starts from real input.
	var buf bytes.Buffer
	valid := &Message{Type: MsgHello, Hello: &Hello{
		DeviceID: 1, DeviceName: "seed", SessionID: "seed-session", Protocol: 2,
	}}
	if err := WriteMessage(&buf, valid); err != nil {
		f.Fatalf("seed frame: %v", err)
	}
	f.Add(buf.Bytes())

	// Length prefix edges: zero, one byte short, huge-but-capped, and a length
	// that promises more than follows.
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 5, 0x7b})
	var huge [4]byte
	binary.BigEndian.PutUint32(huge[:], maxMessageSize+1)
	f.Add(huge[:])
	f.Add(append(append([]byte(nil), huge[:0]...), 0, 0, 0, 2, '{', '}'))

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := ReadMessage(bytes.NewReader(data))
		if err != nil {
			return // rejecting bad input is the correct failure mode
		}
		if m == nil {
			t.Fatal("decoded a nil message with a nil error")
		}
		// A decoded message must round-trip through the encoder without panic.
		var out bytes.Buffer
		_ = WriteMessage(&out, m)
	})
}
