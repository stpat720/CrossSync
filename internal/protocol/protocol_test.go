package protocol

import (
	"bytes"
	"testing"

	"crosssync/internal/version"
)

func TestMessageRoundTrip(t *testing.T) {
	msgs := []*Message{
		{Type: MsgHello, Hello: &Hello{DeviceID: 7, DeviceName: "nas-01"}},
		{Type: MsgIndex, Index: &IndexMsg{
			Folder: "media",
			Update: true,
			MaxSeq: 42,
			Files: []FileMessage{{
				Name: "a/b.txt", Size: 100, ModifiedS: 123, ModifiedNs: 456,
				Mode: 0o644, Type: 0, Version: version.Vector{1: 2, 3: 4},
				BlockSize: 131072, Blocks: [][]byte{{1, 2, 3}},
			}},
		}},
		{Type: MsgRequest, Request: &Request{ID: 9, Folder: "media", Name: "x", Offset: 1024, Size: 4096}},
		{Type: MsgResponse, Response: &Response{ID: 9, Data: []byte("hello block"), Code: RespNoError}},
		{Type: MsgPing},
		{Type: MsgClose, Close: &Close{Reason: "shutting down"}},
	}
	var buf bytes.Buffer
	for _, m := range msgs {
		if err := WriteMessage(&buf, m); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range msgs {
		got, err := ReadMessage(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if got.Type != want.Type {
			t.Fatalf("type = %v, want %v", got.Type, want.Type)
		}
	}
	// Verify a field deep copy.
	got := msgs[1]
	if got.Index.Files[0].Version.Get(3) != 4 {
		t.Fatalf("version not decoded: %v", got.Index.Files[0].Version)
	}
	if string(got.Index.Files[0].Blocks[0]) != string([]byte{1, 2, 3}) {
		t.Fatalf("blocks not decoded")
	}
}

func TestFrameBoundary(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, &Message{Type: MsgPing}); err != nil {
		t.Fatal(err)
	}
	if err := WriteMessage(&buf, &Message{Type: MsgHello, Hello: &Hello{DeviceID: 2, DeviceName: "two"}}); err != nil {
		t.Fatal(err)
	}
	// First frame must be ping, second hello — proves framing boundaries.
	m1, err := ReadMessage(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if m1.Type != MsgPing {
		t.Fatalf("first frame = %v, want ping", m1.Type)
	}
	m2, err := ReadMessage(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Type != MsgHello || m2.Hello.DeviceID != 2 {
		t.Fatalf("second frame wrong: %+v", m2)
	}
}

func TestTruncatedFrame(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, &Message{Type: MsgPing}); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	if _, err := ReadMessage(bytes.NewReader(data[:len(data)-2])); err == nil {
		t.Fatal("expected error on truncated frame")
	}
}
