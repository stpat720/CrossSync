// Package protocol defines the wire messages exchanged between CrossSync
// peers. Messages are JSON encoded and framed with a 4-byte big-endian
// length prefix over a reliable stream.
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"crosssync/internal/version"
)

// maxMessageSize caps a single frame (mirrors BEP's 500 MB limit).
const maxMessageSize = 512 * 1024 * 1024

// Current is the protocol revision this build supports. It is advertised in
// the Hello handshake (Hello.Protocol). A sender only uses a capability
// when the PEER also advertises at least that revision, so a rolling
// upgrade between old and new daemons always falls back to the older
// behavior: revision 0 means single-frame index messages (the full folder
// in one IndexMsg), revision 1 enables chunked index messages so no single
// frame approaches maxMessageSize.
const Current = 1

// MsgType identifies the message kind.
type MsgType int

const (
	MsgHello MsgType = iota
	MsgClusterConfig
	MsgIndex
	MsgIndexUpdate
	MsgRequest
	MsgResponse
	MsgDownloadProgress
	MsgPing
	MsgPhaseDone
	MsgClose
)

func (t MsgType) String() string {
	switch t {
	case MsgHello:
		return "hello"
	case MsgClusterConfig:
		return "cluster-config"
	case MsgIndex:
		return "index"
	case MsgIndexUpdate:
		return "index-update"
	case MsgRequest:
		return "request"
	case MsgResponse:
		return "response"
	case MsgDownloadProgress:
		return "download-progress"
	case MsgPing:
		return "ping"
	case MsgPhaseDone:
		return "phase-done"
	case MsgClose:
		return "close"
	default:
		return fmt.Sprintf("unknown(%d)", int(t))
	}
}

// Message is the envelope for all peer-to-peer traffic.
type Message struct {
	Type     MsgType           `json:"type"`
	Hello    *Hello            `json:"hello,omitempty"`
	Cluster  *ClusterConfig    `json:"cluster,omitempty"`
	Index    *IndexMsg         `json:"index,omitempty"`
	Request  *Request          `json:"request,omitempty"`
	Response *Response         `json:"response,omitempty"`
	Progress *DownloadProgress `json:"progress,omitempty"`
	Close    *Close            `json:"close,omitempty"`
}

// Hello identifies a device at connection start.
type Hello struct {
	DeviceID   uint64 `json:"device_id"`
	DeviceName string `json:"device_name"`
	// SessionID is a random per-process identifier (hex). Peers store it in
	// their delta markers: a marker is only valid while both processes stay
	// in the sessions it records, because in-memory peer views are wiped on
	// restart. Empty on old peers (treated as "unknown" → full exchange).
	SessionID string `json:"session_id,omitempty"`
	// Protocol is the protocol revision the sender supports: 0 (legacy,
	// single-frame index messages) or 1 (chunked index messages). A sender
	// only chunks when the peer advertised a revision >= 1, so rolling
	// upgrades between old and new daemons always fall back to the
	// single-frame format.
	Protocol int `json:"protocol"`
}

// ClusterConfig declares which folders are shared over the connection.
type ClusterConfig struct {
	Folders []ClusterFolder `json:"folders"`
}

// ClusterFolder describes one shared folder from a peer's perspective.
type ClusterFolder struct {
	ID      string   `json:"id"`
	Devices []uint64 `json:"devices"`
}

// IndexMsg carries a peer's view of a folder. If Update is true it is an
// incremental update (only the named entries change); otherwise it is a
// full index that supersedes all prior knowledge for the folder. IndexID
// identifies the sender's index generation; MaxSeq is the highest sequence
// in this batch. Together they let the receiver compute deltas. A folder's
// index may span several IndexMsg frames (see Final): each frame carries a
// slice of the entries, and the frame marked Final completes the folder.
type IndexMsg struct {
	Folder  string        `json:"folder"`
	Files   []FileMessage `json:"files"`
	Update  bool          `json:"update"`
	IndexID string        `json:"index_id"` // sender's index generation
	MaxSeq  int64         `json:"max_seq"`  // highest sequence in this batch
	// Final marks the last index frame for this folder in the current
	// exchange. A folder with few entries is a single frame (Final=true);
	// a large one is split so no single frame approaches maxMessageSize.
	Final bool `json:"final"`
}

// FileMessage is the wire form of a file entry.
type FileMessage struct {
	Name       string          `json:"name"`
	Size       int64           `json:"size"`
	ModifiedS  int64           `json:"modified_s"`
	ModifiedNs int32           `json:"modified_ns"`
	Mode       uint32          `json:"mode"`
	Type       int             `json:"type"`
	Deleted    bool            `json:"deleted"`
	Invalid    bool            `json:"invalid"`
	Version    version.Vector  `json:"version"`
	BlockSize  int32           `json:"block_size"`
	Blocks     [][]byte        `json:"blocks,omitempty"`
}

// Request asks a peer for one block of a file.
type Request struct {
	ID     uint64 `json:"id"`
	Folder string `json:"folder"`
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Size   int    `json:"size"`
}

// ResponseError codes for a block request.
const (
	RespNoError     = 0
	RespGeneric     = 1
	RespNoSuchFile  = 2
	RespInvalidFile = 3
)

// Response carries the requested block data or an error code.
type Response struct {
	ID   uint64 `json:"id"`
	Data []byte `json:"data,omitempty"`
	Code int    `json:"code"`
}

// DownloadProgress advertises which blocks of a file are locally available.
type DownloadProgress struct {
	Folder     string          `json:"folder"`
	Name       string          `json:"name"`
	Version    version.Vector  `json:"version"`
	HaveBlocks []int           `json:"have_blocks"`
}

// Close terminates the connection with a human-readable reason.
type Close struct {
	Reason string `json:"reason"`
}

// WriteMessage encodes m and writes it as a length-prefixed frame.
func WriteMessage(w io.Writer, m *Message) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(data) > maxMessageSize {
		return fmt.Errorf("message too large: %d bytes", len(data))
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// ReadMessage reads one length-prefixed frame and decodes it.
func ReadMessage(r io.Reader) (*Message, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n > maxMessageSize {
		return nil, fmt.Errorf("frame too large: %d bytes", n)
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("decoding frame: %w", err)
	}
	return &m, nil
}
