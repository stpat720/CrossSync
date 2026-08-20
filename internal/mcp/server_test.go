package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crosssync/internal/config"
	"crosssync/internal/control"
	"crosssync/internal/mcp"
	"crosssync/internal/node"
)

func newNode(t *testing.T) *node.Node {
	t.Helper()
	cfg := &config.Config{
		Device:  config.Device{ID: 1, Name: "nas-01"},
		MetaDir: t.TempDir(),
		Folders: []config.Folder{{ID: "data", Path: t.TempDir(), ConflictPolicy: "conflict-copy"}},
	}
	n, err := node.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

// run feeds newline-delimited JSON-RPC messages and returns the responses.
func run(t *testing.T, in string) []string {
	t.Helper()
	n := newNode(t)
	svc := control.New(n, "test")
	var out bytes.Buffer
	s := mcp.New(svc, strings.NewReader(in), &out)
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

func TestMCPInitializeAndListTools(t *testing.T) {
	lines := run(t, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n"+
		"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n")

	if len(lines) != 2 {
		t.Fatalf("expected 2 responses, got %d: %v", len(lines), lines)
	}
	var initResp struct {
		ID     json.RawMessage `json:"id"`
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
			ServerInfo      struct {
				Name    string `json:"name"`
				Version string `json:"version"`
			} `json:"serverInfo"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &initResp); err != nil {
		t.Fatal(err)
	}
	if string(initResp.ID) != "1" || initResp.Result.ProtocolVersion == "" || initResp.Result.ServerInfo.Name != "crosssync" {
		t.Fatalf("bad initialize response: %s", lines[0])
	}

	var toolsResp struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &toolsResp); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tl := range toolsResp.Result.Tools {
		names[tl.Name] = true
	}
	for _, want := range []string{"status", "list_folders", "list_events", "ack_event", "rescan", "sync_now"} {
		if !names[want] {
			t.Fatalf("tools/list missing %q: %v", want, names)
		}
	}
}

func TestMCPNotificationGetsNoResponse(t *testing.T) {
	lines := run(t, "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
	if len(lines) != 0 {
		t.Fatalf("notification should not produce a response, got: %v", lines)
	}
}

func TestMCPToolCallStatus(t *testing.T) {
	n := newNode(t)
	// Create a file and scan so the folder shows a file and an event exists.
	os.WriteFile(filepath.Join(n.Folders["data"].Root, "a.txt"), []byte("x"), 0o644)
	n.ScanFolder("data")

	svc := control.New(n, "test")
	var out bytes.Buffer
	s := mcp.New(svc, strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"status\",\"arguments\":{}}}\n"), &out)
	if err := s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Result struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil {
		t.Fatalf("tools/call status returned error: %s", resp.Error.Message)
	}
	if len(resp.Result.Content) != 1 || !strings.Contains(resp.Result.Content[0].Text, `"device": "nas-01"`) {
		t.Fatalf("unexpected status tool output: %s", out.String())
	}
}

func TestMCPToolCallUnknown(t *testing.T) {
	lines := run(t, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"bogus\"}}\n")
	var resp struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil || resp.Error.Code != -32601 {
		t.Fatalf("expected method-not-found error, got: %s", lines[0])
	}
}
