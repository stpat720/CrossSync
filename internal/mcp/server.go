// Package mcp implements a minimal Model Context Protocol server (JSON-RPC
// 2.0 over stdio, newline-delimited messages) exposing the CrossSync
// control plane as tools. It implements just enough of the protocol for
// real MCP clients: initialize, notifications/initialized, ping,
// tools/list, and tools/call.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"crosssync/internal/control"
	"crosssync/internal/events"
)

// ProtocolVersion is the MCP protocol version we advertise.
const ProtocolVersion = "2024-11-05"

// Server serves MCP requests from in to out.
type Server struct {
	svc     *control.Service
	in      io.Reader
	out     io.Writer
	version string
}

// New creates an MCP server wrapping the control service.
func New(svc *control.Service, in io.Reader, out io.Writer) *Server {
	v := "0.1.0"
	if svc != nil && svc.Version != "" {
		v = svc.Version
	}
	return &Server{svc: svc, in: in, out: out, version: v}
}

// rpcRequest is a JSON-RPC request or notification.
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

const (
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
)

// Run reads newline-delimited JSON-RPC messages and writes responses until
// EOF or ctx cancellation.
func (s *Server) Run(ctx context.Context) error {
	br := bufio.NewReaderSize(s.in, 1<<20)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			line = trimNewline(line)
			if resp, isResp := s.dispatch(ctx, line); isResp {
				if _, werr := fmt.Fprintf(s.out, "%s\n", resp); werr != nil {
					return werr
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// dispatch handles one message, returning the response line and whether a
// response is required (notifications get none).
func (s *Server) dispatch(ctx context.Context, line []byte) (string, bool) {
	var req rpcRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return s.error(nil, codeInvalidRequest, "invalid JSON-RPC message"), true
	}
	if req.Method == "" {
		return s.error(req.ID, codeInvalidRequest, "method required"), true
	}
	switch req.Method {
	case "initialize":
		return s.result(req.ID, map[string]any{
			"protocolVersion": ProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "crosssync", "version": s.version},
		}), true
	case "notifications/initialized", "notifications/cancelled":
		return "", false
	case "ping":
		return s.result(req.ID, map[string]any{}), true
	case "tools/list":
		return s.result(req.ID, map[string]any{"tools": toolDefinitions()}), true
	case "tools/call":
		return s.toolCall(ctx, req)
	default:
		return s.error(req.ID, codeMethodNotFound, fmt.Sprintf("method not found: %s", req.Method)), true
	}
}

func (s *Server) result(id json.RawMessage, result any) string {
	b, _ := json.Marshal(rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
	return string(b)
}

func (s *Server) error(id json.RawMessage, code int, message string) string {
	b, _ := json.Marshal(rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message}})
	return string(b)
}

// toolCall implements tools/call by name.
func (s *Server) toolCall(ctx context.Context, req rpcRequest) (string, bool) {
	var params struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil || params.Name == "" {
		return s.error(req.ID, codeInvalidParams, "tools/call requires name"), true
	}
	var text string
	var err error
	switch params.Name {
	case "status":
		text, err = marshalText(s.svc.Status())
	case "list_folders":
		text, err = marshalText(s.svc.Folders())
	case "list_events":
		var f events.Filter
		if err = json.Unmarshal(params.Arguments, &f); err != nil {
			err = fmt.Errorf("invalid arguments: %w", err)
		} else {
			evs, qerr := s.svc.Events(f)
			if qerr != nil {
				err = qerr
			} else {
				if evs == nil {
					evs = []events.Event{}
				}
				text, err = marshalText(evs)
			}
		}
	case "ack_event":
		var a struct {
			ID int64  `json:"id"`
			By string `json:"by"`
		}
		if err = json.Unmarshal(params.Arguments, &a); err != nil {
			err = fmt.Errorf("invalid arguments: %w", err)
		} else if a.ID == 0 {
			err = fmt.Errorf("id is required")
		} else {
			err = s.svc.Ack(a.ID, a.By)
			if err == nil {
				text = fmt.Sprintf("acknowledged event %d", a.ID)
			}
		}
	case "rescan":
		var a struct {
			Folder string `json:"folder"`
		}
		if err = json.Unmarshal(params.Arguments, &a); err != nil {
			err = fmt.Errorf("invalid arguments: %w", err)
		} else {
			var applied map[string]int
			applied, err = s.svc.Rescan(a.Folder)
			if err == nil {
				text, err = marshalText(applied)
			}
		}
	case "sync_now":
		var a struct {
			PeerID uint64 `json:"peer_id"`
		}
		if err = json.Unmarshal(params.Arguments, &a); err != nil {
			err = fmt.Errorf("invalid arguments: %w", err)
		} else {
			err = s.svc.SyncNow(a.PeerID)
			if err == nil {
				text = "sync completed"
			}
		}
	default:
		return s.error(req.ID, codeMethodNotFound, fmt.Sprintf("unknown tool: %s", params.Name)), true
	}
	if err != nil {
		return s.error(req.ID, codeInternalError, err.Error()), true
	}
	return s.result(req.ID, map[string]any{
		"content": []map[string]string{{"type": "text", "text": text}},
		"isError": false,
	}), true
}

// toolDefinition describes one tool for tools/list.
type toolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

func toolDefinitions() []toolDefinition {
	return []toolDefinition{
		{Name: "status", Description: "Node identity, TLS, peers, folders, and the number of open (needs-attention) events.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}}},
		{Name: "list_folders", Description: "List synced folders with file and tombstone counts.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}}},
		{Name: "list_events", Description: "Query the durable event store (conflicts, skips, applied changes, peer state, errors).", InputSchema: map[string]any{"type": "object", "properties": map[string]any{
			"folder":   map[string]any{"type": "string", "description": "filter by folder id"},
			"category": map[string]any{"type": "string", "description": "applied|conflict|versioned|skipped|unsynced|error|warning|peer|system"},
			"open":     map[string]any{"type": "boolean", "description": "only events needing attention"},
			"limit":    map[string]any{"type": "integer", "description": "max events (default 50)"},
			"after":    map[string]any{"type": "integer", "description": "only events with id greater than this"},
		}}},
		{Name: "ack_event", Description: "Acknowledge an event. The record is kept and re-opens if the condition persists.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{
			"id": map[string]any{"type": "integer", "description": "event id"},
			"by": map[string]any{"type": "string", "description": "who is acknowledging"},
		}, "required": []string{"id"}}},
		{Name: "rescan", Description: "Trigger a scan of a folder (or all folders), returning changes applied per folder.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{
			"folder": map[string]any{"type": "string", "description": "folder id (empty = all)"},
		}}},
		{Name: "sync_now", Description: "Run a one-shot sync with a peer (or all peers).", InputSchema: map[string]any{"type": "object", "properties": map[string]any{
			"peer_id": map[string]any{"type": "integer", "description": "peer id (0 = all peers)"},
		}}},
	}
}

func marshalText(v any) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	return string(b), err
}

func trimNewline(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	if len(b) > 0 && b[len(b)-1] == '\r' {
		b = b[:len(b)-1]
	}
	return b
}
