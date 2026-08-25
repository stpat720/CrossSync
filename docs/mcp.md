# CrossSync MCP Server

CrossSync ships a [Model Context Protocol](https://modelcontextprotocol.io)
(MCP) server that exposes the CrossSync control plane as tools for AI
assistants (Claude Desktop, VS Code Copilot, etc.). It lets an assistant
inspect node status, list folders and events, acknowledge events, and
trigger rescans/syncs — the same operations the web UI and REST API expose,
served over the MCP tool protocol.

The MCP server is a minimal, self-contained implementation of MCP: JSON-RPC
2.0 over **stdio** with newline-delimited messages. It implements just
enough of the protocol for real MCP clients: `initialize`,
`notifications/initialized`, `ping`, `tools/list`, and `tools/call`.

## Running it

```sh
crosssync mcp --config <config.yaml>
```

The server reads the same `config.yaml` as the daemon, builds its own node,
and reads JSON-RPC requests from stdin, writing responses to stdout. It does
**not** need the daemon (`crosssync serve`) to be running — it loads the
node and event store directly from the config/meta dirs.

It is designed to be spawned by an MCP client rather than run in a
terminal. Client configuration examples:

### Claude Desktop

`claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "crosssync": {
      "command": "/path/to/crosssync",
      "args": ["mcp", "--config", "/mnt/user/appdata/crosssync/config.yaml"]
    }
  }
}
```

### Generic MCP clients

Point the client at the `crosssync mcp --config <file>` command with
stdio transport. No HTTP, no port, no credentials — the daemon's control
API does not need to be listening.

## Protocol

- **Transport:** stdio, one JSON-RPC message per line (LF; CRLF tolerated).
- **Protocol version:** `2024-11-05`.
- **Server info:** `{ "name": "crosssync", "version": "<daemon version>" }`.
- **Capabilities:** `{ "tools": {} }`.

### Supported methods

| Method | Purpose |
|---|---|
| `initialize` | Handshake; returns protocol version, capabilities, server info |
| `notifications/initialized` | Client ack (no response) |
| `notifications/cancelled` | Tolerated (no response) |
| `ping` | Liveness check; returns `{}` |
| `tools/list` | List the available tools and their input schemas |
| `tools/call` | Invoke a tool by name |

Anything else returns a JSON-RPC error `-32601 method not found`.

### Errors

Standard JSON-RPC error codes: `-32600` invalid request, `-32601` method
not found, `-32602` invalid params, `-32603` internal error (tool failures
surface here with the underlying message).

## Tools

### `status`

Node identity, TLS, peers, folders, and the number of open (needs-attention)
events. No arguments. Equivalent to `GET /api/status`.

### `list_folders`

List synced folders with file and tombstone counts. No arguments.
Equivalent to `GET /api/folders`.

### `list_events`

Query the durable event store (conflicts, skips, applied changes, peer
state, errors). All arguments optional:

| Argument | Type | Meaning |
|---|---|---|
| `folder` | string | filter by folder id |
| `category` | string | `applied` \| `conflict` \| `versioned` \| `skipped` \| `unsynced` \| `error` \| `warning` \| `peer` \| `system` |
| `open` | boolean | only events needing attention |
| `limit` | integer | max events (default 50) |
| `after` | integer | only events with id greater than this |

Equivalent to `GET /api/events`.

### `ack_event`

Acknowledge an event. The record is kept and re-opens if the condition
persists.

| Argument | Type | Required |
|---|---|---|
| `id` | integer | yes — event id |
| `by` | string | no — who is acknowledging |

### `rescan`

Trigger a scan of a folder (or all folders), returning changes applied per
folder.

| Argument | Type | Meaning |
|---|---|---|
| `folder` | string | folder id (empty = all) |

### `sync_now`

Run a one-shot sync with a peer (or all peers).

| Argument | Type | Meaning |
|---|---|---|
| `peer_id` | integer | peer id (0 = all peers) |

## Example session

A raw exchange (request → response), e.g. `list_folders`:

**Request:**

```json
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_folders","arguments":{}}}
```

**Response:**

```json
{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"[\n  {\n    \"id\": \"abc123\",\n    \"name\": \"Archive\",\n    \"path\": \"/mnt/user/archive\",\n    \"files\": 1204,\n    \"tombstones\": 3\n  }\n]" }],"isError":false}}
```

Tool results are always wrapped in an MCP content block:

```json
{
  "content": [{ "type": "text", "text": "<pretty-printed JSON>" }],
  "isError": false
}
```

## Relationship to the REST API and CLI

All three entry points wrap the same `control.Service`:

- **REST API** (`internal/control/rest.go`) — HTTP + SSE for the web UI.
- **CLI** (`crosssync status`, `events`, `ack`, `scan`, `sync`, …) — one-shot
  terminal commands.
- **MCP server** (`internal/mcp/server.go`) — the same service exposed as
  tools over stdio.

Changes made through any interface (acknowledging an event, rescaling a
folder, triggering a sync) are visible through the others, since they share
the node, the folder indexes, and the durable event store.

## Implementation notes

- `internal/mcp/server.go` — the server: reads newline-delimited JSON-RPC
  from stdin, dispatches, writes responses to stdout.
- `internal/mcp/server_test.go` — protocol-level tests.
- Because the MCP server builds its own node, it uses the same SQLite event
  store and indexes as a running daemon would; concurrent access is safe
  (SQLite WAL, `busy_timeout`).
