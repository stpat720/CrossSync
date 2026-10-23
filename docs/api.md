# CrossSync REST API

The control plane exposes a small JSON REST API plus a Server-Sent Events
(SSE) stream for live updates. It is served by the daemon itself — no
separate process — and is the same service consumed by the CLI, the MCP
server, and the embedded web UI.

## Enabling

The API is enabled by setting `control_listen` in `config.yaml`:

```yaml
control_listen: "0.0.0.0:55556"
```

Then start the daemon:

```sh
crosssync serve --config config.yaml
```

- Base URL: `http://<host>:<port>/api/...`
- Bind to `127.0.0.1` or a Tailnet IP for local/private access; the API has
  no authentication (the web UI served from the same listener is unauthenticated).
- The web UI itself is served at `/` on the same listener; all `/api/*`
  routes take precedence over it.

## Conventions

- All requests and responses are `application/json` unless noted (the SSE
  stream is `text/event-stream`).
- **CORS** is wide open for development convenience:
  `Access-Control-Allow-Origin: *`, methods `GET, POST, PUT, DELETE, OPTIONS`,
  headers `Content-Type`. `OPTIONS` requests return `204` without hitting handlers.
- **Errors** are always `{"error": "<message>"}` with a non-2xx status code.
- Timestamps are **Unix seconds** (integers), except event `TS`/`AckTS`
  fields which are RFC 3339 strings.
- Folder `id` and peer references accept either numeric device ids or
  configured peer *names* (case-insensitive).

---

## Endpoints

### Identity & status

#### `GET /healthz`

Container health probe. `200` while every folder with a configured peer has
completed a sync round within the last hour; `503` (with a `reason`) once one
has not. Read-only and cheap (per-folder atomics, no SQL). This is what catches
the failure that matters most in production — a share that silently stops
converging while the process stays alive. A single-node box (no peers) is
always healthy; staleness is only meaningful when a peer should be feeding the
folder.

```sh
curl -i http://127.0.0.1:55556/healthz
```

```json
{"status":"ok","version":"0.1.32","folders":1,"peers":1,
 "newest_sync_age_seconds":8,"oldest_sync_age_seconds":8}
```

Degraded responses add `"status":"degraded"` and a `reason`, e.g.
`"no completed sync round in over 1h0m0s — check peers and the last open events"`.

#### `GET /api/status`

Top-level health/identity snapshot. Also the payload of the SSE `hello`
event when a stream connects.

```sh
curl http://127.0.0.1:55556/api/status
```

Response — [`Status`](#status):

```json
{
  "device": "nas-01",
  "device_id": 234234,
  "version": "0.1.0",
  "tls": true,
  "fingerprint": "a3f9…",
  "open_events": 2,
  "bytes_up": 1048576,
  "bytes_down": 2097152,
  "last_sync": 1756000000,
  "folders": [ /* …FolderStatus */ ],
  "peers": [ /* …PeerStatus */ ],
  "scans": [ /* …ScanStatus */ ],
  "sync": { /* …SyncActivity */ },
  "ui_version": "3.0.0",
  "ui_override": false,
  "update_url": "https://…/latest.json",
  "update_auto": false,
  "conflict_files": 3,
  "conflict_bytes": 102400,
  "version_files": 10,
  "version_bytes": 512000,
  "auto_archive_enabled": true,
  "auto_archive_older_than": 7776000,
  "alert_url": "https://ntfy.sh/mytopic"
}
```

### Folders

#### `GET /api/folders`

List every synced folder with file/tombstone counts, transfer stats, peer
scoping and attention counts. Returns an array of [`FolderStatus`](#folderstatus).

```sh
curl http://127.0.0.1:55556/api/folders
```

#### `POST /api/folders`

Add a new folder and persist it to `config.yaml`. The `id` is the stable
sync key shared across servers; if omitted a random one is generated. An
empty `peers` list means the folder syncs with **all** peers.

```sh
curl -X POST http://127.0.0.1:55556/api/folders -H "Content-Type: application/json" -d '{
  "id": "",                 // optional; generated when empty
  "name": "Archive",        // human label shown in the UI
  "path": "/mnt/user/archive",
  "conflict": "conflict-copy", // conflict-copy | versioning (default conflict-copy)
  "peers": ["nas-02"]       // peer names or numeric ids; empty = all peers
}'
```

Response `201 Created` — the new [`FolderStatus`](#folderstatus).

Rules on `path`: it must be absolute, must already exist as a directory, and
must not overlap another folder's root — the same tree (or one nested inside
another folder's root) is refused with `400`, because two indexers over one
tree would double-count files, watch the same events twice and fight over
tombstones. Sibling trees are fine.

#### `PUT /api/folders/{id}`

Update a folder's `name` and/or `path`. The folder `id` (the sync key) never
changes; the index is preserved and the folder is re-scanned immediately.

```sh
curl -X PUT http://127.0.0.1:55556/api/folders/abc123 -H "Content-Type: application/json" -d '{
  "name": "Archive v2",
  "path": "/mnt/user/archive-v2"
}'
```

Response — the updated [`FolderStatus`](#folderstatus).

#### `PUT /api/folders/{id}/peers`

Set which peers a folder syncs with (peer scoping). Empty list = all peers.

```sh
curl -X PUT http://127.0.0.1:55556/api/folders/abc123/peers -H "Content-Type: application/json" -d '{
  "peers": ["nas-02", "nas-03"]
}'
```

Response — the updated [`FolderStatus`](#folderstatus).

#### `DELETE /api/folders/{id}`

Remove a folder from the node and the config file (the data on disk is not
deleted).

```sh
curl -X DELETE http://127.0.0.1:55556/api/folders/abc123
```

Response: `{"removed": "abc123"}`.

### Folder data

#### `GET /api/folders/{id}/files`

Every indexed, non-deleted file in the folder. Returns an array of
[`FileStatus`](#filestatus) (empty array if none).

```sh
curl http://127.0.0.1:55556/api/folders/abc123/files
```

#### `GET /api/folders/{id}/pending`

Files still to be pulled from peers to converge. Returns an array of
[`PendingFile`](#pendingfile).

```sh
curl http://127.0.0.1:55556/api/folders/abc123/pending
```

#### `GET /api/folders/{id}/conflicts`

The `.sync-conflict-*` copies currently on disk. Returns an array of
[`FileStatus`](#filestatus).

```sh
curl http://127.0.0.1:55556/api/folders/abc123/conflicts
```

#### `GET /api/folders/{id}/attention`

The default folder-detail view: every file that needs (or may need)
attention — pending pulls, pending uploads, conflict copies, and files with
open warn+ events. Returns an array of [`AttentionFile`](#attentionfile).

```sh
curl http://127.0.0.1:55556/api/folders/abc123/attention
```

#### `GET /api/folders/{id}/versions`

Every archived version in the folder's reserved `.sfx-versions` area.
Returns an array of [`VersionEntry`](#versionentry).

```sh
curl http://127.0.0.1:55556/api/folders/abc123/versions
```

### Folder actions

#### `POST /api/folders/{id}/rescan`

Scan one folder and return the number of changes applied. Errors if a scan
is already running for that folder.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/rescan -H "Content-Type: application/json" -d '{}'
```

Response: `{"abc123": 4}` (folder id → changes applied).

#### `POST /api/folders/{id}/sync`

Rescan one folder, then sync with every peer (rejected if a sync/scan is
already running).

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/sync -H "Content-Type: application/json" -d '{}'
```

Response: `{"synced": "abc123"}`.

#### `POST /api/folders/{id}/deletions/apply`

Legacy operator override for the deletion guard: apply every currently-pending
deletion for the folder. The guard no longer blocks an over-threshold pass (it
holds the copies instead, see the hold endpoints below), so nothing is normally
pending; the endpoint stays for leftover state.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/deletions/apply
```

Response: `{"applied": 12}`.

#### `GET /api/folders/{id}/hold`

List every held deletion of one folder (`count`, `bytes`, and the full
`entries` array). This is the only place the entry list is served: `/api/status`
carries just the `count`/`bytes` summary, because that payload is polled every
couple of seconds and the hold list is unbounded (a mirror-copy delete wave can
park tens of thousands of copies).

```sh
curl "http://127.0.0.1:55556/api/folders/abc123/hold"
```

Response: `{"count": 1515, "bytes": 53197712, "entries": [{"path": "...", "size": 3407, "held_at": 1792599438, "expires_at": 1793204238}, ...]}`.
A folder with nothing held answers `{"count": 0, "bytes": 0, "entries": []}`.

#### `POST /api/folders/{id}/hold/restore`

Restore one **held deletion**: a copy that an over-threshold delete pass parked
in `.sfx-hold` instead of unlinking. The file moves back to its original
relative path, is indexed again and re-syncs to every peer — including the
device that originally deleted it.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/hold/restore \
  -H "Content-Type: application/json" -d '{"path": "media/old/clip.mov"}'
```

Response: `{"restored": "media/old/clip.mov"}`. Errors if the path is not held
or something already exists at its original location.

#### `POST /api/folders/{id}/hold/delete`

Delete a held deletion now, giving up the undo window (the preserved bytes are
removed from this server). An empty `path` purges every held entry of the
folder.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/hold/delete \
  -H "Content-Type: application/json" -d '{"path": "media/old/clip.mov"}'
```

Response: `{"deleted": 1}`.

Held entries expire on their own: the maintenance pass purges anything held
longer than 7 days and records a `system` event.

#### `POST /api/folders/{id}/clean_versions`

Delete every archived version in the folder's `.sfx-versions` area (the
"cache handling" action for versioned files) and report the space freed.
Conflict copies are not touched.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/clean_versions
```

Response:

```json
{"folder": "abc123", "removed_files": 25, "removed_bytes": 1048576}
```

### Conflict handling

#### `POST /api/folders/{id}/conflicts/restore`

Make a conflict copy the winner: copy its content over the live file
(recorded as a local change that propagates to peers) and remove + tombstone
the copy.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/conflicts/restore -H "Content-Type: application/json" -d '{
  "name": "report.docx.sync-conflict-20260824-123456-234234.docx"
}'
```

Response: `{"restored": "<name>"}`.

#### `POST /api/folders/{id}/conflicts/compare`

Return both sides of a conflict (live winner vs the copy) with stats,
hashes and content previews, so the UI can show which is which.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/conflicts/compare -H "Content-Type: application/json" -d '{
  "name": "report.docx.sync-conflict-20260824-123456-234234.docx"
}'
```

Response — [`ConflictCompare`](#conflictcompare):

```json
{
  "current":  { "name": "report.docx", "exists": true, "size": 4096, "sha256": "…", "preview": "…", "produced_by": "nas-01 (this server)", "history": "nas-01 (×3), nas-02 (×2)" },
  "conflict": { "name": "report.docx.sync-conflict-…", "exists": true, "size": 5120, "sha256": "…", "preview": "…", "produced_by": "nas-02", "history": "nas-02 (×1)" },
  "identical": false
}
```

#### `POST /api/folders/{id}/conflicts/resolve_all`

Delete every conflict copy in the folder (current files stay winners; each
deletion propagates). Returns how many were resolved.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/conflicts/resolve_all
```

Response: `{"resolved": 3}`.

#### `DELETE /api/folders/{id}/files/{name...}`

Delete a single conflict copy (propagating the deletion). The `name` is a
`{name...}` wildcard, so it can contain slashes and must be a real conflict
copy. Equivalent to the per-copy "resolve" button in the UI.

```sh
curl -X DELETE "http://127.0.0.1:55556/api/folders/abc123/files/subdir/report.docx.sync-conflict-20260824-123456-234234.docx"
```

Response: `{"deleted": "<name>"}`.

### Version restore

#### `POST /api/folders/{id}/restore`

Copy an archived version back over the live file and record it as a new
local change so the restore propagates to every peer. The archive entry is
kept.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/restore -H "Content-Type: application/json" -d '{
  "path": "docs/report.docx",
  "archive_path": "docs/report.docx/simple/v3"
}'
```

Response: `{"restored": "docs/report.docx"}`.

### Events (durable event store)

#### `GET /api/events`

Query the append-only event store, newest first. All query parameters are
optional.

| Param | Meaning |
|---|---|
| `folder` | filter by folder id |
| `category` | `applied` \| `conflict` \| `versioned` \| `skipped` \| `unsynced` \| `error` \| `warning` \| `peer` \| `system` |
| `path` | filter by exact path |
| `open` | `1`/`true` = only events needing attention (open warn+) |
| `limit` | max events (default unlimited for the API; UI passes 50) |
| `after` | only events with id greater than this |
| `since` | only events at/after this unix timestamp |
| `peer` | peer name or id; events caused by that peer (or `self` for this device) |

```sh
curl "http://127.0.0.1:55556/api/events?open=1&limit=20"
curl "http://127.0.0.1:55556/api/events?folder=abc123&category=conflict"
```

Response — array of [`Event`](#event):

```json
[
  {
    "ID": 42,
    "TS": "2026-08-24T10:15:00Z",
    "Folder": "abc123",
    "Path": "docs/report.docx",
    "Category": "conflict",
    "Severity": 1,
    "Reason": "concurrent edit; conflict copy created",
    "Resolution": 0,
    "AckBy": "",
    "AckTS": null,
    "Linked": "",
    "PeerID": 234234
  }
]
```

#### `POST /api/events/{id}/ack`

Acknowledge one event. The record is kept and re-opens if the condition
persists.

```sh
curl -X POST http://127.0.0.1:55556/api/events/42/ack -H "Content-Type: application/json" -d '{"by": "admin"}'
```

Response: `{"id": 42, "acknowledged": true}`.

#### `POST /api/events/ack_condition`

Acknowledge every open occurrence of a condition (folder + path + category)
at once.

```sh
curl -X POST http://127.0.0.1:55556/api/events/ack_condition -H "Content-Type: application/json" -d '{
  "folder": "abc123",
  "path": "docs/report.docx",
  "category": "conflict",
  "by": "admin"
}'
```

Response: `{"acknowledged": true}`.

#### `GET /api/events/archive_preview`

Report what an archive would delete **without** doing it, so the UI can warn
before confirming.

| Param | Meaning |
|---|---|
| `older_than` | seconds; `0` = "older than the oldest open attention event" (the smart default that never archives anything still needing attention) |
| `include_open` | `1`/`true` = also count unviewed attention events |

```sh
curl "http://127.0.0.1:55556/api/events/archive_preview"
```

Response — [`ArchiveResult`](#archiveresult).

#### `POST /api/events/archive`

Permanently remove events older than the cutoff. Open attention/conflict
events are preserved unless `include_open` is true (caller should warn the
user first — the preview returns the real counts).

```sh
curl -X POST http://127.0.0.1:55556/api/events/archive -H "Content-Type: application/json" -d '{
  "older_than": 0,
  "include_open": false
}'
```

Response — [`ArchiveResult`](#archiveresult):

```json
{
  "deleted": 512,
  "deleted_open": 0,
  "kept_open": 2,
  "cutoff": 1755990000,
  "cutoff_text": "older than the oldest open attention event"
}
```

### Global actions

#### `POST /api/rescan`

Scan a folder (or all folders when `folder` is empty) and return the changes
applied per folder.

```sh
curl -X POST http://127.0.0.1:55556/api/rescan -H "Content-Type: application/json" -d '{}'
```

Response: `{"abc123": 4, "def456": 0}`.

#### `POST /api/sync`

Run a one-shot sync with a peer (`peer_id` 0 = all peers): rescan first,
then sync.

```sh
curl -X POST http://127.0.0.1:55556/api/sync -H "Content-Type: application/json" -d '{"peer_id": 0}'
```

Response: `{"synced": true}`.

### Settings

#### `PUT /api/settings/auto_archive`

Turn routine event-history auto-pruning on/off and set the age (seconds)
after which routine logs are removed. Open attention/conflict events are
never auto-pruned. Persisted to `config.yaml`.

```sh
curl -X PUT http://127.0.0.1:55556/api/settings/auto_archive -H "Content-Type: application/json" -d '{
  "enabled": true,
  "older_than": 7776000
}'
```

Response: `{"enabled": true, "older_than": 7776000}`.

#### `PUT /api/settings/alerts`

Set (or clear, with `""`) the ntfy/webhook endpoint for page-worthy alerts.
Persisted to `config.yaml`.

```sh
curl -X PUT http://127.0.0.1:55556/api/settings/alerts -H "Content-Type: application/json" -d '{"url": "https://ntfy.sh/mytopic"}'
```

Response: `{"url": "https://ntfy.sh/mytopic"}`.

#### `POST /api/alerts/test`

Fire a test notification through the configured endpoint.

```sh
curl -X POST http://127.0.0.1:55556/api/alerts/test
```

Response: `{"sent": true}`.

### Web-UI self-update

#### `GET /api/ui/check`

Check whether a newer web UI is available at `ui_update_url` (fetched and
compared). Fetch failures are reported in `error` rather than failing.

```sh
curl http://127.0.0.1:55556/api/ui/check
```

Response — [`UIUpdateInfo`](#uiupdateinfo):

```json
{
  "configured": true,
  "current": "3.0.0",
  "latest": "3.1.0",
  "update_available": true
}
```

#### `POST /api/ui/update`

Fetch a newer UI and atomically write it to `ui_dir` so the override takes
effect immediately (requires both `ui_update_url` and `ui_dir`).

```sh
curl -X POST http://127.0.0.1:55556/api/ui/update
```

Response: `{"updated": true, "version": "3.1.0"}`.

### Daemon self-update

#### `GET /api/update/check`

Report whether a newer daemon release is available at `update_url`. Add
`?refresh=1` to force a fresh manifest fetch instead of the throttled cache.

```sh
curl http://127.0.0.1:55556/api/update/check
```

Response — [`DaemonUpdateInfo`](#daemonupdateinfo):

```json
{
  "configured": true,
  "current": "0.1.0",
  "latest": "0.1.1",
  "update_available": true,
  "auto": false,
  "target": "/mnt/user/appdata/crosssync",
  "checked_at": 1756000000
}
```

#### `POST /api/update/apply`

Download, verify and swap in a newer daemon binary, then restart the process
in place. The response is sent **before** the swap so the client sees the
result; `config.yaml` and `meta_dir` are never touched.

```sh
curl -X POST http://127.0.0.1:55556/api/update/apply
```

Response: `{"updated": "0.1.1", "restarting": true}` (then the daemon
restarts ~800 ms later).

### Folder picker

#### `GET /api/browse`

List a directory for the folder-picker popup. Empty `path` starts at the
default root (`/mnt/user` on Linux, system drive on Windows). Only
directories are selectable; files are listed for context.

```sh
curl "http://127.0.0.1:55556/api/browse?path=/mnt/user"
```

Response — [`BrowseResult`](#browseresult):

```json
{
  "path": "/mnt/user",
  "parent": "/mnt",
  "entries": [
    { "name": "archive", "path": "/mnt/user/archive", "is_dir": true },
    { "name": "backup.txt", "path": "/mnt/user/backup.txt", "is_dir": false }
  ]
}
```

### Web UI

#### `GET /`

The single-page web UI (embedded, or overridden by `ui_dir`). All `/api/*`
routes take precedence over it.

### Live event stream (SSE)

#### `GET /api/events/stream`

Server-Sent Events feed of new events. On connect the server first sends a
`hello` event with the current [`Status`](#status), then an `event` event per
new event, plus a `ping` heartbeat every 15 seconds so proxies don't time
out the connection. A slow client never blocks the daemon (the subscription
drops rather than buffers forever).

```sh
curl -N http://127.0.0.1:55556/api/events/stream
```

```text
event: hello
data: {"device":"nas-01", …}

event: event
data: {"ID":43,"TS":"2026-08-24T10:16:00Z", …}

event: ping
data: {"ts":"2026-08-24T10:16:15Z"}
```

---

## Data types

### `Status`

| Field | Type | Meaning |
|---|---|---|
| `device` | string | this node's configured name |
| `device_id` | number | this node's short device id |
| `version` | string | daemon version |
| `tls` | bool | TLS transport enabled |
| `fingerprint` | string | this node's certificate fingerprint (when TLS) |
| `open_events` | number | events needing attention (open warn+) |
| `bytes_up` / `bytes_down` | number | totals across folders |
| `last_sync` | number? | max last-sync time over folders (unix seconds) |
| `folders` | [`FolderStatus`](#folderstatus)[] | per-folder summary |
| `peers` | [`PeerStatus`](#peerstatus)[] | per-peer summary |
| `scans` | [`ScanStatus`](#scanstatus)[] | live scan progress per folder |
| `sync` | [`SyncActivity`](#syncactivity) | live sync activity |
| `ui_version` | string | UI version currently served |
| `ui_override` | bool | served from `ui_dir` (not embedded) |
| `ui_update_url` | string? | configured UI self-update URL |
| `update_url` | string? | daemon self-update manifest URL |
| `update_auto` | bool | auto-apply daemon updates |
| `conflict_files` / `conflict_bytes` | number | global conflict-copy count/bytes |
| `version_files` / `version_bytes` | number | global archived-version count/bytes |
| `auto_archive_enabled` / `auto_archive_older_than` | bool / number | event-history auto-prune state |
| `alert_url` | string? | configured alert endpoint |

### `FolderStatus`

| Field | Type | Meaning |
|---|---|---|
| `id` | string | sync key shared across servers |
| `name` | string | human label (falls back to `id`) |
| `path` | string | absolute folder path |
| `files` / `directories` / `tombstones` | number | index counts |
| `size` | number | live file bytes |
| `pending_files` / `pending_bytes` | number | files/bytes to download from peers |
| `pending_up` | number | files peers need from us (uploads) |
| `bytes_up` / `bytes_down` / `files_down` / `syncs` | number | transfer stats |
| `last_sync` | number? | unix seconds of last sync |
| `conflict_files` / `conflict_bytes` | number | conflict copies on disk |
| `version_files` / `version_bytes` | number | archived versions in `.sfx-versions` |
| `open_events` | number | distinct open warn+ conditions for this folder |
| `peers` | number[] | peer ids this folder syncs with (empty = all) |
| `peers_all` | bool | true when `peers` is empty or covers every configured peer |
| `peer_names` | string[]? | configured names of `peers` |
| `shared_peers` | number | peers known to have this folder and allowed by scoping |
| `not_shared` | bool | peers exist but none (known) will sync this folder |
| `guard_tripped` | bool | legacy: the deletion guard no longer blocks (over-threshold passes hold their copies, see `hold`) |
| `pending_deletions` | number | live files globally deleted, waiting to be removed |
| `hold` | [`FolderHold`](#folderhold)? | soft-deleted copies kept in `.sfx-hold` (summary only: `count` + `bytes`; omitted when nothing is held — fetch `GET /api/folders/{id}/hold` for the entry list) |
| `peer_unsynced` | [`PeerUnsynced`](#peerunsynced)[]? | per peer, the open unsynced set that peer advertised at the end of its last successful round |

### `FolderHold`

The soft-deletion hold area of a folder. When a peer's delete pass exceeds the
folder's delete threshold, this device still applies the deletions (peers
delete their copies exactly as before) but MOVES its own copies into the
reserved `.sfx-hold/<relative path>` directory instead of unlinking them, so
the operator has an undo window. The hold area is never scanned, indexed,
watched or synced.

| Field | Type | Meaning |
|---|---|---|
| `count` | number | held paths |
| `bytes` | number | bytes preserved |
| `entries` | object[]? | held paths — **only** on `GET /api/folders/{id}/hold`; `/api/status` omits the list |

Each entry: `path` (original path relative to the folder root), `size`,
`held_at` (unix seconds) and `expires_at` (unix seconds, `held_at` + 7 days).

### `PeerUnsynced`

One peer's own view of what it has not synced in a shared folder, learned
from the end-of-round announcement (protocol rev 2). It is transient: it is
replaced wholesale on every round (an empty set means the peer is fully
synced and the block is omitted), and dropped when the peer is removed or a
session with it fails. Only peers that advertise rev 2 send it; older peers
simply never appear here.

| Field | Type | Meaning |
|---|---|---|
| `peer_id` | number | device id |
| `peer_name` | string | configured name |
| `connected` | bool | reachable within the last ~2 minutes |
| `entries` | [`UnsyncedFile`](#unsyncedfile)[] | paths that peer reports as not yet synced |

### `UnsyncedFile`

`{ "path": string, "reason?": string, "dir?": string }` — one path a peer
reports as unsynced, with the peer's own direction tag (`down` \| `up` \|
`both` \| absent), using the same vocabulary as `AttentionFile.dir`.

### `PeerStatus`

| Field | Type | Meaning |
|---|---|---|
| `id` | number | device id |
| `name` | string | configured name |
| `addresses` | string[] | tailnet IPs / host:port |
| `pinned` | bool | TLS fingerprint configured |
| `connected` | bool | reachable within the last ~2 minutes |
| `last_online` | number? | unix seconds peer was last reachable |
| `last_sync` | number? | unix seconds data last synced with peer |
| `known` | bool | we've seen this peer's folder list in a session |
| `known_folders` | string[]? | folder ids the peer advertises |

### `FileStatus`

| Field | Type | Meaning |
|---|---|---|
| `name` | string | path relative to folder root |
| `size` | number | bytes |
| `modified` | number | unix seconds |
| `type` | string | `file` \| `dir` \| `symlink` |
| `deleted` | bool | tombstoned |

### `PendingFile`

`{ "name": string, "size": number }` — one file still to be pulled.

### `AttentionFile`

| Field | Type | Meaning |
|---|---|---|
| `name` / `size` / `modified` / `type` | | same as `FileStatus` |
| `pending` | bool | still to be downloaded from a peer |
| `pending_up` | bool | a peer needs this file from us |
| `conflict` | bool | `.sync-conflict` copy |
| `conflict_of` | string? | live file the copy belongs to |
| `event` | bool | has an open warn+ event |
| `event_cat` | string? | category of the first open event |
| `reason` | string? | reason of the first open event |
| `dir` | string? | direction tag: `down` (we still need it from a peer), `up` (a peer still needs it from us), `both`, or absent for an event-only row with nothing pending either way |

### `FileCompare` / `ConflictCompare`

`FileCompare`: `name`, `size`, `created`, `modified` (unix seconds),
`produced_by?`, `history?` (version-vector summary), `sha256`, `preview`
(first bytes as text or a `[binary] <hex>` marker), `exists`.

`ConflictCompare`: `current` (`FileCompare` for the live winner),
`conflict` (`FileCompare` for the copy), `identical` (bool).

### `VersionEntry`

`{ "archive_path": string, "path": string, "size": number, "modified": number }`
— `archive_path` is relative to the versions area, `path` is the original
file it belongs to.

### `Event`

| Field | Type | Meaning |
|---|---|---|
| `ID` | number | event id |
| `TS` | string (RFC 3339) | when it happened |
| `Folder` | string | folder id (`peers` for connectivity events) |
| `Path` | string | file path (may be empty) |
| `Category` | string | `applied` \| `conflict` \| `versioned` \| `skipped` \| `unsynced` \| `error` \| `warning` \| `peer` \| `system` |
| `Severity` | number | `0` info, `1` warn, `2` error |
| `Reason` | string | human-readable reason |
| `Resolution` | number | `0` open, `1` acknowledged, `2` resolved, `3` auto-resolved |
| `AckBy` | string | who acknowledged ("" if none) |
| `AckTS` | string? (RFC 3339) | when acknowledged |
| `Linked` | string | optional linked state (e.g. the matching ignore rule) |
| `PeerID` | number | device that caused the event (0 = this device) |

### `ArchiveResult`

`{ "deleted": number, "deleted_open": number, "kept_open": number, "cutoff": number, "cutoff_text": string }`.

### `BrowseResult`

`{ "path": string, "parent": string, "entries": [{ "name": string, "path": string, "is_dir": bool }] }`.

### `ScanStatus`

`folder`, `running` (bool), `started`, `phase` (`walking` \| `hashing` \|
`deleting` \| `applying` \| `done`), `walked`, `hash_done`, `hash_total`,
`changed`, `finished`, `error` (all times unix seconds).

### `SyncActivity`

`running` (bool), `peer_id?`, `started`, `finished`, `error` (unix seconds).

### `UIUpdateInfo`

`{ "configured": bool, "current": string, "latest": string, "update_available": bool, "error?" : string }`.

### `DaemonUpdateInfo`

`{ "configured": bool, "current": string, "latest": string, "update_available": bool, "auto": bool, "target?" : string, "checked_at": number, "error?" : string }`.
