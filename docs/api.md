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

The daemon binds this API **before** its first index pass, so the probe is
answerable from the first second after a restart. While that pass is still
running for every folder it answers `200` with `"status":"indexing"` and a
`reason`: being indexed is not a failure, and a container that reported `503`
through a 40-minute first walk would be restarted mid-hash. The moment one
folder has finished its first pass the probe judges normally again (`ok` /
`degraded`), folder by folder — a folder still indexing does not mask a sibling
that has genuinely stopped converging.

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
  "startup_scanning": false,
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
  "alert_url": "https://ntfy.sh/mytopic",
  "pull_concurrency": 4,
  "hash_workers": 8,
  "max_folder_sessions_per_peer": 4
}
```

`open_events`, the per-folder counts (`files`, `directories`, `size`,
`tombstones`, `conflict_files`, `conflict_bytes`, `open_events`) and the
per-folder disk figures (`free_bytes`, `version_files`, `version_bytes`) are
**served from cache**, refreshed by the daemon's ~5 s background ticker (and
right after a scan completes, which is what moves the index counts). The handler
therefore does no SQL and no filesystem I/O at all, and a change can take up to
~5 s to appear. `pull_concurrency` is the live K the next pull batch will use,
`hash_workers` is the live reader pool the next scan will use, and
`max_folder_sessions_per_peer` is how many folders of one peer may sync at the
same time (all three are changed by `PUT /api/settings/sync`; `hash_workers` is
`0` in `config.yaml` when unset, which means one reader per CPU, and is always
reported here as the effective number).

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

`pull_fsync` (optional, bool) is the folder's **pull durability** knob, and it is
the one field where omitting it is a real answer: absent = leave the folder's
setting alone, so a client that does not know the field can never change it.
`"pull_fsync": false` turns fsync off for this folder's pulls and records the
trade in the config (`pull_fsync: false`); `"pull_fsync": true` restores the
default and removes the key from the config. The new value applies to the **next
file pulled**, including on a round that is already running — it is read at commit
time, not cached when the folder was created.

```sh
curl -X PUT http://127.0.0.1:55556/api/folders/abc123 -H "Content-Type: application/json" -d '{
  "name": "Archive v2",
  "path": "/mnt/user/archive-v2",
  "pull_fsync": false
}'
```

Response — the updated [`FolderStatus`](#folderstatus), whose `pull_fsync` is the
**effective** value. Read [Pull durability](#pull-durability) before turning it
off.

#### Pull durability

`pull_fsync` (default **on**, including when the key is absent from the config)
decides whether a pulled file is `fsync`ed before it is renamed into place:

* **on** — the file's bytes are on stable storage before it appears under its
  real name, so "committed" means "survives a power cut". This is the default and
  the reason the knob is a pointer in the config: an absent key must never
  disable durability by accident, and a config written before the knob existed
  keeps it.
* **off** — the fsync is skipped. It is a throughput/durability trade, not a
  tuning tweak: the rename can land while the data is still only in the page
  cache, so a power loss (or a hard reset) can leave a file that is shorter than
  what the index says, or whose tail was never written. The next scan re-hashes
  it and the file is pulled again, so the cluster converges — but there is a
  window in which a file that the daemon reported as synced is not.

What is *not* affected either way: the temp is always renamed atomically (no
partial file ever appears under the real name), the index is only written after
the rename, and a pull whose blocks fail verification is never committed.
On a share that cannot lose power mid-write (a UPS-backed array) the trade buys
the fsync of every pulled file; on anything else, leave it on.

`/metrics` carries `crosssync_pull_fsyncs_total` next to `crosssync_pull_files_total`,
so a device that stops fsyncing is visible rather than assumed.

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

Scan one folder and return the number of changes applied. Errors if a scan is
already running for that folder.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/rescan -H "Content-Type: application/json" -d '{}'
```

Response: `{"abc123": 4}` (folder id → changes applied).

A folder whose scan is already running — including its **first index pass after
a start**, which is the case an operator hits most (a restarted node indexes for
minutes to hours) — is not scanned a second time. That answers `202
{"started":false,"already_running":true,"folder":"abc123","verify":false,
"note":"…"}`, not an error: nothing failed, the pass in flight is doing the
work, and the request is deliberately **not** queued. Watch it in
`/api/status` → `scans[]` and ask again when it finishes. This is per folder: a
different folder can be rescanned, synced and reported while a slow one is still
being indexed.

A **paused** folder is not scanned now and the request is not thrown away: it is
recorded and runs *exactly once* when the folder is resumed. Ask for it again
while paused and the requests coalesce into that one pass.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/rescan -H "Content-Type: application/json" -d '{"verify": true}'
```

Response: `202 {"queued": true, "folder": "abc123", "verify": true, "note": "folder is paused — this rescan will run once when it is resumed"}`.
`POST /api/rescan` (all folders) answers `202 {"applied": {...}, "queued": ["abc123"]}`
in that case, and reports a queued folder as `-1` in `applied`.

**Verified scan.** `{"verify": true}` re-hashes **every** file instead of
trusting `(size, mtime)`. Use it after a restore or an `rsync -a`-style copy,
because those preserve both size and timestamp — a content change is then
invisible to the normal scan (the fast path trusts the stat; SMB/exFAT
timestamp granularity makes this worse). Files whose content is re-hashed and
found identical record their new stat **without a version bump**, so a verified
scan never re-transfers data to peers.

Because it reads every byte, a verified scan is **asynchronous**: the call
answers `202 Accepted` with `{"started":true,"folder":"abc123","verify":true}`
and the scan runs in the background. Watch it in `GET /api/status` → `scans[]`
(`phase`, `hash_done`, `hash_total`) or the folder row in the UI. It is a
maintenance action and the folder's sync waits for it to finish: budget roughly
1 minute per 8 GB of data (~140 MB/s measured, 4m46s for 4,355 files / 39 GB on
an idle array) and much longer when the array is busy. Restarting the daemon
aborts it (nothing is lost: an interrupted scan only ever adopts stats it had
already verified). Unchanged files are reported as **zero changes**, not as
"everything changed".

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/rescan \
  -H "Content-Type: application/json" -d '{"verify": true}'
```

CLI equivalent (synchronous, so it prints the applied count when it finishes):
`crosssync scan --config <file> --verify [folder-id]`.

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

#### `POST /api/folders/{id}/pause`

Pause or resume **scanning and syncing** for one folder on this device. Paused,
the folder is not scanned (local edits are not indexed) and takes no part in any
peer round — it is not advertised, so peers see it as *not shared* instead of
erroring on transfers. Files and the index are untouched: pausing never deletes
anything, and resuming reconciles with an ordinary round.

The setting is per-device (like `direction`/`include`) and persisted to
`config.yaml` as `paused: true`, so a restart keeps the folder paused.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/abc123/pause \
  -H "Content-Type: application/json" -d '{"paused": true}'
```

Response: the minimal pause record — `{"id": "abc123", "paused": true, "transfer_paused": false}`.
Deliberately **not** a full [`FolderStatus`](#folderstatus): building one costs
per-folder SQL aggregates plus a `statfs` on the share (measured at ~3.3 s on a
34k-file Unraid folder), and this is the request whose whole purpose is to stop
disk I/O immediately. Everything else is on the next `/api/status` poll.

`"paused"` is required — `{}` is a `400`, never a silent resume. A rescan
requested while the folder is paused is **queued** and runs once on resume (see
`POST /api/folders/{id}/rescan`). An explicit sync-now on a paused folder answers
`400` with `folder is paused — resume it to scan or sync`.

Two levels: `{"transfers": true}` stops only data movement — scanning continues,
so the index stays current and resuming needs no re-walk — while
`{"paused": true}` stops everything, including a scan that is already running
(it abandons within about a second and applies nothing).

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

A folder that is already scanning (including its first index pass after a start)
answers `409 Conflict` with the reason instead of a `500` — not a fault. The
walk stops there, so with a large share being indexed rescan the folders you
care about one at a time (`POST /api/folders/{id}/rescan`) rather than all of
them.

#### `POST /api/sync`

Run a one-shot sync with a peer (`peer_id` 0 = all peers): rescan first,
then sync.

```sh
curl -X POST http://127.0.0.1:55556/api/sync -H "Content-Type: application/json" -d '{"peer_id": 0}'
```

Response: `{"synced": true}`.

The request starts one session per (peer, folder) — see
[Sync sessions and folder independence](#sync-sessions-and-folder-independence) —
and answers as soon as those sessions have their answer from the peer, rather than
holding the HTTP request for the length of the slowest transfer. `/api/status`
(`sync.running`, `folders[].last_sync`) and `crosssync_sync_sessions` show them
running; a folder already syncing is skipped, not queued twice. The `crosssync
sync` CLI is the blocking form: it waits for every session it started to finish.

#### `POST /api/restart`

Re-execute the daemon without reinstalling it. It exists because the only restart
used to be `/api/update/apply`, i.e. downloading and installing the same version,
which needs the update server reachable exactly when the daemon is misbehaving.

```sh
curl -X POST http://127.0.0.1:55556/api/restart
```

Response: `202 {"restarting": true}`. The exec happens ~300ms later, so the reply
is not cut off by it. On Linux the process is replaced (same PID); on Windows a
new process is spawned and the old one exits.

## Sync sessions and folder independence

Separate folders are separate syncs. A session between two nodes carries exactly
**one folder** — it advertises that folder in its cluster config, so the peer's
folder intersection is that folder and both directions cover it over that one
connection — and the session slot is keyed by `(peer, folder)`.

What that buys: a folder with work starts at once, on its own connection with its
own transfer pipeline, and neither waits for another folder to finish before it
starts or before it reports. What it still guarantees: **no two rounds on the same
folder with the same peer**, inbound or outbound. That exclusivity is not
bookkeeping — two writers staging the same paths to the same deterministic `.part`
temp is what produced the live `chtimes …/.sfx-*.part: no such file or directory`
churn.

- **Scheduling.** Each tick, every folder it may sync is offered a session, least
  recently synced first, up to `node.max_folder_sessions_per_peer` at a time. A
  folder already syncing is skipped, never queued twice; a peer that declines
  because a folder's slot is held is reachable-and-busy (`ErrPeerBusy`), not a
  failure, while a peer that answers nothing backs off exactly as before.
- **Inbound.** The initiator's cluster config is read FIRST, so the responder
  claims slots only for the folders the session really covers and advertises
  exactly those. A folder whose slot is held is left out; if every folder the
  initiator wants is already syncing, it answers the busy `Close` immediately
  instead of parking an idle connection.
- **Memory ceiling.** Each session is its own connection with its own pull
  pipeline, so the per-session budget is multiplied by the cap:
  `max_folder_sessions_per_peer × pull_inflight_mb` (16 MiB default) = **64 MiB
  per peer at the defaults**, 128 MiB at the cap of 8 — plus the block windows
  (`pull_concurrency × 8` blocks per session) and the responder's serve pool
  (4 blocks per session). At `K = 4` with 256 KiB blocks that is ~32 MiB of block
  buffers per peer; the pathological ceiling (K = 8, 16 MiB blocks) is 1 GiB per
  session, which is why the cap is 8 and not unbounded.
- **`max_folder_sessions_per_peer: 1`** restores the old shape: one folder at a
  time per peer, still round-robining across folders (least recently synced
  first) so no folder is starved.
- **Old peers.** An old daemon advertises its whole folder list in one session.
  The intersection is whatever that daemon lists, so the new side takes slots for
  each of those folders (or declines those already syncing) and the round is the
  old multi-folder one. No protocol change is involved: a cluster config is a
  folder list either way.

## Drift check

`POST /api/folders/{id}/drift` walks the share and compares it with the index,
changing nothing: no settling, no deletions, no writes to either side. It answers
which index entries have no file, and which files have no entry.

```sh
curl -X POST http://127.0.0.1:55556/api/folders/7szmpg7h6xzloozo/drift
```

```json
{"folder":"7szmpg7h6xzloozo","on_disk":889,"indexed":889,
 "missing_count":0,"unknown_count":0,"checked":"2026-10-31T14:22:01Z"}
```

Up to 50 names are returned per side (`missing`, `unknown`) so a caller has
somewhere to start; the counts are exact. Non-zero counts also appear in the
folder status as `drift_missing`/`drift_unknown`, as the
`crosssync_folder_drift_*` gauges, and (once a day, from maintenance) as a
warning event that clears itself when a later check is clean.

A scan reconciles the two sides, so a clean folder answers 0/0. A non-zero answer
means one of: a name the walk deliberately ignores (see `ignored_bytes`), a file
that vanished while nothing was scanning, or a pass that was cut short.
## Authentication

By default the control plane has none, which is why it should listen on loopback
or a tailnet address. Set `control_token` in `config.yaml` to require one on every
request:

```yaml
control_token: "a long random string"
```

```sh
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:55556/api/status
```

The web UI asks for the token once and keeps it in localStorage. SSE
(`/api/events/stream`) and download links (`/api/backup`) accept `?token=` because
the browser cannot attach a header to them. A wrong or missing token answers
`401 {"error": "control token required", "code": "unauthorized"}`. A listener on
a non-loopback address with no token logs a warning at startup (not fatal, so an
existing deployment is not broken by upgrading).

#### `GET /metrics`

Prometheus text exposition (`text/plain; version=0.0.4`) of the daemon's
operational numbers, so alerting does not have to parse `/api/status`:

- `crosssync_build_info{version,device,device_id}` — 1
- `crosssync_folder_files|directories|tombstones|bytes{folder}` — index gauges
- `crosssync_folder_pending_files|pending_up{folder}` — queue depth
- `crosssync_folder_paused{folder}` — 0/1
- `crosssync_folder_last_sync_timestamp_seconds{folder}` — 0 = never
- `crosssync_folder_bytes_up_total|bytes_down_total|files_down_total|sessions_total{folder}` — counters
- `crosssync_peer_connected{peer,peer_id}` / `crosssync_peer_last_sync_timestamp_seconds{peer}`
- `crosssync_open_events`, `crosssync_sync_running`
- `crosssync_sync_sessions` / `crosssync_sync_sessions_cap` — folder sync sessions
  in flight right now (one per peer **and** folder) and the configured ceiling per
  peer. Above 1 means separate folders are really moving data side by side; the
  cap is `node.max_folder_sessions_per_peer`.
- `crosssync_pull_stage_seconds_total{folder,stage}` — seconds spent per pull stage,
  summed over committed pulls: `wait_first_block` (batch admitted the file → its
  first block was written), `transfer` (first block → last block), `fsync` (last
  block → `Pull.Finish`'s fsync returned), `commit` (fsync → atomic rename + index
  write done). A file small enough to be asked for in ONE coalesced run has no
  measurable `transfer`: its first and last block land in the same frame, so its
  whole wire time shows up in `wait_first_block`.
- `crosssync_pull_files_total{folder}` — committed pulls those stage totals cover
- `crosssync_pull_stage_seconds_per_file{folder,stage}` — the derived average per
  file, i.e. the per-file cost split by stage. Summing the four is the average
  lifetime a pulled file costs; measured live at ~9-10 s fixed plus
  size ÷ ~8.6 MB/s per stream, this is the field that says WHICH part that fixed
  cost is.
- `crosssync_pull_batch_seconds_total{folder}` / `crosssync_pull_batches_total{folder}`
  — wall time of whole pull batches and how many ran
- `crosssync_pull_index_seconds_total{folder}` / `crosssync_pull_index_writes_total{folder}`
  / `crosssync_pull_index_entries_total{folder}` — the batched index write that
  records committed pulls: one SQLite transaction per batch rather than one per
  file. Entries ÷ writes is the batching factor (it is 1 again if this ever
  regresses), and seconds ÷ writes is what one commit costs.
- `crosssync_pull_fsyncs_total{folder}` — pulls that fsynced their file before the
  rename. It equals `crosssync_pull_files_total` unless the folder set
  `pull_fsync: false` (see [Pull durability](#pull-durability)), so a device that
  stopped fsyncing is visible rather than assumed.

```sh
curl http://127.0.0.1:55556/metrics
```

It is cheap by design (one aggregate query per folder, no directory walks), so a
15–60s scrape interval is fine.

#### `GET /api/backup`

Download this node's restore-critical state as a `tar.gz` attachment:

- `config.yaml` — topology, folders, peers, settings (comments preserved)
- `cert.pem` / `key.pem` — the TLS identity, when TLS is enabled
- `MANIFEST.txt` — what is inside, what is deliberately **not**, and restore steps

Only identity and topology are included. Per-folder index DBs are derived from
the filesystem (a missing or corrupt index is rebuilt by the first scan) and
`events.db` is history, so neither is exported — copying a live SQLite file out
of a running daemon is how "backups" end up restoring corrupt.

```sh
curl -OJ http://127.0.0.1:55556/api/backup
```

> ⚠ The archive contains the node's **private key**: whoever holds it can
> impersonate this node to its peers. Store it like a password. Restoring
> `cert.pem` + `key.pem` (from `meta_dir`) keeps the fingerprint, so no peer
> needs re-pairing — losing them means every peer must re-pin the new print.

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

#### `PUT /api/settings/sync`

Change sync and scan tuning on the **running** daemon and persist it to
`config.yaml`, so tuning needs no shell and no restart. Intended for the
production boxes, which have no reachable shell: `pull_concurrency` used to
require editing the file and restarting.

```sh
curl -X PUT http://127.0.0.1:55556/api/settings/sync -H "Content-Type: application/json" -d '{"pull_concurrency": 8}'
curl -X PUT http://127.0.0.1:55556/api/settings/sync -H "Content-Type: application/json" -d '{"hash_workers": 8}'
curl -X PUT http://127.0.0.1:55556/api/settings/sync -H "Content-Type: application/json" -d '{"max_folder_sessions_per_peer": 2}'
```

| Field | Type | Meaning |
|---|---|---|
| `pull_concurrency` | number? | how many FILES a round pulls at once over one peer connection (K). Clamped to `1..8`. **Omitted = leave unchanged** — an absent field is not a request to lower it. |
| `hash_workers` | number? | how many files the index hasher reads and hashes **at the same time**. Clamped to `1..NumCPU` (`0` and negatives mean the minimum, `1`; anything above the CPU count is pulled down to it). **Omitted = leave unchanged.** |
| `max_folder_sessions_per_peer` | number? | how many **folders** of one peer may sync at the same time (default `4`, clamped to `1..8`; `0` and negatives mean the minimum, `1`). Each folder gets its own session, connection and transfer pipeline, so this is the concurrency ceiling; `1` restores the old one-folder-at-a-time behaviour. **Omitted = leave unchanged.** |

Response: `{"pull_concurrency": 8, "hash_workers": 8, "max_folder_sessions_per_peer": 4}` —
the values actually in effect after the request (a clamped request answers with
what it clamped to).

- `pull_concurrency` applies to the **next** pull batch; a batch already in
  flight keeps the K it was admitted with, so a change cannot raise memory past
  the `K x 8` block-buffer budget mid-round.
- `hash_workers` applies to the **next scan**: the pool is sampled once, when the
  hash phase of a pass starts, so a pass already running keeps the readers it
  launched and a change cannot alter a scan in progress. It is the lever for a
  node whose folder lives on one spinning disk — 37 readers on one platter
  delivered ~55-70 MB/s at 100 % utilisation (864 reads/s at ~126 kB, 27.8 ms
  `r_await`) while the same corpus hashed at ~156 MB/s on an idle node. Which
  value wins is measured, never assumed: see
  [hash-throughput.md](hash-throughput.md) for the 4 / 8 / 16 / 32 runbook.
- `max_folder_sessions_per_peer` applies to the **next scheduling decision**: the
  cap is sampled when a session slot is claimed, so lowering it starts no new
  sessions until the live count drops under it and never disturbs one in flight.
  See [Sync sessions and folder independence](#sync-sessions-and-folder-independence)
  for what it costs.
- Persisted as the top-level `pull_concurrency` key, the nested
  `scan.hash_workers` key and the nested `node.max_folder_sessions_per_peer` key
  (`config.yaml`) — the same keys `config.example.yaml` documents — so the values
  survive a restart. `scan.hash_workers` is written only when the request
  actually carried the field; an unrelated settings write (alert URL,
  auto-archive) never invents it. The write is atomic (temp file + fsync +
  rename) and preserves comments and every other key.
- If the config file cannot be written, **all** of the values in that request
  roll back and the response is `400`, so memory and disk never disagree.
- Invalid JSON, a non-integer or a wrong type is `400 {"error": "..."}`; the
  running values are left untouched.
- The live values are also published as `pull_concurrency`, `hash_workers` and
  `max_folder_sessions_per_peer` in `GET /api/status`.

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

### Convergence test data

#### `POST /api/folders/{id}/testdata`

Writes deterministic test data **inside `<root>/testdata/` only**, so a pair can be
seeded with the same corpus from each node's own daemon — no shell, no SMB
credentials, no manual copying. Content is derived from `(seed, index)` and
mtimes are derived from the index too, so the same spec on two nodes produces
byte- and mtime-identical trees: the pair starts genuinely in sync, not merely
similar.

```json
{"kind":"base","dir":"testdata/base","files":2000,"size":4194304,"seed":42,"confirm":true}
```

| kind | does |
|------|------|
| `base`, `add` | create `files` files of `size` bytes (`start_at` to append rather than overwrite) |
| `modify` | rewrite `files` files with different content and a newer mtime — a real newer version |
| `rename` | move `dir` to `to` (both under `testdata/`) |
| `cleanup` | remove the `dir` subtree |

Rails, because this writes real data: the path must be under `testdata/` (escapes
are refused), `"confirm": true` is required, one call is capped at 200,000 files
and 8 GiB, the folder lock is held for the whole write so a scan never indexes a
half-built corpus, and every use records a `warning`-category event in the
folder's history.

Typical differential scenario: pause both folders, generate the same corpus on
both, perturb each side (`base` into a new folder, `rename` a directory,
`modify` some files, `modify` the *same* files on both for a conflict), resume,
and check that only the delta moves — see
[docs/testing.md](testing.md) for the runbook.

Response: `{"kind":"base","dir":"testdata/base","files":2000,"bytes":8388608000,"elapsed_ms":74480}`.

---

## Data types

### `Status`

`GET /api/status` is a cache read: it performs no filesystem access and no index
walk, so it stays fast on a slow array (measured before the cache: 5.8-8.3 s per
request on Tower, 0.28 s for the identical call on NAS-02) and the UI can poll it
every couple of seconds. The consequence is that a few per-folder fields are
**up to ~5 s stale**, refreshed by the daemon's background ticker:

* `free_bytes` — a `statfs` of the folder's volume;
* `version_files` / `version_bytes` — a walk of the `.sfx-versions` area;
* `pending_files` / `pending_bytes` / `pending_up` / `pending_deletions` — see
  [Pending figures](#pending-figures).

Everything else is live in-memory state.

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
| `startup_scanning` | bool | at least one folder is still taking its first index pass since start |
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
| `pending_verified` | bool | the DOWNLOAD figure (`pending_files`/`pending_bytes`) is a fact about a COMPLETE peer view — an old one is still published, dated by `pending_as_of` — see [Pending figures](#pending-figures) |
| `pending_up_verified` | bool | the UPLOAD figure (`pending_up`) is a fact as well: it additionally needs this index to have been sent to every peer |
| `pending_reason` | string? | why a figure is withheld — or, with `pending_verified: true`, why the published numbers are old (empty when both are verified and current) |
| `pending_as_of` | number | unix seconds of the oldest peer-index exchange the pending numbers rest on (`0` = no completed exchange). Survives a daemon restart |
| `free_bytes` | number | free space on the folder's volume — **cached**, refreshed every ~5 s |
| `bytes_up` / `bytes_down` / `files_down` / `syncs` | number | transfer stats |
| `last_sync` | number? | unix seconds of last sync |
| `conflict_files` / `conflict_bytes` | number | conflict copies on disk |
| `version_files` / `version_bytes` | number | archived versions in `.sfx-versions` — **cached**, refreshed every ~5 s |
| `open_events` | number | distinct open warn+ conditions for this folder |
| `peers` | number[] | peer ids this folder syncs with (empty = all) |
| `peers_all` | bool | true when `peers` is empty or covers every configured peer |
| `peer_names` | string[]? | configured names of `peers` |
| `shared_peers` | number | peers known to have this folder and allowed by scoping |
| `not_shared` | bool | peers exist but none (known) will sync this folder |
| `guard_tripped` | bool | legacy: the deletion guard no longer blocks (over-threshold passes hold their copies, see `hold`) |
| `pending_deletions` | number | live files globally deleted, waiting to be removed |
| `direction` | string | effective sync direction: `send-receive` \| `send-only` \| `receive-only` |
| `paused` | bool | scanning and syncing stopped for this folder on this device (see `POST /api/folders/{id}/pause`) |
| `pull_fsync` | bool | **effective** durability of pulled files: `true` (the default, also when the key is absent) means each pulled file is fsynced before it is renamed into place, so "committed" survives a power cut. See [Pull durability](#pull-durability) |
| `include` / `ignore` | string[]? | configured pattern lists (omitted when empty) |
| `hold` | [`FolderHold`](#folderhold)? | soft-deleted copies kept in `.sfx-hold` (summary only: `count` + `bytes`; omitted when nothing is held — fetch `GET /api/folders/{id}/hold` for the entry list) |
| `peer_unsynced` | [`PeerUnsynced`](#peerunsynced)[]? | per peer, the open unsynced set that peer advertised at the end of its last successful round |

### `Pending figures`

`pending_files`/`pending_bytes` (download) and `pending_up` (upload) are two
different claims about two different snapshots, and the API says which of them
is currently a fact:

* `pending_verified` covers the **download** figure. It is `true` while a
  complete peer index view backs it. A stale view can only make this figure
  *under*-report (we pull less than we could), never invent work, so it is still
  published — including in the middle of a sync round, where it is "as of round
  start". `pending_as_of` is when that view exchange completed, so a client can
  say `↓ 146 · as of 12m` instead of hiding the number. Measured failure this
  replaces: during a 74-minute round every folder on both nodes reported
  "awaiting sync" for the whole round, idle and perfectly in-sync folders
  included.
* `pending_up_verified` covers the **upload** figure. It needs everything the
  download figure needs *plus* that this index has been sent to every peer
  (`sentSeq >= ownSeq`); otherwise the peer's copy of us is older than our own
  and its "down" cannot match our "up". `sentSeq < ownSeq` is always true while a
  round is applying received changes, so a row legitimately shows a download
  count and no upload count.
* A peer view more than 90 s old with no round in flight is NOT one of those
  cases: the download figure is still published, dated by `pending_as_of`, and
  `pending_reason` says how old the view is. The number is never presented as
  current — it carries its own age.
* `pending_verified: false` is reserved for the cases where the download number
  genuinely cannot be known: no completed exchange with a peer yet (never, and
  not since the daemon started), or an exchange in progress (a truncated view is
  never truth). `pending_reason` names which. Numbers are always present in the
  payload; the flags say whether they may be shown as facts.
* The exchange time survives a restart: it is recorded in the folder's index DB
  when an exchange completes and restored when the daemon opens the folder, so a
  restart no longer blanks every folder's figure until its own exchange lands. A
  restored time never makes a stale view look fresh — the age is measured against
  the wall clock — and the upload figure stays withheld until the peer's index has
  actually been re-exchanged in this process.

Both figures are published from cache — computed off the request path by the
daemon's 5 s background refresh — so a change can take up to ~5 s to appear.

The same is true of the index counts (`files`, `directories`, `size`,
`tombstones`, `conflict_files`), the per-folder `open_events` and the top-level
`open_events`: they are one SQL aggregate and two SQL counts, which used to run on
every `/api/status` request (measured: ~83 ms for the aggregate, and 519 ms for an
unfiltered open-event count on the same node). They are refreshed on the same ~5 s
cadence, and immediately after a scan finishes — a scan is what moves them — so a
count can lag the index by at most one tick.

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
