# CrossSync

A bidirectional, peer-to-peer file sync engine for Unraid servers (2→N peers),
built from scratch in Go. Designed around the things commercial sync tools do
poorly: **transparency** (every file has a state and a reason), **graceful edge
case handling**, **share-level-only I/O** (Unraid parity untouched), and
**self-healing** (the filesystem is the source of truth; indexes are rebuildable).

## Status

Working, tested core (v0.1):

- Per-folder SQLite index (WAL, `synchronous=FULL`), filesystem is source of truth
- Metadata-first parallel scanner (inotify-ready; skips unchanged files without hashing)
- Version-vector conflict resolution with deterministic winners
- Per-folder policies: `conflict-copy` (`.sync-conflict-<ts>-<device>` copies that
  propagate) and `versioning` (trashcan / simple / staggered archive area)
- Block-level transfer with per-block SHA-256 verification (bad transfers rejected)
- Move/rename detection: same-content moves (e.g. reorganizing a folder tree)
  are detected by comparing block hashes the receiver already has and applied
  as LOCAL RENAMES — no re-transfer, and verified moves bypass the deletion
  guard and mass-change signal (a move is not a deletion; unpaired deletions
  are still guarded). A folder-level "moved N file(s) locally (same-content
  rename, no transfer)" event records the reorganization.
- Smart staging: central `.sfx-tmp/` with same-directory fallback so the atomic
  rename never double-writes; mtime is set on the temp BEFORE the rename
- TLS 1.3 transport with certificate-fingerprint device pinning (Syncthing model:
  self-signed identity per node, peers pinned by cert hash — no CA needed)
- Delta index exchange: after the first full exchange, sessions only send entries
  newer than what the peer last acknowledged (index generation id + max sequence),
  so steady-state syncs are proportional to changes, not to the whole tree
- Connection manager: exponential backoff for offline peers (5s → 5 min cap) — one
  log line on failure, one on recovery, instead of spamming every interval
- Durable event store: every condition (conflict, versioned archive, applied change,
  skip-by-rule, peer state, error) is an append-only, queryable record. Acknowledge
  = record who/when, never delete; a persistent condition re-opens on recurrence
  ("nothing is ever dismissed forever")
- Revert/restore from the UI: browse archived versions per folder (versions panel)
  and restore one over the live file, or make a conflict copy the winner — both
  record a new local change so the restore propagates to every peer
- Control plane: REST API + SSE event stream (status, folders, events, ack, rescan,
  sync) and an MCP server (status, list_folders, list_events, ack_event, rescan,
  sync_now) — one service consumed by the CLI, HTTP, and MCP clients
- Filesystem watcher (fsnotify/inotify): changes trigger a scan within ~250ms
  instead of waiting for the interval; our own staging/versioning writes are
  filtered out (self-trigger suppression) so the daemon never rescans itself;
  the periodic scan remains the safety net
- Three-layer web UI served by the daemon (single self-contained page, no build
  step, no CDN): Glance = Resilio-style folder rows (files/size, pending files +
  bytes, last sync, data up/down, per-folder ⋯ menu with rescan/sync/remove,
  click a row for an attention-only file list — pending/conflict/event files
  with filter checkboxes, no more thousands of rows) → Event history = a
  scrollable bottom window with column headings (When/Category/Severity/State/
  File), category tabs + counts, a time-range filter (default 24h) and a folder
  filter (default all) → Settings panel (device id, TLS fingerprint, UI version,
  peers, add/remove folders persisted to the config file, and a Storage section
  showing how much space conflict copies + archived versions use per folder and
  globally). A docked bottom bar shows sync activity, last sync, status, connected
  peers/IPs and live up/down speeds with hover details. Light/dark theme toggle
  (☀/🌙, persisted).
- Per-folder peer scoping: a folder syncs with every configured peer that also
  has a folder with the same id — unless the folder lists `peers: [<ids>]`, in
  which case it only syncs with those peers. Scoping is enforced on both sides
  (each node only advertises folders allowed for that peer), so a folder never
  appears on a peer it isn't shared with. The UI shows "⇄ all peers" / "⇄ nas-02"
  on each folder row and lets you set peers when adding a folder or via the
  folder's ⋯ menu. How matching works: a sync session exchanges a list of folder
  ids, and only the ids present on BOTH sides (intersection) are synced — the id
  is a unique string you generate once (or paste in); paths never need to match.
  Folder rows show the human Name large, the unique id underneath, then the path.
- Conflicts: two concurrent edits (neither side saw the other's change) produce
  a conflict. The file with the NEWER modification time is the default winner;
  the older one is preserved as a `.sync-conflict-*` copy (conflict-copy policy)
  or an archived version (versioning policy), and can be compared / restored /
  resolved later from the UI. Identical content is never treated as a conflict.
- Web-UI self-update: the UI is embedded in the binary, but `ui_dir` lets you
  serve an override `index.html` from a bind-mounted appdata share (drop a file
  in, no rebuild/restart), and `ui_update_url` + the ⟳ update button let the UI
  fetch, validate and apply a newer UI at runtime
- CLI: `serve` (daemon loop), `scan`, `sync`, `status`, `rebuild` (quarantine +
  rebuild a folder index from the filesystem), `events`, `ack`, `mcp`, `fingerprint`

## Quick start

```sh
go build -o crosssync ./cmd/crosssync

# 1) On each node, generate its TLS identity and print its fingerprint:
crosssync fingerprint --config config-a.yaml   # paste the output into config-b.yaml
crosssync fingerprint --config config-b.yaml   # paste the output into config-a.yaml

# 2) Run the daemon on each node (tls: true in the config):
crosssync serve --config config-a.yaml --interval 10s
crosssync serve --config config-b.yaml --interval 10s
```

With `tls: true`, each node auto-generates `cert.pem` + `key.pem` in `meta_dir`
on first run, and **rejects any connection whose certificate fingerprint is not
listed in `peers[]`** — an unpinned peer is refused at the TLS handshake, before
any data is exchanged. Set `tls: false` only for throwaway/dev setups.

With `control_listen` set, `serve` also exposes the REST API **and the web UI**
(open http://127.0.0.1:55556/ in a browser):

```sh
curl http://127.0.0.1:55556/api/status
curl "http://127.0.0.1:55556/api/events?open=1"
curl -X POST http://127.0.0.1:55556/api/rescan -d '{}'
curl -N http://127.0.0.1:55556/api/events/stream   # SSE live feed
```

The MCP server runs on stdio (wire it into any MCP client):

```sh
echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"status"}}' \
  | crosssync mcp --config config-a.yaml
```

Write a file into a share on either node; within one interval it appears on the
other. See `config.example.yaml`.

## Deploy on Unraid

See [`docs/unraid-setup.md`](docs/unraid-setup.md) for the full guide: Tailscale
setup, fingerprint exchange, the Docker image (`deploy/Dockerfile`, PUID/PGID,
`--network=host`), the SMB `veto files` step that hides the `.sfx-tmp/` transfer
area, the safe pilot-then-expand rollout, and the Resilio migration steps.

The container is exposed as a proper **Unraid app**: drop
`deploy/unraid/crosssync.xml` into `/boot/config/plugins/dockerMan/templates-user/`
and add it from the Unraid web UI's **Docker → Add Container** page, so it
shows up on the Docker tab with start/stop/autostart/edit controls (a plain
`docker run` container will not).

```sh
# cross-compile the static Linux binary (pure-Go sqlite, no cgo):
./build.sh                          # -> dist/crosssync-linux-amd64

# or build the container:
docker build -f deploy/Dockerfile -t crosssync .
```

Recovery is built in: a corrupt per-folder index is auto-quarantined
(`<folder>.db.corrupt-<ts>`) and rebuilt from the filesystem at startup; you can
force a regenerate at any time with `crosssync rebuild --config <file>`.

## Layout

- `cmd/crosssync` — CLI
- `internal/version` — version vectors
- `internal/hash` — block hashing + BEP block-size rule
- `internal/index` — per-folder SQLite index
- `internal/ignore` — ignore rules (extension/name/path)
- `internal/scanner` — metadata-first scan with parallel hashing
- `internal/protocol` — wire messages + framing
- `internal/transfer` — TCP + TLS 1.3 transport
- `internal/certs` — per-device TLS identity + fingerprint pinning
- `internal/staging` — smart temp placement + atomic commit
- `internal/sync` — engine: global model, conflicts, versioning, pulls
- `internal/events` — durable, append-only event store (transparency layer)
- `internal/watcher` — fsnotify change detection with self-trigger suppression
- `internal/control` — REST API + SSE control plane
- `internal/mcp` — MCP server (JSON-RPC over stdio)
- `internal/ui` — embedded web UI (served at the control API root)
- `internal/node` — daemon + peer sessions

## Tests

```sh
go test ./... -count=1
```

Includes end-to-end tests that run two real nodes over loopback TCP (plain and
TLS) and verify initial sync, modification propagation, conflict resolution,
deletion propagation, that unpinned peers are rejected at the TLS handshake,
that delta exchange really shrinks subsequent sessions (one modified file =
one entry sent), and that the durable event store records conflicts/skips/applies
and re-opens acknowledged events when a condition persists. The REST API, MCP
server, watcher, folder management (add/remove persisted to config), and
embedded UI have their own suites (httptest, stdio, fake-fs events, served-HTML
assertions).

## Roadmap (from design)

- Stall watcher / alerting (ntfy/apprise/webhook hook for page-worthy conditions)
- Scale tuning pass against millions of files / multi-TB shares
- Fault-injection hardening tests (kill mid-transfer/resume, EXDEV simulation)
- DB rebuild-from-FS: done (auto-quarantine + rebuild at startup, `crosssync rebuild`)
- Docker image for Unraid + PUID/PGID + migration guide: done (see `docs/unraid-setup.md`,
  `deploy/`)
