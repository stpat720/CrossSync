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
- Include gates cooperate with renames: when a device's `include` list covers
  only part of a folder and a local rename moves an included tree out of that
  gate (`proxy/` → `dev/`), the rename still reaches peers as a MOVE (no bytes
  transferred) and the device only releases its own tracking rows once every
  peer that owned the old paths has confirmed — never broadcasting a deletion
  first, and never touching its files on disk. Widening the gate later pulls
  the content back byte-identically.
- Soft deletion guard: an over-threshold delete pass from a peer is applied
  (peers delete exactly as before) while the RECEIVING device moves its copies
  into the reserved `.sfx-hold/` area instead of unlinking them — a 7-day undo
  window, listed with Restore / Delete now in the folder detail and purged by
  the maintenance pass when it expires. Below the threshold nothing changes.
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
- Sync direction (per device, per folder): `direction: send-receive` (default,
  bidirectional), `send-only` (this device serves its files but never acquires
  anything — no pulls, moves or remote deletions) or `receive-only` (it applies
  peer changes but never indexes its own, so nothing local is advertised or
  propagated; a corrupt-index rebuild finishes with an empty index). Set it when
  adding a folder or via the folder's ⋯ menu; like the include list it is a
  local setting, so each device carries its own. `include: [<patterns>]` is a
  whitelist — only matching paths are indexed/synced, and empty means everything
  the ignore rules allow — while `ignore: [<patterns>]` excludes paths.
- Stability gate (`settle_scans`, per folder, default 2): a changed file becomes
  transfer-eligible only after its `(size, mtime)` has been observed unchanged
  across that many consecutive scans. A large file copied into a synced folder is
  therefore never hashed, versioned or advertised at an intermediate size — peers
  see it once, at its final state, instead of pulling a growing partial every
  round (and leaving `.part` churn behind). Held-back files stay known to the
  scanner (no false deletions, renames still pair) and are reported without
  per-file logging: the folder's scan status carries `inflight` ("files held back
  — still being written"), the folder card carries `inflight_files` /
  `inflight_bytes` ("30.5 GB held back while copying"), and ONE summary event is
  recorded when the last held file starts syncing. `settle_scans: 1` disables the
  gate (settle on first sight, the pre-gate behaviour); receive-only folders and
  corrupt-index rebuilds always bypass it. A file that changed while it was being
  hashed is re-verified on the next scan instead of being indexed with a stale
  size. On the receiving side the index records the stat the local filesystem
  actually reports after a pull (mtime is per-node metadata; the version vector
  carries ordering), so a coarse-mtime share cannot manufacture a conflicting
  version of a file it just adopted.
- Conflicts: two concurrent edits (neither side saw the other's change) produce
  a conflict. The file with the NEWER modification time is the default winner;
  the older one is preserved as a `.sync-conflict-*` copy (conflict-copy policy)
  or an archived version (versioning policy), and can be compared / restored /
  resolved later from the UI. Identical content is never treated as a conflict.
- Web-UI self-update: the UI is embedded in the binary, but `ui_dir` lets you
  serve an override `index.html` from a bind-mounted appdata share (drop a file
  in, no rebuild/restart), and `ui_update_url` + the ⟳ update button let the UI
  fetch, validate and apply a newer UI at runtime
- Daemon self-update: `update_url` points at a JSON release manifest (version +
  per-platform binary url + sha256, built by `./release.sh` or the CI workflow).
  The daemon polls it, downloads a newer binary, verifies the checksum, swaps it
  into `update_dir` (a writable appdata dir) and restarts itself in place.
  `update_auto: true` applies without asking; otherwise the web UI shows a
  "⟳ daemon update" button (Settings → Daemon updates). **`config.yaml` and your
  indexes are never touched.** Every push to `main` auto-builds + publishes a
  rolling release, so updating is: push → daemon detects → one click (or
  automatic). Manifest URL:
  `https://github.com/<owner>/<repo>/releases/download/nightly/latest.json`
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

## Documentation

- [`docs/api.md`](docs/api.md) — REST API + SSE control-plane reference
  (status, folders, events, conflicts, restores, settings, self-update)
- [`docs/mcp.md`](docs/mcp.md) — MCP server reference (tools, protocol,
  client setup)
- [`docs/unraid-setup.md`](docs/unraid-setup.md) — full Unraid deployment guide
- [`docs/unraid-easy-setup.md`](docs/unraid-easy-setup.md) — novice-friendly
  Unraid guide

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

## Updating

Two ways — the self-update path is the easy one.

**A. Self-update (recommended).** Publish builds by enabling
`.github/workflows/release.yml` (it builds on every push to `main` and publishes
a rolling `nightly` release) or by running `./release.sh` with `UPDATE_BASE_URL`
set. Then in `config.yaml`:

```yaml
update_url: "https://github.com/<owner>/<repo>/releases/download/nightly/latest.json"
update_auto: true   # optional: apply without asking (default: button in the UI)
```

The daemon polls the manifest every few minutes; when a newer version is
published it downloads, SHA-256-verifies, swaps the binary and restarts itself.
`config.yaml` and your indexes are never touched. With `update_auto` off, the
web UI shows a "⟳ daemon update" button instead.

**B. Manual (fallback).** On each server: `git pull`, rebuild the image
(`docker build -f deploy/Dockerfile -t crosssync .`), then **remove and re-add**
the container on the Docker page so it picks up the new image. `config.yaml`
lives outside the repo and is untouched.

## Production operations

Everything below is reachable without a shell on the box (which matters on
NAS-02-style nodes that are API-only).

**Health.** `GET /healthz` answers `200` while every folder with a peer keeps
completing sync rounds, and `503` with a `reason` once one stops — the silent
failure a container runtime otherwise cannot see. The CLI wraps it for
healthchecks and cron: `crosssync health --config /config/config.yaml` (exit 0
healthy, 1 degraded/unreachable). The Dockerfile ships that as a `HEALTHCHECK`,
so `docker ps` shows `(unhealthy)` instead of a daemon that looks fine.

**Metrics.** `GET /metrics` is Prometheus text format: per-folder files, bytes,
tombstones, pending ↓/↑, paused, last-sync timestamp, transfer counters, per-peer
connected/last-sync, open events, and `crosssync_build_info`. Scrape it every
15–60s; it is deliberately cheap (one aggregate query per folder, no directory
walks).

**Alerts.** With `alert_url` set, the daemon pages on page-worthy conditions
(folder errors, peer offline, deletion guard) and on a **stalled sync** — a folder
that used to sync and has not completed a round in 2 hours — throttled to one page
per folder per 15 minutes.

**Pause a share.** Each row's `⋯` menu has *Pause sync* / *Resume sync*, or
`POST /api/folders/{id}/pause {"paused":true}`. Paused, the folder is not scanned
and not advertised to peers (they see it as not shared, with no error churn);
files and the index are untouched and resuming reconciles normally. Per-device
and persisted across restarts.

**Deep / verified rescan.** *⋯ menu → Deep rescan (verify content)*, or
`POST /api/folders/{id}/rescan {"verify":true}`. Re-hashes every file instead of
trusting `(size, mtime)`, which is the only way to see a content change that
preserved both — `rsync -a`, restore tools and coarse SMB timestamps all do
that. Unchanged files are adopted with their new stat and **no version bump**,
so a deep scan never re-transfers the share. It reads every byte, so it answers
`202` immediately and runs in the background (progress in `scans[]`); on a large
or busy array expect hours, and that folder's sync waits until it finishes.
A daemon restart aborts it harmlessly.

**Backup.** `GET /api/backup` downloads a `tar.gz` with `config.yaml`, `cert.pem`,
`key.pem` and a manifest with restore steps. Only identity + topology: indexes are
derived from the shares and rebuilt automatically, so they are not exported.
⚠ The archive contains the private key — store it like a password. Restoring
cert+key keeps the fingerprint, so no peer needs re-pairing.

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

Chaos and scale coverage lives beside the features it protects (corrupt block,
mid-pull write failure, simultaneous dual sessions, disconnect mid-index,
unicode/deep-path filenames, 1M-entry index pipelines, three-node convergence,
rolling-upgrade protocol gating).

Two native fuzz targets are checked in and run as ordinary tests on their seed
corpus; run them longer when touching the wire format or pattern matching:

```sh
go test ./internal/protocol/ -run '^$' -fuzz FuzzReadMessage -fuzztime 60s
go test ./internal/ignore/   -run '^$' -fuzz FuzzParse       -fuzztime 60s
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
