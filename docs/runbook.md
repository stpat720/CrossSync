# CrossSync runbook

Short procedures for the situations that actually happen. Each entry says what you
see, what it means, and what to do. Everything here is reachable from the control
API (`http://<node>:55556`) unless stated otherwise.

## The numbers on the screen

Three different things are shown per folder, and they answer different questions:

| figure | meaning | source |
|---|---|---|
| files / folders / bytes | what the **last completed scan** saw | index |
| `· as of 12s ago` | how old that observation is | last scan finish |
| Pending ↓/↑ | work to transfer, **derived from both sides' indexes** | pending cache |
| Deleting | files a peer deleted that this node must still remove | deletion queue |
| Held | copies parked in `.sfx-hold` by the soft-deletion guard | hold manifest |
| Ignored | bytes on the share the index deliberately skips | last scan |

`Pending` shows **awaiting sync** instead of a number when no current index
exchange exists (peer offline, exchange in flight, or this index has changes the
peer has not been sent). That is deliberate: a number computed against a stale
copy of the peer's index is a guess, and the two nodes would disagree.

## A peer shows `connected: false`

1. Is the peer's sync port reachable? `nc -vz <tailscale-ip> 55557`. TCP working
   while both nodes report disconnected means a session is/was stuck.
2. Sessions now carry a 5-minute no-progress deadline, so a dead peer or a
   half-open socket fails the round and the connection manager retries (backoff
   caps at 5 minutes and never gives up). Give it five minutes.
3. Still wedged? `curl -X POST http://<node>:55556/api/restart` — that re-executes
   the daemon. No reinstall needed (before 0.1.109 the only restart was
   `update/apply`, i.e. reinstalling the same version).

## Sync says `interrupted`

The last round that *finished* failed, and nothing has succeeded since. It clears
by itself on the next successful round, in either direction (`0.1.106` and later
count inbound rounds; before that, a node that only received rounds never cleared
it). If it persists, the peer is genuinely unreachable — see above.

## Files look out of sync but both nodes say 0 pending

Check, in this order:

1. `· as of …` under the file count. Changes made by another machine over SMB only
   reach the index when a walk runs (`scan.full_interval`, default 3m, up to
   `scan.full_max`, default 15m). A recent walk means the count is current.
2. `awaiting sync` on Pending: no current exchange, so the counters are withheld.
3. `Ignored: N GB`: bytes on the share that are not data (`.fuse_hidden*`,
   `.smbdelete*`, `.nfs*`, `.sfx-*`). These are never synced. If a big value
   appears after deleting large files, something still holds those files open —
   it clears when the handle closes.
4. Force an observation: `POST /api/folders/<id>/rescan` (add `{"verify":true}` to
   re-hash content instead of trusting size+mtime).

## A folder was wiped, or a mass deletion was triggered by mistake

- Peers do not apply a huge deletion silently: above the guard threshold the
  copies are **held**, not removed. `Held: N` in the folder row;
  `GET /api/folders/<id>/hold` lists them, `POST .../hold/restore` puts one back.
- Held copies expire after 7 days and are then purged for real.
- To free the space deliberately: folder menu → **Clear held data (N · size)**,
  or `POST /api/folders/<id>/hold/delete` with `{"path": ""}`. This is per folder.
- The hold area warns once it passes `hold_warn_bytes` (default 20 GiB). It is
  never auto-purged: that would destroy the undo window.

## Deletions are crawling

`Deleting: N` falling at a few files per second. Each deletion is one filesystem
move into the hold area; `0.1.109` removed the lock that serialised them and runs
them through a 4-worker pool, so the floor is the array, not the daemon. If it is
much slower than a few dozen per second on spinning disks, check array load first
(other containers saturating the share make every operation slower).

## Restart, backup, restore

- Restart: `POST /api/restart`. Config changes need it (or a container restart).
- Backup: `GET /api/backup` → tar.gz with `config.yaml`, `cert.pem`, `key.pem`,
  `index/<folder>.db` (a VACUUM INTO snapshot, safe while running) and a MANIFEST.
  Keep it secret: it contains the node's private key.
- Restore: extract `config.yaml` (+ cert/key) into the config dir, and
  `index/<folder>.db` into `meta_dir/<folder>.db` **before** starting. Restoring
  the index keeps the version vectors, so peers recognise the files as the same
  content instead of a mass re-add. Without it, the first scan rebuilds the index
  and peers may see a large change.

## Security

The control API can delete files, clear hold areas and force updates.

- If it listens on anything other than loopback, set `control_token` in
  `config.yaml`. Every request then needs `Authorization: Bearer <token>` (the web
  UI asks once and remembers it); SSE and download links accept `?token=`.
- A listener without a token logs a warning at startup — it is not fatal, so an
  existing deployment is not broken by upgrading.
- Peer traffic is TLS with certificate-fingerprint pinning; the control plane is
  not TLS unless you put a proxy in front of it.

## Upgrading

- Nodes may run different versions during a rollout; indexes carry a schema
  version, and a node refuses an index written by a newer build rather than
  mis-reading it (message names both versions).
- Updates come from the release manifest (`update_url`) and are verified by
  checksum with retries; `update_auto` applies them unattended.
