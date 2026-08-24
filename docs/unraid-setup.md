# CrossSync on Unraid — setup guide

CrossSync is a static Go binary (no runtime deps) that syncs user shares
between two (or more) Unraid servers over Tailscale. It never touches raw
disk paths (`/mnt/diskN`, `/mnt/cache`) — all I/O goes through
`/mnt/user/<share>`, so Unraid parity and the share abstraction are
untouched.

---

## 1. Prerequisites

- **Tailscale** installed and logged in on every server that will sync
  (the Tailnet gives us direct, encrypted peer paths).
- An **appdata share** (default `appdata` exists on Unraid) to hold the
  config and per-folder databases:
  `/mnt/user/appdata/crosssync/`
- **Decide the run identity.** The daemon must be able to read/write the
  shares you sync. Unraid's default nobody/users = **PUID 99 / PGID 100** —
  the container defaults to exactly that.

## 2. Install the binary (choose one)

**A. Container via the Unraid Docker page (recommended).**

Unraid manages Docker apps through **templates**, so the container shows up
in the web UI's **Docker** tab as a proper app (start / stop / autostart /
edit dialog) instead of being an invisible command-line container.

1. Build the image once (SSH to the server, or build on another machine and
   `docker load` it there):
   ```sh
   cd /mnt/user/appdata/crosssync   # repo copied here
   docker build -f deploy/Dockerfile -t crosssync ..
   ```
2. Copy the template to the Unraid template folder:
   ```sh
   cp deploy/unraid/crosssync.xml /boot/config/plugins/dockerMan/templates-user/
   ```
3. In the Unraid web UI: **Docker → Add Container**, pick **CrossSync** from
   the template dropdown, set the **Config path** (appdata) and **User
   shares** (`/mnt/user`) if they differ, then **Apply**.

The app now appears on the Docker page with its own icon, start/stop, and
"auto start" — no command line needed. `PUID`/`PGID`/`TZ` are editable
fields in the same dialog.

> Why a template? Containers created with a plain `docker run` DO show up in
> Unraid's Docker page list, but as "manually added" — no Unraid template
> behind them, so there is no edit dialog, no autostart switch, and
> Community Apps won't recognize them. The template fixes all of that.

**B. Container via command line (no Unraid app integration):**
```sh
# build the image from this repo
docker build -f deploy/Dockerfile -t crosssync .
```
Run it (on each server):
```sh
docker run -d \
  --name crosssync \
  --network=host \
  -e PUID=99 -e PGID=100 \
  -e TZ=America/New_York \
  -v /mnt/user/appdata/crosssync:/config \
  -v /mnt/user:/mnt/user \
  --restart unless-stopped \
  crosssync
```
Notes:
- `--network=host` is the simplest way to reach peer Tailnet IPs and to
  let peers reach this container on `listen` / `control_listen` ports.
  (A Tailscale sidecar works too; then map the ports.)
- Mounting `/mnt/user` into the container exposes the user shares at the
  same paths your `folders[].path` entries use (`/mnt/user/<share>`).
- This works fine, but prefer option A on Unraid so the app is managed from
  the Docker page.

**C. Plain binary (no container):**
```sh
# on this repo, cross-compile for Unraid:
./build.sh                      # -> dist/crosssync-linux-amd64
# copy the binary to the server, then run via User Scripts / rc.local:
/mnt/user/appdata/crosssync/crosssync serve \
  --config /mnt/user/appdata/crosssync/config.yaml
```

## 3. Configure each node

> **Not on the Unraid Docker page.** The Docker page's *Edit* dialog only
> exposes the template fields (`PUID`/`PGID`/`TZ` and the volume paths) — it
> can't edit the app's own config. These one-time settings live in
> `config.yaml`, which is a plain file on the Unraid share at
> `/mnt/user/appdata/crosssync/config.yaml` — edit it from the Unraid
> console, file manager, or `nano` over SSH.

Do this on **each** server:

1. Copy the example file into place:
   ```sh
   cp config.example.yaml /mnt/user/appdata/crosssync/config.yaml
   ```
2. Open it: `nano /mnt/user/appdata/crosssync/config.yaml`
3. **`device.name`** — set to this server's name:
   ```yaml
   name: nas-01
   ```
4. **`device.id`** — pick any unused number (1 on nas-01, 2 on nas-02,
   3 on nas-03):
   ```yaml
   id: 1
   ```
5. **`peers:`** — the example file already has one peer block (for
   "nas-02"). Edit it to describe one of your **other** servers, and copy
   it for each additional server. For each other server, set:
   - `id:` → the number you picked for that server in step 4
   - `name:` → that server's name
   - `addresses:` → that server's Tailnet IP, port `:55555`
   - `fingerprint:` → leave the placeholder for now (filled in at step 4)

   One server = one block:
   ```yaml
   peers:
     - id: 2                       # that server's device.id
       name: nas-02
       addresses:
         - "100.64.0.2:55555"      # that server's Tailnet IP + port
       fingerprint: "PASTE_FINGERPRINT_HERE"   # filled in at step 4
   ```

   For a 3rd server, copy the whole block (from `- id:` to `fingerprint:`)
   and paste it right below.
6. Save and close. `meta_dir`, `listen`, `control_listen` stay as-is.
   Folders are **not** in this file — you add them in the web UI
   (Settings → Folders → "+ Add folder").

Repeat steps 1–6 on every server, then go to step 4 and replace each
`fingerprint:` placeholder.

After that first edit, day-to-day management happens in the **CrossSync web
UI** (⚙ Settings), not in the file: add/remove folders, choose which peers
sync with each folder, alert URL, and event-history auto-archive are all
editable there and are saved back to `config.yaml` automatically. The UI's
*Device* and *Peers* panels are read-only by design — identity and TLS
fingerprints stay file-only. After hand-editing `config.yaml` (device id,
peers, ports), **restart the container** so the daemon reloads it.

### 4. Exchange TLS fingerprints

On each server, print the fingerprint from *inside* the running container:

```sh
docker exec -it crosssync crosssync fingerprint --config /config/config.yaml
```

(The binary is also on the host if you installed the plain binary —
option C — in which case run
`/mnt/user/appdata/crosssync/crosssync fingerprint --config /mnt/user/appdata/crosssync/config.yaml`.)

It prints two values:

- **`fingerprint:`** — copy this into **every other** server's
  `peers[].fingerprint` (replace the `PASTE_FINGERPRINT_HERE` placeholder
  from step 5).
- **`device id:`** — informational (derived from your certificate). You can
  ignore it; you already set `device.id` yourself in step 4.

A peer with a missing or wrong fingerprint is rejected at the TLS handshake.

### 5. SMB hygiene — hide the transfer area

In-flight transfers live in a hidden `.sfx-tmp/` directory at each share
root (excluded from scanning/sync by design). Add to each share's SMB
export so Windows/macOS clients never see it:

```
veto files = /.sfx-tmp/
delete veto files = no
```

Also exclude `.sfx-versions/` (archived versions) from backups if you want
backups to see only live files:
```
veto files = /.sfx-tmp//.sfx-versions/
```

## 6. First run (the pilot)

Start one server, then the other. Watch:

- **Settings → Peers** shows `● connected` + last online/sync per peer.
- Add **one small test folder** first (same id on both), drop a few files,
  confirm they appear on the other side, then resolve/compare a conflict.
- Only after that, add a **large media file** to prove big-file transfer +
  resume, then add your real shares.
- Audit: `status`/UI shows only `/mnt/user/...` paths. Unraid parity is
  untouched because we never write outside user shares.

If a share is left read-only or the PUID/PGID is wrong, files surface as
`unsynced` events with an actionable reason ("permission denied — check
PUID/PGID ownership", "share is read-only", …).

## 7. Day-to-day

- Web UI is served by the control plane at `http://<node>:<control_listen>/`
  — folder glance, attention, conflicts (compare/restore/resolve),
  archived versions, event history with manual + automatic archive.
- **Event history growth**: routine logs can be auto-pruned in
  Settings → Event history (open attention/conflict items are never
  auto-pruned), or archived manually with the **Archive…** button.
- **Deletion safety**: a single sync can never silently wipe a folder past
  `max_delete_pct` (default 25%). Blocked deletions show a banner with an
  "Apply pending deletions" override.
- **Moves & renames are free**: moving or renaming files/folders on one
  server is detected as a same-content move and applied on the other
  servers as a local rename — no re-transfer, no deletion-guard trip, no
  babysitting. The event history records "moved N file(s) locally
  (same-content rename, no transfer)". (A move you also edited falls back to
  a normal transfer.)
- **Recovery**: a corrupt per-folder index is auto-quarantined
  (`<folder>.db.corrupt-<ts>` kept for forensics) and rebuilt from the
  filesystem. Force a rebuild anytime with
  `crosssync rebuild --config <file> [folder-id]`.
- Logs go to stdout (`docker logs crosssync`).

## 8. Migrating from Resilio Sync

1. Set up CrossSync on both servers with one pilot folder; verify it
   converges and the UI matches expectations.
2. Add the remaining shares one at a time (same folder id on both sides).
   Initial scan hashes every file once — run it in the background; there
   is no downtime.
3. Once all shares are green and both servers agree, stop the Resilio
   container and remove its appdata. Keep the Resilio config around for a
   week in case you need to go back.
4. Remove `veto files` additions if they were added for `.rsls`-style
   placeholders, and confirm the CrossSync `veto files` lines above.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Peer shows `○ offline`, `last sync: never` | Tailnet IP unreachable from the container (use `--network=host`); `listen` port not open; fingerprint mismatch (see the peer event reason) |
| "TLS fingerprint mismatch" | Cert regenerated or wrong fingerprint pasted — run `crosssync fingerprint` on that server and update `peers[]` |
| Files stuck `unsynced` / "permission denied" | PUID/PGID don't match the share owner — set `-e PUID`/`-e PGID` to the share's owner |
| "share is read-only" | SMB export read-only / array read-only — make the share writeable for the daemon |
| "file is locked/in use" | An SMB client has the file open; the daemon retries and the event clears when it's released |
| Watchdog: `.sfx-tmp` visible over SMB | `veto files` not applied / share not reloaded — add the line above and restart SMB |
