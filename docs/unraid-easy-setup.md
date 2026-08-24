# CrossSync on Unraid — Easy Setup Guide

A step-by-step, novice-friendly guide to getting CrossSync running on two (or
more) Unraid servers. Everything is done from the Unraid web UI and its
built-in terminal — you never have to export or expose any shares.

**What you're building:** CrossSync is a small app that syncs folders between
your Unraid servers over Tailscale, with a web UI for managing folders,
conflicts, and history. You install it once per server, then "pair" the
servers by swapping fingerprints. The web UI loads before you configure any
peers, so you can look around first.

## What you need

- **Two Unraid servers** (this guide syncs between two; add more the same way)
- **Tailscale** installed and logged in on **both** servers (same tailnet)
- The CrossSync **GitHub repo URL** (where you cloned it)
- The **Unraid web terminal** (the **>_** icon, top-right of the web UI) —
  that's where most of this happens

## The pieces (read once, saves confusion)

| Thing | What it is |
|---|---|
| **Tailnet IP** | Each server's private Tailscale address, starts with `100.x.y.z`. Servers use these to find each other. |
| **Port 55557** | The **sync port** — where servers talk to each other. (It's 55557, *not* 55555 — 55555 is Resilio Sync's port.) |
| **Port 55556** | The **web UI port** — what you open in your browser. |
| **Fingerprint** | A 64-character ID each server prints. You paste it into the other server's config to "pin" them together securely. |

---

## Step 1 — Install Tailscale on both servers

1. In the Unraid web UI go to **Apps**, search **Tailscale**, and install it
   on **both** servers.
2. Open Tailscale on each server and click **Log in** (follow the browser
   prompt). Both servers must be on the **same tailnet**.
3. Find each server's **Tailnet IP** (starts with `100.x.y.z`) — shown in the
   Tailscale page, or run `tailscale ip -4` in the terminal. Write both IPs
   down; you'll need them in Step 8.

## Step 2 — Get the code onto each server

Use the Unraid web terminal (the **>_** icon, top-right). The appdata share
does **not** need to be exported for any of this.

On **each** server, clone the repo (paste your actual repo URL):

```sh
git clone https://github.com/<you>/crosssync /mnt/user/appdata/crosssync
```

Verify it landed:

```sh
ls /mnt/user/appdata/crosssync
```

You should see `deploy`, `cmd`, `go.mod`, etc.

> **Not on GitHub?** Two alternatives: copy the project over with SCP from
> your PC (`scp -r "C:\path\to\crosssync" root@<server-ip>:/mnt/user/appdata/`),
> or install **Dynamix File Manager** from *Apps* and upload a ZIP of the
> project into a `crosssync` folder you create under `/mnt/user/appdata/`.

## Step 3 — Build the app image

In the terminal on the first server:

```sh
cd /mnt/user/appdata/crosssync && docker build -f deploy/Dockerfile -t crosssync .
```

- The first build downloads build tools, so it takes a few minutes and prints
  a lot of text. That's normal.
- It's done when the last line is `Successfully tagged crosssync:latest`.
- Do this on **each** server.

## Step 4 — Install the Unraid template

The template makes CrossSync show up on the **Docker** page as a proper app
(start / stop / autostart / edit). On each server:

```sh
cp /mnt/user/appdata/crosssync/deploy/unraid/crosssync.xml /boot/config/plugins/dockerMan/templates-user/
```

## Step 5 — Add the container

On each server, in the web UI:

1. **Docker → Add Container**
2. Pick **CrossSync** from the *Template* dropdown
3. Keep the two default paths (*Config path* = `/mnt/user/appdata/crosssync/`,
   *User shares* = `/mnt/user`)
4. **Apply**

CrossSync now appears on the Docker page. (The `????` in its IP/Port column
is normal — it uses host networking, so it shares the server's IP directly
instead of mapping ports.)

## Step 6 — Create the config file

On **each** server, in the terminal:

```sh
cp /mnt/user/appdata/crosssync/config.example.yaml /mnt/user/appdata/crosssync/config.yaml
nano /mnt/user/appdata/crosssync/config.yaml
```

`nano` is a keyboard text editor: **Ctrl+O** saves (then Enter), **Ctrl+X**
exits, arrow keys move, PageUp/PageDown scroll.

Change these lines:

- `name: nas-01` → this server's name (e.g. `tower`)
- `id: 1` → a unique number (server 1 = `1`, server 2 = `2`)
- `control_listen: "127.0.0.1:55556"` → **`"0.0.0.0:55556"`** — this is what
  lets you open the web UI from your PC's browser
- `listen: ":55555"` → **`:55557`** — 55555 is Resilio Sync's port; if you
  still run Resilio (e.g. during migration), this avoids the clash
- **Leave `peers: []` empty** — you'll add the other server in Step 8. The
  UI works before any peers exist.

Save and exit. Do this on **both** servers, each with its own name and id.

## Step 7 — Start the servers and open the web UI

1. On each server's **Docker** page, click **Start** on CrossSync.
2. On your PC, open: `http://<server-1-ip>:55556/` (use that server's normal
   LAN IP, e.g. `192.168.1.50`).

You should see the CrossSync dashboard. It lists no peers and no folders yet
— that's expected and fine.

> **UI won't load?** 9 times out of 10 it's `control_listen` still set to
> `127.0.0.1`, or the container showing `Restarting` instead of `Up`. See
> Troubleshooting.

## Step 8 — Pair the servers (swap fingerprints)

**On server 1**, open the config and replace `peers: []` with a block
describing server 2:

```sh
nano /mnt/user/appdata/crosssync/config.yaml
```

```yaml
peers:
  - id: 2
    name: nas-02
    addresses:
      - "100.64.0.2:55557"     # server 2's Tailnet IP + the sync port
    fingerprint: ""
```

Save, exit, and **restart** the container (it starts fine even unpinned).

Now generate each server's fingerprint. This works whether the daemon is
running or not. On **server 1**:

```sh
docker run --rm \
  -v /mnt/user/appdata/crosssync:/config \
  -v /mnt/user:/mnt/user \
  crosssync fingerprint --config /config/config.yaml
```

Copy the `fingerprint:` value it prints (a long string of hex). Do the same
on **server 2** and copy its value.

Then:

- On **server 1**: paste **server 2's** fingerprint into the peer block,
  replacing `""`.
- On **server 2**: add a peer block for server 1 (using server 1's Tailnet
  IP + `:55557`) and paste **server 1's** fingerprint into it.

Restart **both** containers. In the web UI, **Settings → Peers** should soon
show **● connected**.

## Step 9 — Add a folder and test

1. In the UI on server 1: **Settings → Folders → + Add folder**
2. Give it any **id** — it must be **identical on both servers**. The id is
   the pairing key; paths don't have to match.
3. Set the path, e.g. `/mnt/user/systems`
4. Save. Do the **same** on server 2 with the **same id**
5. Drop a test file into the share on one server — within seconds it appears
   on the other

## Step 10 — Finish up

- Turn on **Autostart** for both containers (Docker page) so they survive a
  reboot.
- (Optional) Hide the `.sfx-tmp/` transfer folder from Windows/macOS clients
  — see the *SMB hygiene* section of `unraid-setup.md`.
- **Adding a 3rd server:** repeat Steps 2–7 on it, then add a peer block for
  it on every existing server and swap fingerprints with each.

---

## Updating CrossSync later

Two ways — the self-update path is the easy one:

### A. Self-update (recommended)

1. Publish builds: either run `./release.sh` locally (set `UPDATE_BASE_URL`
   to wherever you host `dist/`) and upload `dist/`, or enable the included
   GitHub Action (`.github/workflows/release.yml`) which auto-builds on
   every push to `main` and publishes a rolling release. The manifest lands
   at:
   `https://github.com/<owner>/<repo>/releases/latest/download/latest.json`
2. In `config.yaml` on each server:
   ```yaml
   update_url: "https://github.com/<owner>/<repo>/releases/latest/download/latest.json"
   # update_auto: true   # optional: apply without asking (default off)
   ```
   and restart the container once.
3. From then on the daemon polls the manifest. When a new build is pushed,
   the web UI shows a **"⟳ daemon update"** button (Settings → Daemon
   updates → Apply update). One click downloads, checksum-verifies, swaps
   the binary in (`/config/crosssync`) and restarts — **`config.yaml` and
   your indexes are never touched**. With `update_auto: true` it does this
   by itself (it waits for an idle moment, i.e. no sync in flight).

> If you point `update_url` at a location only reachable inside your
> network, `file://` URLs also work (e.g. a path on a mounted share).
> Remove `/config/crosssync` to revert to the image's binary.

### B. Manual rebuild (fallback)

1. On each server, inside `/mnt/user/appdata/crosssync`:
   ```sh
   git pull
   ```
   If git complains about *"dubious ownership"*, run this once first:
   ```sh
   git config --global --add safe.directory /mnt/user/appdata/crosssync
   ```
2. Rebuild:
   ```sh
   cd /mnt/user/appdata/crosssync && docker build -f deploy/Dockerfile -t crosssync .
   ```
3. **Remove** the container on the Docker page and **Add Container** again
   (same defaults) so it picks up the new image, then **Start**. Your
   `config.yaml` is untouched — it lives outside the repo.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Container shows `Restarting (127)` | Run `docker logs CrossSync` (capital C) — it says why. If it mentions `groupadd: not found`, your image is old: `git pull`, rebuild, re-add the container. |
| `docker logs crosssync` says "No such container" | The name is **`CrossSync`** (capital C) — or use the container ID from `docker ps -a`. |
| Web UI won't load | In the config, `control_listen` must be `"0.0.0.0:55556"`; restart the container. Also confirm the container is `Up`, not `Restarting`. Try the server's IP instead of its name. |
| Peer shows `○ offline` / "fingerprint mismatch" | Run `crosssync fingerprint` on that server and paste the result into the other server's peer block, then restart both. |
| "address already in use" on 55555 | You're still running Resilio — use `:55557` for `listen` and in every peer's `addresses`. |
| Files show `unsynced` / "permission denied" | The container's PUID/PGID don't match your shares' owner — edit them on the Docker page (defaults 99/100 = Unraid's `nobody`/`users`). |
| Git says "dubious ownership" | `git config --global --add safe.directory /mnt/user/appdata/crosssync` |

**Ports cheat sheet:** sync = **55557**, web UI = **55556**. Every server must
use the same sync port.

---

For day-to-day use, migration from Resilio Sync, and deeper background, see
[`unraid-setup.md`](unraid-setup.md).
