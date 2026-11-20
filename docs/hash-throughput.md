# Hash throughput: choosing `scan.hash_workers` by measurement

`scan.hash_workers` sets how many files the index hasher reads and hashes at the
same time (the pool inside `scanner.hashPending`). `0` — the default, and the
behaviour before this knob existed — means **one reader per CPU**. On a
many-core node with one folder on one spinning disk that default is a seek
storm, and the right value can only be found by measuring, not by argument.

> **Do not hard-code a "best" worker count.** The correct pool is a property of
> the disk, the array's other load, and the corpus's file sizes. What follows is
> the procedure that decides it, and the evidence that made the knob necessary.

## The measurement that motivated the knob (Tower, 2026-11-20)

A 6891-file / 234.1 GiB folder was being hashed after re-add. Both nodes ran the
same build; the folder lived on ONE data disk on Tower.

| | Tower (37 readers, one platter) | NAS-02 (uncontended) |
|---|---|---|
| wall clock | ~50 min | ~25 min |
| delivered | **~55–70 MB/s** | **~156 MB/s** |

Tower was not CPU-bound — it was waiting:

```
%Cpu(s):  7.2 us,  6.2 sy, 10.3 id, 76.3 wa        load average 17.80 (15-17 in D state)

Device   r/s      rkB/s   r_await  rareq-sz  aqu-sz  %util
sdh     864.0  108,648.0   27.77    125.75   23.99  100.00
sda-sdg   0.0        0.0    0.00      0.00    0.00    0.00
```

864 reads/s, ~126 kB per request, 27.8 ms average latency, queue depth 24,
100 % utilisation, every other disk idle. That is interleaved random I/O from N
readers on one platter, not a sequential scan. `Unraid` dashboards count iowait
as CPU, so "CPU near peak" during a hash usually means the disk is saturated.

Note on request size: `hash.BlockHashes` already reads 1 MiB per syscall
(`hash.readChunk`), so the ~126 kB `rareq-sz` is being produced BELOW us — shfs
(FUSE) splitting our 1 MiB reads into its own I/O units, not a read-size bug in
CrossSync. There is no read-size knob here to turn; if request size ever needs
to change it has to change in shfs/FUSE or by bypassing it (a container bind
mount of `/mnt/diskN` instead of `/mnt/user`), and that is a separate experiment.
What this knob changes is concurrency: how many independent read streams exist.

## How to measure (per node, one folder, one value at a time)

Choose the folder and record the corpus ONCE: `/api/folders` → its `files` and
`size` (bytes). A verify pass re-hashes every file, so the byte total is the
same for every run and the MB/s figure is comparable.

For each candidate `W` in **4, 8, 16, 32** (and, when the box has fewer cores,
cap at `nproc`; also run `0` = default as the control):

1. Decide the pool for the NEXT scan, and confirm what was applied:

   ```sh
   curl -s -X PUT http://127.0.0.1:55556/api/settings/sync \
        -H 'Content-Type: application/json' -d '{"hash_workers": 8}'
   # {"pull_concurrency":4,"hash_workers":8}   <- clamped value in effect
   curl -s http://127.0.0.1:55556/api/status | grep -o '"hash_workers":[0-9]*'
   ```

2. Start the instrument BEFORE the pass, on the node's shell:

   ```sh
   # the disk that holds the folder — find it once with `df <folder>` / `lsblk`
   iostat -x 5 > /tmp/iostat-W.txt &
   vmstat 5 > /tmp/vmstat-W.txt &
   ```

3. Kick a VERIFIED pass (re-hashes every file, so the pass is cold with respect
   to content hashing; it leaves the index intact, nothing re-transfers because
   identical content is reported as `Touched`):

   ```sh
   curl -s -X POST http://127.0.0.1:55556/api/folders/<folder-id>/rescan \
        -H 'Content-Type: application/json' -d '{"verify":true}'
   ```

4. Sample the pass from `/api/status` every 10 s until `scans[].running` goes
   false, keeping `started`, `hash_done`, `hash_total`, `finished`:

   ```sh
   while :; do
     curl -s http://127.0.0.1:55556/api/status \
       | python3 -c 'import json,sys; s=json.load(sys.stdin)["scans"][0]; print(s["running"],s["phase"],s["hash_done"],"/",s["hash_total"],s["started"],s["finished"])'
     sleep 10
   done
   ```

5. Stop the instruments, then compute from the SAME window (the pass's
   `started`..`finished`), not from the whole file:

   - **delivered MB/s** = `size_bytes / (finished - started)` — the headline
     number, because it is what the operator waits for;
   - from `iostat -x`: mean `rkB/s` over the interval, mean `r_await`, mean
     `rareq-sz`, mean `aqu-sz`, and `%util` (should be ~100 throughout if the
     disk is the limit);
   - from `vmstat`: `wa` and the `r` column (runnable/D-state count).

## What counts as a win

- **Primary**: a **≥15 % higher delivered MB/s** for the same corpus on the
  same disk, reproduced in a second run. Below that it is noise on a share.
- **Corroboration**: `r_await` falls and `rkB/s` rises while `%util` stays near
  100 — the disk is still the limit but is doing more useful work per seek.
- **Disqualifying**: `%util` drops well below 100 and MB/s does NOT rise. Then
  the bottleneck was never the reader pool (it is FUSE serialisation, the
  network, or another container), and `hash_workers` is not the lever — say so
  rather than shipping a number.
- **Correctness check, every run**: the pass reports the same `hash_total` as
  the file count, no events of kind `modified` for unchanged files (a verify
  pass emits `Touched`), and `/api/folders` `files`/`size` are unchanged. Every
  hash still comes from real bytes; only the concurrency changed.

## Keeping the sample clean

- One folder, one node at a time. Two nodes hashing the same corpus share the
  array's spindles and the numbers become meaningless.
- Stop the other readers of that disk for the duration: Resilio, Immich,
  Krusader, mover/parity operations, `docker` writes to the same disk. Parity
  check and mover are disqualifying, not "a bit noisy".
- Page cache is the biggest lie. A 234 GiB corpus on a 64 GB box is safely
  larger than RAM; a small test tree is not. If the corpus does not dwarf RAM,
  every run after the first is reading from memory — then either use a big tree,
  or `echo 3 > /proc/sys/vm/drop_caches` before each run and add ~2 min of
  warm-up noise to the numbers.
- Run the candidates in a fixed order twice (4, 8, 16, 32, 4, 8, 16, 32) so a
  warming cache or a drifting neighbour affects all of them equally; report both
  runs.
- Record the pool actually in effect (`/api/status` `hash_workers`) with each
  number. A pass that started before the PUT keeps the old pool — the value is
  sampled once, at the start of the hash phase — so wait for the previous pass
  to finish before changing it.
- A change applies to the NEXT scan only. Do not judge a value from the pass
  that was already running.

## Recording the result

Write the table into the deploy notes for that node:

```
node   date         corpus              W    wall    MB/s   r/s    r_await  %util  note
Tower  2026-11-2x   6891 files/234.1GB  37   50m     60     864    27.8     100    baseline
Tower  2026-11-2x   6891 files/234.1GB   8   ...     ...    ...    ...      ...    chosen
```

Then set it permanently in `config.yaml` (or with the PUT, which persists it):

```yaml
scan:
  hash_workers: 8
```

and leave a one-line comment in the node's notes saying what it was measured
against — the next operator will otherwise "helpfully" remove it.
