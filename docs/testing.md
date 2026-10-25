# Differential (mostly-in-sync) test runbook

How to prove, on two real boxes, that a large mostly-in-sync pair converges by
transferring only the difference — and that the scan survives the volume.

Everything here goes through the daemons' control API. No shell on either box,
no share credentials, no copying between machines: each node writes its own side
with [`POST /api/folders/{id}/testdata`](api.md#convergence-test-data), which
produces byte- and mtime-identical trees from the same spec.

Set these for the examples below:

```sh
A=http://tower:55556          # node A (the box you drive from)
B=http://nas:55556            # node B
FID=<folder id>               # /api/status shows it
```

## Why mtimes matter

Two independently generated corpora are "in sync" only if content **and**
mtime match. If mtimes differ, every file is a change on both sides and the run
measures conflict handling, not convergence. `testdata` derives both from the
spec, so `{"seed":42,"files":N}` on both nodes yields identical files.

## Procedure

**1. Quiesce.** Pause both folders so the baseline scanner does not index a
half-built corpus:

```sh
curl -sX POST $A/api/folders/$FID/pause -d '{"paused":true}' -H 'content-type: application/json'
curl -sX POST $B/api/folders/$FID/pause -d '{"paused":true}' -H 'content-type: application/json'
```

**2. Seed the same corpus on both.** One call is capped at 8 GiB / 200k files, so
a 100 GiB corpus is ~13 calls per node; use `start_at` to append:

```sh
for node in $A $B; do
  for chunk in 0 1 2 3 4 5 6 7 8 9 10 11 12; do
    curl -sX POST $node/api/folders/$FID/testdata -H 'content-type: application/json' \
      -d "{\"kind\":\"base\",\"dir\":\"testdata/base\",\"files\":1600,\"size\":4194304,\"seed\":7,\"start_at\":$((chunk*1600)),\"confirm\":true}"
  done
done
```

**3. Diverge each side** — the four cases that matter:

| case | call |
|------|------|
| new files + folder, one side | `{"kind":"base","dir":"testdata/only-a/nested","files":200,"size":4194304,"seed":11,...}` on `$A` |
| new nested folder, other side | `{"kind":"base","dir":"testdata/only-b/deep/x/y","files":150,"size":4194304,"seed":22,...}` on `$B` |
| renamed directory | `{"kind":"rename","dir":"testdata/renamable","to":"testdata/renamed-by-a",...}` on `$A` |
| newer version, one side | `{"kind":"modify","dir":"testdata/versions","files":20,...}` on `$A` |
| concurrent edit (conflict) | `{"kind":"modify","dir":"testdata/versions","files":5,"seed":999,...}` on **both**, different seeds |

**4. Resume and measure.** Record `bytes_up`/`bytes_down` from
`/api/status` immediately before resuming, then poll until both nodes report the
same `files` and `size` with `pending_files` and `pending_up` at 0.

```sh
before_up=$(curl -s $A/api/status | jq '.folders[0].bytes_up')
before_down=$(curl -s $B/api/status | jq '.folders[0].bytes_down')
# ... unpause both ...
after_up=$(curl -s $A/api/status | jq '.folders[0].bytes_up')
after_down=$(curl -s $B/api/status | jq '.folders[0].bytes_down')
```

## Acceptance criteria

A run passes only when **all** of these hold:

- **Delta only.** Bytes moved ≈ the divergence you created (new files + newer
  versions + conflict copies), not the corpus size. A rename must cost **zero**
  bytes. If the corpus was 100 GiB and 5 GiB of it differs, expect ~5 GiB.
- **Identical end state.** Same file count and byte total on both nodes, and the
  per-path listing agrees (`/api/folders/{id}/files?limit=...`).
- **No duplication.** After a rename, the old path exists on *neither* node. Two
  paths holding the same content is a failure even though nothing was lost.
- **No open anomalies.** `open_events` is 0 on both, and the event log has no
  `warning`/`conflict` rows the scenario did not ask for. Conflicts you
  deliberately created are expected; anything else is not.
- **Scan survived.** `pending_compute_ms` stays bounded (a full-index pass is the
  scale limit: ~24 s per round at 1M files on this hardware), and the control
  plane stays responsive *during* the transfer — `/api/status` must answer in
  about a second while the node hashes. It used to block for minutes; that was a
  real bug, so treat a slow status as a finding, not noise.
- **Deletion safety.** Bulk deletions must be held/archived, not destroyed;
  `GET /api/folders/{id}/hold` shows what was preserved.

## Cleaning up

```sh
curl -sX POST $A/api/folders/$FID/testdata -H 'content-type: application/json' \
  -d '{"kind":"cleanup","dir":"testdata","confirm":true}'
```

Do the same on `$B`, then let both settle and confirm the share is back to its
pre-test file count. A cleanup of tens of thousands of files trips the deletion
guard, so expect the copies to appear in the hold area rather than vanish
immediately — that is the safety feature working.
