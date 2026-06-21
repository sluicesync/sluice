# sluice v0.99.96

**The root face of the PlanetScale storage-auto-grow stall is now handled: a transient target out-of-disk.** This completes the resilience arc started in v0.99.92 — a large cold-copy / CDC apply into a non-Metal PlanetScale target now rides through the volume's mid-copy auto-grow instead of dying on the actual "No space left on device".

## Fixed

**Transient target out-of-disk (`No space left on device`, OS errno 28) is now retriable.** While a non-Metal PlanetScale volume is full and auto-growing, a write to the target surfaces "No space left on device". MySQL/vttablet wraps this inconsistently — as `Error 3` "Error writing file" (`code = Unknown`), `ER_DISK_FULL` (1021), or the bare ENOSPC text — and none of those were recognized by the Error-1105 vttablet-code branch of the retry classifier (it only inspects the vttablet gRPC code when the MySQL error number is 1105). So the cold-copy / CDC apply aborted on the actual disk-full, even though the *surrounding* faces of the same auto-grow stall were already handled: the primary-reparent error (v0.99.92, ADR-0108), the source-read timeout from backpressure (v0.99.93, ADR-0109), and the vttablet query-killer (v0.99.94).

The fix routes a target disk-full through sluice's existing `isDiskFullSignal` matcher (the errno-28 text plus `ER_DISK_FULL`) to a **bounded** retry. A transient auto-grow adds space and the retry succeeds; a genuinely-full, non-growing target (for example an undersized fixed-storage Metal instance) exhausts the retry budget and fails **loudly** — it is never an infinite wait. Found live on the v0.99.95 PS-320 storage-grow validation: the copy rode roughly eight minutes of query-killer retries (v0.99.94 working as designed) and then died on this unretried disk-full — the last observed face of the auto-grow stall. Pinned in the classifier test set (both the `Error 3` errno-28 shape and `ER_DISK_FULL` classify retriable).

## Compatibility

No configuration changes and no behaviour change for an untroubled migration. The only difference engages on a transient target disk-full that previously aborted the copy: it is now retried (bounded, loud on exhaustion) instead of being fatal. No resume-format, wire, or result-state changes.

## Who needs this

Anyone running a large `sluice` cold-copy or CDC apply into a **non-Metal PlanetScale MySQL** target whose storage auto-grows mid-copy. With v0.99.92 + v0.99.93 + v0.99.94 + this release, the copy now rides through every observed face of a storage-grow stall — reparent, source-read timeout, query-killer, and the root out-of-disk — instead of dying at the boundary. Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.96
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.96
```
