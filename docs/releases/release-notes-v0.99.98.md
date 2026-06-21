# sluice v0.99.98

**Completes the target out-of-disk coverage from v0.99.96.** A managed InnoDB target out of tablespace can report "The table is full" (`Error 1114`) instead of `ER_DISK_FULL` / errno-28 — the same root condition, a different code — which slipped through the v0.99.96 disk-full retry and aborted a cold-copy at the next storage-grow step. v0.99.98 recognizes it.

## Fixed

**`ER_RECORD_FILE_FULL` (`Error 1114`, "The table is full") is now treated as a transient out-of-disk and retried.** v0.99.96 made a target out-of-disk retriable via `isDiskFullSignal`, but it only matched `ER_DISK_FULL` (1021) and the errno-28 / "No space left on device" text. While a non-Metal PlanetScale volume is full and auto-growing, an InnoDB write can instead surface `Error 1114` "The table '<t>' is full" — vttablet wraps it as `code = ResourceExhausted desc = The table '<t>' is full (errno: 28 - No space left on device)`. That is the *same* out-of-disk condition with a different MySQL error number, and it slipped through, aborting the cold-copy at the next grow step (the v0.99.97 PS-320 validation rode further still and then died here). `isDiskFullSignal` now also matches `ER_RECORD_FILE_FULL` (1114) and the "table is full" / "is full (errno" text, so the existing bounded retry rides the auto-grow out. A genuinely-capped, non-growing target still exhausts the retry budget and fails loudly. Pinned in the classifier test set alongside the 1021 / errno-28 disk-full cases.

## Compatibility

No configuration changes and no behaviour change for an untroubled migration. An `Error 1114` "table is full" that previously aborted is now retried (bounded, loud on exhaustion). No resume-format, wire, or result-state changes.

## Who needs this

Anyone running a large cold-copy / CDC apply into a **non-Metal PlanetScale MySQL** target whose storage auto-grows mid-copy — this closes the last observed out-of-disk variant in the storage-grow resilience set (v0.99.92–v0.99.98). Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.98
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.98
```
