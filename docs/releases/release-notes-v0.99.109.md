# sluice v0.99.109

**Bug 159 (HIGH, loud-safe): the concurrent CDC apply path now persists the resume watermark on a low-volume marker-less stream — it was frozen at `{"last_id":0}` for a sparse postgres-trigger source.** No data loss, but a real watermark/scalability fix: without it the source change-log could never be pruned and every warm-resume re-read it from the start.

## Fixed

The concurrent key-hash apply orchestrator (`internal/laneapply`, the default since ADR-0106 / v0.99.91) persisted the resume `source_position` only every 2000 routed changes, at a barrier, or at clean end-of-stream. A **low-volume, marker-less** stream — notably a `postgres-trigger` source, whose reader emits row changes but no `TxBegin`/`TxCommit` markers — hits none of those, so the persisted watermark never advanced past the cold-start anchor (`{"last_id":0}`).

The data still converged byte-perfectly and warm-resume still produced correct data — so there was **no data loss**. But the consumed watermark never persisting had two consequences: the source `sluice_change_log` could never be pruned (unbounded growth), and every warm-resume re-read the whole change-log from id 0 and idempotently re-applied it (an at-least-once re-read, O(everything) instead of O(new)).

The serial applier was always immune via its item-18 100 ms idle flush, which persists the position on every quiet-stream flush; the gap surfaced only once ADR-0106 made the concurrent path the default for everyone.

**The fix** adds a 1-second idle-checkpoint tick to the orchestrator's coordinator loop that persists the **durable frontier** on a quiet stream (mirroring the serial idle flush), plus records the trailing boundary of a marker-less position-run so the last applied change becomes checkpointable. It only ever persists a fully-durable frontier boundary, so the position can **never lead committed data** — the exactly-once contract is unchanged — and it is a **no-op on a marker stream** (MySQL binlog, Postgres with Tx markers), so the v0.99.89 / v0.99.90 mid-transaction-position protections are untouched.

Found by the post-release regression cycle and root-caused with the three-phase protocol against a live rig (ground truth captured: persisted `last_id=0` versus change-log `max(id)=4` at production-default concurrency). Pinned by a unit test (a low-volume marker-less stream checkpoints mid-stream, channel held open) and a postgres-trigger integration test (`source_position` advances past 0 after CDC changes apply) — both verified to fail against the unpatched code.

## Compatibility

No configuration changes. No behaviour change for a marker stream (MySQL binlog / Postgres logical) or for a high-volume stream that already hit the count-based checkpoint cadence. The only change is that a quiet / low-volume stream's resume watermark now stays current within ~1 second, where before it lagged until 2000 changes accrued. No resume-format, wire, or value changes; the exactly-once contract is unchanged.

## Who needs this

Anyone running `sluice sync` from a **postgres-trigger** source (the trigger-based Postgres CDC capture) — especially a low-write-rate source — should upgrade: your resume watermark now advances, the source change-log becomes prunable, and warm-resume reads only new changes instead of re-reading the whole log. The fix also covers any future low-volume marker-less CDC source on the concurrent apply path.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.109
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.109
```
