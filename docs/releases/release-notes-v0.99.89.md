# sluice v0.99.89

**HIGH fix: the serial MySQL CDC applier could persist a mid-transaction resume position, which crash-looped every warm-resume with "no corresponding table map event."** If you run MySQL/Vitess CDC on the serial apply path (the default, i.e. without `--apply-concurrency`) against a target slow enough to collapse the adaptive batch size — a cross-region PlanetScale target during a large backlog drain is the classic case — upgrade. The concurrent `--apply-concurrency` path was already immune.

## Fixed

**The serial CDC applier now checkpoints only at source-transaction boundaries (item 29).** The shared serial batched-apply loop persisted the *last applied row's* binlog position on every flush — row-cap, byte-cap, idle-grace, keyless guard, channel-close — not only on a source-transaction boundary (`TxCommit`). That is fine as long as each batch ends at a transaction boundary, but it stops being fine when a single source transaction has **more rows than the batch size**:

- The adaptive batch-size controller (AIMD) collapses the batch toward 1 when the target is slow — for example a cross-region PlanetScale target while it drains a large accumulated backlog.
- A large multi-row source transaction then splits across many batches, and a mid-batch flush persists a binlog position pointing **into** the transaction, at a row event.
- MySQL file/pos resume cannot start mid-transaction. On the next reconnect, go-mysql seeks to that byte offset, reads a `ROWS` event whose `TABLE_MAP` appeared earlier in the same transaction, and fails fatally with **`no corresponding table map event`**. The stream can no longer warm-resume — it crash-loops until a fresh resnapshot.

This was also the real root cause behind a symptom previously attributed to binlog transaction compression (item 28): on a clean, compression-off source the failing event was a plain `UPDATE_ROWS`, not a compressed payload.

The fix gives the serial loop the invariant the concurrent `--apply-concurrency` path already enforced through its commit frontier: **the persisted resume position advances only to a source-transaction (or DDL) boundary.** A mid-transaction flush still commits its data — memory stays bounded, throughput is unchanged — but it skips the position write, so the persisted checkpoint never points inside a transaction. On a crash or restart, the stream re-reads the whole in-flight transaction from the previous boundary and idempotently re-applies it (ADR-0010): at-least-once for the interrupted transaction, exactly as sluice's keyless guard and concurrent frontier already are, and exactly-once for keyed tables. The trailing commit of a transaction whose rows committed in earlier batches is checkpointed in a dedicated position-only transaction, so the resume point still advances promptly under batch-size 1.

Postgres is unchanged. PG logical-replication resume is by LSN and the walsender resends whole transactions from the slot's restart LSN, so a mid-transaction restart point is already valid; PG keeps persisting its position on every flush and its replication-slot acknowledgement keeps advancing.

Pinned at the shared-apply-loop seam: a transaction split across batches persists no mid-transaction position and the only position written is the boundary; the Postgres path still persists every flush (regression guard); a transaction that fits in one batch writes the boundary atomically with its data. These compose with the existing reader-level resume tests, which prove go-mysql resumes cleanly from a transaction-boundary position.

A note on the symptom that surfaced this: the same run showed the apply latency spiking to ~10s per batch and the batch size pinned at 1. That was a transient effect of the initial backlog drain running large batches that each took several seconds, polluting the adaptive controller's latency window — not a slow target. The target's commit latency is in the tens of milliseconds; once the checkpoint bug is fixed (or `--apply-concurrency` is used), apply keeps pace.

## Compatibility

No interface, flag, or default-behavior changes. The fix is internal to the MySQL serial CDC apply path. Postgres CDC is byte-for-byte unchanged, and the concurrent `--apply-concurrency` apply path (all engines) was already correct. Both file/pos and GTID MySQL resume modes benefit (the conservative boundary-only checkpoint is correct for both).

## Who needs this

Anyone running **MySQL or Vitess/PlanetScale CDC on the default serial apply path** (no `--apply-concurrency`) against a target that can be slow enough to drive the adaptive batch size down — most commonly a **cross-region or heavily-loaded PlanetScale target draining a large backlog**, where a single large source transaction split across batches could leave a mid-transaction checkpoint that crash-looped on resume. If you already run with `--apply-concurrency > 1`, you were not affected. Postgres-target users are unaffected.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.89
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.89
```
