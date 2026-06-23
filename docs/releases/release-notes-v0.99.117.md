# sluice v0.99.117

**CRITICAL fix: concurrent restore (the v0.99.116 within-table parallelism) could *silently under-copy rows* into a target undergoing a live storage-grow reparent. v0.99.117 makes parallel restore correct-by-construction by re-deriving any reparent-touched table from its backup chunks (ADR-0113 reconciliation), on top of the ADR-0110 coordinated grow-gate.**

Anyone who ran `sluice restore` on **v0.99.116** into a target that auto-grows storage mid-restore (notably a fresh non-Metal **PlanetScale** instance crossing its first grow) should upgrade and re-restore. Serial restore (`--bulk-parallelism 1`) on v0.99.116 was not affected.

## Fixed

The Track-C live A/B caught this: a full MySQL `commerce` backup restored into a fresh PlanetScale-MySQL PS-10, **serially vs. in parallel**. The parallel restore finished `rc=0` with every table logged complete — but several tables were short by millions of rows versus the backup manifest, while the **serial restore matched the manifest exactly**.

Root cause (Phase-A log forensics + a second live run): during the fresh instance's **first storage auto-grow**, the volume filled (`Error 1114 "table is full"`), replication stalled, MySQL semi-sync timed out and **fell back to async**, and the grow **reparented** (`Error 1105 QueryList.TerminateAll()`). The new primary was promoted from a replica behind the async-acked window, so committed-and-acked rows were dropped. The parallel path (8-wide) builds a large async-acked window → large loss; the single RTT-bound serial writer never outruns semi-sync, so it loses nothing. **Zero tolerated duplicate-key collisions** on retry confirmed the lost rows were never re-attempted — they were dropped *before* any worker observed a transient.

**Why a coordinated pause alone wasn't enough.** Wiring the ADR-0110 grow-gate into restore (so all workers quiesce together on the first transient) *substantially reduced* the loss, but the gate is reactive — the lost rows are committed in the window *before* the first transient, so no pause can recover them.

**The fix (ADR-0113) exploits the one thing restore has that a CDC stream doesn't: its immutable, replayable backup chunks.** A writer marks any table that hits a grow/reparent transient. After the fast parallel bulk-copy, the restore **re-derives each marked table from its chunks** — non-DataOnly: `TRUNCATE` + a **serial** redo into the now-grown volume (the pace that never outruns replication; no primary-key or UPSERT needed, since indexes/constraints are later phases); DataOnly chain segment: idempotent re-apply — looping until a pass observes no new touches. Because a reparent is the *only* loss vector, "a clean redo pass" ⇔ "byte-perfect", so the reconciliation **is** the guarantee — with no impractical full-table `COUNT(*)` gate. The cost is bounded to the touched subset and only paid when a reparent actually occurred: the bulk restore stays fast (parallel), and only the few reparent-exposed tables get a serial redo.

The grow-gate wiring stays (it calms the target and cuts retry churn); reconciliation is the correctness guarantee layered on top.

## Validation

- Unit pin: a target that drops a row on a simulated reparent (and reports the table touched) is reconciled — the restore TRUNCATE+redoes exactly that table and recovers the full row set; an untouched table is left alone.
- Unit pin: every writer the restore opens receives the coordinated grow-gate.
- Live re-validation: a full parallel restore into a fresh reparenting PlanetScale PS-10 converges **byte-identical to the manifest** (the exact scenario that lost millions of rows on v0.99.116, and that the grow-gate alone did not fully fix).
- Concurrency change — landed through CI's `-race` integration gate before tagging.

## Compatibility

No API or flag changes. Restore parallelism remains auto-on by default (`--bulk-parallelism`); this release makes that default *correct* on storage-growing targets. No effect on `migrate`, `sync`, or `backup`.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.117
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.117
```
