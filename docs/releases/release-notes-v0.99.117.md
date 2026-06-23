# sluice v0.99.117

**CRITICAL fix: concurrent restore (the v0.99.116 within-table parallelism) could *silently under-copy rows* into a target undergoing a live storage-grow reparent. v0.99.117 wires the ADR-0110 coordinated grow-gate into the restore path so concurrent writers quiesce through the reparent instead of outrunning the target's replication.**

Anyone who ran `sluice restore` on **v0.99.116** into a target that auto-grows storage mid-restore (notably a fresh non-Metal **PlanetScale** instance crossing its first grow) should upgrade and re-restore. Serial restore (`--bulk-parallelism 1`) on v0.99.116 was not affected.

## Fixed

The Track-C live A/B caught this: a full MySQL `commerce` backup restored into a fresh PlanetScale-MySQL PS-10, **serially vs. in parallel**. The parallel restore finished `rc=0` with every table logged complete — but **six tables were short by up to 6.1 M rows** versus the backup manifest, while the **serial restore matched the manifest exactly**.

Root cause (Phase-A log forensics): during the fresh instance's **first storage auto-grow**, the volume filled (`Error 1114 "table is full"`) and then reparented (`Error 1105 QueryList.TerminateAll()`). Restore's concurrent writers — the ADR-0112 table×chunk fan-out — each *independently* hammer-retried the struggling target, and because the parallel path writes ~2.4× faster than serial, it **outran the target's replication**: the new primary was promoted without the recently-committed-but-unreplicated rows. There were **zero tolerated duplicate-key collisions** on retry, confirming the lost rows were never re-attempted — they were committed + acked on the old primary and dropped on the reparent. The serial path, naturally calm (one writer), stayed within replication and lost nothing.

**The fix** is the coordinated **grow-gate** the `migrate` cold-copy already uses (ADR-0110, `internal/pipeline/grow_gate.go`): one engine-neutral pause primitive, signal-driven, shared across all of a run's write workers. Restore now constructs it unconditionally and wires it onto **every** restore writer — primary, cross-table-pool, and within-table chunk-worker — through the single `openTargetRowWriter` construction path. When the first worker hits a classified grow transient, it **trips the gate and all workers quiesce together**; with writes stopped, the target's replication drains and the reparent completes calmly, then the gate reopens and workers resume against the new primary. This gives the concurrent path serial's "calm during reparent" property. Restore was the one bulk-copy path that never had the gate wired — `migrate`'s concurrent cold-copy, with the *same* writer + plain-INSERT path, has been byte-perfect through live storage-grow reparents precisely because it has the gate.

## Validation

- Unit pin: every writer the restore opens receives a non-nil grow-gate (the wiring the bug lacked).
- Live re-validation: a full parallel restore into a fresh reparenting PlanetScale PS-10 converges **byte-identical to the manifest** (the exact scenario that lost 6.1 M rows on v0.99.116).
- The serial restore was, and remains, byte-perfect — it was the control that exposed the gap.

A scale-aware, opt-in **post-restore row-count verification** is tracked separately as defense-in-depth. A mandatory full `COUNT(*)` gate is impractical on very large tables (it can exceed the target's statement-timeout), so **prevention via the grow-gate is the primary fix**, with verification as a backstop rather than the guarantee.

## Compatibility

No API or flag changes. Restore parallelism remains auto-on by default (`--bulk-parallelism`); this release makes that default *safe* on storage-growing targets. No effect on `migrate`, `sync`, or `backup`. Concurrency change — landed through CI's `-race` integration gate before tagging.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.117
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.117
```
