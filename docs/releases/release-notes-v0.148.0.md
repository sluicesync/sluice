# sluice v0.148.0

> **Correction (2026-09-09, v0.148.1):** the "Stopping a cold start after the copy no longer destroys the replication slot" item below says a stop that lands after the copy can be resumed by re-running with the same `--stream-id`. **That is false for the window it names.** The kept-slot door fires only before the CDC anchor is written (the copy and index phases), so no position is persisted; a re-run finds none, cold-starts, and refuses on the existing slot. Only a stop that lands during the anchor write itself resumes, and that window was already protected before this release. The slot IS kept and DOES pin WAL, as stated; what it buys today is nothing but the consistent point a future handoff resume will need. v0.148.1's `STOPPED-SLOT-KEPT` message says so and gives the real way out (`sluice slot drop … --yes`, then `sync start --reset-target-data`). Tracked as A0909-STOP-1. **Resolved in v0.149.0**, which ships the handoff resume this item described before it existed: on a PostgreSQL source whose copy had finished every in-scope table, re-running with the same `--stream-id` now skips the copy, finishes the remaining phases and starts CDC from the slot's consistent point (`COLD-START-RESUMED`). This correction stands for v0.148.0 through v0.148.3 — on those releases the way out is still the drop-and-re-copy above.

> **Second correction (2026-09-10, v0.149.0):** this release's guidance below — "`LAST PROGRESS WRITE` is the column that matters, not the phase … an age that keeps climbing means the run is gone" — **is wrong, and wrong in the direction that gets a healthy migration killed.** That age comes from a header row which moves only when the PHASE moves; per-table progress is written to a separate table `sync status` does not read. So a long copy of one large table holds the age perfectly still while every table is streaming. The PHASE advancing is the liveness signal; a climbing age means the run is gone only once the phase has stopped moving too, and before acting on it, compare the target's row counts against the source. Corrected in the command output, in `sync health`'s error, and in the cross-region migration guide as of v0.149.0.

**A `sync` cold start is no longer invisible.** sluice's first outside migration — an AWS → GCP `us-east4` move of a PlanetScale MySQL database — succeeded, and the operator sent back a candid field report. Most of this release is their findings. The headline is that `sync status` could not tell a running cold start from a dead process, and said so for hours.

## Features

**`sync status` and `sync health` can see a cold start in flight.** A stream's row in `sluice_cdc_state` is written only at the very end of a cold start, after the copy, the index build **and** the FLOAT exact re-read. That ordering is load-bearing for crash safety and has not changed. What changed is that the cold start now records its phase and per-table progress as it goes, so `sync status` reports it instead of reporting nothing:

```
cold start in progress (no CDC anchor yet — this is expected, not a stall):
STREAM        PHASE      STARTED               LAST PROGRESS WRITE
prod-cutover  bulk_copy  2026-09-08T10:31:02Z  4s ago
```

`LAST PROGRESS WRITE` is the column that matters, not the phase — a phase alone cannot distinguish a run that is working from one that died mid-phase and left its last row behind. Run the command twice: an age that keeps climbing means the run is gone. `sync health` says the same in its error and still exits non-zero, because a cold start is not a healthy stream and a cron probe that treated it as one would go quiet exactly when a stuck cold start needed attention. Available on MySQL and PostgreSQL targets; a target engine with no migration-state store still reports the stream as absent, and the code says so rather than implying otherwise.

**The FLOAT exact re-read announces itself.** The post-copy repair phase logged nothing at all until it finished. It now emits `FLOAT-EXACT-REREAD` up front with a table count — saying in the line itself that a "not found" status is expected there — and logs each table as it completes. The operator who reported this resorted to `strace` on the PID to find out whether sluice was still alive. It was.

**A new operator page: [`docs/operator/cross-region-migration.md`](../operator/cross-region-migration.md).** Where to run sluice (near the **target** — reads pay latency once per stream, writes pay it per batch), how to tell which throughput bound you are on, the safe-migrations index route, and what the quiet post-copy window is.

## Fixed

**Stopping a cold start after the copy no longer destroys the replication slot.** This is the most consequential fix in the release and it predates it by two months. The cold start abandoned its snapshot stream on *any* copy-phase error — including a plain `context.Canceled` — and abandoning drops the just-created replication slot. So Ctrl-C during a long index build dropped the slot for a bulk copy that had **already committed every row**: warm resume then has no position, cold start refuses the populated target, and the only escape was `--reset-target-data` and copying everything again.

A stop and a failure arrive at that code as the same thing — a non-nil error — and they are not the same event. A stop now keeps the slot; a genuine failure still drops it, because there the slot is debris. This is the same call the v0.116-era anchor fix made one phase later, which put the CDC anchor write on an uncancellable context for exactly this reason and left the copy and index phases uncovered.

**The kept slot pins WAL on the source, and sluice says so.** A slot preserved silently would trade a recoverable re-copy for a full disk on a busy source. `STOPPED-SLOT-KEPT` names the slot — already resolved through the `sluice_` prefix, so it is the name that exists on the server — states that it is retaining WAL, and gives both ways out: resume with the same `--stream-id`, or `sluice slot drop … --yes` if you are abandoning the migration. See [`cdc-streaming.md`](../operator/cdc-streaming.md).

**The copy-throughput hint was wrong outside the smallest tiers, and it cost a real operator a tier upgrade.** sluice logged that writes to a PlanetScale target are "tier-CPU-bound, not connection-bound" and that "a larger tier (or Metal) is the real lever". That came from one measurement — a PS-10, the smallest tier, pinning at 100% CPU under a 2-wide copy — generalised to the whole range. Following it, the reporting operator scaled M-160 → M-640 (2 → 8 vCPU) and saw throughput move 14k → 16k rows/s with target CPU at **11%**. They were bound by the single cross-region INSERT connection, and the hint had explicitly steered them away from the fix: `--copy-fanout-degree 16 --vstream-copy-table-parallelism 4` took the copy from an estimated 18–36h to about 4h.

The line now names the **discriminator** — read the target's CPU — and both regimes with their opposite fixes, instead of naming one lever. It carries the grep-stable marker `COPY-BOTTLENECK`. Worth knowing: `--vstream-copy-table-parallelism` defaults to **1, serial**, and `--copy-fanout-degree` to 4.

**A stalled `--capture-replicated-writes` setup no longer records its own DDL as source-side DDL** (`postgres-trigger`). The DDL-suppression evidence is fresh for an hour against `clock_timestamp()`, which advances inside a transaction — and the plan's `DROP`/`CREATE TRIGGER` statements take ACCESS EXCLUSIVE and queue behind any long transaction on a busy table. A plan that waited past the window recorded its own `ENABLE ALWAYS` ALTERs as source-side DDL, and the next open refused with a drain / `--restart-from-scratch` remedy — telling the operator to rebuild, for DDL sluice wrote. The ALTERs are now emitted together after one re-arm. The window is bounded, not eliminated, and the code says so.

**`trigger setup` warns about a declaratively partitioned parent** (`postgres-trigger`, `PARTITIONED-PARENT-CAPTURE`). PostgreSQL clones the row trigger onto every partition and a cloned trigger records the partition's name. `sync start` already refused such a table at preflight, so nothing was ever lost — but the operator learned at the wrong end. Setup now says it, pointing at the same recovery.

**An over-long `--stream-id` costs the status surface, never the migration.** Found by the pre-release trigger, in this release's own new code: `--stream-id` has no length validation and both control tables declare `VARCHAR(255)`, so a 251–255 character stream id would have overflowed the progress table — `Error 1406` on strict-mode MySQL, silent truncation and cross-attribution elsewhere. Recording now degrades with a WARN and the copy is untouched.

**Cold-start progress writes are throttled.** `writeTableProgress` sits inside the per-batch copy loop, so the new recording would have added a synchronous control-table round trip per batch — worst on exactly the RTT-bound cross-region copies this release exists to make legible. Intermediate writes are capped at one per table per 2s; terminal states and each table's first write always pass. `migrate` is deliberately **not** throttled: there the same row is a `--resume` cursor, and coarsening it would widen the replay window on every interrupted migration.

## Internal

Six audit LOW-tail items closed, each with a gate rather than a resolution: an AST-derived index requiring every operator log marker to have a doc home (it caught three markers within minutes of them being written, including two in this release); the session-GUC roster now binds its markers to a single refusal rather than to a file; the ServerUUID roster stops failing open on a `Mode` it cannot read statically; the local pre-commit gate no longer runs over gitignored scratch that CI never sees; and two `x/crypto` advisories with available fixes are cleared.

## Compatibility

No schema change, no new flags, no new error codes, and no change to any position or manifest format. The cold-start progress rows go into the existing `sluice_migrate_state` / `sluice_migrate_table_progress` tables under a `sync-` prefixed id that no `migrate` run can derive, so the two populations cannot alias. `sync status --format json` gains `cold_starts_in_progress` under `omitempty`, so a target with none encodes byte-identically and existing `jq` filters are unaffected. `sync health` exit codes are unchanged.

## Who needs this

Anyone running `sluice sync start` against a large source — especially **cross-region** — where the cold start takes long enough that "is it still alive?" is a real question. Anyone on PlanetScale who has read sluice's throughput hint and considered scaling their tier. And `postgres-trigger` operators using `--capture-replicated-writes` on busy tables.

## Install

```
brew upgrade sluice
scoop update sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.148.0
```
