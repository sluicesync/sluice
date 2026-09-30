# ADR-0020: Slot-ack-after-apply for Postgres CDC

## Status

Accepted; the RULE stands, the MECHANISM was replaced by GC-41 (2026-09-30, unreleased) — see the amendment at the end. The paragraph below describes the original implementation.

Implemented in `internal/engines/postgres/lsn_tracker.go` (the producer/consumer holder), `internal/engines/postgres/cdc_reader.go` (consumer-side wiring on the keepalive path), `internal/engines/postgres/change_applier{,_batch}.go` (producer-side reporting on commit), and `internal/pipeline/streamer.go` (cross-engine wiring via the `lsnTrackerProvider` / `lsnTrackerAttacher` structural interfaces).

## Context

Postgres logical replication slots advertise progress via two LSNs the consumer ack's on the standby-status update path:

- `WALWritePosition` — what the consumer has **received** off the wire.
- `WALFlushPosition` / `WALApplyPosition` — what the consumer has **durably applied**.

The pre-v0.5.0 Postgres CDC reader collapsed both into a single value (`confirmedLSN`) and advanced it as soon as the pump parsed a `Commit` message off the WAL stream. The keepalive routine sent that value as `WALWritePosition`; the slot's `confirmed_flush_lsn` advanced to it. **In other words, the slot was being told "I durably applied this" before any apply had actually happened.**

When `--apply-batch-size > 1` lands on the target, the applier buffers up to N source changes in an in-memory transaction before the commit fsync. The commit-LSN of every change in the batch has been parsed by the pump (and ack'd to the slot) BEFORE the batch's data is durable. If the streamer exits cleanly mid-batch (operator-issued `sluice sync stop`, ctx cancellation, etc.) the buffer is dropped from memory, but the slot has already advanced past those events.

On warm-resume, the slot delivers WAL strictly after `confirmed_flush_lsn`. The dropped events are gone — permanently.

This was Bug 15 in the v0.4.0 night soak: a 43-row gap on the `customers` table corresponding to the 37-second window immediately preceding `sync stop`. Cross-engine confirmed (PG → MySQL hits the same surface — the bug is in the PG-source CDC reader path, not the applier). Two-stage data loss observed: initial in-flight buffer dropped at stop, plus continued ack-without-apply after restart that wedged the stream entirely until manual `slot drop --force` + state reset + cold-start.

## Decision

Split the LSN bookkeeping into two values on the reader and only ack the durably-applied value:

- **`streamedLSN`** — highest commit-LSN parsed off the WAL stream. Local to the pump goroutine; used internally for keepalive cadence bookkeeping and as the fallback ack value when no applier feedback is wired (legacy non-streamer callers, tests).
- **`appliedLSN`** — highest LSN whose data has been committed to the target. Lives in a small `lsnTracker` struct shared between the applier (single producer) and the reader's keepalive routine (single consumer). The applier reports each successfully-committed change's LSN; the reader's keepalive path reads the latest value and sends it as `WALWritePosition`/`WALFlushPosition`/`WALApplyPosition` on every standby-status update.

The standby update sends `appliedLSN` for all three positions. The slot's `confirmed_flush_lsn` only advances when the applier has confirmed durable apply; the slot retains WAL until then.

### Plumbing shape

The reader and applier are built independently by the engine factory (`Engine.OpenCDCReader`, `Engine.OpenChangeApplier`). The streamer wires them together at run time via two structural interfaces, both opaque (typed `any`) so the pipeline package stays engine-neutral:

```go
// pipeline package
type lsnTrackerProvider interface { LSNTracker() any }
type lsnTrackerAttacher interface { AttachLSNTracker(t any) }
```

The Postgres `ChangeApplier` implements `lsnTrackerProvider` (returns its lazily-allocated `*lsnTracker`); the Postgres `CDCReader` implements `lsnTrackerAttacher` (type-asserts back to `*lsnTracker` and stores it). MySQL implements neither — its applier is synchronous-per-commit, so there's no buffer to drop and no feedback to wire.

The streamer probes both interfaces. On a same-engine PG pair, the wiring lights up. On cross-engine pairs (PG-source, MySQL-target) the streamer fetches the MySQL applier's `LSNTracker()` (which doesn't exist, so the type assertion fails the `lsnTrackerProvider` check) and the wiring stays unwired — which is wrong; the PG reader still leaks ahead of the apply on a MySQL target. The fix: ANY engine that has an asynchronous-batch-apply path needs the producer side. ADR-0017's batched apply is the apply-side path that creates the gap; future engines wanting batched apply implement `LSNTracker()` so the PG reader (when chained as source) gets feedback. For Postgres-only buffering today, the cross-engine surface is closed because the PG applier owns the tracker and the PG reader owns the consumer side — same package, both wired.

### Concurrency

The tracker uses a CAS loop on a single `atomic.Uint64` to enforce monotonic advance. Single-producer/single-consumer is the realistic shape (the applier loop is single-goroutine; the keepalive routine is single-goroutine in the pump), but the CAS keeps the invariant correct under any future concurrency. A zero-LSN report is a no-op, so an empty position token from the per-change Apply path doesn't reset the floor.

## Why not the alternatives

- **Persist `source_position` to state-table BEFORE ack'ing to slot, and on warm-resume always start streaming from `source_position` (asking the slot to rewind if needed).** This works at the cost of a state-table write per batch on the latency-critical ack path. The tracker is cheaper (an atomic store) and achieves the same correctness for the same window: the slot's `confirmed_flush_lsn` is the source of truth for "where to resume from", and we keep it pinned to applied work.
- **Ack on every batch commit instead of every keepalive interval.** That's effectively what the new wiring does — the applier's report and the keepalive read are decoupled, so the latest applied value is sent on the next keepalive (≤ 10 seconds). Pinning the keepalive cadence to commit cadence would tighten the window further at the cost of more standby-status traffic; the 10-second default is comfortable headroom for `wal_sender_timeout` (60s default) and matches what Debezium and pg_ridge do.

## Consequences

- **Slot retains WAL longer.** The slot's `confirmed_flush_lsn` no longer advances ahead of the apply. Worst case, the slot retains WAL for the duration of one apply batch — a few seconds of additional disk usage per batch on the source. PlanetScale Postgres recommends `max_slot_wal_keep_size > 4GB` for Patroni-backed clusters; that's a generous headroom for this overhead.

- **Stop-and-restart is now safe.** The canonical confirmation lives in `internal/pipeline/streamer_bug15_integration_test.go::TestStreamer_PostgresToPostgres_StopRestartNoLoss`: cold-start a stream, drive a sustained writer, mid-stream `RequestStop`, restart, assert `MAX(id) = COUNT(*)` on the resumed dst. Pre-fix, this test fails with two distinct gaps. Post-fix, the invariant holds.

- **Memory footprint unchanged.** The tracker is a single `atomic.Uint64`; no per-batch allocation.

- **No changes to the IR.** The structural interfaces live in the pipeline package and are matched via `any` plus internal type assertions; engines opt in by exposing the right method names without taking a hard dependency on a cross-package interface declaration. Future engines that ship asynchronous-batch apply pick up the safety with two additions: `LSNTracker()` on the applier, `AttachLSNTracker(any)` on the reader.

## Migration notes

No state-format changes. Pre-v0.5.0 streams resume cleanly: the persisted `source_position` is the last applied change's commit-LSN, which is what the new tracker would have reported anyway. The slot's `confirmed_flush_lsn` may have advanced past applied work for streams that were stopped mid-batch on the old code; on warm-resume the new reader's tracker is freshly allocated (applied=0) and the keepalive falls back to streamedLSN until the applier reports its first commit. That's correct: the slot stays pinned to its current `confirmed_flush_lsn`; new applies advance it from that floor. The pre-existing Bug 15 surface (lost rows from a previous stop) cannot be retroactively recovered — the WAL has been recycled — but the stream itself stays usable post-resume.

## Amendment 2026-09-30 — GC-41: the tracker is replaced by a durable-position ceiling

The rule is unchanged: the slot is acknowledged only to WAL whose changes the consumer holds durably. The applied-LSN tracker that implemented it had two defects, both found by the 2026-09-30 slot-acknowledgement triage:

- **(a) CRITICAL, silent loss — it existed for one target engine.** Only the Postgres applier provided `LSNTracker()`. For every other target the reader fell back to acking the STREAMED LSN, and the streamer's comment called that fallback "correct for engines without an async-batched apply layer" — no such target existed. A Postgres → MySQL-family sync stopped mid-batch resumed past the buffered changes (measured 22 rows against 221).
- **(b) HIGH, unbounded WAL retention — within that engine, one write path never reported.** The concurrent lanes' checkpoint (`laneApplierAdapter.WriteCheckpoint`) persisted the position but never reported it, so on the default `--apply-concurrency auto` path the tracker stayed at 0 and the slot was acked at the stream's start LSN for the life of the stream (v0.99.130, when the tracker was first actually attached, through v0.156.6).

Both are the same shape: a feedback input that is correct only while every position-writing site in every engine remembers to feed it. The replacement has no such input:

- The reader never acks past `max(startLSN, ackCeil)`, **unconditionally** (the zero value holds at the start position). `ackCeil` is raised only by `ReleaseSlotAckTo`.
- The streamer's apply-phase sidecar (`startSlotAckCeiling`, `internal/pipeline/streamer_slot_ack.go`) reads the stream's persisted position back from the TARGET through the mandatory `ChangeApplier.ReadPosition` and releases it. The evidence it reads IS the durable write, whichever engine, batch size, lane count or apply path wrote it. The backup chain releases committed manifest ends through the same seam, as before, without the former opt-in `HoldSlotAckAtCommitted`.
- `lsn_tracker.go`, `LSNTracker()`, `AttachLSNTracker`, the applier's `reportAppliedToken`, the shared batch loop's `AfterCommit` hook and the pipeline's `lsnTrackerProvider` / `lsnTrackerAttacher` are deleted.

Gates: `TestSlotAckInputsRoster_EveryInputIsDurableEvidence` (every field `ackLSN` reads is listed as durable evidence; the ceiling moves only in `ReleaseSlotAckTo`), `TestSlotAckReleaseRoster_EveryStreamChangesSiteReleases` (every pipeline `StreamChanges` site hands its reader to a release mechanism), `TestSlotAckCeilingRoster_EveryRegisteredEngine` (every registered engine classified: a sync target whose `ReadPosition` reads the durable row, or checked as having no applier), and end to end `TestStreamer_PostgresToMySQL_SlotAckNeverPassesTheDurablePosition` and `TestStreamer_PostgresToPostgres_SlotAckFollowsTheDurablePosition` (W ∈ {1, 4, 0}).

Cost relative to the tracker: one primary-key read of `sluice_cdc_state` per 5 s interval, and the slot trails the target's durable position by up to one interval plus the 10 s keepalive rather than one keepalive.
