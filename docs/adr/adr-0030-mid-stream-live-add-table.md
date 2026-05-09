# ADR-0030: Mid-stream live add-table (Phase 2, no drain)

## Status

Accepted. Implemented in `internal/pipeline/add_table.go::AddTable.LiveMode` (the orchestrator gate), `internal/engines/postgres/slot_manager.go::ReadSlotPosition` (the new optional engine surface), and `cmd/sluice/schema_add_table.go::SchemaAddTableCmd.NoDrain` (the CLI flag). Integration coverage in `internal/pipeline/add_table_live_pg_integration_test.go`.

## Context

Phase 1 of mid-stream add-table (v0.10.x cycle, "Recently landed" in `docs/dev/roadmap.md`) shipped the drained-stream workflow: operator runs `sluice sync stop --wait`, then `sluice schema add-table TABLE`, then `sluice sync start --resume`. The orchestrator (`internal/pipeline/add_table.go`) is conservative — `preflightStream` refuses if the stream's `stop_requested_at IS NOT NULL`, on the assumption that running add-table against a live stream is unsafe.

Re-reading the Phase 1 orchestrator after it shipped: **the conservative refusal is wider than the actual correctness gap.** The flow already does the load-bearing safe-overlap work:

- Step 4: `AddPublicationTables` (`ALTER PUBLICATION ... ADD TABLE`) runs **before** the snapshot. PG's pgoutput evaluates publication membership at decode time, not write time — so any event on the new table at LSN ≥ the publication-add LSN gets delivered to the slot.
- Step 5: snapshot uses a **temp slot** (default `sluice_addtable_<table>`) — independent from the active stream's `sluice_slot`. The snapshot's consistent-point LSN is the floor for what its rows cover.
- Step 6: bulk-copy via the temp-slot snapshot stream, then drop the temp slot.
- Persisted CDC position is intentionally NOT updated. The active stream's existing position is still the right resume point for the other tables; the **idempotent applier** (ADR-0010, `INSERT ... ON CONFLICT DO NOTHING`) absorbs the [persisted_LSN, snapshot_LSN] overlap on the new table when the operator runs `sync start --resume`.

The ONLY thing keeping Phase 1 from being live-safe is the explicit refusal in `preflightStream` (lines 326–360 in the Phase 1 file). The `stop_requested_at` check is a partial-detection of stream activity that erred conservative because the proto-ADR (`docs/dev/design-mid-stream-add-table.md`) flagged Strategy B/C as v2 work. After implementation, Phase 1 already implements the correctness story for Strategy C variant (c) — publication-add-then-snapshot — and the refusal is the only gate.

## Decision

Add a `LiveMode bool` field to `pipeline.AddTable` (CLI flag `--no-drain` on `sluice schema add-table`). When set, the orchestrator skips the `stop_requested_at` refusal and applies tighter safety checks instead:

1. **Engine gate.** Live mode requires the source engine to implement `publicationAdder`. Engines without publications (MySQL) refuse loudly: live add-table is PG-only in this phase.

2. **Slot-position capture.** Before publication-add, read the active stream's slot `confirmed_flush_lsn` via the new optional surface `slotPositionReader` (PG: `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = ...`). This gives the orchestrator a baseline LSN to validate against later.

   The slot name is recovered from the cdc-state row populated by the streamer's per-position `SetSlotName` plumbing: `ChangeApplier.SetSlotName` records the streamer's resolved slot name on the applier; `writePositionTx` threads it into every `sluice_cdc_state` row write via the new `slot_name` column. The orchestrator reads it back via `ListStreams`'s `StreamStatus.SlotName`. Empty values (legacy rows that pre-date the column, default-named streams) fall back to the engine default `sluice_slot`. Operators running multiple concurrent streams against the same source via custom `--slot-name` get the right slot's position queried automatically — no operator surface for "which slot does live-add target" is needed.

3. **Snapshot-after-publication-add invariant.** The publication change is issued, then the snapshot is taken (same ordering as Phase 1). Capture the snapshot's consistent-point LSN. **Refuse loudly** if `snapshot-LSN < confirmed_flush_lsn` — this would mean the slot has somehow advanced past the snapshot's start point and events on the new table between [snapshot-LSN, confirmed_flush_lsn] would be lost. In normal operation snapshot-LSN ≥ pg_current_wal_lsn() at the moment of slot creation, which is by construction ≥ the running slot's confirmed_flush_lsn — but the explicit check pins the invariant so a regression in the flow's ordering trips a test rather than silently dropping rows.

The success log nudges the operator toward "the slot will pick up new-table events on the next CDC consumption" rather than the Phase 1 message about `--resume`, since live mode doesn't require a restart.

## Correctness story (why publication-add-then-snapshot works)

PG's pgoutput plugin evaluates publication membership **at decode time**, not at WAL-write time. Concretely:

- Publication-add runs at LSN P. From P onward, `ALTER PUBLICATION ... ADD TABLE foo` is committed; pgoutput's catalog snapshot for any subsequent decode reflects foo as a member.
- Events on foo with LSN < P are filtered out by pgoutput when the slot processes them — the publication didn't include foo at the time those events were produced from pgoutput's perspective.
- Events on foo with LSN ≥ P are delivered to the slot.
- The temp slot's snapshot is created **after** publication-add. Its consistent-point LSN S satisfies S ≥ P (PG's slot creation captures the current WAL position, which has already advanced past P).
- The snapshot's row set is a consistent read at LSN S, covering all rows that existed on foo at S.
- The active stream's main slot will deliver events on foo with LSN ≥ P. Some of those events (with LSN in [P, S]) describe rows already present in the snapshot — the idempotent applier handles the overlap (INSERT ON CONFLICT DO NOTHING; UPDATE/DELETE on a row already present in the target are no-ops or already-correct writes).
- Result: every row on foo is delivered to the target exactly-once-effectively, with no gap and no duplicate visible to downstream consumers.

## What could go wrong

The mitigation maps to one specific failure mode:

**Hazard: snapshot-LSN < confirmed_flush_lsn.** If, by the time the snapshot opens, the main slot has already advanced its `confirmed_flush_lsn` past the snapshot's start point, events on foo in [snapshot-LSN, confirmed_flush_lsn] are between the snapshot's view (which excludes them) and the slot's already-acked position (which won't replay them). Those rows would be silently dropped.

In practice this can't happen with the ordering above — the snapshot's LSN is monotonically advancing relative to slot creation time, which is always ≥ the running slot's confirmed_flush_lsn at the moment of capture. But the orchestrator captures `confirmed_flush_lsn` before publication-add and verifies `snapshot-LSN ≥ confirmed_flush_lsn` after the snapshot opens, so a future regression in the flow's ordering (or an unexpected interaction with Patroni / sync_replication_slots that forces a slot rewind) trips a loud failure rather than producing silent drift.

**Hazard: half-completed live add.** If the bulk-copy crashes after publication-add succeeds but before the snapshot completes, the operator re-runs `sluice schema add-table --no-drain TABLE`. The Phase 1 resume path already handles this:
- `AddPublicationTables` is idempotent (existing tables in scope are skipped, ADR-0021's helper).
- The temp snapshot slot from the failed run is named `sluice_addtable_<table>`; on re-run it hits the slot-already-exists check in `OpenSnapshotStreamWithSlot` (`internal/engines/postgres/cdc_snapshot.go:81`) which refuses loudly. Operator drops the leftover slot via `sluice slot drop sluice_addtable_<table>` and re-runs.
- The empty-target check fires a clear refusal if the bulk-copy partially landed; operator drops the partial table and re-runs.

These are all "fail loudly with a clear message" paths — there's no silent recovery surface that could mask a real correctness bug.

## Threat model

| Risk | Mitigation |
|------|------------|
| Snapshot-LSN < confirmed_flush_lsn (silent row drop) | Explicit invariant check in `preflightStream`; refuse loudly. |
| Two operators race add-table for different tables | `ALTER PUBLICATION` is serialisable on PG (single DDL command); concurrent runs against different tables both succeed. Test deferred until a real operator surfaces it. |
| Publication-add succeeds, bulk-copy crashes | Idempotent re-run; temp-slot leftover surfaces as a loud refusal directing operator to `sluice slot drop`. |
| Live mode used against MySQL | Refused at preflight with a clear PG-only message; no engine-side change for MySQL. |
| `confirmed_flush_lsn` read fails (slot not present, network blip) | Refuse loudly — the invariant check is mandatory in live mode; an operator who can't get a clean reading should drain first. |

## Why not Strategy B (dual-slot)

The proto-ADR (`docs/dev/design-mid-stream-add-table.md`, "Strategy B" section) sketches a dual-slot approach: a separate replication slot streams alongside the main one, then atomically swaps publication scope at the LSN the new snapshot ended at. It avoids the conservative refusal but adds:

- A second slot consuming WAL on the source until the swap completes.
- The "atomic swap" requires careful coordination: the new slot's progress must overlap with the main slot's at the swap LSN, and the swap itself isn't a single command.
- Determinism for testing is harder: the LSN race is between the snapshot stream's progress and the main slot's; test flakes would track real bugs.

The ordering used here (Strategy C variant c — publication-add-then-snapshot, single slot) gives the same operator UX (no drain required) without the second slot. The trade-off is that a true regression in the flow's ordering would silently drop rows; the explicit invariant check is the test-able shape of that risk.

If real operator demand surfaces for sub-second add-table latency on workloads where the temp-slot's brief WAL pin is unacceptable, Phase 3 reserves Strategy B as the next step.

## MySQL deferred

MySQL has no publication concept; the binlog auto-includes every table with no opt-in. The Phase 2 PG mechanism (publication-add-then-snapshot) doesn't translate. MySQL's add-table flow today already works without a publication step — the Phase 1 orchestrator skips publication-add when the source doesn't implement `publicationAdder`. The remaining gap for MySQL is in the streamer's table-filter (`--include-table` / `--exclude-table`): a new table on the source isn't in the filter set, so the streamer's CDC dispatch drops events for it.

MySQL Phase 2 (live add-table for binlog sources) requires either:
- A streamer-side filter-flip mechanism: tell the running streamer "now also include table foo", which extends `applyTableFilter`'s scope mid-run.
- Or no filter (default `--include-table` empty, accept all): in which case the only gap is the schema cache, and the `recordingApplier`-style WARN-and-skip-then-pick-up pattern from ADR-0021 might suffice.

This is a separate chunk; the design space is meaningfully different from the PG one and shouldn't be bundled here.

## Consequences

- **Operators with HA workloads no longer need a drain window.** `sluice schema add-table TABLE --no-drain` runs against an active stream; the new table joins the stream's scope and is bulk-copied, with no lull in CDC consumption for the existing tables.
- **Default behaviour is unchanged.** The flag defaults off; Phase 1's drained-stream refusal remains the conservative default, so an operator who expects the v0.10.x semantics gets the same behaviour. Live mode is opt-in.
- **PG-only first.** MySQL operators still get a clear error; the message points at the drained-stream flow as the working alternative.
- **One new optional engine surface.** `slotPositionReader` is the second slot-related optional interface in the pipeline package after `slotDropper`. PG implements it on `SlotManager`; MySQL omits it.

## Verification

Unit tests in `internal/pipeline/add_table_test.go`:
- `LiveMode=true` skips the `stop_requested_at` refusal.
- `LiveMode=true` against an engine without `publicationAdder` returns a PG-only error.
- The snapshot-LSN < publication-add-LSN invariant fires when a stub engine reports a regressed snapshot LSN.

Integration tests in `internal/pipeline/add_table_live_pg_integration_test.go`:
- `TestAddTable_LiveMode_PG`: PG → PG, active stream, add a NEW table that already has rows on the source, verify rows + post-add INSERTs flow through CDC to the target.
- `TestAddTable_LiveMode_PG_UnderLoad`: same but with a goroutine driving INSERTs on the new table during the add. Pin: zero data loss, no duplicates.
- `TestAddTable_LiveMode_MySQL_Refused`: MySQL refuses live mode with the PG-only error.

## See also

- `docs/dev/design-mid-stream-add-table.md` — the proto-ADR for Phase 1 + Phase 2 design space; the "Phase 2 status" section references this ADR.
- `docs/adr/adr-0021-publication-scope-by-table.md` — the publication-scope-by-table baseline that makes `AddPublicationTables` an additive operation.
- `docs/adr/adr-0010-idempotent-applier.md` — the INSERT ON CONFLICT DO NOTHING semantics that absorb the [snapshot-LSN, slot-LSN] overlap.
