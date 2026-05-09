# sluice v0.24.0

**Mid-stream live add-table.** Operators with high-availability workloads no longer need to drain a running CDC stream to bring a new source table into scope. `sluice schema add-table TABLE --no-drain` runs against an active stream — the new table is created on the target, bulk-copied at a snapshot LSN ≥ the active slot's confirmed-flush position, and joins the publication scope without a measurable lull in CDC consumption for the existing tables. ADR-0030 formalizes the correctness story (Strategy C variant c per `docs/dev/design-mid-stream-add-table.md`); the heavy lifting was already in Phase 1 (publication-add-then-snapshot ordering, idempotent applier overlap handling) — Phase 2 lifts the conservative active-stream refusal, adds an explicit LSN-floor invariant check, and plumbs the active stream's slot name through the per-target `sluice_cdc_state` control table so live-add picks the right slot when the operator is running multiple concurrent streams via `--slot-name`. PG-only in this release; MySQL Phase 2 has a meaningfully different design space and is queued as a separate chunk.

## Features

- **`sluice schema add-table TABLE --no-drain`** — opt-in flag on the existing `schema add-table` command. Default off (preserves Phase 1's drained-stream refusal as the conservative default). When set, the orchestrator:
  1. Verifies the active stream's row exists on the target.
  2. Reads the slot name from the cdc-state row's new `slot_name` column (falls back to engine default `sluice_slot` for legacy rows / default-named streams).
  3. Reads the slot's `confirmed_flush_lsn` via `pg_replication_slots`.
  4. Runs the existing Phase 1 flow: `ALTER PUBLICATION ... ADD TABLE`, open a temp-slot snapshot, bulk-copy, drop the temp slot.
  5. Verifies `snapshot-LSN ≥ confirmed_flush_lsn` and refuses loudly if the invariant is violated (would never happen in normal operation per ADR-0030's threat model; the explicit check pins a regression in flow ordering rather than silently dropping rows).
  6. Returns; the active stream's main slot picks up CDC for the new table on its next consumption — no `--resume` needed.

- **Slot-name plumbing through `sluice_cdc_state`.** The streamer now records its resolved slot name on every position-write via the new `slot_name` column (idempotent migration for existing tables). Operators running multiple concurrent streams against the same source via `--slot-name=shard_a` get the right slot's position queried automatically by live add-table — no operator surface needed for "which slot does live-add target."

- **New optional engine surfaces** (in `internal/pipeline/add_table.go`): `slotPositionReader`, `snapshotLSNExtractor`, `lsnComparer`, `slotNameSetter`. PG implements all four; MySQL implements none (no slot concept); the structural type-assertions skip cleanly. The orchestrator stays engine-neutral.

- **ADR-0030 — Mid-stream live add-table.** Formalizes the correctness story: pgoutput evaluates publication membership at decode time (not write time), so publication-add at LSN P plus snapshot at LSN ≥ P plus idempotent applier covers all rows on the new table exactly-once-effectively. Documents the threat model (six hazards, all mitigated), why Strategy B (dual-slot) was deferred, and why MySQL's Phase 2 is a separate chunk.

## Use cases this unlocks

| Scenario | Before v0.24.0 | With v0.24.0 |
|---|---|---|
| **HA workload with rolling schema migrations** | `sluice sync stop --wait` drains the stream (seconds-to-minutes of CDC lag); operator runs `add-table`; resumes via `sync start --resume`. Customer-visible lag spike during the drain. | `sluice schema add-table TABLE --no-drain` runs against the live stream; CDC keeps consuming throughout. No customer-visible lag. |
| **Multi-stream operator with `--slot-name=shard_a`** | `add-table` hardcoded the slot lookup to `sluice_slot` — wrong slot for any operator running with a custom slot name. Workaround was operator-edited DDL. | Slot-name resolution flows through cdc-state automatically; live-add picks the right slot. |
| **Routine `db.NewTable` from app developer** | The new table is silently dropped (per ADR-0021); operator needs to run `--reset-target-data` (full re-snapshot) or out-of-band `migrate`. | Operator runs `add-table TABLE --no-drain`; the new table joins the stream's scope with a brief temp-slot snapshot. |

## Compatibility

- **No format changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. The new `slot_name` column on `sluice_cdc_state` is additive (idempotent `ADD COLUMN IF NOT EXISTS`); legacy rows surface as empty `SlotName` via `COALESCE`.
- **No CLI breaking changes.** `--no-drain` is opt-in; existing `sluice schema add-table` invocations without it continue to require a drained stream identically.
- **Drop-in upgrade from v0.23.2.** No DDL migration needed; the new column lands on first `EnsureControlTable` call.
- **MySQL operators unaffected** — `--no-drain` refuses cleanly with a PG-only error directing them at the drained-stream flow. MySQL Phase 2 is a separate, future chunk (the design space is meaningfully different — binlog auto-includes everything; the gap is in the streamer's table-filter, not in publication scope).
- **Existing same-engine + cross-engine paths regression-clean.** The slot-name plumbing is wiring-only (writer side); no read-path changes.

## What `--no-drain` does NOT cover

- **Pause-window budget for very large tables.** A 10M-row bulk-copy still takes time. Live mode runs the bulk-copy via the temp slot (independent from the main slot, so CDC consumption isn't affected), but the table itself isn't fully populated on the target until bulk-copy completes. Subsequent CDC events on the new table land before bulk-copy's INSERTs in some orderings — the idempotent applier (`INSERT ON CONFLICT DO NOTHING`) handles this correctly per ADR-0030's correctness story.
- **Concurrent live-adds for the same table.** `ALTER PUBLICATION` is serialisable on PG; concurrent `add-table TABLE --no-drain` calls for the same TABLE serialize at the publication level. Different tables can be added concurrently. (Test deferred until a real operator surfaces this pattern.)
- **MySQL sources.** As noted above — separate chunk.

## Test coverage

- **Unit tests** (`internal/pipeline/add_table_test.go`):
  - `LiveMode` field round-trip + skips active-stream refusal
  - `LiveMode` against engine without `publicationAdder` → PG-only error
  - Snapshot-LSN < publication-add-LSN invariant fires (stub engine)
  - Empty-LSN floor skips the invariant (legacy / read-error path)
  - Recorded slot name resolution (custom slot via cdc-state)
  - Default-fallback resolution (legacy row / default-named stream)
- **PG engine tests** (`internal/engines/postgres/engine_test.go`): `CompareLSN` numeric vs lexicographic, bad-input error path, `ExtractSnapshotLSN` round-trip + zero-position + wrong-engine.
- **Integration tests** (`internal/pipeline/add_table_live_pg_integration_test.go`, gated `//go:build integration`):
  - `TestAddTable_LiveMode_PG` — happy path; PG → PG, active stream, add a new table with existing rows, verify all rows + post-add INSERTs flow through CDC.
  - `TestAddTable_LiveMode_PG_UnderLoad` — sustained-INSERT goroutine on the new table during the add. Pin: `COUNT == DISTINCT` for no-dup; zero data loss.
  - `TestAddTable_LiveMode_MySQL_Refused` — MySQL refuses live mode with the PG-only error.

## What's next

- **MySQL Phase 2** — live add-table for binlog sources. Different mechanism (table-filter flip in the streamer; no publication concept). Separate chunk.
- **Multi-source aggregation** — N→1 streams. Proto-ADR has open questions (schema-collision strategy, per-source vs aggregated `sync status`) worth operator input before kicking off.
- **Roadmap items 6–8** — GEOMETRY/SPATIAL support (closes Bugs 26/27), backup chunk compression investigation, analytics-friendly source research doc. See `docs/dev/roadmap.md`.

## Who needs this

- HA-sensitive workloads where the seconds-of-latency drain that Phase 1 requires is operationally unacceptable.
- Multi-stream deployments using `--slot-name` to isolate concurrent sluice instances against the same source.
- Operators with routine schema-migration cadence — `db.NewTable` from a developer no longer requires destructive recovery.
