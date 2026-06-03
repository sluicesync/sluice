# ADR-0022: Slot-missing fall-through to cold-start

## Status

Accepted. Implemented in:

- `internal/ir/change.go` — `ErrPositionInvalid` sentinel (v0.5.2).
- `internal/engines/postgres/cdc_reader.go::resolveStartPosition` — wraps the slot-missing branch with `ErrPositionInvalid` (v0.5.2).
- `internal/engines/mysql/cdc_reader.go::resolveStartPosition` — pre-flights the persisted position via `verifyPositionResumable`, wraps with `ErrPositionInvalid` for missing binlog file (file/pos mode) or purged GTIDs (GTID mode) (v0.6.0).
- `internal/pipeline/streamer.go::Run` — catches the sentinel from `warmResume`, logs a WARN, falls through to `coldStart` (v0.5.2; engine-neutral by design).

## Context

Pre-v0.5.2, when a Postgres CDC stream's replication slot was missing on warm resume — typically because the operator dropped it explicitly after sluice surfaced `wal_status='lost'` — sluice errored out with:

```
postgres: replication slot "sluice_slot" no longer exists; cannot resume from supplied LSN (start a fresh stream with empty position)
```

The error was correct as a diagnosis but offered no recovery path. There is no `--reset-position` flag, and "start a fresh stream with empty position" can't be expressed via the CLI. Operators recovering from a lost slot had to:

1. `sluice slot drop` — already done by definition (this is the trigger).
2. `DELETE FROM sluice_cdc_state WHERE stream_id='X'` against the target — manual SQL.
3. `DROP PUBLICATION sluice_pub` against the source — manual SQL (in v0.4.0; v0.5.0's `EnsurePublication` made this automatic via drop-and-recreate, but the manual step lingered as folklore).
4. Drop dest tables (or pass `--force-cold-start`) — Bug 9 pre-flight refuses populated dest.
5. Re-run `sluice sync start`.

Steps 2 and 3 are state cleanup that should not require operators to edit databases by hand. They're also error-prone — an operator who skips step 2 gets the same error message on the next run. The whole flow violates the "Contain Postgres complexity" tenet.

This was Item F in the v0.4.0 real-world testing report. The accompanying `wal_status='lost'` error message was praised as gold-standard operator UX *because* it explained recovery in detail; the pre-v0.5.2 follow-on error was the opposite — it described an impossible state without offering a way to leave it.

## Decision

Treat a missing-slot warm-resume as a deterministic "fall through to cold-start" trigger, with two layered design choices:

1. **Engine-neutral sentinel.** Define `ir.ErrPositionInvalid` in the IR package. CDC readers return this (wrapped via `%w`) whenever the persisted position references state that no longer exists on the source. Other engines can opt into the contract:
   - MySQL binlog: when the persisted file+pos has been purged.
   - VStream: when GTIDs are too old for the source's binlog retention.
   - Per-engine wording stays specific (the wrap message names the slot, the LSN, the binlog file, etc.); only the *kind* is shared.

   The pipeline package detects the kind via `errors.Is(err, ir.ErrPositionInvalid)` and stays engine-neutral.

2. **Always-on, no flag.** The fall-through triggers automatically when the sentinel is present. No `--reset-position`, no `--auto-recover`. Rationale:
   - The persisted position is *by definition* invalid if its referenced state is gone. There is no semantic alternative — sluice cannot resume from an LSN whose slot doesn't exist.
   - The operator already made the destructive decision elsewhere (dropping the slot). Falling through respects that decision.
   - Bug 9's pre-flight refusal still gates destructive dest-table operations: cold-start refuses populated dest unless `--force-cold-start` is set or dest tables are dropped manually. Auto-fall-through does not silently destroy data.
   - A loud `slog.WarnContext` line names the slot and the persisted LSN, so monitoring/alerting still catches the recovery event.

The streamer's `Run` catches the sentinel from `warmResume`, logs the WARN, and re-enters `coldStart` with the same `lsnTracker`. The stale `sluice_cdc_state` row is overwritten by cold-start's eventual position write — no explicit DELETE step. If cold-start crashes mid-way, the stale row remains; the next run re-triggers the fall-through (idempotent).

### Trigger condition is narrow

Only the **slot-missing** branch of `resolveStartPosition` returns `ir.ErrPositionInvalid`. The other slot-error states stay strict:

- `unreserved` and `lost` — operator must explicitly run `sluice slot drop` before fall-through engages. These states represent ambiguity: the slot still exists, sluice doesn't know whether the operator wants recovery or wants to investigate. Erroring loudly preserves operator agency.
- Slot-name mismatch (position references slot A, reader configured for slot B) — stays strict. This is configuration drift, not invalid state.

## Consequences

- **Recovery from `wal_status='lost'` collapses from five steps to two.** Drop the slot, then re-run `sluice sync start`. The WARN line names the recovery event explicitly so the operator sees what happened.

- **Bug 9 pre-flight refusal preserved.** Cold-start still refuses populated dest unless `--force-cold-start` is set. Operators who deliberately dropped the slot and want a fresh bulk-copy must either pass `--force-cold-start` or drop dest tables manually. This is the same destructive-action gate as a fresh cold-start; we don't auto-engage it.

- **Persisted-position cleanup is implicit, not explicit.** No DELETE step. Cold-start overwrites the stale row when it writes its own first position. If the operator wants to inspect the stale state for debugging before re-running, the row is still there.

- **Other engines can opt into the same contract.** v0.5.2 implements only the PG slot path. **v0.6.0 extends the contract to MySQL**: `verifyPositionResumable` runs as a pre-flight in the binlog reader's `resolveStartPosition`, returning `ir.ErrPositionInvalid`-wrapped errors when (a) file/pos mode and the named binlog file is missing from `SHOW BINARY LOGS`, or (b) GTID mode and `@@gtid_purged` contains GTIDs absent from the resume set (`GTID_SUBSET(@@gtid_purged, ?) = 0`). The streamer's fall-through path is unchanged — engine-neutrality means the per-engine wrap text differs but the recovery flow is identical.

- **Bug 15's recovery procedure benefits.** Bug 15's manual recovery (described in BUG-CATALOG.md) follows the same shape: drop slot + clear state + drop tables + cold-start. Option B reduces it to drop slot + cold-start, with `--force-cold-start` for the dest-data step.

## Why not a `--reset-position` flag

A flag-gated version was considered. Reasons we don't add it:

- **No semantic alternative to fall-through.** When the slot is gone, there is no "stay strict" path — the operator must eventually fall through. Adding a flag just means asking the operator to opt into the only available recovery, which is friction without benefit.
- **Loud-WARN is sufficient for alerting.** Operators monitoring sluice can alert on `slog.Warn level=WARN msg="warm resume: slot referenced by persisted position no longer exists"`. The WARN names the slot and LSN, so dashboards have everything needed.
- **Bug 9 already gates destructive operations.** The dest-table refusal is the conservative gate; the position fall-through is non-destructive on its own (state row is overwritten, not deleted).

## Why not delete the stale `sluice_cdc_state` row

The cleaner-looking approach would be: when fall-through triggers, DELETE the stale row before calling coldStart. Reasons we don't:

- **Cold-start's position write is idempotent.** It uses the same UPSERT path as a fresh cold-start. The stale row is overwritten, not appended-to.
- **Crash recovery is identical.** If cold-start crashes after the DELETE but before the UPSERT, the next run finds no row at all — which means it'll start from empty and re-bulk-copy. Same outcome as today (where the stale row triggers re-fall-through). No advantage.
- **Debugging is easier with the row preserved.** An operator inspecting `sluice_cdc_state` after a crashed fall-through sees the stale position and knows what state was being recovered from.

## Verification

Integration test in `internal/pipeline/streamer_item_f_integration_test.go::TestStreamer_PostgresToPostgres_SlotMissingFallsThroughToColdStart`:

1. Cold-start sluice with one table; insert seed rows; let a CDC change land so `sluice_cdc_state` has a real position.
2. Stop sluice cleanly.
3. Drop the slot directly via `pg_drop_replication_slot` (simulates the operator's `sluice slot drop` after `wal_status='lost'`).
4. Drop dest tables (Bug 9 pre-flight gate; the test exercises the fall-through, not the populated-dest refusal).
5. Re-run `sluice sync start`.
6. Assert: `slog.Warn` line fires; cold-start runs; new slot is created; CDC events flow; the persisted position is overwritten with a fresh value.

Pre-fix, step 5 errors out at "replication slot ... no longer exists." Post-fix, step 5 logs the WARN and proceeds.

## Related PG-internals research

PG's logical-replication slot state is persisted to disk in `$PGDATA/pg_replslot/<slot>/state` (per *The Internals of PostgreSQL* Ch 11.4 — "Replication slot state file persistence"). The on-disk state file is the source of truth that survives PG restarts; `pg_replication_slots` is a view on top of it. This is the load-bearing detail behind why the slot-missing branch must be a hard refusal: when the state file is absent, the slot is unrecoverable by definition (sluice cannot resume a slot it has no LSN reference frame for).

ADR-0051 (PG CDC source-identity pinning) extends the same `ErrPositionInvalid` machinery to the timeline-change case: a slot whose state file still exists but lives on a different timeline post-PITR triggers the same fall-through, just via a different precondition check (`IDENTIFY_SYSTEM`'s sysid/timeline rather than the slot-existence check). Sluice's F8 / F9 findings (`sluice-pg-internals-research-chapters-9-10-11-2026-05-22.md`) document the chain.
