# Snapshot → CDC handoff

Operator reference for the boundary between sluice's cold-start
bulk-copy phase and the start of CDC apply. The handoff is the
most operationally-sensitive moment in a sluice stream — it's
where "is anything missing?" anxieties live. This doc names the
guarantees, the log lines to watch for, and the failure modes
that have real recovery procedures.

If you're starting your first sluice stream and want one sentence
to take with you: **the handoff is gapless by design** — sluice
captures the CDC position *before* the bulk-copy reader, then
holds it through the snapshot, then resumes from that exact
position when bulk-copy completes. No race window.

The rest of this doc covers how that's accomplished, what to
watch for, and what to do when it doesn't go that way.

## Operator-visible lifecycle

A `sluice sync start` runs goes through these phases in order
(observable via INFO log lines):

1. **Open source connection + capture position** (`msg="cold start; snapshot captured"`).
   The source's current CDC position is persisted into the
   `sluice_cdc_state` control table before the bulk reader starts.
   This is the "anchor" — any change after this point will be
   replayed by CDC.

2. **Schema apply on target** (`msg="migration: phase complete" phase=tables`).
   Tables, indexes (later), constraints (later) get DDL'd into
   place on the target.

3. **Bulk copy** (`msg="bulk copy progress"` / `msg="bulk copy complete"`).
   Every source table reads from a consistent snapshot at the
   anchor position. Tables above `--bulk-parallel-min-rows` (80k
   as of v0.62.0; see [throughput-tuning.md](throughput-tuning.md))
   split into N PK ranges and copy concurrently.

4. **Index + constraint creation** (`phase=indexes`, `phase=constraints`).
   Deferred until after bulk copy completes — populating tables
   first then building indexes is ~5-10× faster than building
   indexes first then INSERT'ing into them.

5. **CDC apply starts at the anchor position** (`msg="cdc start; resuming from position_token=..."`).
   This is the handoff. The position token here equals the one
   captured at step 1. Every change since the snapshot now
   replays into the target.

6. **Catch-up** (`msg="cdc apply progress"` / `msg="bulk copy + cdc catch-up complete"`).
   Target reaches "live" — the apply lag (source's latest position
   vs target's last-applied position) shrinks to zero or near-zero.

The handoff is operationally complete when phase 6 reports the
apply lag has reached steady state. Streams in production run
indefinitely after that; one-shot migrations cut over after
verifying the lag is acceptable.

## Why the handoff is gapless

The anchor capture (step 1) and snapshot read (step 3) are
coordinated by the source's transactional semantics:

- **Postgres**: sluice creates a logical replication slot at the
  position it wants to start from, then opens a `SERIALIZABLE`
  read transaction. The slot retains all WAL from that position
  forward; the read transaction sees the same snapshot the slot
  was created with. When bulk copy finishes, CDC starts replaying
  from the slot's LSN — exactly where the snapshot ended.

- **MySQL**: sluice uses MySQL's `FLUSH TABLES WITH READ LOCK`
  + binlog-position read + `START TRANSACTION WITH CONSISTENT
  SNAPSHOT` (or VStream's equivalent for PlanetScale) to capture
  the GTID + position the snapshot is consistent with. Bulk copy
  reads from that snapshot; CDC starts at the captured GTID.

In both cases, the position token is captured BEFORE any data is
read. The retention guarantee (PG: slot retains WAL; MySQL:
binlog retention covers the window) holds the change stream alive
until sluice catches up.

**Edge case — slot/binlog retention exhausted during bulk copy.**
On large source tables where bulk copy takes longer than the
configured retention window, the change stream can be reclaimed
before CDC starts. Sluice surfaces this as a clear refusal at
CDC-start time: `"replication slot dropped"` (PG) or `"binlog
position no longer available"` (MySQL). Recovery is documented in
[postgres-source-prep.md](postgres-source-prep.md) (the PG-source
slot lifecycle section).

## Position persistence

Sluice persists the snapshot-anchor + every CDC apply position to
the `sluice_cdc_state` table on the target. The contract per
[ADR-0007](adr/adr-0007-position-persistence.md): every applied
batch updates the row atomically with the batch's commit, so
crash-and-restart resumes from the last successfully-applied
position — no double-apply, no missed events.

For the handoff specifically: between snapshot capture (phase 1)
and CDC start (phase 5), the persisted position is the snapshot
anchor. A crash anywhere in phases 2-4 restarts from the same
anchor — the bulk-copy work is idempotent (replays from snapshot;
target tables either get DROPPED + recreated under the
`--reset-target-data` flag, or are detected as already-populated
and the pre-flight refuses).

## Diagnosing a stuck handoff

If the stream reports `phase=bulk_copy complete` but doesn't
emit the CDC-start log line within 30 seconds, here's the
short diagnostic:

```bash
# What does sluice think the position is?
sluice sync status --stream-id <id>

# What's the source's current position?
psql -c "SELECT pg_current_wal_lsn()"          # PG
mysql -e "SHOW MASTER STATUS"                  # MySQL

# Is the slot still alive?
psql -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
         FROM pg_replication_slots"            # PG

# Is the binlog file still available?
mysql -e "SHOW BINARY LOGS"                    # MySQL
```

Common findings:

- **Position token NULL in sluice_cdc_state** → phase 1 didn't
  complete. Re-run `sluice sync start --stream-id <id>` —
  sluice will re-capture the anchor (or fail with the original
  failure reason if it's persistent).

- **Slot/binlog absent** → see "edge case" above. Recovery via
  `--reset-target-data` or a fresh stream id.

- **Apply lag growing not shrinking** → target write throughput
  is below the source's change rate. See
  [throughput-tuning.md](throughput-tuning.md) `--apply-batch-size`.

## Stress test (local rig)

The sluice-testing repo's local-rig (`local-rig/`) supports
sync-mode CDC. To exercise the handoff:

```powershell
# 1. Boot the rig + seed the medium fixture.
cd sluice-testing/local-rig
.\bootstrap.ps1 -Engine mysql -Fixture medium-25t-100k

# 2. Launch sync (this includes cold-start + CDC handoff).
.\run-throughput.ps1 -Engine mysql -Mode sync -DurationSec 120

# 3. (Optional) In a second terminal, drive INSERT traffic during
#    the cold-start window to force a non-trivial catch-up.
.\traffic_gen.ps1 -Rate 500 -Seconds 30
```

After the run, query the target:

```sql
SELECT COUNT(*) FROM dst_app.users;  -- should match source COUNT(*)
SELECT * FROM dst_app.sluice_cdc_state;  -- shows the position
                                          -- token sluice persisted
```

If counts match, the handoff is clean. If counts diverge by a
small number (typically < 100 rows), the catch-up phase is still
in flight; wait longer or use `--apply-batch-size=100` to speed
it up.

## See also

- [ADR-0007](adr/adr-0007-position-persistence.md) — position-store
  durability + atomicity contract.
- [ADR-0010](adr/adr-0010-idempotent-applier.md) — why the applier
  can be safely replayed across the handoff.
- [ADR-0009](adr/adr-0009-streamer-vs-mode-flag.md) — orchestrator
  shape; why cold-start and CDC are integrated into one binary.
- [throughput-tuning.md](throughput-tuning.md) — `--apply-batch-size`,
  `--bulk-parallelism`, `--bulk-parallel-min-rows`.
- [postgres-source-prep.md](postgres-source-prep.md) — PG slot
  lifecycle, recovery from slot loss.
