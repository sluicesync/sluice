# Design: mid-stream add-table

**Status:** Proto-ADR / design exploration. Not yet a numbered ADR. Captures the design space, tradeoffs, and a concrete implementation plan for handling `CREATE TABLE` on a CDC source while sluice is streaming. ADR-0021 deliberately punted on this; this doc lays out what landing it would actually look like.

## Context

### What sluice does today

ADR-0021 made publication scope a per-table list rather than `FOR ALL TABLES`. This avoids the failure mode where a sibling table with no PK or RLS would crash the applier on the first event. The trade-off is that **a `CREATE TABLE` on the source mid-stream is silently dropped**:

- The new table isn't in the publication, so its events never reach sluice's CDC reader.
- The applier's defence-in-depth WARN logs the unknown OID and skips. No data corruption, but the new table is invisible to the stream.
- The operator's recovery path is `sync stop` → `sync start --reset-target-data` (full re-snapshot) or manually run `migrate` on just the new table out-of-band. Both are heavy-handed for what's typically a routine `db.NewTable` call from a developer.

### What operators actually want

The operator hasn't done anything wrong — they ran a routine schema migration on the source. Sluice's streaming pipeline should accommodate this without a destructive recovery cycle.

The high-level shape:

1. Detect that a new table appeared on the source.
2. Read its schema; translate it; create it on the target.
3. Capture a snapshot of just that table.
4. Bulk-copy its existing rows.
5. Add it to the publication scope so future CDC events flow.
6. Continue streaming with the new table now in scope.

The hard parts are (3) and (5): coordinating the snapshot LSN with the in-flight CDC stream, and adding to the publication without dropping events on tables already in flight.

## Design space

### Where the trigger lives

Three candidate triggers:

**(a) Operator-driven CLI command.** New subcommand `sluice schema add-table TABLE` (or a flag on `sync start`). Operator runs it after their schema migration; sluice handles steps 1-6 above. Requires explicit operator action — same UX shape as `--reset-target-data`'s confirmation prompt.

**(b) Automatic detection from DDL events.** Sluice's CDC reader sees the `CREATE TABLE` DDL event in the binlog / WAL and triggers the add-table flow automatically. Operator does nothing.

**(c) Periodic schema-rescan.** Sluice's streamer periodically re-reads the source schema and notices new tables. Triggers the add-table flow on detection.

(b) is the most ergonomic but the riskiest: it converts an operator's source-side action into a multi-step write on the target, and a half-completed mid-stream add could leave the target in a confusing state. A single `CREATE TABLE` on the source would silently produce a target-side `CREATE TABLE` + bulk-copy + publication update — that's a lot of automatic action for one operator gesture.

(c) has timing-dependent UX: the operator runs `CREATE TABLE`, then waits an unspecified amount of time before sluice notices. Confusing.

(a) is the conservative choice and matches sluice's existing pattern — operator-driven, explicit, with a dry-run option. **This design assumes (a).** (b) could be a future opt-in flag once (a) is solid.

### Snapshot capture for the new table

This is the load-bearing tricky part. Two sub-problems:

**Sub-problem 1:** The new table has existing rows. Sluice needs to copy them via a snapshot read. But the in-flight CDC stream is past the LSN where those rows were inserted, so a naïve "read the table now" race-condition: a row inserted between the snapshot read and the publication-add could be missed (read after snapshot LSN, but before the new table is in publication).

**Sub-problem 2:** The CDC stream is positioned at LSN X. The new table's snapshot must be at some LSN ≥ X (so any inserts during snapshot read are caught by CDC after the publication-add). Coordinating these LSNs requires either a pause in the CDC stream or a careful order-of-operations dance.

Three implementation strategies:

**Strategy A — Pause + add.** `sync stop --wait` (already in v0.9.0) drains the in-flight CDC stream cleanly. Operator runs `add-table` while the stream is paused. Sluice creates the table on the target, snapshot-reads via the same `OpenSnapshotStream` that cold-start uses, bulk-copies, adds to publication. `sync start --resume` picks up CDC again. Simple but requires operator to drain the stream first.

**Strategy B — In-stream snapshot.** Sluice's streamer keeps running. The add-table command opens a *new* snapshot stream (alongside the existing one) for just the new table, bulk-copies via that new stream, then atomically swaps the publication scope to include the new table at the LSN the new snapshot ended at. The CDC reader's existing replication slot keeps streaming; the publication-add ensures events on the new table from that LSN onwards are included. The publication-add itself is atomic on PG (single ALTER PUBLICATION command); MySQL has no equivalent (publications are a PG concept).

**Strategy C — Coordinated LSN handoff.** The CDC reader pauses at a specific LSN, the snapshot is taken at exactly that LSN, the publication is updated, and the CDC reader resumes. Most precise but most complex; requires the CDC reader to expose a "pause at LSN" API.

Strategy A is the simplest and probably what v1 should ship. Operator already has `sync stop --wait` for ALTER coordination (per `docs/schema-change-runbook.md`); add-table just plugs into that existing pattern. Strategy B is a v2+ optimisation if pause-time becomes a real operational concern.

### Per-engine differences

**Postgres.** Publication updates are atomic via `ALTER PUBLICATION ... ADD TABLE ...`. Logical replication slots have a snapshot-export mechanism (`CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`) that can be reused for the new-table snapshot. The slot's `consistent_point` is independent of the publication scope, so adding a table to the publication doesn't affect the slot.

**MySQL.** No publications — the binlog is the stream. Every table on the source is in the binlog by default. Sluice's add-table flow on MySQL is simpler in one sense (no publication to update) but requires the new table to have already been on the source long enough that its `CREATE TABLE` DDL is past in the binlog by the time sluice picks it up. If the operator runs `CREATE TABLE` and immediately `INSERT`s without giving sluice time to see the DDL, the inserts could arrive before sluice's schema cache knows about the table — same failure mode as today's silent-drop, but now with the add-table flow we can rescan and pick up the table.

**PlanetScale (Vitess).** VStream events include a schema-change signal. Sluice could detect the `CREATE TABLE` from there. But VStream is enough of a separate code path that this should land after the vanilla MySQL + PG paths are working.

### Operator confirmation

The add-table flow involves DDL on the target plus a bulk-copy. Mirroring `--reset-target-data`'s typed confirmation prompt is reasonable: `sluice schema add-table TABLE [--yes]` requires typing the table name to confirm, unless `--yes` is supplied.

### Failure handling

Half-completed adds are the operator-frustration case: target table created, bulk-copy started, then sluice crashes. The recovery path should be:

- Idempotent re-run: `sluice schema add-table TABLE` again notices the target table exists, the bulk-copy state has progress, and resumes from where it left off (mirroring `--resume` on the migrate path).
- Eject path: `sluice schema add-table TABLE --reset` drops the target table and starts fresh.

The bulk-copy resume mechanism (sluice_migrate_state.table_progress, ADR-0018) already exists — this can reuse it.

## Concrete implementation plan

Phased so each phase is independently shippable.

### Phase 1: PG-only operator-driven add (v0.11.0 candidate)

- New CLI subcommand `sluice schema add-table TABLE [--yes] [--dry-run]`. Source / target driver and DSN flags mirror `migrate` and `sync start`.
- Pre-flight checks: stream must be stopped (the operator should run `sync stop --wait` first); target schema must not already have the table (or operator must pass `--reset` to drop it first).
- Read the source schema for just that one table (filter on table name).
- Run translation pipeline (Mappings + ExpressionMappings + RetargetForEngine). Surface DDL via `schema preview` style.
- On the PG side: `OpenSnapshotStream`-equivalent that exports a snapshot for just one table. Bulk-copy. Add to publication via `ALTER PUBLICATION ... ADD TABLE ...`.
- On the MySQL side: read schema, translate, create on target, bulk-copy. No publication step (binlog is the stream).
- After successful add, the operator runs `sync start --resume` to pick CDC back up.

Estimated size: ~600-900 LOC including tests + ADR.

### Phase 2: Resume / recovery (v0.11.x)

- Add bulk-copy state to `sluice_migrate_state.table_progress` so a crashed mid-add is recoverable.
- Re-running `add-table TABLE` resumes from progress; `--reset` ejects.

### Phase 3: Cross-engine coordination polish (v0.12.x?)

- Streamline the `sync stop --wait` → `add-table` → `sync start --resume` flow into a single command (`sluice schema add-table-now TABLE` that handles the drain internally). Optional QoL improvement.

### Phase 4: Automatic detection (v1.x or later)

- Opt-in flag on `sync start`: `--auto-add-tables`. Sluice's CDC reader watches for `CREATE TABLE` DDL events, queues them, and runs the add-table flow automatically. Default off — operators who want the automatic behaviour explicitly opt in.

## Open questions

1. **What's the right scope for "the table"?** Does add-table also handle indexes / FKs / CHECK constraints / triggers? v1 should probably mirror migrate's three-phase approach: create table without constraints, bulk-copy, add indexes, add constraints.
2. **What about cross-table FKs?** If the new table references an existing table, the FK should be added after the bulk-copy. If an existing table references the new one (less common but possible), the FK might already exist on the source but not target — needs a separate check.
3. **Concurrent operator workflows.** What if two operators run `add-table` simultaneously for different tables? The PG `ALTER PUBLICATION` is serialisable; the MySQL path is per-table. Probably fine but worth a test.
4. **What if the new table's schema can't be translated?** (E.g., a PG `polygon` column without PostGIS on the target.) The operator gets a loud failure at translation time, same as `migrate` would. They use `--type-override` or skip the table.

## Why not now

This is real engineering work — ~1000-1500 LOC minimum, plus tests, plus ADR. The current sluice usage pattern hasn't surfaced this as a blocker (operators are using `--reset-target-data` for now, or not adding tables mid-stream). When real-world testing reports it as a concrete need, this design should kick off.

## See also

- `docs/adr/adr-0021-publication-scope-by-table.md` — the original punt and the rationale.
- `docs/adr/adr-0023-reset-target-data.md` — the destructive-recovery pattern this design mirrors for failure handling.
- `docs/schema-change-runbook.md` — the operator-facing companion for ALTER coordination, which add-table will slot alongside.
