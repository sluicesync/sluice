# sluice v0.17.0

Logical backups Phase 3 — **incremental backups + chain-aware restore**. The two-part chunk that closes the resync-avoidance story for irrecoverable position loss: take periodic incrementals against a full, restore the whole chain into a fresh target without re-bulking from source.

This release is the storage + restore plumbing (Phase 3.1 + 3.2). The next release (v0.17.1) adds the **CDC handoff** UX (`sync start --position-from-manifest`) so the chain-restored target picks up replication automatically. v0.17.0 already supports the manual catch-up path: `restore --from=<chain-url>` then `sync start --resume` against the existing slot.

## Features

- **`sluice backup incremental --since=<full-backup-url-or-id>`.** Opens the source's CDC pump at the parent manifest's terminal position; streams change events for a bounded window; writes a chain-linked manifest + serialised change chunks. Window is bounded by either `--window=DURATION` (default 5m) or `--max-changes=N` (default 100k), first-fired wins; window extends to the next `TxCommit` so the chain never ends mid-transaction. Manifest gains `Kind`, `BackupID`, `ParentBackupID`, `StartPosition`, `EndPosition`, `SchemaHash`, `SchemaDelta` fields. Both engines supported (PG via logical replication slot at the parent's `EndLSN`; MySQL via binlog reader at the parent's `EndGTID`).

- **`sluice restore --from=<chain-url>` walks the chain.** Lists `manifests/incr-*.json` alongside the full's `manifest.json`; builds the chain via `ParentBackupID` linkage; validates single full root, no branching, no cycles, no orphans, every incremental's `StartPosition` matches its parent's `EndPosition`. Applies the full first, then each incremental in order — schema deltas first via the new `ir.SchemaDeltaApplier` surface, then change chunks via the engine's idempotent `ChangeApplier.ApplyBatch` (ADR-0010). Same-engine restore only in v0.17.0; cross-engine chain restore is loudly refused and deferred to Phase 5+.

- **`sluice backup verify --from=<chain-url>` walks chains too.** Re-checksums every chunk in the store across all manifests (the full's row chunks and every incremental's change chunks), so cron-style integrity probes cover the whole chain in one call.

- **Schema evolution within a chain.** When the CDC pump observes RELATION-message changes (PG) or binlog DDL events (MySQL) during an incremental's window, the manifest records typed `ir.SchemaDeltaEntry` entries: `AddTable` / `DropTable` / `AlterTable`. Restore replays these in order before applying the incremental's row events. **Rename-shaped deltas** (a drop + an add for the same table within one window) are flagged as ambiguous and surface a clear "force fresh full + new chain" recovery message — the alternative would be silent data corruption on restore.

- **`ir.SchemaDeltaApplier` interface** with `AlterAddColumn(ctx, table, cols)` — implemented on PG (`ALTER TABLE … ADD COLUMN IF NOT EXISTS …`) and MySQL (information-schema probe + `ALTER TABLE … ADD COLUMN`). Same-engine column-add deltas apply cleanly during chain restore. Other ALTER shapes (DROP COLUMN, type changes, constraint additions) are not yet covered — they surface as a clear "delta kind not supported in v0.17.0" refusal so the operator knows to take a fresh full.

## Compatibility

- **No breaking IR / CLI changes.** All Phase 3 fields on `ir.Manifest` are forward-compatible additions; pre-v0.17.0 fulls (v0.15.0 / v0.16.x) still restore cleanly via the chain-aware path's "orphan full" handling. Older sluice ignores the new fields entirely; manifests written by v0.17.0+ are readable by v0.16.x for the row data, but incremental manifests appear as un-chainable orphans (the right degraded behaviour for incrementals nobody can chain anyway).

- **`ir.SchemaDeltaApplier` interface added.** Out-of-tree engine implementations will need to add the method (no-op acceptable for engines without column-add support).

- **`ir.Manifest.FormatVersion` stays at 1.** Reserved for a future change that would actually break older readers.

## Phase 3.3 — coming in v0.17.1 (next release)

These three pieces close the end-to-end automation gap and are explicitly **not** in v0.17.0:

- **Full-backup writer recording `EndPosition`.** v0.17.0 fulls don't yet record their snapshot LSN/GTID on the manifest; the first incremental against a v0.17.0 full surfaces a `parent has no EndPosition; chain will start from CDC's current position` warning and starts the chain from current CDC position. Operationally usable but not the cleanest shape. v0.17.1 closes it by capturing the snapshot point during the existing `pipeline.Backup` flow.

- **`sluice sync start --position-from-manifest=<chain-url>`.** Reads the chain's terminal position and resumes CDC from there automatically. Without this, operators run `sluice sync start --resume` against the existing slot — works today, but doesn't auto-coordinate the slot's `restart_lsn` advancement against the chain's terminal.

- **PG soft-warning pre-flight checks.** `wal_keep_size` sufficiency math against the chain's cadence + WAL volume; idle-slot failover trap warning when Patroni is detected (see `docs/postgres-source-prep.md` for the full operator guidance, strengthened in this release cycle).

## Who needs this

- **Anyone building toward zero-rebuild disaster recovery.** v0.17.0's chain restore puts a target back into the source's data state without re-bulking. Pair with a continuously-running sluice CDC stream and you have point-in-time recovery via "restore the chain through the right incremental." v0.17.1's auto-handoff makes it one command instead of two.

- **Operators with low-volatility schemas** who want to keep backup churn small. Most workloads see hours-or-days between schema changes; an incremental that captures only the row deltas is a fraction of a full's size.

- **Operators of low-traffic databases** worried about slot loss — a maintained backup chain becomes a recovery path that doesn't depend on the source's WAL retention. The chain holds the data; the chain holds the position.

## Operator notes (PG)

The `docs/postgres-source-prep.md` doc gained a new "idle-slot failover trap" section this release cycle, capturing an operator-confirmed production failure mode: even with full Patroni + sync-config in place (`Logical slot name` + `sync_replication_slots = on` + `hot_standby_feedback = on`), a slot whose `confirmed_flush_lsn` hasn't advanced during the slot-sync window can still be lost on failover. The four-tier mitigation hierarchy is documented; v0.17.1 will surface this as an in-CLI soft warning when `--position-from-manifest` is used against a Patroni-managed source.
