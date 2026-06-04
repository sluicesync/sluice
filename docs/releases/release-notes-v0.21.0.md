# sluice v0.21.0

Logical backups Phase 5 lands. **Cross-engine chain restore.** A PG-rooted backup chain can now restore (and stream-apply via `sync from-backup`) into a MySQL target, and vice versa. Closes the loud refusal at `chain_restore.go:99` (`"cross-engine chain restore is a Phase 5+ topic"`) that v0.17.0 through v0.20.x raised when the chain's source engine differed from the target. Implementation supplement: `docs/dev/design/logical-backups-phase-5.md`.

## Features

- **Cross-engine `sluice restore --from=<chain-url> --target-driver=<engine>`** — was supported for full-only chains since v0.16.x; v0.21.0 extends it to chains with incrementals. Schema deltas in incremental manifests now route through `internal/translate.RetargetForEngine` before invoking `ir.SchemaDeltaApplier.AlterAddColumn` on the target. PG-source `ADD COLUMN UUID` lands as MySQL `CHAR(36)`; PG-source `ADD COLUMN INET` lands as `VARCHAR(45)`; PG-source `ADD COLUMN <Array>` lands as MySQL `JSON`. Existing `RetargetForEngine` rules are reused verbatim — no new translation surface.

- **Cross-engine `sluice sync from-backup run --target-driver=<engine>`.** The broker variant. Same delta-translation pass on each tick's incremental. Detects cross-engine at startup and logs `INFO broker: cross-engine chain — chain's EndPosition not written to sluice_cdc_state; use --at-chain-id for cross-engine resumption assertions`. The broker still writes its own `_engine="backup-broker"` envelope to `sluice_cdc_state` (warm resume works); the chain's source-engine-flavored terminal `EndPosition` is intentionally omitted because PG LSN ↔ MySQL GTID is not a meaningful translation.

- **Change-event value translation reuses live-CDC machinery.** Cross-engine row payloads in change chunks land at the engine appliers' existing live-CDC value-translation path: each applier looks up its own *target* column types and routes every value through `prepareValue` for target-shape preparation. PG → MySQL: UUID strings bind to `CHAR(36)` natively; JSONB `[]byte` is shaped to a string for MySQL JSON columns (no `_binary` charset prefix). MySQL → PG: TINYINT(1) → `bool` (the cross-engine MySQL → PG bool path) handled at the CDC reader's decode layer; `pgx` accepts `bool` natively for `BOOLEAN`.

- **Loud refusal for unsupportable types.** PG-source PostGIS `Geometry` columns refuse cross-engine restore to MySQL with an operator-actionable message naming the offending table + column + recovery hint (`--exclude-table` to skip, or `--type-override` for a portable IR type). Pre-flighted at chain start so the operator gets a clear failure before any work happens. Same refusal pattern as full cross-engine restore, extended to cover incremental schema deltas (a delta that introduces a PostGIS column refuses with the incremental's BackupID named).

## Use cases this unlocks

| Scenario | Before v0.21.0 | With v0.21.0 |
|---|---|---|
| **PG → MySQL one-shot migration** with chains | Restore the full only; lose the chain's incremental tail. | Restore full + every incremental in chain order; complete chain replay across engine boundaries. |
| **MySQL → PG migration with downtime budget** | Manually re-run logical dump after each incremental window. | Single `sluice restore --from=<chain-url>` lands every change up to the chain's tail. |
| **Cross-engine analytics replication** | Direct `sluice sync start` PG → MySQL works; pairs that need backup-as-message-log were same-engine only. | `sync from-backup` against a PG-rooted chain into a MySQL analytics target — same fan-out story, now cross-engine. |
| **Cross-engine air-gapped sync** | Source-side stream writes to backup; target-side restore was full-only across engines. | Source-side `backup stream` + target-side `sync from-backup`: full chain replay across engines, no direct connectivity. |

## Compatibility

- **No CLI breaking changes.** Existing `sluice migrate` / `sync start` / `sync status` / `sync stop` / `sync health` / `backup *` / `restore` / `verify` / `slot *` / `schema *` flag surfaces are unchanged.
- **No format changes.** Manifest schema, change-chunk format, and `sluice_cdc_state` schema are unchanged. Pre-v0.21.0 chains restore + verify identically across same-engine and cross-engine targets.
- **Same-engine paths regression-clean.** Existing same-engine chain-restore tests pass unchanged; same-engine broker happy paths unchanged.
- **`backup verify` is engine-agnostic.** Verify is read-only; integrity checks pass on cross-engine-target-bound chains identically.
- **No new dependencies.** Cross-engine work reuses the existing `RetargetForEngine` translation table + the per-engine appliers' live-CDC value-translation helpers.

## Operator notes

- **Cross-engine restore lands every change.** A PG-rooted chain (full + N incrementals) restoring into a fresh MySQL target produces a target whose final state matches the chain's terminal post-condition. UPDATE/DELETE events from the source are applied via the engine-specific applier with the same idempotency contract as live CDC.

- **Resumption after a cross-engine restore.** The broker drops the chain's terminal `EndPosition` because PG LSN ↔ MySQL GTID isn't a meaningful translation. Operators continuing CDC from a cross-engine restored target run a fresh `sluice sync start` against the source's native engine (the source's CDC pump opens a new slot at current LSN/GTID; the target is anchored by the restore's data). For broker-driven workflows, pass `--at-chain-id=<BACKUP-ID>` to assert the target's current chain ID and transition to live polling.

- **PostGIS refusal shape.** A PG chain with a `geometry` column → MySQL target refuses with the table + column named and an `--exclude-table` recovery hint. The refusal lands at chain-restore start (pre-flight), so the operator gets the message before any restore work happens. Operators who want the table without the geometry column can re-run with `--exclude-table=<offending>` or supply a `--type-override` mapping the column to a portable IR type (e.g. `varbinary`).

- **Type-override + cross-engine.** Operator-supplied `--type-override` mappings run before `RetargetForEngine`, so a custom mapping survives the retarget pass. Useful for opting into MySQL `BINARY(16)` for PG UUID columns instead of the default `CHAR(36)`.

- **Same-engine paths unchanged.** PG → PG and MySQL → MySQL chain restores follow the same code path they did in v0.20.x; the cross-engine routing only fires when the chain's source engine differs from the target's.

## What's next

- **Cross-engine CDC handoff with engine-translated `EndPosition`** — translating PG LSN to MySQL GTID set isn't meaningful (different change-log shapes). Operators wanting cross-engine continuous CDC after restore set up a fresh `sluice sync start` against the source's native engine; the chain restore lands the data, sluice sync handles ongoing replication separately. Lift in a future minor if operator demand emerges.
- **Extend `RetargetForEngine`'s rule table.** PostGIS, hstore, custom enums beyond the existing PG enum support — refused loudly today; adding rewrite rules in a future minor would lift those refusals where a clean translation exists. Per-type triage; not a single-PR effort.
- **Phase 6 — KMS-backed client-side encryption** of backup chunks. Independent of cross-engine restore; once it lands, encrypted-chain consumption works transparently across engine boundaries.

## Who needs this

- **Migration teams transitioning** from PG to MySQL (or MySQL to PG) with chain-restore tooling already wired into their workflow. Phase 5 closes the gap that forced them to choose between "use chain restore (same-engine only)" and "use full-only restore + manual catch-up across engines".
- **Analytics shops with PG sources + MySQL warehouses** (or vice versa) running `sync from-backup` for fan-out. Phase 5 lets the broker workflow span engine boundaries.
- **Air-gapped target operators** who took the v0.20.0 broker path; v0.21.0 lifts the same-engine-only restriction on the consumed chain.
- **Anyone running a cross-engine `sluice restore --from=<chain-url>` today** and hitting the v0.20.x refusal on chains with incrementals; this release lifts that loud refusal.
