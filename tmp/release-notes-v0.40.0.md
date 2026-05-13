# sluice v0.40.0 — CDC apply path filters generated columns

**Closes GitHub issue #12.** A source table with a `STORED` generated column previously caused every CDC INSERT to fail and exit the entire `sluice sync start` process. Both MySQL (vanilla + Vitess/PlanetScale) and PostgreSQL rejected the INSERT because the applier included the generated column's value in the column list — and both engines refuse non-DEFAULT values on generated columns:

- **MySQL**: `Error 3105 (HY000): The value specified for generated column 'margin' in table 'products' is not allowed.`
- **PostgreSQL**: `ERROR: cannot insert a non-DEFAULT value into column "margin" (SQLSTATE 428C9)`

The fix is the symmetric apply-path counterpart to the existing filter the bulk-load (`LOAD DATA INFILE` / `COPY FROM STDIN`) writers already applied per [ADR-0026:100](docs/adr/adr-0026-mysql-load-data-infile-writer.md): the column-list builders consult the column-type cache and skip every column whose `GeneratedExpr` is non-empty. The target engine recomputes the value from the column's `GENERATED ALWAYS AS (...)` expression at insert time.

## Fixed

- **MySQL applier (`internal/engines/mysql/change_applier.go`)** — `buildInsertSQL`, `buildSetClause`, `buildWhereClause` now route their column lists through a new `nonGeneratedRowKeys` helper that consults the applier's cached column-type map and skips any column whose `Column.GeneratedExpr` is non-empty. The `ON DUPLICATE KEY UPDATE` SET-list (derived from the same column list) is automatically filtered.
- **Postgres applier (`internal/engines/postgres/change_applier.go`)** — same fix on the PG side. Required a parallel `generatedColCache` plus an extended `loadColumnTypes` query against `information_schema.columns.is_generated` because PG's existing `colTypeCache` is `map[string]ir.Type` (no `GeneratedExpr` field on `ir.Type`). The five SQL builders (`buildInsertSQL`, `buildUpdateSQL`, `buildDeleteSQL`, `buildSetClause`, `buildWhereClause`) gained a `generated map[string]bool` parameter.
- **WHERE-clause is filtered too.** Including a `STORED` generated column in UPDATE / DELETE WHERE risks silent zero-rows-affected when the target's recomputation differs from the source's stored value (floating-point precision, cross-engine `COALESCE` semantics). PK + remaining-column equality identifies the row; skipping generated columns from WHERE removes a silent-divergence class the pre-fix shape exposed.

## Migration / Compatibility

- **Drop-in upgrade from v0.39.x.** No CLI changes, no IR changes, no engine-interface changes. Operators on the prior failure path (any continuous-sync stream against a source table with a `STORED` generated column) need to restart the stream — warm-resume continues from the persisted source position; no replay needed.
- **PG 12+ for the new SELECT.** `information_schema.columns.is_generated` ships with PG 12; on older PG it returns `'NEVER'` for every row and the applier behaves exactly as pre-fix.
- **No behaviour change for sources without generated columns.** Same INSERT / UPDATE / DELETE SQL shape.

## Who needs this release

- **Anyone running `sluice sync start` against a source table with a `STORED` generated column** on PostgreSQL, vanilla MySQL, or PlanetScale-MySQL: **upgrade immediately**. The bug was 100% reproducible — the first CDC INSERT after cold-start exited the stream.
- **Cross-engine MySQL → PG operators**: schema translation of generated columns has always been correct (the bulk-load path is fine); this release closes the CDC apply gap that previously made the columns continuous-sync-incompatible.
- **Operators not using generated columns**: drop-in; no behaviour change.

## Verification surface

- 4 new unit tests across `internal/engines/{mysql,postgres}/change_applier_test.go` exercising INSERT / UPDATE SET / UPDATE WHERE / DELETE WHERE filter behaviour + `nil`-fallthrough.
- 2 new integration tests in `internal/engines/{mysql,postgres}/change_applier_integration_test.go` (`TestChangeApplier_GeneratedColumn`): boot a real container, create a table with `margin DECIMAL(12,2) AS (price - COALESCE(cost, 0)) STORED`, drive Insert + Update + Delete CDC events, and assert the target's computed margin matches engine-recomputed values (not source-emitted ones).

## Design proposal — applier retry on transient errors (ADR-0038)

This release also publishes [ADR-0038](docs/adr/adr-0038-applier-retry-on-transient-errors.md) as a proposal — the design response to GitHub issue #13 (PlanetScale-MySQL Vitess `Error 1105` tx-killer errors exit the stream today). The ADR specifies a pipeline-side bounded retry policy (default: exponential backoff base 100ms → cap 30s, max 8 attempts), the per-engine error classifier for retriable transients, and three new CLI flags (`--apply-retry-attempts`, `--apply-retry-backoff-base`, `--apply-retry-backoff-cap`). Status: `Proposed`; implementation in a follow-on release after operator review.
