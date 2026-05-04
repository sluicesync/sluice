# ADR-0013: Applier value-shaping via column-type cache + `CAST(? AS JSON)`

## Status

Accepted. Implemented in v0.2.2 (`internal/engines/{mysql,postgres}/change_applier.go`).

## Context

The bulk-copy path (`RowWriter.WriteRows`) routes every value through a `prepareValue(v, ir.Type) any` helper that handles per-engine wire-format quirks: MySQL's `Set` becomes a comma-joined string, `Geometry` gets a 4-byte SRID prefix, and (since v0.2.0) `JSON` `[]byte` is converted to `string` to bypass go-sql-driver/mysql's `_binary` charset prefix.

The CDC change applier (`ChangeApplier.Apply` → `dispatch` → `buildInsertSQL`/`buildSetClause`/`buildWhereClause`) didn't use `prepareValue`. It bound row values straight to the parameterised SQL. v0.2.0's bulk-copy fix didn't carry over to the CDC path, leaving the applier exposed:

- **PG → MySQL CDC UPDATE on a JSON column** crashed loudly with `Cannot create a JSON value from a string with CHARACTER SET 'binary'`.
- **MySQL → MySQL CDC UPDATE on a JSON column** silently matched zero rows. Investigation surfaced a deeper issue: MySQL's `=` operator does not implicitly cast a `?` parameter to JSON regardless of whether the bound value is `[]byte` or `string`. The applier tolerates zero-rows-affected (resume idempotency, [ADR-0010](adr-0010-idempotent-applier.md)) and silently advanced the position. **Data divergence with no error signal.**

The applier needed both the value-shaping `prepareValue` routing *and* a SQL-side fix for the JSON comparison.

## Decision

Two-part change to the applier:

1. **Per-table column-type cache** (`colTypeCache map[string]map[string]ir.Type`), populated lazily on first sight of a table. The cache is parallel to the existing `pkCache`. With the type known per column, every bound value goes through `prepareValue(v, type)` exactly the way the bulk-copy path does.

2. **`CAST(? AS JSON)` placeholder for JSON-typed columns in `WHERE` clauses.** A small `placeholderFor(type)` helper consults the column type and emits the cast wrapper for `ir.JSON`. Other types use the bare `?` placeholder. The Postgres applier got the parallel structural cleanup for symmetry, but its WHERE doesn't need a cast equivalent — pgx inspects per-column type metadata natively.

A new `slog.Debug` line fires when an Update or Delete matches zero rows. Resume idempotency still depends on tolerating that case, but the silence now leaves an observable footprint.

## Consequences

JSON columns work end-to-end across both MySQL and Postgres targets, in all four migration directions, on both bulk-copy and CDC paths. Future per-type wire-format quirks (e.g. binary geometry, custom enum encodings) extend `prepareValue` once and benefit both paths.

The cost is one extra `INFORMATION_SCHEMA` query per table on first-sight per applier lifetime — amortised over the lifetime of a stream this is negligible. Engines now carry `loadColumnTypes` infrastructure that wasn't there before; for the Postgres applier this couldn't fully reuse the schema reader (which knows about geometry SRID metadata via PostGIS extensions) — a documented limitation that becomes load-bearing only when PostGIS replication lands.
