# sluice v0.25.1

Two-bug patch from the v0.25.0 cycle. Both bugs were introduced by v0.25.0's `--target-schema` flag and surfaced in the load-bearing happy-path scenario (Bug 45) and the v0.24.0 + v0.25.0 interaction (Bug 46). Both ship with their fix shapes documented in the v0.25.0 cycle's BUG-CATALOG entries.

## Fixed

- **Bug 45 — `--target-schema` + PG enum columns failed CREATE TABLE with SQLSTATE 42704.** The PG schema writer correctly schema-qualified `CREATE TYPE "customer_svc"."orders_status_enum" AS ENUM (...)`, but the column-type ident inside `CREATE TABLE` and the `::cast` in column DEFAULT expressions were emitted unqualified — PG's parser with default `search_path` couldn't find the unqualified type and bailed. Fix: `qualifiedEnumTypeRef` helper qualifies enum column-type idents and the `::cast` suffix on DEFAULT expressions when `--target-schema` is non-empty. Default-public operators (no `--target-schema`) see no behaviour change — a new `schemaExplicit` flag on the SchemaWriter only triggers qualifying emission when `SetSchema` was called from operator override.

- **Bug 46 — `schema add-table --no-drain` on a `--target-schema` stream silently dropped CDC events on the new table.** The new table was created in `public.<table>` while the active stream's CDC applier (still running with `--target-schema=NAME`) routed new-table CDC events to `<NAME>.<table>` — which didn't exist. Events emitted a single WARN per drop and silently disappeared. Three-part fix that bundles the (a)+(b)+(c) approach from BUG-CATALOG:
  - **(a)** `--target-schema=NAME` flag on `sluice schema add-table` (mirroring `migrate` / `sync start` / `schema preview`).
  - **(b)** `target_schema` persisted to `sluice_cdc_state` at sync start via the new `targetSchemaSetter` engine surface (PG implements; MySQL doesn't — same shape as v0.24.0's slot_name plumbing).
  - **(c)** `AddTable.preflightStream` resolves the target schema from operator-supplied flag → recorded cdc-state value → (legacy) empty, with a 5-case resolution table covering inherit, override, mismatch-refusal, agreement, and legacy-row back-fill. The mismatch refusal also closes ADR-0031's previously-documented "mid-flight `--target-schema` change is NOT detected" caveat — a future warm-resume `sync start` with a different `--target-schema` against an existing stream-id refuses the same way.

## Compatibility

- **No format-breaking changes.** Manifest schema, change-chunk format, CLI surface — all unchanged for existing flows. The new `target_schema` column on `sluice_cdc_state` is additive (idempotent `ADD COLUMN IF NOT EXISTS`); legacy rows surface as empty `TargetSchema` via `COALESCE` and skip the resolution check.
- **Drop-in upgrade from v0.25.0.** No DDL migration needed; the new column lands on first `EnsureControlTable` call.
- **Default behavior unchanged for default-public operators.** Operators not using `--target-schema` see no emission style changes — the new `schemaExplicit` flag triggers qualifying emission only when `SetSchema` was called from operator override.
- **MySQL operators unaffected.** `--target-schema` continues to refuse cleanly on MySQL with the DSN-choice-workaround error (no MySQL impl of `targetSchemaSetter` either; structural type-assertion skips cleanly per the v0.24.0 pattern).

## Why it matters

| Pre-fix (v0.25.0) | Post-fix (v0.25.1) |
|---|---|
| Any PG schema with enum columns blocked from `--target-schema` migrate/sync. PG enums are common (status, role, type discriminator). Workaround required dropping the enum or skipping `--target-schema`. | Enum-bearing schemas migrate cleanly under `--target-schema`. Schema-qualified type ref + cast emission verified end-to-end. |
| `add-table --no-drain` on a `--target-schema` stream → new table in `public.<t>`, CDC events silently drop. Operators with log monitoring caught the WARN; operators without it discovered the gap when the new table appeared empty days later. | `add-table --no-drain` either auto-detects the recorded `target_schema` from cdc-state (no flag needed; ergonomic) or refuses loudly when the operator-supplied flag disagrees. New table lands in the right schema; CDC events flow correctly. |
| Mid-flight `--target-schema` change on warm-resume not detected (ADR-0031 caveat). | Closed. Mismatch refusal fires on `add-table --no-drain` AND on warm-resume `sync start`. |

## Test coverage

- **Unit tests**:
  - `qualifiedEnumTypeRef` shape (qualified vs bare path)
  - `qualifyingSchema` toggle on `SetSchema` (operator-explicit vs default)
  - Full CREATE TABLE shape under `TargetSchema` with enum + default
  - `resolveAddTableTargetSchema` 5-case resolution table
  - `AddTable.TargetSchema` field round-trip; inherit-recorded + mismatch-refuse + agreement + legacy back-fill
  - `SetTargetSchema` mutator semantics on PG ChangeApplier
- **Integration tests** (gated `//go:build integration`):
  - PG → PG migrate with `--target-schema=customer_svc` + enum + DEFAULT column verifies CREATE TABLE succeeds, row count, type lives in qualified namespace, INSERT-with-default round-trips
  - `target_schema` column migration on existing v0.25.0-shape control table (legacy-NULL → still works)
  - Full `writePositionTx` + `SetTargetSchema` + `ListStreams` round-trip + COALESCE-preserves-empty-input

## Who needs this

- Operators running v0.25.0's `--target-schema` against PG sources with enum-typed columns (status fields, role enums, type discriminators — common pattern). Without v0.25.1, `migrate` and `sync start` fail at CREATE TABLE for those schemas.
- Operators combining v0.24.0 mid-stream live add-table (`--no-drain`) with v0.25.0 multi-source `--target-schema`. Without v0.25.1, live-add silently drops CDC events for the new table.
- Operators wanting a loud-failure guard against "I changed `--target-schema` on warm resume by mistake" — v0.25.1's mismatch refusal closes ADR-0031's previously-documented caveat as a bonus.

## What's next

- **Roadmap items 3 (Phase 2 strict zero-loss correctness), 4 (MySQL Phase 2), 5 (Shape A multi-source), 7 (GEOMETRY/SPATIAL), 8 (compression), 9 (analytics-friendly source), 12 (PG → PG extension passthrough), 13 (PG extensions deployment-frequency research)** — see `docs/dev/roadmap.md`. The PG extensions research doc (item 13) is queued as the next chunk after the v0.25.1 cycle clears.
