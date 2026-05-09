# sluice v0.23.2

Single-bug patch closing **Bug 44** — same-engine MySQL → MySQL migrate of any column with `DEFAULT (UUID())` or `DEFAULT (RAND())` failed at CREATE TABLE with **MySQL Error 1064**. The MySQL writer was emitting `DEFAULT uuid()` (without outer parens) because INFORMATION_SCHEMA strips the parens before returning the default value; MySQL 8.0.13+ requires `DEFAULT (uuid())` for function-call expression defaults. Symmetric writer-side counterpart to v0.11.3's reader-side fix (Bugs 28/29). **Not a v0.23.1 regression** — pre-existing on v0.23.1 and earlier; surfaced when the v0.23.1 cycle exercised the MySQL → MySQL same-engine path for the first time.

## Fixed

- **Bug 44** — MySQL → MySQL same-engine migrate of `(UUID())` / `(RAND())` expression defaults now emits `DEFAULT (uuid())` / `DEFAULT (rand())` per MySQL 8.0+ syntax. New `wrapMySQLExpressionDefault` helper runs after the existing `pgToMySQLDefaultExpr` translation and `matchTimestampDefaultPrecision` precision pass; recognises three cases and dispatches accordingly:

  | Input shape | Treatment | Reason |
  |---|---|---|
  | Already outer-wrapped (e.g. `(UUID())`, `(RAND() * 100)`) | Pass through | Avoid double-wrap; preserves Bug 42's translation entries and operator-supplied wrapped expressions |
  | Bare temporal keyword family — `CURRENT_TIMESTAMP[(N)]`, `LOCALTIME[(N)]`, `LOCALTIMESTAMP[(N)]`, `NOW[()]`, `CURRENT_DATE[()]`, `CURRENT_TIME[(N)]` | Pass through bare | Wrapping these is itself a syntax error: MySQL's grammar treats temporal keywords as a separate production from function calls |
  | Function-call shape (`identifier(args)` with no outer parens) | Wrap in outer parens | The fix — MySQL 8.0.13+ requires this form for function-call expression defaults |

  Bare-keyword detection is case-insensitive and strips an optional trailing `(N)` precision suffix or empty `()` (so MySQL's various capitalisations and the `CURRENT_TIMESTAMP` / `CURRENT_TIMESTAMP()` synonyms are all recognised).

## Why this matters

`(UUID())` and `(RAND())` expression defaults are common in MySQL 8.0+ schemas. Pre-fix, any MySQL → MySQL migrate or chain restore of a table with `uuid_col CHAR(36) NOT NULL DEFAULT (UUID())` rejected at CREATE TABLE with `Error 1064 ... near 'uuid(),`. Operator workaround was to drop the default before migrating, then add it back via raw SQL post-migration — friction that defeats the migration tool's purpose.

Together with v0.21.2's Bug 41 (PG CDC value-decode for UUID columns) and v0.23.1's Bug 42 (cross-engine PG → MySQL UUID-default translation), v0.23.2 completes UUID-default coverage across all four migration directions:

| Direction | Status pre-v0.21.2 | Status post-v0.23.2 |
|---|---|---|
| **PG → PG** (same-engine, UUID PK with `gen_random_uuid()` default) | Worked (verbatim emission, same dialect both sides) | Unchanged |
| **PG → MySQL** (cross-engine, `gen_random_uuid()` → `(UUID())`) | Failed: Error 1064 | Fixed v0.23.1 |
| **MySQL → MySQL** (same-engine, `(UUID())` default) | Failed: Error 1064 | **Fixed v0.23.2** |
| **MySQL → PG** (cross-engine, `(UUID())` → `gen_random_uuid()`) | Failed: PG syntax error on the bare `uuid()` | Fixed v0.11.3 (Bug 28) |
| **PG → PG CDC streaming** (UUID column value-decode) | Failed: stream crash on first INSERT | Fixed v0.21.2 (Bug 41) |

## Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.23.1.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **MySQL 8.0+ baseline preserved.** The wrap-in-outer-parens emission is exactly what MySQL 8.0.13+ requires for function-call expression defaults; the project already declares MySQL 8.0+ as the supported baseline. MySQL 5.7 doesn't support function-call expression defaults at all, so this code path was never reachable on 5.7 schemas.
- **No behaviour change for cross-engine PG → MySQL paths.** The Bug 42 translation entries (`gen_random_uuid() → (UUID())`, `random() → (RAND())`) already emit pre-wrapped expressions; the wrap helper is a no-op on those.
- **Existing temporal-default behaviour preserved.** The `matchTimestampDefaultPrecision` precision-promotion path runs before the wrap helper; bare `CURRENT_TIMESTAMP` on a TIMESTAMP(6) column still emits `CURRENT_TIMESTAMP(6)` (not `(CURRENT_TIMESTAMP(6))`) — matching the bare-keyword passthrough rule.

## Test coverage

- **11 new test cases in `internal/engines/mysql/ddl_emit_test.go::TestEmitDefault`** covering the three dispatch arms:
  - Function-call wrap: `uuid()` → `(uuid())`, `rand()` → `(rand())`, uppercase variants
  - Already-wrapped passthrough: `(UUID())`, `(RAND() * 100)`
  - Bare-keyword passthrough: `current_timestamp` (lowercase), `CURRENT_TIMESTAMP()` (empty parens), `LOCALTIMESTAMP`, `LOCALTIME(3)`, `NOW()`, `CURRENT_DATE`
- Existing `pg now()` / `pg gen_random_uuid()` / `pg random()` tests pinned (v0.23.1 entries continue to work; the wrap helper is a no-op on their pre-wrapped output).

## Who needs this

- Anyone migrating MySQL 8.0+ schemas where UUID PKs or random-default columns use the `(UUID())` / `(RAND())` expression-default pattern. Common in greenfield MySQL apps and in MySQL schemas that took the MySQL 8.0 expression-defaults feature when it shipped.
- Operators previously hitting Error 1064 on same-engine MySQL → MySQL migrate of UUID-bearing tables — no operator workaround needed post-upgrade.
- Operators upgrading from v0.23.1 (Bug 42 fix). Together with v0.23.2 the UUID-default migration matrix is fully covered.

## What's next

- Mid-stream add-table Phase 2 — design discussion (Strategy B in-stream snapshot vs Strategy C coordinated LSN handoff).
- Multi-source aggregation — design discussion (schema-collision strategy + per-source vs aggregated `sluice sync status`).
- Roadmap items 6 (GEOMETRY/SPATIAL — closes Bugs 26/27), 7 (chunk compression investigation), 8 (analytics-friendly source research doc) — see `docs/dev/roadmap.md`.
