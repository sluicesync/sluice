# sluice v0.23.1

Single-bug patch closing **Bug 42** — cross-engine PG → MySQL restore of any column with `DEFAULT gen_random_uuid()` failed at CREATE TABLE with **MySQL Error 1064**. PG's UUID-generator function name landed verbatim in the target's DDL because the MySQL-side default-translator catalog didn't have an entry for it. v0.23.1 closes the symmetric reverse of v0.11.3's Bug 28/29 fix: the catalog now translates **both directions** between PG's `gen_random_uuid()` / `random()` and MySQL's `(UUID())` / `(RAND())`. Together with v0.21.2's Bug 41 (CDC value-decode), this completes "first-class UUID support in cross-engine restore" — Bug 41 fixed the value-decode side; Bug 42 fixes the schema-side default-translation gap.

## Fixed

- **Bug 42** — Cross-engine PG → MySQL restore of `DEFAULT gen_random_uuid()` columns now translates to MySQL's `DEFAULT (UUID())`. Extends `pgToMySQLDefaultExpr` in `internal/engines/mysql/ddl_emit.go` with two entries: `gen_random_uuid()` → `(UUID())` and `random()` → `(RAND())`. Both PG's `gen_random_uuid()::text` and MySQL's `UUID()` return canonical hyphenated 36-char form; PG's `random()` and MySQL's `RAND()` both return `[0, 1)` doubles. Outer parens on the MySQL side satisfy MySQL 8.0+'s function-call-as-default syntax requirement.

## Why this matters

`gen_random_uuid()` is the default UUID-generator in modern PG schemas (Rails 7+, Django 4+, Hasura, Supabase all default-emit it for UUID PKs). Pre-fix, any of those tables required operator-side schema munging — manually rewriting the default to `(UUID())` in a staging script — to migrate to MySQL. Post-fix, the migration is fully automatic in the standard PG → MySQL flow (`sluice migrate`, cross-engine `sluice restore`, cross-engine `sluice sync from-backup`).

| Pre-fix (v0.23.0) | Post-fix (v0.23.1) |
|---|---|
| `sluice migrate --source-driver=postgres --target-driver=mysql` against a table with `uuid_col UUID DEFAULT gen_random_uuid()` failed with MySQL Error 1064 at CREATE TABLE. Operator workaround was to drop the default before migrating, then add it back in MySQL syntax post-migration. | The same migrate command translates the default to `(UUID())` automatically. CREATE TABLE succeeds; INSERTs default to a server-generated UUID matching the source's semantics. |
| Bug 41 (v0.21.2) closed the CDC value-decode side; Bug 42 was the remaining schema-side gap. | UUID-bearing PG schemas now migrate cleanly end-to-end via either bulk-copy or backup/restore. |

## Compatibility

- **No format changes.** Manifest schema, control-table schema, change-chunk format, CLI surface — all unchanged.
- **No CLI breaking changes.** All existing `sluice` subcommands keep their flag surfaces verbatim.
- **Drop-in upgrade from v0.23.0.** No DDL migration on `sluice_cdc_state`; no operator action required.
- **MySQL 8.0+ baseline preserved.** The `(UUID())` and `(RAND())` expression-default syntax requires MySQL 8.0.13+. The project already declares MySQL 8.0+ as the supported baseline.
- **No behaviour change for same-engine paths.** PG → PG and MySQL → MySQL migrations of UUID-bearing tables are unaffected; the new translation only fires when the IR's expression default carries the PG-canonical function name **and** the target engine is MySQL.

## Test coverage

- Five new test cases in `internal/engines/mysql/ddl_emit_test.go::TestEmitDefault` — three for `gen_random_uuid()` (canonical, uppercase, leading/trailing whitespace), two for `random()`. Existing `now()` test patterns extended consistently.
- Existing unrelated-expression-passthrough test pinned: `uuid_generate_v4()` (the legacy PG extension function) still falls through to verbatim emission so MySQL surfaces the loud-failure rejection rather than guessing at translation.

## Who needs this

- Anyone migrating modern PG schemas to MySQL where UUID PKs use `DEFAULT gen_random_uuid()`. Rails 7+, Django 4+, Hasura, Supabase patterns are all affected.
- Operators previously hitting Error 1064 on cross-engine restore of UUID-bearing tables — no operator workaround needed post-upgrade.
- Operators upgrading from v0.21.2 (Bug 41 fix) who held back from migrating UUID-bearing PG schemas to MySQL because the schema-side gap was still open.

## What's next

- Phase 6.3 — GCP Cloud KMS + Azure Key Vault encryption (continuation of v0.23.0's Phase 6.2).
- Roadmap items 6 (GEOMETRY/SPATIAL support — closes Bugs 26/27), 7 (chunk compression investigation), and 8 (analytics-friendly source research doc) — see `docs/dev/roadmap.md`.
