# sluice v0.85.1 — Bug 77 hotfix: CHECK constraint POSIX-regex refuse-loudly

**Headline:** Patch release that delivers the cross-engine CHECK-constraint refuse-loudly behavior v0.85.0's notes described — and corrects the recovery guidance those notes got wrong. v0.85.0 claimed PG → MySQL migrations "refuse loudly" on non-translatable CHECK expressions, but a PG `CHECK (sku ~ '^[A-Z]{3}-[0-9]{4}$')` regex constraint still reached MySQL verbatim and failed at CREATE TABLE with an opaque `Error 1064`. v0.85.1 closes the two gaps that caused it and fixes the recovery hint.

This was found by the v0.85.0 post-release regression cycle (Bug 77), not in production — no users were affected. The failure mode was always **loud** (CREATE TABLE fails, target stays clean, no data lands), so this is a UX/contract fix, not a data-correctness fix. Severity MEDIUM.

## Fixed

- **`fix(engines/mysql): Bug 77 — CHECK constraint POSIX-regex refuse-loudly on the CREATE TABLE path`**

  ### Two gaps in v0.85.0

  1. **Token list incomplete.** The cross-dialect untranslatable-token list (`untranslatedPGToMySQLTokens`) carried only `~*` (PG case-insensitive regex). The other three POSIX-regex operators — bare `~` (case-sensitive), `!~` (negated), `!~*` (negated case-insensitive) — were missing. The Bug 77 repro used bare `~`, so it sailed through.

  2. **Refuse-loudly not wired into CREATE TABLE.** `refuseUntranslatedCheckExprMySQL` was called from the Shape A `AlterAddCheck` / `AlterModifyCheck` live-migration applier, but the cold-start CREATE TABLE emit (`emitCheckConstraint` in `ddl_emit.go`) emitted the CHECK expression verbatim with no pre-flight check. A plain `sluice migrate` goes through CREATE TABLE, so the refuse-loudly never fired for the most common path.

  ### The fix

  - Added `~`, `!~`, `!~*` to the untranslatable-token list (bare `~` subsumes the rest as a substring, but all four are spelled out for clarity).
  - `emitCheckConstraint` now returns `(string, error)` and runs `refuseUntranslatedCheckExprMySQL` before producing DDL; the CREATE TABLE caller wraps the error with the table name. Result: a cross-dialect CHECK carrying any POSIX-regex operator fails **before any DDL is issued**, with a structured error naming the table, the constraint, and the offending token — instead of the MySQL parser's opaque `Error 1064`.

  ### Recovery guidance corrected

  v0.85.0's release notes referenced a `--checks` recovery flag, and the in-code error message referenced `--expr-override=<constraint-name>=...`. **Neither works for CHECK constraints**: there is no `--checks` flag, and `--expr-override` only rewrites *generated-column* expressions, not CHECK predicates. The v0.85.1 error message now states the recovery that actually works:

  > drop the CHECK on the source before migrating (`ALTER TABLE ... DROP CONSTRAINT <name>`), then re-create an equivalent MySQL CHECK on the target post-migration using MySQL syntax (e.g. `REGEXP` instead of the PG `~` operator). sluice does not auto-translate dialect-specific CHECK predicates.

  ### Tests

  - Unit (`schema_writer_check_test.go`): all four POSIX-regex operators (`~`, `~*`, `!~`, `!~*`) now refuse; same-dialect and translated cross-dialect CHECKs still pass.
  - Unit (`ddl_emit_test.go`): new `TestEmitTableDef_CheckRefusesRegexCrossDialect` pins the CREATE TABLE path (the Bug 77 gap) for all four operators, and asserts the error names both the table and the constraint.

## Compatibility

- **Drop-in upgrade from v0.85.0.** No schema, config, or IR changes. The only behavior change is that a PG → MySQL migrate of a CHECK constraint using a POSIX-regex operator now fails with a clear refuse-loudly error at CREATE TABLE time instead of an opaque MySQL `Error 1064` — both exit non-zero with no data landed, so no migration that previously succeeded will now fail.
- **Patch version bump (v0.85.1)** — bug fix only, no new surface.
- **Severity MEDIUM** — loud failure both before and after; the fix makes the error operator-actionable and matches the binary to the contract the v0.85.0 notes documented.

## Who needs this

- **Operators migrating PG → MySQL with CHECK constraints that use regex operators** (`~`, `~*`, `!~`, `!~*`) — you now get a clear, actionable error naming the constraint and the recovery path, instead of a raw MySQL `Error 1064`. (You still have to handle the regex CHECK manually — sluice does not auto-translate it — but you'll know exactly what to do.)
- **Everyone on v0.85.0** — drop-in patch; no reason not to take it.
