# sluice v0.99.270

MariaDB flavor Phase 2 — type fidelity (roadmap item 73 Phase 2, ADR-0169) — plus two cross-engine correctness fixes surfaced while landing it, one of them a CRITICAL silent-loss.

## Added

- **MariaDB type fidelity: JSON identity, native `uuid`/`inet6`/`inet4`, and geometry SRID now round-trip.** A MariaDB `longtext` column whose only CHECK is exactly `json_valid(<that column>)` — MariaDB's JSON storage shape — is read as `ir.JSON` (textual), and that MariaDB-internal auto-CHECK is stripped from the IR so it is not re-emitted as an invalid `json_valid()` CHECK on a Postgres target; a user-authored CHECK on the same column is preserved. MariaDB's native `uuid` (10.7+), `inet6` (10.5+), and `inet4` (10.10+) types read to `ir.UUID` / `ir.Inet` (both address types collapse to `inet`; the value round-trips losslessly — Postgres native `uuid`/`inet`, a MySQL-family target as `CHAR(36)`/`VARCHAR(45)`).
- **Geometry is now a supported MariaDB type, SRID round-tripping both ways.** The per-column SRID is read from `information_schema.GEOMETRY_COLUMNS` and written back as MariaDB's `REF_SYSTEM_ID=<n>` type attribute, so a `geometry(POINT, 4326)` column keeps its SRID in both directions. This corrects the original plan on ground truth: MariaDB does **not** echo `REF_SYSTEM_ID` in `SHOW CREATE TABLE`, and has no `srs_id` catalog column — the mandatory live read-back against MariaDB 11.4 and 10.11 caught that the `SHOW CREATE` approach would have silently read every SRID back as 0, so `GEOMETRY_COLUMNS` is used instead.
- **Invisible-object census upgraded to per-class remedies.** MariaDB SEQUENCEs and SYSTEM VERSIONED tables — invisible to a `table_type = 'BASE TABLE'` filter — remain a loud, pre-data refusal (carrying a system-versioned table as a plain current-state table would silently drop its temporal history), now naming the offending objects per class with an actionable remedy.

## Fixed

- **CRITICAL: `migrate` Postgres → MariaDB silently dropped EXCLUDE constraints (and skipped every Postgres-native cross-engine refusal).** `migcore.IsMySQLFamilyEngine` excluded `mariadb` while the dialect predicate `translate.IsMySQLFamily` included it, so a Postgres → MariaDB migration computed its cross-engine gate as false and skipped the pre-flight refusals for EXCLUDE constraints, standalone sequences, unsupportable extension column types, and operator-class indexes — and the MariaDB writer then dropped an EXCLUDE constraint with a clean exit (row-count `verify` is blind to a missing constraint, so the loss was silent). This is the same new-engine-misses-a-name-branch class the vitess precedent documented. Fixed by delegating `IsMySQLFamilyEngine` to the single registry-parity-tested `translate.IsMySQLFamily` — the two predicates can no longer drift — and converting the family test to registry-parity so any future MySQL-dialect engine that misses it fails CI. Pre-existing since the Phase-1 MariaDB target landed in v0.99.268; if you have run a Postgres → MariaDB migration of a schema with EXCLUDE constraints or standalone sequences, re-check the target.
- **MariaDB source: a JSON column's implicit `json_valid()` CHECK was captured multiple times when two tables in one database shared a constraint name, breaking migrate/backup.** MariaDB constraint names are unique per-table, not per-schema (unlike MySQL 8), so the `check_constraints` ↔ `table_constraints` join — matched on schema + name only — fanned out and emitted duplicate CHECKs, failing `CREATE TABLE` (`Error 1826` on a MySQL/MariaDB target, `42710` on Postgres). Because MariaDB names a JSON column's auto-CHECK after the column, two tables sharing a JSON column name (`meta`, `data`, …) triggered it in the common case. Fixed with a MariaDB-only join predicate on `table_name` (MySQL 8's `CHECK_CONSTRAINTS` has no `TABLE_NAME` column, so the MySQL-8 query is unchanged and byte-identical).

## Compatibility

- **MariaDB reads gain fidelity; the MySQL-8 path is byte-identical** (every flavor gate carries the MySQL-8 behavior as its zero value). The one behavior change for existing users is the CRITICAL fix above: a Postgres → MariaDB migration that previously exited 0 while dropping an EXCLUDE constraint or a standalone sequence now refuses loudly, pre-data, with an actionable message — the correct, loss-free outcome. CDC from a MariaDB source remains refused with the coded `SLUICE-E-CDC-MARIADB-UNSUPPORTED` (domain-GTID CDC is Phase 3).

## Who needs this

Anyone migrating a MariaDB database that uses JSON columns, native `uuid`/`inet` types, or geometry with a declared SRID — or anyone running Postgres → MariaDB migrations, who should take the CRITICAL fix above.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.270
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.270`
