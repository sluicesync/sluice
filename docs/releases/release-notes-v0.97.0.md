# sluice v0.97.0

## v0.97.0 — PG → MySQL DOMAIN CHECK now lands as a real MySQL CHECK

v0.96.2 made the cross-engine DOMAIN-CHECK silent-drop class loud-observable via a structured WARN. v0.97.0 closes the remaining gap from "operator-actionable observability" to "in-database enforcement on MySQL targets" — by translating two well-defined DOMAIN CHECK shapes into MySQL 8.0.16+ table-level CHECK constraints inline at CREATE TABLE time.

### Added

- **PG DOMAIN CHECK → MySQL inline CHECK translation.** When the target MySQL is 8.0.16+ (the version that started enforcing CHECK constraints), the writer now translates two well-defined PG DOMAIN CHECK shapes into MySQL table-level CHECK clauses emitted alongside the existing `table.CheckConstraints` on the same CREATE TABLE statement:
  - **Regex DOMAINs:** `CHECK (VALUE ~ 'pattern')` → `CHECK (REGEXP_LIKE(<col>, 'pattern'))`. The basic regex surface (anchors, character classes, repetition operators) round-trips exactly between PG POSIX regex and MySQL ICU regex; the email-address DOMAIN exemplar from BUG-CATALOG.md Bug 113 works out of the box.
  - **Range DOMAINs:** `CHECK (VALUE >= X AND VALUE <= Y)` → `CHECK (<col> >= X AND <col> <= Y)`. Numeric range comparisons are universally portable.
- **MySQL version probe at writer open.** `OpenSchemaWriter` now runs a `SELECT VERSION()` probe and parses the result via `mysqlVersionSupportsInlineCheck`. MariaDB is excluded regardless of version (separate regex dialect + cast rules; conservative default). Older MySQL targets continue to fall through to the v0.96.2 WARN-only behavior — no regression on un-upgraded deployments.
- **v0.96.2 WARN suppression for translated columns.** When every attached DOMAIN CHECK on a column translates AND inlines, the v0.96.2 WARN is suppressed for that column (no silent-loss class remains). Columns with mixed translatable / untranslatable CHECKs continue to surface the WARN but the `check_constraint_dropped` count now reflects only the un-translated CHECKs.

### Compatibility

- Pure additive feature on MySQL 8.0.16+. No format change, no manifest version bump.
- DOMAIN CHECK shapes outside the regex / range whitelist (function calls, IN lists, negated regex, single-sided ranges, non-numeric range literals) are **silently dropped at emit time** — a wrong CHECK on the target is more dangerous than no CHECK (operators see the CHECK in `SHOW CREATE TABLE` and assume parity), so the v0.96.2 WARN continues to cover those columns. Broadening the whitelist is queued for future releases pending a per-feature semantic-equivalence audit.
- MariaDB and pre-8.0.16 MySQL targets are unchanged from v0.96.x — the v0.96.2 WARN fires, no inline CHECK is emitted.
- Same-engine PG → PG is unchanged — the orchestrator routes to the PG SchemaWriter, which handles DOMAIN via Phase 1a' (CREATE DOMAIN emission).

### Who needs this

- **Operators migrating PG schemas with regex / range DOMAIN-typed columns to MySQL 8.0.16+.** Before v0.97.0 the DOMAIN's CHECK constraint silently disappeared on the MySQL target (closed by the v0.96.2 WARN to observability tier). v0.97.0 preserves the constraint as a real MySQL table-level CHECK — input validation runs on MySQL just as it did on the source.
- **Compliance-driven cross-engine migrations** where DOMAIN-encoded invariants (email format, percentage range, ISBN, country code, etc.) are non-negotiable and the operator targets MySQL 8.0.16+.

### Open backlog after this release

**Zero numbered bugs. Zero tracked follow-ups.** v0.94.0 → v0.97.0 closed 9 numbered bugs (108, 110, 113-PG→PG-loud-refuse, 113-PG→PG-round-trip, 113-PG→MySQL-WARN [v0.96.2], 114, 115, 116, 117-verify-path [v0.94.1], 117-ingestion-path [v0.96.3], 122) and shipped one feature (v0.97.0 inline MySQL CHECK). Bug 118 was also re-verified closed during the v0.94.1 cycle. The remaining DOMAIN CHECK shapes (function calls, IN lists, etc.) outside the whitelist remain as the v0.96.2 WARN class — they aren't tracked as open follow-ups because broadening the whitelist requires per-feature semantic-equivalence audits to avoid re-introducing the original Bug 113 silent-loss class.
