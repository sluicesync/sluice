# sluice v0.97.1

## v0.97.1 — PG → MySQL DOMAIN CHECK regex byte-fidelity + cookbook breadth

Closes the strict-fidelity follow-up flagged by the v0.97.0 post-release cycle: PG `\.` in a DOMAIN regex CHECK now lands as `\.` in the stored MySQL CHECK expression, not `.` (which would match any character).

### Fixed

- **PG → MySQL DOMAIN CHECK regex backslash byte-fidelity (v0.97.0 strict-fidelity follow-up).** v0.97.0 emitted the source regex pattern into MySQL's SQL string literal without escaping backslashes. MySQL's string-literal parser treats `\` as an escape character by default, so the literal `'\.'` arrived at the regex engine as `.` (any character) rather than `\.` (literal dot). The email-DOMAIN regex stayed **functionally** correct (the `@` and negated character classes carried the rejection — an input without `@` was still rejected), but the stored expression diverged from PG's source semantics — a strict-fidelity gap the v0.97.0 cycle subagent flagged honestly. v0.97.1 closes the gap: `translateRegexCheckBody` now doubles backslashes in the pattern before passing to `quoteSQLString`, so the SQL literal `'\\.'` arrives at the regex engine as `\.` regardless of the operator's `SQL_MODE` setting. PG regex shorthands (`\d`, `\s`, `\w`, `\b`) translate the same way. Pinned by updated `TestTranslateDomainCheckToMySQL` sub-pins for the email + shorthand cases.

### Added (docs)

- **Three new cookbook recipes** extending the v0.97.0 cookbook scaffolding:
  - **`docs/cookbook/compare-pg-dump.md`** — sluice vs. `pg_dump` + `pg_restore`. "Why not just use pg_dump?" is the first question every PG operator asks; this is the honest framing of where each tool fits + the use-both pattern.
  - **`docs/cookbook/recipe-heroku-migration.md`** — slot-less managed-PG walkthrough that doubles as the canonical RDS / Crunchy Bridge / Supabase template. Covers what sluice does, what it deliberately refuses (no `REPLICATION` role, no event triggers, no extension installs), and the cutover sequence.
  - **`docs/cookbook/recipe-postgis.md`** — cross-engine PG ↔ MySQL geometry round-trip with SRID preserved. Demonstrates the Bug 26 / Bug 27 closure operator-facing.

All three linked from `docs/cookbook/README.md`.

### Compatibility

- Pure additive fix on the MySQL inline-CHECK emit path. No behavior change for the FUNCTIONAL outcome on the email-DOMAIN regex (constraint still rejects malformed inputs). Operators with strict-fidelity audit needs (regex review tooling, `SHOW CREATE TABLE` diffing) now get the byte-faithful regex pattern they expected.
- Operators on v0.97.0 should upgrade if they care about the stored CHECK expression matching the source's semantic shape exactly.
- No migration / restart needed. The fix applies to all future CREATE TABLE statements; existing tables on the target have the v0.97.0 shape (still functionally enforcing; rebuild via `ALTER TABLE` if strict-fidelity matters).

### Open backlog after this release

**Zero numbered bugs. Zero tracked follow-ups.** v0.97.1 closes the last identified gap from the v0.94.0 → v0.97.0 arc. The roadmap's "Next up" items remain demand-gated per their per-entry analysis.
