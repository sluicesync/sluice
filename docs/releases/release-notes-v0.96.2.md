# sluice v0.96.2

## v0.96.2 — PG → MySQL DOMAIN-CHECK downgrade is now loud

Closes the last residual silent-loss class from the v0.95.x Bug 113 round-trip arc: on the cross-engine PG → MySQL path, the MySQL writer no longer silently downgrades DOMAIN-typed columns to their base type. A structured `slog.WARN` per writer lifetime now names every affected column, the source DOMAIN, the MySQL target base type, and the count of dropped DOMAIN CHECK constraints.

### Fixed

- **PG → MySQL DOMAIN-CHECK silent-downgrade follow-up (Bug 113 cross-engine residual).** v0.95.2/v0.95.3 closed Bug 113's same-engine PG → PG round-trip carry — `CREATE DOMAIN` emitted ahead of tables, CHECK preserved, row stream byte-faithful. The cross-engine PG → MySQL row stream also carried (Bug 122 fix covers cross-engine), but the MySQL writer silently downgraded the DOMAIN column to its base type with no WARN and no MySQL-level CHECK inlined — same family as the original Bug 113 silent-constraint-loss class. v0.96.2 adds `maybeWarnDomainCheckDrop` to `internal/engines/mysql/schema_writer.go` alongside the existing RLS-drop WARN pattern: at `CreateTablesWithoutConstraints` time, the writer walks every table's columns, collects tuples of `(table.column, source_domain, target_base_type, check_count)` for every `ir.Domain`-typed column, and emits one structured `slog.WarnContext` per writer lifetime (one per stream, sync.Once-gated to avoid per-column flooding) carrying `affected_column_count`, `affected_columns`, `source_domains`, `target_base_types`, `check_constraint_dropped`, and an actionable `hint` line (`MySQL has no DOMAIN equivalent; add a MySQL table-level CHECK (MySQL 8.0.16+) manually if input validation matters, or re-target to PG to preserve the DOMAIN`). The MySQL base type is computed by recursing through the writer's existing `emitColumnType` so the WARN names the actual target MySQL spelling (e.g. `TINYTEXT`/`LONGTEXT` for PG `text` DOMAINs, `DECIMAL(65,30)` for unconstrained `numeric` DOMAINs). Pinned by `TestMaybeWarnDomainCheckDrop_*` covering text-DOMAIN, numeric-DOMAIN, MySQL-source-no-op, sync.Once-across-many-columns, sync.Once-across-many-calls, CHECK-less DOMAIN still WARNs, per-writer independence, and nil-schema defensiveness — 8 sub-pins mirroring the RLS WARN test matrix.

### Compatibility

- Pure additive WARN emission. No schema, no migration semantics, no manifest version bump.
- Same-engine MySQL → PG / MySQL → MySQL is unaffected — MySQL has no DOMAIN concept; the MySQL SchemaReader never populates `ir.Domain`, so the WARN no-ops cleanly on the symmetric direction.
- Same-engine PG → PG also unaffected — the orchestrator routes to `internal/engines/postgres`'s SchemaWriter, which handles DOMAIN round-trip via Phase 1a' (CREATE DOMAIN emission).

### Who needs this

- **Operators migrating PG schemas with DOMAIN-typed columns to MySQL.** Before v0.96.2, the DOMAIN's input-validation CHECK constraint silently disappeared on the MySQL target. v0.96.2 surfaces a structured WARN line per migration stream so the drop is visible and operator-actionable (the recovery: add a MySQL 8.0.16+ table-level CHECK manually, or re-target to PG).
- **Compliance-driven cross-engine migrations** where DOMAIN-encoded invariants (email format, percentage range, ISBN, country code, etc.) are non-negotiable. The WARN's structured fields are machine-parseable for ops pipelines piping slog JSON to alerting.

### Open backlog after this release

**Zero numbered bugs and zero tracked follow-ups.** v0.96.2 closes the last open item — PG → MySQL DOMAIN-CHECK silent-downgrade was the residual carry-over from v0.95.2/v0.95.3's Bug 113 round-trip closure. The optional stretch goal (inlining the DOMAIN's CHECK as a MySQL 8.0.16+ table-level CHECK via PG-regex-to-MySQL-REGEXP_LIKE translation) is NOT included in v0.96.2 — operators apply that recovery manually per the WARN's hint. v0.96.3+ may revisit that stretch if operator demand surfaces.
