# sluice v0.78.1 — Bug 86 hotfix: PG normalizer NUMERIC / TEXT / temporal coverage

**Headline:** Patch release closing Bug 86, a critical regression in v0.78.0's RENAME COLUMN classifier that refused on any PG schema carrying a nullable `NUMERIC`, `TEXT`, or default-precision temporal column — effectively breaking the just-shipped feature on most real-world schemas. Same Bug 84-family pgoutput-vs-SchemaReader IR-canonicalization asymmetry; two additional fields the v0.73.2 normalizer didn't cover.

## Fixed

- **`fix(engines/postgres): Bug 86 — extend NormalizeForCDCComparison to cover Nullable + temporal Precision (#41)`**

  v0.78.0's RENAME shape uses `pipeline.ClassifyShape` to diff the pre- and post-DDL `SchemaSnapshot` boundaries. Cold-start populates one side via `engines/postgres.SchemaReader`; CDC populates the other via `projectRelation` on the pgoutput `RelationMessage` + `oidToType`. The normalizer (`internal/engines/postgres/cdc_normalize.go`, introduced in v0.73.2 for Bug 84) erases the fields cold-start reads from `information_schema` that the pgoutput wire format cannot carry. v0.73.2 covered `Integer.AutoIncrement`, `Varchar.Collation`, `Char.Collation`, `Text.Collation`, and `Decimal.Unconstrained`. **It did not cover `ir.Column.Nullable`, `Default`, `Comment`, or `DateTime.Precision` / `Time.Precision` / `Timestamp.Precision`.**

  **`ir.Column.Nullable` is the catalogued repro's smoking gun.** pgoutput's `RelationMessage` carries `(name, OID, typmod, key-flag)` only — there is no `attnotnull` byte in the protocol. `projectRelation` leaves `Nullable=false` on every CDC-projected column. Cold-start's `SchemaReader.populateColumns` reads `information_schema.columns.is_nullable` faithfully. Any nullable column on the source-side schema (the cycle's `price NUMERIC(10,2)` and `description TEXT`) triggered `diffAlteredColumn`'s `nullDiffers` branch with a phantom `ShapeKindAlterColumnNullability` — which combined with the RENAME's added=1/dropped=1 into a multi-shape combo refusal. The v0.78.0 pin used `name VARCHAR(64) NOT NULL` + a BIGSERIAL PK — both NOT NULL on both sides — so the asymmetry never surfaced.

  **`ir.DateTime.Precision` (plus `Time` / `Timestamp`) is a Type-level asymmetry** surfaced by the post-fix matrix test when `TIMESTAMP` was added. Cold-start reads `information_schema.columns.datetime_precision=6` (PG's reported default for a plain `TIMESTAMP`); CDC's `temporalTypmod(-1)` returns `0`. `diffAlteredColumn` fires `ShapeKindAlterColumnType` on the type mismatch. The cycle's original `TIMESTAMP DEFAULT NOW()` columns weren't in the v0.78.0 pin's RENAME test, but the matrix probe found this in the same CI roundtrip as the Nullable fix.

  The fix extends `NormalizeForCDCComparison` to:
  - Zero `Nullable` / `Default` / `Comment` on every seed column (CDC's wire format cannot carry any of them; cold-start must match).
  - Collapse `Precision == 6 → 0` on `DateTime` / `Time` / `Timestamp` when the wire-shape is the default-precision zero. Explicit non-default precisions (`TIMESTAMP(3)`) pass through unchanged via a negative-precision-passthrough test.

## Tests

- **`test(engines/postgres): cdc_normalize_test.go` (+151 LOC)** — Five new `TestNormalizeForCDCComparison_PG/*` subtests pin the new normalizer behaviour: numeric-nullable zeroing, text-nullable zeroing, varchar-nullable zeroing, temporal-default-precision collapse (6→0), and the explicit-non-default-precision negative passthrough. The negative pin is the load-bearing guard against over-normalization: `TIMESTAMP(3)` and `TIME(2)` MUST diff against their non-default counterparts; only `precision == 6` (the PG default report) collapses.

- **`test(pipeline): shard_consolidation_rename_pg_integration_test.go` (+156 LOC) + `shard_consolidation_rename_mysql_integration_test.go` (+118 LOC)** — Per the Bug 74 "pin the class, not the representative" lesson, both engines' RENAME integration tests now exercise a **six-cell type matrix** at the boundary: a `name VARCHAR(64) NOT NULL → product_name VARCHAR(64) NOT NULL` rename against schemas carrying `extra_numeric_nullable NUMERIC(10,2)`, `extra_text_nullable TEXT`, `extra_varchar_nullable VARCHAR(64)`, `extra_integer_nullable INTEGER`, `extra_timestamp_nullable TIMESTAMP`, and `extra_boolean_nullable BOOLEAN`. Each cell drives cold-start → CDC → in-flight RENAME on the source → post-RENAME INSERT → asserts the target's schema reflects the rename + the new row replicates + existing rows preserved. The v0.78.0 RENAME pin used a single fixture whose representative-of-one didn't expose either asymmetric field; the matrix would have caught Bug 86 in the same CI roundtrip that produced v0.78.0. **The matrix now pins the class on both engines.**

## Compatibility

- **Drop-in upgrade from v0.78.0.** Pure bugfix; no flag surface change. Operators who hit v0.78.0's spurious "Unrecognized combo" refusal on RENAME against PG schemas with nullable NUMERIC / TEXT / temporal columns now get the auto-apply behaviour ADR-0054's RENAME shape advertises. Operators who DIDN'T hit it (schemas where every column is NOT NULL with non-temporal types, or operators not running Shape A streams) see no change.
- **Severity a.** The v0.78.0 RENAME feature was effectively broken on the majority of real-world PG schemas (any column reading default precision from `information_schema`, or any nullable NUMERIC / TEXT). The cycle subagent hit it on the first realistic test schema. We're treating this as the kind of silent-feature-regression the loud-failure tenets exist to prevent — the operator-claimed feature didn't work on the schemas the operator was likely to use.

## Who needs this

- **Anyone on v0.78.0** running Shape A streams with `--inject-shard-column` against PG schemas containing nullable NUMERIC / TEXT / temporal columns. Without this upgrade, RENAME COLUMN on the source forces a drained-model fallback even though v0.78.0's release notes advertise auto-apply.
- **Anyone NOT on v0.78.0** or NOT running Shape A: no observable change.

## The Bug 74 cost framing

This is the second cycle in a row where the "pin the class, not the representative" lesson would have caught the bug before it shipped. The v0.78.0 RENAME pin used `name VARCHAR(64) NOT NULL` — a single fixture that:

1. Kept `Nullable` symmetric on both sides (true→false didn't apply because both were false), masking the pgoutput-vs-SchemaReader gap on `Nullable`.
2. Didn't probe temporal types at all, masking the precision-default-reporting gap.

The same cycle that produced v0.78.0 would have caught both with a six-cell type matrix in one CI roundtrip. Instead, v0.78.0 shipped; the testing rig hit Bug 86 on the first realistic schema; the production fix needed a second normalizer extension when the matrix uncovered the temporal asymmetry; and the integration tests now pin the class on both engines so the next pgoutput-vs-SchemaReader gap regression surfaces here, not in user reports.

**The discipline going forward** — for any normalizer / classifier / codec / serializer change that dispatches on a type family, the pin MUST exercise the matrix of element families × shape variants. "One representative is green" is not coverage; it's a sampled hypothesis.

## Cross-references

- [v0.78.0 release notes](https://github.com/orware/sluice/releases/tag/v0.78.0) — the prior release whose RENAME shape this hotfix completes
- [v0.73.2 release notes](https://github.com/orware/sluice/releases/tag/v0.73.2) — the Bug 84 hotfix that introduced `NormalizeForCDCComparison`; v0.78.1 extends the same surface
- [ADR-0054 — Shape A Phase 2: live cross-shard DDL coordination](https://github.com/orware/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md)
- Bug 74 lesson: see `CLAUDE.md` § *Pin the class, not the representative*
