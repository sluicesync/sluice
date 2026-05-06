# ADR-0029: `sluice schema diff` — drift detection against an existing target

## Status

Proposed. Implementation gated on operator review of this design.

## Context

v0.6.0's `sluice schema preview` (ADR-0024) answered the question *"what would sluice produce on a fresh target?"* — operators see the target DDL with translation notes and advisory hints before committing to a migration.

The complementary question — *"does what's currently running on the target match what sluice would produce?"* — comes up in two operator scenarios:

1. **Re-migration safety.** Before re-running `sluice migrate` against a target that's been live for a while (where someone may have added a column, dropped an index, or changed a column type out-of-band), operators want to know what's about to change. Today the only path is reading both schemas by hand.

2. **Continuous drift detection.** A long-running CDC stream's destination is supposed to mirror the source's shape (modulo translations). Schema drift on the dst — a column added by a different tool, an index dropped during maintenance — silently breaks the implicit contract. Operators want a CI gate that fails if drift is detected.

ADR-0024's "out of scope (future work)" section explicitly flagged this as warranting its own ADR.

## Decision

Add a new subcommand: **`sluice schema diff`**. Reads:

- **Expected schema**: source DSN → `SchemaReader.ReadSchema` → translation pipeline (mappings + cross-engine type policy, same path as `schema preview`).
- **Actual schema**: target DSN → `SchemaReader.ReadSchema` (the same engine surface — `SchemaReader` doesn't care whether the DSN points at a source or a target).

Computes an IR-level diff. Emits structured output (text or JSON), with optional file output and CI-friendly exit codes.

### Reuse existing `SchemaReader` for both sides

The load-bearing simplification: no new engine surface. Each engine's `SchemaReader.ReadSchema` already produces an `*ir.Schema` from any DSN. Pointing it at the destination database reads that database's actual schema in IR form, ready to compare against the source-derived expected schema.

This means engines without source-specific reverse-engineering work get drift detection for free — the moment they implement `SchemaReader`, they can be diffed against.

### IR-level diff, not DDL-string diff

The diff core lives in `internal/ir/schema_diff.go` as a pure function:

```go
func DiffSchemas(expected, actual *Schema) SchemaDiff
```

Returns a structured `SchemaDiff` value with categorised changes:

```go
type SchemaDiff struct {
    TablesMissing    []string                 // in expected, not in actual
    TablesExtra      []string                 // in actual, not in expected
    TablesMismatched []TableDiff
}

type TableDiff struct {
    Name              string
    ColumnsMissing    []string
    ColumnsExtra      []string
    ColumnsMismatched []ColumnDiff
    IndexesMissing    []string
    IndexesExtra      []string
    // ... constraints, identity sequences ...
}

type ColumnDiff struct {
    Name           string
    ExpectedType   Type
    ActualType     Type
    ExpectedNullable bool
    ActualNullable   bool
    // ... default value, generated expression, etc.
}
```

IR comparison is more robust than DDL-string comparison: `VARCHAR(45)` and `varchar (45)` and `character varying(45)` all map to the same `ir.Varchar{Length: 45}`. The diff output formatter renders the diff back to DDL for the human-readable text format.

### Bookkeeping-table filter

Sluice's `sluice_cdc_state` and `sluice_migrate_state` tables exist on the target but not in any source schema. Filter them out of the diff by default — they're sluice's internal bookkeeping, not user data. The schema readers already exclude them from `ReadSchema` (per ADR-0015 + v0.3.0 work), so this is automatic on the actual side.

### Output format

Text (default), human-readable, mirrors `schema preview`'s structure but with a diff overlay:

```
-- sluice schema diff
-- source:  postgres (5 tables expected after translation)
-- target:  mysql at app@host:3306 (5 tables found)
-- result:  1 missing column, 1 type mismatch, 1 extra table

-- ──────────── users (mismatched) ────────────
ALTER TABLE `app`.`users` ADD COLUMN `created_at` DATETIME(6) NOT NULL;
-- ^ missing on target

ALTER TABLE `app`.`users` MODIFY COLUMN `email` VARCHAR(255) NOT NULL;
-- ^ on target: VARCHAR(100) NOT NULL → expected: VARCHAR(255) NOT NULL

-- ──────────── deprecated_log (extra on target) ────────────
DROP TABLE `app`.`deprecated_log`;
-- ^ not in source schema; sluice would not create it
```

The `ALTER`/`DROP` SQL renders are *suggestions* for closing the diff, not statements sluice would execute — `schema diff` is read-only. Operators reviewing the diff can copy-paste the suggestions into a manual reconciliation script if they want to converge.

JSON format mirrors the `SchemaDiff` shape directly, suitable for `jq` queries and CI gating:

```json
{
  "source_engine": "postgres",
  "target_engine": "mysql",
  "summary": {
    "tables_missing": 0,
    "tables_extra": 1,
    "tables_mismatched": 1,
    "columns_missing": 1,
    "columns_mismatched": 1
  },
  "tables_extra": ["deprecated_log"],
  "tables_mismatched": [
    {
      "name": "users",
      "columns_missing": ["created_at"],
      "columns_mismatched": [
        {
          "name": "email",
          "actual": "VARCHAR(100)",
          "expected": "VARCHAR(255)"
        }
      ]
    }
  ]
}
```

`--output FILE` writes atomically (same temp+rename pattern as `schema preview`).

### Exit codes for CI gating

The load-bearing operator UX for the drift-detection use case:

- **0**: no diff. Schemas match.
- **1**: drift detected. Suitable for CI failure.
- **2**: operational error (couldn't read source or target schema). Distinct from "drift exists" so CI scripts don't conflate "the gate failed" with "we couldn't run the gate."

### CLI shape

```bash
sluice schema diff \
  --source-driver postgres --source 'postgres://...' \
  --target-driver mysql    --target 'mysql://...' \
  [--config sluice.yaml] \
  [--include-table foo,bar] [--exclude-table baz] \
  [--type-override TABLE.COL=TYPE] \
  [--format text|json] \
  [--output FILE] \
  [--ignore-charset-collation] \
  [--ignore-extras]
```

`--ignore-extras` suppresses "extra on target" diffs — useful when the target hosts other applications' tables that sluice should ignore. `--ignore-charset-collation` suppresses MySQL-specific charset/collation diffs that operators often manage out-of-band via server defaults.

## Consequences

- **Re-migration safety becomes a one-command check.** Operators run `sluice schema diff` before `sluice migrate` and see exactly what's about to change. The DDL-suggestion render makes "and here's how to reconcile" obvious without operators having to write the SQL.

- **CI drift gating is supported as a primary use case.** Exit code 1 + JSON output + `--output FILE` for CI artifacts mean a `.github/workflows/schema-drift.yml` job becomes ~10 lines of YAML.

- **No new engine surface.** Reusing `SchemaReader` means every engine that already implements it (today: PG, MySQL) gets diff support immediately. Future engines that ship `SchemaReader` get it for free.

- **DDL-suggestion accuracy is best-effort.** The text format renders `ALTER TABLE` / `DROP TABLE` SQL as a copy-paste starting point for operator reconciliation scripts. It's not guaranteed to be a syntactically-perfect migration script — index-method differences, identifier-namespace edge cases (PG schema-qualified vs MySQL flat scope), and target-engine constraints around column ordering may still need hand-editing. Operators wanting verified migration scripts use a dedicated migration tool (Atlas, sqitch); sluice's diff is for *visibility*, not authoring.

- **Cross-engine default-expression equivalences are intentionally narrow.** `now()` ↔ `CURRENT_TIMESTAMP` (with optional precision) and `CURRENT_DATE` round-trip cleanly via the IR's `defaultEquivalents` map; everything else surfaces as drift. Expression-vs-expression mismatches outside the equivalence map are flagged `default_low_confidence` in the JSON output and prefaced with `-- (default may differ across engines; verify before applying)` in the text output, rather than silently equated. The map is conservative by design — wrong entries suppress real drift — and grows additively as new engine-reader pairs land. Notable omissions documented inline: PG `nextval('seq')` (no MySQL counterpart; auto-increment is a column attribute, not a default expression) and PG `gen_random_uuid()` ↔ MySQL `UUID()` (semantically equivalent but produce incompatible binary representations).

- **Generated-column expression drift is reported but not auto-rewritten.** Both engines require DROP + ADD COLUMN to change a STORED generated expression, which is destructive enough that the renderer emits a comment describing the drift rather than a copy-paste-ready ALTER. Same trade-off as the missing-on-target case — operators get visibility, the migration is theirs to author.

## Why not auto-converge

A natural extension: `sluice schema diff --apply` that runs the suggested ALTER/DROP statements against the target. Reasons we explicitly don't:

- **DROP TABLE on the target is destructive.** The diff shows `extra on target` for tables that weren't in the source schema. Auto-dropping them is exactly the foot-gun `--reset-target-data` (ADR-0023) was carefully designed to gate. The same operator-confirmation pattern would apply, plus the dest-data risk would now be triggered by drift detection rather than explicit recovery — an unfortunate UX coupling.
- **ALTER TABLE on the target is risky on populated tables.** Adding a NOT NULL column without a default fails on a populated table; widening a column may take an exclusive lock for hours; dropping a column may break consumers. These are decisions a migration author makes deliberately, not something a diff tool should automate.
- **Schema reconciliation is a different tool's job.** Atlas, sqitch, liquibase, and similar tools own this surface. Sluice's value-add is the cross-engine translation visibility — `schema diff` exposes that as a comparison; making it an authoring tool would dilute the focus.

The DDL-suggestion render is the right level of help: it tells the operator *what* needs to change, leaves *how* to converge to the operator's chosen migration tool.

## Why not a separate "TargetSchemaReader" interface

The first design instinct was to add a parallel `TargetSchemaReader` that produces a richer schema (with charset/collation/index-method details that source-side `SchemaReader` doesn't always capture). Reasons we don't:

- **`SchemaReader` is already engine-agnostic about source-vs-target.** PG's `SchemaReader` reads `pg_catalog` regardless of whether the DSN happens to be a "source" or "target." Same for MySQL's `information_schema` reader.
- **The richer-schema gap is a SchemaReader gap, not a diff gap.** If operators want charset/collation in the diff, the right fix is enriching `SchemaReader` to capture it on read — which benefits every consumer, not just diff.
- **Two-interface designs invite drift.** Engines would have to implement both, with subtle differences risking "the diff sees X but the migrate sees Y" surprises.

## Out of scope for v1

Documented up front so the implementation chunk has a clear stopping point:

- **Column reordering**: dst column order can differ from source order. Treat columns as a set keyed by name, not a sequence. Reordering as a "mismatch" produces too much noise for too little operator value.
- **Index column ordering** within a multi-column index: same reasoning. Compare index column sets, not sequences.
- **FK constraint name normalisation**: PG and MySQL generate FK names differently when not explicitly named. Treat FKs as anonymous structurally — match by columns + referenced table/columns, not by name.
- **Trigger / function / procedure / view comparison**: sluice doesn't translate these today; they're outside the schema-diff scope until they are.
- **Snapshot-vs-running-state diff**: comparing what the target *was* (e.g., from a `pg_dump` snapshot) against what it is now is a generic database-tool problem, not specific to sluice's translation pipeline. Out of scope.
- **Charset / collation comparison**: the `--ignore-charset-collation` flag is plumbed and the preamble flags it, but the underlying comparison is still inert pending an IR enrichment to capture charset/collation on read for both engines. Once the IR carries them, the diff is additive — same shape as the v0.8.0 default/generated/CHECK additions.

### Added in v0.8.0

The first three categories below were originally listed as out-of-scope; v0.8.0 lifts that because the IR already carries the underlying fields and the comparison shape is additive on `ColumnDiff` / `TableDiff`:

- **Default values** (`ExpectedDefault` / `ActualDefault` / `DefaultLowConfidence` on `ColumnDiff`). Textual comparison with a tiny equivalence map for the common cross-engine pairs; mismatches outside the map flagged low-confidence rather than silently equated. Renderer emits `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT` / `DROP DEFAULT` suggestions.
- **Generated-column expressions** (`ExpectedGeneratedExpr` / `ActualGeneratedExpr` on `ColumnDiff`). Verbatim string comparison after trim; no equivalence map. Renderer emits a comment describing the drift plus a `DROP + ADD COLUMN` reconciliation hint (engines don't support in-place generated-expr ALTERs).
- **Table-level CHECK constraints** (`ChecksMissing` / `ChecksExtra` / `ChecksMismatched` on `TableDiff`). Matched by name (set semantics, same as indexes); unnamed CHECKs are silently dropped from the comparison to avoid false positives on cross-engine spelling. Renderer emits `ALTER TABLE ... ADD/DROP CONSTRAINT ... CHECK (...)` suggestions.
- **Per-column ALTER type rendering**. Engines now expose an `EmitColumnDef(table, col)` method via the optional `ir.ColumnDDLPreviewer` interface (implemented on the same struct as `SchemaWriter` / `DDLPreviewer` in both PG and MySQL). The diff renderer uses it to fill in the actual type, default, and generated expression on `ALTER TABLE ... ADD COLUMN` suggestions for missing-on-target columns; the previous `-- TYPE` placeholder remains as a defensive fallback for engines that don't implement the new interface.

## Verification

- Unit tests on `ir.DiffSchemas`: table additions/removals, column additions/removals, type mismatches (each major IR type), nullability mismatches, index additions/removals, charset/collation handling under the ignore flag.
- Unit test on the JSON output shape (golden file).
- Snapshot test on the text output (golden file in `cmd/sluice/testdata/diff/`).
- Integration test in `internal/pipeline/diff_integration_test.go`: boot PG + MySQL containers, set up source schema + drifted dst schema, assert the diff captures the drift correctly. JSON output unmarshal + assertion on summary counts.
- Exit-code integration tests: assert 0 on no-diff, 1 on drift, 2 on operational failure (e.g., bad target DSN).
