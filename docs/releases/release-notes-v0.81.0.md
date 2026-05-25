# sluice v0.81.0 — Per-table schema-drift diff in CDC refuse-loudly messages (F11)

**Headline:** Minor release closing the operator-trust class where sluice's existing refuse-loudly catalog said "schema change detected on table X" with no detail of *what* changed. Operators were spending 15-30min running `pg_dump` on both sides and diffing manually. v0.81.0 makes the refusal one-shot informative: paste the message into Slack and the on-call DBA knows exactly which column/index/constraint drifted, with the operator-action hint inline.

## Added

- **`feat(pipeline): F11 — per-table schema-drift diff in CDC refuse-loudly messages (#47 / ADR-0060)`**

  When the ADR-0058 intercept refuses a non-ADD-COLUMN source DDL, the surfaced error message now includes a structured per-table diff:

  - Columns added (name, type)
  - Columns dropped (name, last-seen type)
  - Columns type-changed (name, old type, new type)
  - Columns renamed (when detectable via positional matching)
  - CHECK constraint changes (added / dropped / altered)
  - FK changes (added / dropped / altered via OnDelete change)

  Each entry uses a **greppable category prefix** (`[column-added]`, `[column-dropped]`, `[column-renamed]`, `[index-added]`, `[index-dropped]`, `[check-added]`, `[check-dropped]`, `[fk-altered]`) plus an operator-action hint inline. Operators can paste the refusal into a ticket or Slack channel and the receiving DBA gets actionable detail without round-tripping for clarification.

  ### Example refusal output

  ```
  pipeline: apply changes: pipeline: forward schema add-column: refuse non-ADD-COLUMN shape "drop-column" on "public.widgets" (ADR-0058):
    [column-dropped] legacy_col VARCHAR(100) — destructive change. Drop column on target via drained model: 'sluice sync stop --wait', then drop on target, then resume via 'sluice sync start --resume'.
  ```

  ### Architecture

  - `ir.DiffTable(pre, post) -> SchemaDriftReport` — pure function over IR table snapshots
  - `pipeline.RenderSchemaDriftReport(report)` — engine-neutral rendering with operator-action hints
  - Intercept wiring (`internal/pipeline/schema_forward_intercept.go`): on refuse paths, compute diff between cached pre-DDL snapshot and observed post-DDL snapshot, fold rendered entries into the refusal error

## Known limitation (documented in ADR-0060 §6)

- **Index-only DDL (CREATE INDEX / DROP INDEX) is not detected via F11.** pgoutput's `RelationMessage` describes column-shape only — CREATE INDEX doesn't trigger a new RelationMessage, so the F11 diff detector can't observe it. Operators see index drift through chain-restore at backup boundaries instead; live detection deferred to F47 schema-drift catalog.

## Tests

- **19 unit tests on `ir.DiffTable`** — Bug-74 class matrix: column add/drop/type/nullable/default/rename + multi-kind, index add/drop, CHECK add/drop/alter, FK alter via OnDelete change, multi-shape combo, deterministic ordering, nil-handling
- **8 renderer unit tests** — greppable prefixes + per-category hint content verification
- **6+ intercept subtest pins** — refusal-message output per category
- **PG → PG integration test** — 3 refused shapes (drop column / rename column / alter type) end-to-end with testcontainers
- **All 17 pre-existing ADR-0058 `TestForwardAddColumn_*` subtests remain green** — F11 only augments refuse-message strings; happy paths untouched

## Docs

- **ADR-0060 — CDC apply-side schema-drift diff** (`docs/adr/adr-0060-cdc-schema-drift-diff.md`). Covers motivation (Reddit-research F11), diff structure, operator-action mapping, scope exclusions, and ADR-0058/ADR-0029/ADR-0054 relationships.

## Compatibility

- **Drop-in upgrade from v0.80.0.** No new flag surface; F11 augments existing refuse-loudly messages.
- **Minor version bump (v0.81.0)** because the refusal-message shape is a new observable behavior operators can grep on.
- **Severity a** — operator-trust improvement: reduces median time-to-diagnose for source-side DDL drift from ~15-30min (manual pg_dump diff) to ~30 seconds (paste refusal into ticket).

## Who needs this

- **Any operator who has hit a sluice refusal** for source-side DDL outside the ADR-0058 forwarding catalog — drop/rename/alter-type cases that surface mid-stream. Pre-v0.81.0, the refusal named the shape but not the column; v0.81.0 names exactly what changed plus the action.
- **Multi-tenant operators with DBAs on rotation** — the refusal text now flows into incident tooling without manual investigation.
- **Operators NOT seeing source-side DDL drift** — no observable change.

## Cross-references

- [ADR-0060 — CDC apply-side schema-drift diff](https://github.com/orware/sluice/blob/main/docs/adr/adr-0060-cdc-schema-drift-diff.md)
- [ADR-0058 — Online schema-change forwarding](https://github.com/orware/sluice/blob/main/docs/adr/adr-0058-online-schema-change-forwarding.md) — the "do it automatically when opted-in" sibling; F11 is the "tell operator loudly" half
- Bug 74 lesson: see `CLAUDE.md` § *Pin the class, not the representative* — 19 unit tests + per-category integration coverage follow this discipline
