# Design: schema completeness — FK edge cases + view support

**Status:** proto-ADR (research / design draft, not yet implementation-bound)
**Author:** main session
**Date:** 2026-05-07
**Related:** ADR-0029 (`sluice schema diff`), v0.4.0 FK-failure test (FUTURE-TESTS.md item L), `sluice-verify.md` (cross-reference for completeness verification)

## Why

Two questions raised by the user 2026-05-07 that map to the same design space — "what schema objects should sluice handle, and how completely does it handle them?":

1. **Foreign key constraint dependencies and ordering** — does sluice handle the FK ordering problem (table A references table B; A must be created after B; and DROP order is reversed)? What edge cases exist?
2. **Views** — are views copied over by `sluice migrate`? Should they be?

Investigation findings (verified against current code):

- **FKs**: Sluice handles the create-order problem cleanly via the three-phase apply pattern (tables-without-constraints → bulk-copy → indexes → constraints). FKs land in the LAST phase, after all tables exist. This sidesteps topological sorting entirely. Edge cases exist; documented below.
- **Views**: Sluice does NOT currently handle views. The IR has no `View` type; `Table` schema readers don't query `information_schema.views` (MySQL) or `pg_views` / `pg_matviews` (PG). Views on the source are silently ignored at schema-read time.

This proto-ADR covers both — FK edge cases that need test coverage (small fixes), and view support as a feature addition (substantial work).

## Decision

### Foreign keys: stay with three-phase apply; close the documented edge cases

The three-phase apply (ADR foundational, ADR-0015) is the right architecture for FKs. The decision is to **keep it as-is** and close the specific edge cases that aren't fully tested today.

**Edge cases worth test coverage** (each is a small pinned regression test, not a feature):

1. **Self-referential FKs.** A table whose column FKs to its own PK (`employees.manager_id → employees.id`). Should "just work" because FKs land in phase 5 after the table exists, but no test currently pins this. Add to genchk-style integration test.

2. **Circular FKs (A → B → A).** Both tables exist before any FK is added (phase 1), so the cycle resolves at phase 5. Should work; not currently tested.

3. **DROP-order on `--reset-target-data`** (v0.6.0). Reset must drop FKs *before* dropping tables, or `DROP TABLE` fails with "cannot drop table; other objects depend on it." Current implementation likely uses `DROP TABLE ... CASCADE` on PG (which handles dependencies automatically) and explicit FK-drop on MySQL (which doesn't have CASCADE). Verify and pin in a test.

4. **CDC-side FK violations during catch-up.** When a stream is stopped mid-CDC and resumed, the events between stop and restart get replayed. If those events include a parent-child INSERT pair, the events arrive in source-commit order so the parent INSERT comes first — sluice's event-driven CDC apply path handles this naturally. But: what if source uses `ON DELETE CASCADE` and the cascade creates events that arrive before the triggering DELETE in the CDC stream? PG and MySQL's CDC streams emit events in commit-order; the parent DELETE and the cascaded child DELETEs are part of the same transaction, so they arrive together. Confirm with a test that exercises this on both engines.

5. **`DEFERRABLE INITIALLY DEFERRED`** (PG-only). Applies the FK check at COMMIT instead of statement boundary; useful for circular updates. Sluice currently emits `NOT DEFERRABLE` always (the default). Operators with deferrable constraints in their source schema need this to round-trip; today it's silently dropped. Document the limitation and the workaround (`--type-override` doesn't apply to constraints; would need `--constraint-override` or similar). File as a known-gap entry in `BUG-CATALOG.md` if not already.

6. **`ON DELETE` / `ON UPDATE` action round-trip.** MySQL → PG and PG → MySQL should preserve `CASCADE` / `RESTRICT` / `SET NULL` / `NO ACTION` / `SET DEFAULT`. Worth a parameterised cross-engine test.

7. **FK to a table that's `--exclude-table`'d.** Already tested (FUTURE-TESTS.md item L); produces a clear loud-failure. Document as expected behavior.

8. **Composite-PK FKs** (multiple-column FK). Already tested for shape; round-trip across engines worth a regression check.

**Implementation effort**: ~2-3 days for the test coverage. No code changes required (the architecture is correct); just pin the behaviors with regression tests so they can't silently break.

### Views: add support as a new schema-object class

Sluice currently has zero view support. Adding it is a meaningful feature with several design choices.

**Recommendation: add view support as a Phase 1 (read + emit, simple cases) + Phase 2 (CDC + complex cases)**.

#### Phase 1: schema-only view round-trip

- New `ir.View` type (separate from `ir.Table`) with `Name`, `Definition` (the SQL body), `Columns` (the projected columns; may be derivable from the definition but useful to carry explicitly), `Materialized bool` (PG-only initially).
- New `ir.Schema.Views []*View` field.
- MySQL schema reader queries `information_schema.views` for `view_definition`, `is_updatable`, etc. PG schema reader queries `pg_views` and `pg_matviews`.
- New `ir.SchemaWriter.CreateViews(ctx, schema) error` method on the engine. Engines emit `CREATE VIEW ... AS <definition>`.
- Phase ordering: views are created **after** constraints (phase 6) since they can reference tables and other views. View-to-view dependency ordering needs the topological sort that FKs avoided — but the dependency surface is small (each view's definition references named tables / views), so a single pass over view definitions parsing for `FROM <name>` patterns gets us 95% of the way. The other 5% (subqueries, CTEs, `LATERAL`) need either a real SQL parser or fall through to "create in declared order, retry-on-failure" loop.
- View definitions are dialect-specific. Cross-engine view round-trip means running the definition through a translator analogous to ADR-0016's expression translator. Initial scope: same-engine view round-trip works; cross-engine emits the source-dialect definition verbatim and fails loud at apply time if the target rejects it. Operator escape: `--view-override TABLE.NAME=DEFINITION` analogous to `--expr-override`.
- Materialized views (PG): same shape, plus `WITH DATA` / `WITH NO DATA` and `REFRESH MATERIALIZED VIEW` semantics. Phase 1 emits `WITH DATA` (immediate refresh from source via a SELECT against the just-loaded target tables); Phase 2 covers continuous refresh on CDC.

#### Phase 2: views + CDC

Views aren't part of CDC streams (they're virtual; the row events come from underlying tables). The CDC streamer doesn't need to know about views beyond "ignore CDC events that name a view as the table" (which shouldn't happen in well-formed binlog/pgoutput streams).

Materialized views ARE physical and would need refresh-on-CDC if the operator wants the target's materialized view to stay in sync with the source's. Default: no auto-refresh; operator runs `REFRESH MATERIALIZED VIEW` themselves on a schedule. Phase 2 could add `--refresh-mat-views=on-demand|on-each-commit|every-N-seconds` for operators who want sluice to manage this.

#### Phase 3: cross-engine view-definition translation

The view-definition language is much larger than the expression-translator surface (full SELECT grammar, joins, CTEs, window functions, set operations). Sluice's current translator handles a few-dozen idioms in DDL bodies; full SQL-SELECT translation is its own multi-month effort. Phase 3 picks up incrementally: per-engine helper functions for the most common cross-engine differences (PG `LIMIT N OFFSET M` ↔ MySQL same-shape, `STRING_AGG` ↔ `GROUP_CONCAT`, etc.), with `--view-override` as the always-works escape hatch for everything else.

**Implementation effort**:
- Phase 1: ~2 weeks (new IR type, both engine readers, both engine writers, basic-shape tests).
- Phase 2: ~1 week (materialized-view refresh).
- Phase 3: open-ended; reactive to operator demand.

## Tenet check

### FK edge-case tests

- **Clean elegant code.** No new code; just regression test additions.
- **Validate end-to-end.** Tests are the entire point; they pin behavior so it can't silently break.

### View support

- **Clean elegant code.** New `ir.View` type is small (~5 fields); engine readers and writers are mechanical; the dependency-ordering pass is a simple topological sort. No special-case branches polluting the existing code.
- **IR-first.** Views go through the same IR-first translation pipeline tables do. View-definition translation (Phase 3) is the concerning bit; the loud-failure tenet keeps Phase 1/2 honest by passing definitions verbatim and failing loud at apply.
- **Contain Postgres complexity.** Materialized views are PG-only initially; the IR field is optional and engines without materialized-view support skip them. PG's view system has security definer/invoker, RULES on views, TRIGGER ON VIEW — sluice intentionally doesn't propagate those (out of scope; surface as warnings if seen).
- **Loud failure beats silent corruption.** Cross-engine view definitions that don't translate fail at apply time with a clear PG/MySQL error. `--view-override` is the operator escape. Better than a translator that quietly produces broken SQL.

## Consequences

### FK edge-case tests

- Test surface grows by ~6-8 small integration tests across the engines. CI Integration job runtime grows by ~30s.
- Documented limitations (e.g., `DEFERRABLE` not round-tripped) become discoverable via `BUG-CATALOG.md` instead of "operator finds out at apply time on a real schema."
- No runtime behavior change.

### View support

- New IR type; engine surface grows by `CreateViews` method on `SchemaWriter` and view enumeration on `SchemaReader`. Existing engines opt-in; engines without view support skip.
- New phase 6 in the orchestrator (views-after-constraints).
- New CLI surface: `--include-view` / `--exclude-view` glob flags; `--skip-views` for operators who want tables-only behavior. `--view-override TABLE.NAME=DEFINITION` for cross-engine override.
- `sluice schema diff` extended to compare views: name, columns, definition (textual). Same shape as existing column / index / CHECK comparison.
- `sluice schema preview` extended to show views.
- New CHANGELOG entries; the IR type is a public surface contract once shipped.

## Open questions

1. **MySQL view + PG view definition compatibility.** MySQL views are constrained (no UNION, no subquery in FROM in the WHERE clause until 8.0). PG views are more permissive. A MySQL → PG view almost always works; PG → MySQL views often need the operator to simplify the definition. Cross-engine round-trip is asymmetric.

2. **MySQL `CHECK OPTION` and `SQL SECURITY` clauses.** Per-view metadata that doesn't translate. Sluice should preserve them when same-engine, drop them with a warning when cross-engine.

3. **PG's `RULE` system on views.** Sluice doesn't read PG rules today. INSERT/UPDATE/DELETE rules on views are how PG implements updatable views. Out of scope for Phase 1; document as known limitation.

4. **Recursive CTEs in view definitions.** Both engines support but with subtle differences (MySQL doesn't allow more than one CTE in some shapes). Phase 3 problem.

5. **Schema-qualified table references in view definitions.** PG: `public.users`. MySQL: `mydb.users`. Cross-engine: needs renaming / requalification. Phase 3 problem.

6. **Materialized-view refresh during CDC stop+resume cycles.** If sluice is stopped, source materialized view gets `REFRESH`'d, then sluice resumes — the target materialized view's data is now out of date relative to the source's underlying tables. Phase 2 design question.

## What this is not

- **Not a SQL parser.** Sluice doesn't (and shouldn't) implement a full SQL parser to translate view definitions cross-engine. The Phase 3 translator catches common patterns; everything else uses `--view-override`.
- **Not a view-as-CDC-source feature.** Views aren't part of CDC streams. Sluice doesn't try to make them so.
- **Not stored procedures, triggers, or functions.** Same shape of question (different schema-object types) but each is its own design space. Out of scope here.

## Sequencing

1. **FK edge-case test coverage (~3 days).** Small, no-code-change additions. High confidence this can land in any patch release.
2. **View support Phase 1 (~2 weeks).** Schema-only view round-trip. Probably v0.12.0 or v0.13.0.
3. **View support Phase 2 (~1 week).** Materialized-view refresh. Same release as Phase 1 if simple, separate otherwise.
4. **View support Phase 3 (open-ended).** Cross-engine view-definition translation. Reactive to operator demand.

## Recommendation

**Yes on FK edge-case tests** — small effort, closes documented gaps, increases operator confidence.

**Yes on view support Phase 1** — meaningful capability gap that operators with non-trivial schemas will hit. Same-engine round-trip is the easy case and covers most demand. Cross-engine and materialized-view refresh follow as pull requests / operator requests come in.

Path to no on view support: only if operator demand stays at zero through ~3 more test cycles. The recurring "we have a few views in our schema" pain shape is real for non-trivial source databases; expecting sluice to handle them is reasonable user-facing scope.
