# sluice v0.95.0

# sluice v0.95.0 — operator-class IR-carry, step 1: Bug 115

**Headline:** First fix in the v0.95.x PG IR-carry arc. Pre-fix, an operator-explicit non-default core PG operator class on a single-column index — `btree (col text_pattern_ops)`, `btree (col varchar_pattern_ops)`, or `gin (col jsonb_path_ops)` — was silently dropped on PG→PG migrate. The index NAME survived, the structure looked right, but the operational semantics differed: `LIKE 'prefix%'` queries fell off the index in C locale, `@>` containment queries on jsonb were ~2× slower against indexes ~50% larger. "We migrated and our queries got slower" became a debug mystery. v0.95.0 extends the reader's catalog query to fetch `pg_opclass.opcdefault` alongside `opcname` and carries non-default core opclasses through `ir.IndexColumn.OperatorClass` so the same-engine writer emits them verbatim.

## Fixed

- **`fix(postgres): carry non-default core operator classes through schema reader (Bug 115 closure)`** — pre-fix the PG schema reader populated `ir.IndexColumn.OperatorClass` only when (a) the index used an extension-introduced access method (pgvector's hnsw), (b) the opclass was owned by an enabled extension on a core AM (pg_trgm's `gin_trgm_ops` / `gist_trgm_ops`), or (c) an uncatalogued extension-owned opclass surfaced under the ADR-0047 verbatim tier. Operator-explicit **non-default core PG opclasses** on core AMs fell through every branch and were silently dropped: `btree (col text_pattern_ops)` (required for `LIKE 'prefix%'` index use in C locale), `btree (col varchar_pattern_ops)` (same case for varchar), and `gin (col jsonb_path_ops)` (~50% smaller, substantially faster for `@>` containment vs default `jsonb_ops`) all migrated PG→PG to an index using the default opclass — index name preserved, structure intact, but the operational semantics differ and "we migrated and our `@>` queries are 10× slower" became a debug mystery. v0.95.0 extends the reader's SQL to fetch `pg_opclass.opcdefault` alongside `opcname` and adds a dispatch branch: when `opclass != "" && !opclassExtOwned && !opclassDefault`, the bareword is carried verbatim through `ir.IndexColumn.OperatorClass`. The existing same-engine PG writer at `emitIndexColumnList` already emits `<column> <opclass>` for any non-empty `OperatorClass`, so the fix is purely on the reader side. Default-opclass cases on built-in types continue to leave `OperatorClass` empty so the writer emits nothing extra (preserves the Bug 47 invariant that a non-empty value through the IR is an honest "operator-significant opclass" marker and keeps DDL diffs stable against `pg_get_indexdef` across PG major versions). Pinned by `TestSchemaReader_NonDefaultCoreOpclasses_Bug115` (integration test against a real Postgres covering the three documented Bug 115 cases — `text_pattern_ops`, `varchar_pattern_ops`, `jsonb_path_ops` — plus a default-opclass negative control that asserts the IR stays empty).

## Compatibility

- **Minor bump (v0.95.0).** Drop-in from v0.94.1 except for the behavior change below.
- **Behavior change:**
  - PG→PG migrate of a source with a single-column index using `text_pattern_ops` / `varchar_pattern_ops` / `jsonb_path_ops` (or other non-default core opclasses) now emits the opclass on the target index — pre-v0.95.0 the opclass was silently dropped. No action needed; the migration now matches operator intent.
  - PG→PG migrate of a source with a default-opclass single-column index continues to emit no opclass on the target (preserves Bug 47 invariant + DDL diff stability).
  - Cross-engine PG→MySQL is unchanged: opclasses are PG-specific and continue to drop on the cross-engine path.

## Who needs this

- **Anyone running `sluice migrate` PG→PG against sources whose schema uses `text_pattern_ops` / `varchar_pattern_ops` / `jsonb_path_ops`** — the silent perf-regression on the target is closed. **Upgrade**.
- **Everyone else** — drop-in upgrade, no action needed.

## Coming next

The v0.95.x PG IR-carry arc continues with:

- **Bug 113** — `CREATE DOMAIN` constraint is silently converted to the underlying base type on PG→PG migrate; the DOMAIN's CHECK constraint is lost. CRITICAL silent-constraint-loss class. Needs a new `ir.Domain{Name, BaseType, Checks}` IR concept + reader `pg_type.typtype='d'` detection + writer CREATE DOMAIN phase before tables + cross-engine MySQL WARN+downgrade.

After v0.95.x, **v0.96.x** covers operator-quality-of-life (Bugs 108 / 114). Open backlog post-v0.95.0: 108 / 113 / 114 = 3.
