# ADR-0133: SQLite schema-feature detection — loud-WARN on un-carried CHECK / generated / partial-expr indexes

## Status

**Accepted (2026-06-27).** Roadmap item 49 follow-up (#2 of the SQLite source queue).
The SQLite/D1 source engine silently omits three schema features it doesn't read —
table CHECK constraints, generated columns (carried as plain columns), and
expression/partial indexes. This makes the omissions **loud** (a one-time WARN per
table naming what was not carried) so an operator sees the fidelity gap rather than
discovering it later. It does NOT add cross-dialect translation (that would need IR
modeling these features, across every engine — out of scope).

## Context

The SQLite reader (ADR-0128/0129/0130/0132) reads columns/PK/FK/plain indexes via
PRAGMAs. It does not surface:

- **CHECK constraints** — they live only in the `CREATE TABLE` SQL (no PRAGMA), so the
  target is created without them. Silent: the target is less-constrained than the source.
- **Generated columns** — `PRAGMA table_info` returns them as ordinary columns, so their
  *computed values are copied* (no data loss), but the column lands as a **regular**
  column on the target and the generation expression is lost. Silent downgrade.
- **Expression / partial indexes** — skipped (an expression index has a NULL column in
  `index_info`; a partial index has `index_list.partial = 1`). Silent: a performance/
  semantics structure is dropped.

The IR does not model any of these on `ir.Column`/`ir.Index`/`ir.Table` (only
`Capabilities.SupportsCheckConstraint`/`SupportsGeneratedColumns` flags and `Domain`
checks exist), so faithfully *translating* them to PG/MySQL would require new IR shape
across all engines. That is a much larger effort than the gap warrants today; the
loud-failure tenet's minimum bar — make the omission visible — is what this delivers.

## Decision

1. **Detect and loud-WARN, do not silently omit.** During schema read, detect each of
   the three features and emit a clear one-time WARN per table naming the feature and the
   consequence, e.g.:
   - `sqlite: table "t": CHECK constraint(s) are not carried to the target (sluice does not yet translate SQLite CHECK constraints) — re-add them on the target if required`
   - `sqlite: table "t" column "c": generated column carried as a REGULAR column (its current computed values are copied; the generation expression is not translated)`
   - `sqlite: table "t" index "ix": expression/partial index not carried (performance/partial-filter structure; recreate on the target if required)`
   These are WARNs, not refusals: the data migrates correctly; these are target-side
   schema features the operator can re-add. Refusing would block a migration over a
   constraint, which is too aggressive.

2. **Detect generated columns via `PRAGMA table_xinfo`** (the `hidden` column: 2 = virtual
   generated, 3 = stored generated; 0/1 = ordinary). Continue to read them as ordinary
   `ir.Column`s and copy their computed values (preserves data); the WARN records the
   downgrade. (`table_xinfo` is a superset of `table_info`, available on every supported
   SQLite/D1 version.)

3. **Detect CHECK constraints** by scanning the table's `CREATE TABLE` SQL from
   `sqlite_master` for a top-level `CHECK` (paren-and-string-aware so a `CHECK` inside a
   string literal or column name doesn't false-positive). Presence → WARN. (Extracting +
   translating the expression is the deferred follow-up.)

4. **Detect partial / expression indexes** from `index_list.partial = 1` and the existing
   NULL-column (`index_info`) expression-index signal → WARN-and-skip (already skipped;
   add the WARN).

5. **Applies to both transports** — the file/`.sql` reader and the `d1` query-API reader
   share the schema path, so both get the detection + WARNs.

## Consequences

- An operator migrating a SQLite/D1 database with CHECK constraints, generated columns,
  or partial/expression indexes is told, at read time, exactly what is not carried — the
  silent fidelity gaps become visible (the loud-failure tenet's minimum bar). Data is
  unaffected (generated values are still copied).
- No IR change, no migration-failure risk (WARN, not refuse), no cross-dialect expression
  translator. Full translation of any of the three (CHECK→target CHECK, generated→target
  generated, partial/expr index→target) remains a per-feature follow-up requiring IR
  modeling.

## Alternatives considered

- **Translate the features** (emit target CHECK / GENERATED / partial indexes). Rejected
  for now: needs IR fields on Column/Index/Table across every engine + a SQLite→PG/MySQL
  expression translator — a large, fragile effort disproportionate to current demand.
- **Refuse loudly on any of them.** Rejected: blocks a perfectly migratable dataset over
  a target-side constraint the operator can re-add; a WARN is the right severity (data is
  correct; the gap is recoverable).
- **Keep silently omitting.** Rejected: violates the loud-failure tenet — the whole point
  of this item.
