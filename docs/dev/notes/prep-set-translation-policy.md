# Prep: SET → TEXT[] (default) + mappings overrides

Roadmap reference: not in the original roadmap. Surfaces from the post-walkthrough conversation about the GEOMETRY/SET friction items the sakila walkthrough exposed. Depends on [prep-mappings-config-wiring.md](prep-mappings-config-wiring.md) being merged.

## Goal

Today, MySQL `SET` columns reach the Postgres writer as `ir.Set{Values: [...]}` and the writer rejects them with:

```
postgres: column "special_features": postgres: SET has no native equivalent;
translate to TEXT[] or similar before writing
```

The error is honest — Postgres has no native SET — but the rejection is a v1 punt. With the mappings config wired (separate prereq chunk), we can implement a real default policy plus operator overrides.

After this chunk:

- **Default**: PG writer emits `ir.Set` columns as `TEXT[]` with a `CHECK` constraint enforcing the values are members of the source SET's value list. Order is preserved (MySQL SET semantics).
- **Override via `sluice.yaml` mappings**: operators can pick alternative shapes per-column.

Out of scope:

- **Junction-table strategy.** A SET column of cardinality N in a parent table of M rows expands to a junction table of M×avg(SET-elements) rows. Real, useful, but a substantially bigger schema change than `TEXT[]`. Ships as its own follow-up if a real use case demands.
- **Boolean-per-member strategy.** Works only for tiny fixed SETs. The schema explosion (one column per SET member, indexed individually) makes this a niche choice. Defer until requested.
- **The reverse direction (PG → MySQL).** This chunk is one-directional: SET on the source means MySQL source. PG → MySQL with a `TEXT[]` source going to a MySQL target would need a different policy, and SET isn't really the reverse case anyway (PG doesn't have SET-shaped data unless someone built it manually).

## Default emission shape

For an `ir.Set{Values: ["A", "B", "C"]}` column named `flags` on a table named `events`:

```sql
CREATE TABLE "public"."events" (
    ...
    "flags" TEXT[] NOT NULL DEFAULT '{}',
    CONSTRAINT "events_flags_set" CHECK (
        "flags" <@ ARRAY['A','B','C']::TEXT[]
    ),
    ...
);
```

Two design choices baked in:

1. **`<@` containment** (every element of `flags` is in the value list) rather than enforcing the literal value list as an enum type. Matches MySQL SET semantics: any subset of the declared values is valid, including the empty set.
2. **`DEFAULT '{}'`** (empty array) when the source has no DEFAULT. MySQL's SET default is the empty string; the IR's `DefaultLiteral` translation could land on `'{}'` directly via the existing default-translation work.

Value translation on the row-write side:

- **Source (MySQL)**: SET arrives as a comma-separated string `"A,B,C"`. The MySQL value decoder already converts this to `[]string` (per `decodeValue`'s Set handling).
- **Target (PG)**: pgx's `CopyFrom` accepts `[]string` natively for TEXT[] columns. No new conversion code.

## Override surface via `sluice.yaml` mappings

Three target_type aliases the registry should learn:

```yaml
mappings:
  - table: events
    column: flags
    target_type: text_array            # default; the same as no mapping

  - table: events
    column: flags
    target_type: junction_table        # future; placeholder for the second strategy
    target_type_options:
      junction_table_name: event_flags # optional; default = "<table>_<column>"

  - table: events
    column: flags
    target_type: booleans              # future; useful for tiny fixed SETs
```

For v1 of *this* chunk, only `text_array` is implemented. The other two return a clear "not yet implemented" error from `ApplyMappings`. They're listed here so the registry shape is clear; landing them is a separate (smaller) chunk each.

## Files to add / touch

- `internal/engines/postgres/ddl_emit.go` — `emitColumnType` learns to emit `TEXT[]` for `ir.Set`. The CHECK constraint goes alongside in `emitTableDef` (CHECK constraints are inline in CREATE TABLE; they don't need a phase 3 ALTER TABLE). ~30 lines.
- `internal/engines/postgres/row_writer.go` — `prepareValue` for `ir.Set` already handles `[]string` (per the existing Array support); confirm it does and add a test if the path isn't covered. ~10 lines if any change needed.
- `internal/translate/mappings.go` — registry entry for `text_array` → `ir.Array{Element: ir.Text{Size: TextLong}}`. The IR-side rewrite happens during ApplyMappings.
- `internal/engines/postgres/ddl_emit_test.go` — unit test for the CREATE TABLE emission with a SET column. ~30 lines.
- `internal/pipeline/migrate_cross_integration_test.go` — extend the existing MySQL→PG cross-engine test seed (already has `role ENUM(...)` from §7) to add a `tags SET('a','b','c')` column. Assert it lands as `ir.Array{Element: ir.Text}` on the PG side and that round-trip values come through as `[]string`. ~30 lines.

~100 lines net.

## Anticipated rough edges

- **Default value translation for SET.** MySQL `SET DEFAULT 'a,b'` arrives at the PG writer as `DefaultLiteral{Value: "a,b"}`. The PG TEXT[] column needs `DEFAULT ARRAY['a','b']::TEXT[]` — a small translator step from comma-separated string to PG array literal. Either handle in the PG `emitDefault` for the `ir.Array` case (when the literal looks comma-separated and the column is array-typed), or skip the default and document that mapped SET columns lose their default unless explicitly set in `sluice.yaml`.
- **Ordering semantics.** MySQL SET preserves declaration order in queries (`SELECT flags FROM ...` returns the canonical order). PG TEXT[] preserves insertion order. The two are mostly compatible but a query that depends on MySQL's canonical-order semantics may produce different results on PG. Document, don't try to enforce.
- **Empty SET handling.** MySQL SET column value `''` (empty string) decodes to `[]string{}`. PG TEXT[] empty value is `'{}'::TEXT[]`. The existing decoder already handles this; verify the integration test covers it.
- **Cross-engine ordering through the value pipe.** SET values in a MySQL row arrive as a comma-separated string; the value decoder splits on `,`. If a SET member contains a comma (rare but legal in MySQL), the split is wrong. The MySQL value decoder already has this hazard for the existing same-engine path; not a regression, worth a comment.

## Open questions for the user

1. **Default policy: `text_array` or `text`?** Above I'm picking `text_array` (most semantically faithful). The alternative — flatten to `TEXT` with the comma-separated form preserved — is simpler but less queryable. *Recommendation:* `text_array`. Confirm?
2. **CHECK constraint by default.** Above I'm emitting a CHECK enforcing the value list. Some operators consider CHECK constraints expensive; an opt-out (`target_type_options.check: false`) would be cheap to add. *Recommendation:* CHECK on by default for v1; opt-out later if requested. Confirm?
3. **Default-value handling.** Above I sketched two paths (translate the literal, or document the loss). *Recommendation:* translate when the source default is a comma-separated literal (small, predictable); document that operators using `target_type: text_array` get the default drop in any other case. Confirm?
4. **Reverse-direction (PG → MySQL) of a TEXT[] column.** Out of scope above. Confirm we punt this until someone asks?

## Suggested first-cut prompt

> "Read CLAUDE.md, docs/dev/notes/prep-set-translation-policy.md, and the existing internal/engines/postgres/ddl_emit.go's emitColumnType. Propose the design before writing: (1) the exact emitColumnType branch for ir.Set + the CHECK constraint emission, (2) the default-value translation (comma-separated → PG array literal), (3) the integration test seed addition, (4) how the mappings registry learns 'text_array'. Note any deviation from the prep doc with a why. Stop after the design for review."
