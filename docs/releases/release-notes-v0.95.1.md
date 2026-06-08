# sluice v0.95.1

# sluice v0.95.1 — PG schema-fidelity arc, step 2: Bug 113

**Headline:** Second fix in the v0.95.x PG schema-fidelity arc. Pre-fix, a column referencing a Postgres `CREATE DOMAIN` user-defined type (e.g. `CREATE DOMAIN email_address AS text CHECK (VALUE ~ '...@..\..*')`) migrated PG→PG to a plain `text` column with no CHECK constraint: `information_schema.columns` silently unwraps DOMAINs (`data_type` returns `text`, not `USER-DEFINED`) so the schema reader saw a plain text column and the operator's DOMAIN CHECK invariants vanished on the target. CRITICAL silent-constraint-loss class — the target accepted values the source would reject. v0.95.1 reads `pg_type.typtype` directly per-column; when `'d'` (DOMAIN), the reader refuses loudly at the read boundary so no partial schema lands on the target, with a clear error naming table, column, domain, and the recovery `ALTER TABLE` shape. Round-trip DOMAIN carry is queued for v0.95.2; this release ships the loud-refusal AND the IR scaffolding (`ir.Domain`, `ir.DomainCheck`, `ir.ExtDomain`, full JSON tagged-union round-trip) the v0.95.2 follow-up will populate.

## Fixed

- **`fix(postgres): refuse loudly when a column references a PG DOMAIN (Bug 113 closure)`** — pre-fix a column referencing a Postgres `CREATE DOMAIN` user-defined type (e.g. `CREATE DOMAIN email_address AS text CHECK (VALUE ~ '...@..\\..*')`) surfaced through `information_schema.columns` as the underlying base type — `information_schema` silently unwraps DOMAINs (`data_type` returns `text`, not `USER-DEFINED`) — so the PG schema reader translated the column to `ir.Text{}` and the DOMAIN's CHECK constraints disappeared on PG→PG migrate (CRITICAL silent-constraint-loss class: every input-validation invariant the operator encoded as a DOMAIN was silently destroyed on the target). The bug catalog (Bug 113) records this as a same-engine PG→PG class: the target accepted values the source would reject, with no WARN, no error, and exit 0. v0.95.1 reads `pg_type.typtype` directly alongside the per-column dispatch; when `typtype == 'd'` the reader refuses loudly at the read boundary so no partial schema lands on the target, and the operator gets a clear actionable error naming the table, column, and domain name (resolved via `pg_type.typname` since `information_schema.columns.udt_name` ALSO unwraps DOMAINs and returns the base type — caught by the PR's integration pin during Phase A of the three-phase debug protocol), plus the recovery `ALTER TABLE … ALTER COLUMN … TYPE … USING …::…` shape. Per the bug-catalog suggested-fix: "Either is acceptable; silent-drop is not." Round-trip DOMAIN carry is queued for a v0.95.2 follow-up; this release ships the IR scaffolding (`ir.Domain{Name, BaseType, Checks}`, `ir.DomainCheck{Name, Body}`, `ir.ExtDomain` ExtensionKind, JSON tagged-union round-trip in `MarshalType` / `UnmarshalType`) and the loud-refusal. Negative control pinned alongside: a column referencing a `CREATE TYPE ... AS ENUM` (`pg_type.typtype == 'e'`, also `USER-DEFINED` in `information_schema`) continues to round-trip cleanly so the DOMAIN refusal doesn't over-broaden to every user-defined type and regress the v0.16.x ENUM handling. Pinned by `TestSchemaReader_DomainRefusal_Bug113` + `TestSchemaReader_DomainRefusal_NonDomainUserDefinedStillRoundTrips`.

## Compatibility

- **Patch bump (v0.95.1).** Drop-in from v0.95.0 EXCEPT for the deliberate behavior change below.
- **Behavior change:**
  - `sluice migrate` / `sluice sync start` against a Postgres source whose schema contains any column referencing a `CREATE DOMAIN` type now **refuses loudly** at schema-read with a clear actionable error. Pre-v0.95.1 the same migration silently dropped the DOMAIN's CHECK constraints on the target. Operators relying on the pre-fix silent-drop must either (a) inline the DOMAIN's CHECK constraints as table-level CHECKs on the source and ALTER the column to the underlying base type before migrating, or (b) wait for v0.95.2's round-trip carry.
  - No effect on schemas that don't use DOMAINs (the common case).

## Who needs this

- **Anyone running `sluice migrate` PG→PG against a source schema that uses `CREATE DOMAIN`** — Bug 113's silent-constraint-loss class is closed. The migration will now refuse loudly instead of silently producing a target without the operator's input-validation invariants. **Upgrade.**
- **Everyone else** — drop-in upgrade, no action needed.

## Coming next

The v0.95.x PG schema-fidelity arc continues with:

- **v0.95.2 — Bug 113 round-trip DOMAIN carry.** Reader populates `ir.Domain` from `pg_type` + `pg_constraint` joins; writer adds a Phase 1a' that emits `CREATE DOMAIN ... AS ... CHECK (...)` before any table referencing it (mirrors the existing Phase 1a ENUM-type creation). Cross-engine PG → MySQL drops the DOMAIN with a WARN and inlines the CHECK as a table-level CHECK on the base type (MySQL 8.0.16+).

After v0.95.x, **v0.96.x** covers operator-quality-of-life (Bugs 108 / 114). Open backlog post-v0.95.1: 108 / 114 = 2 (Bug 113 closed loud-refuse on this release; round-trip carry tracked for v0.95.2).
