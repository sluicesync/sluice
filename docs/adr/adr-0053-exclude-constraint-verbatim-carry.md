# ADR-0053 — EXCLUDE constraint verbatim carry (PG-only)

**Status:** **Accepted (2026-05-21).** Closes a silent fidelity loss
surfaced by the real-world corpus (GitLab `db/structure.sql`,
iteration-3 finding). Sibling to [ADR-0051](adr-0051-core-pg-type-verbatim-carry.md)
(core-PG-type verbatim carry) — same verbatim-text approach via PG's
own `pg_get_constraintdef`, same same-engine-PG-only safety boundary,
same cross-engine-MySQL loud-refuse. Different IR surface
(constraint, not type).

## Context

### What's broken

PG's `pg_constraint` system catalog stores six `contype` values:
`f` (foreign key), `c` (check), `p` (primary key), `u` (unique), `t`
(constraint trigger), and **`x` (EXCLUDE)**. Today's PG schema reader
(`internal/engines/postgres/schema_reader.go`) queries only `contype =
'f'` (foreign keys, line 1062) and `contype = 'c'` (CHECK constraints,
line 1148). The other three are recovered through different surfaces
(`p`/`u` via indexes; `t` is rarely used) — but **`x` is never
queried at all**.

Net effect: a PG → PG (or PG → backup → PG-restore) migration of a
schema with EXCLUDE constraints reads the source, builds an IR with
zero EXCLUDE constraints, and lands target tables **missing the
semantic constraint**. The operator discovers it at runtime — a row
that should have been rejected by the constraint (overlapping range,
duplicate-key-pair) gets accepted on the target. The source's
business invariant silently doesn't transfer.

### Why this is a silent-loss class, not just a "missing feature"

The CLAUDE.md zero-users tenet's central worry is silent data
corruption. An EXCLUDE constraint's job is to **prevent** specific
data states (overlapping ranges in `ci_partitions.builds_id_range`,
duplicate on-call shift assignments in
`incident_management_oncall_shifts`). Silently dropping the
constraint means the target accepts states the source rejects — a
semantic divergence the operator can't see at migration time, only at
runtime under a triggering write.

This is the *constraint-side* analogue of ADR-0051's *type-side*
range-type gap: same corpus, same root cause (the iteration-3 finding
called this out as the "adjacent" surface). ADR-0051 closed the type
gap; this ADR closes the constraint gap.

### Why the corpus harness didn't catch it

`TestCorpus_GitLab_PGToPG_VerbatimCarry` runs with `DryRun: true`.
DryRun reads the source schema and plans target DDL, but does NOT
apply that DDL or compare source vs target post-apply. So the silent
drop is invisible to the harness — sluice reads what it can read,
plans what it sees, and nothing fails. Tightening the harness leg to
post-apply assertion is part of this ADR's test scope.

## Decision

A new IR shape — `ir.ExcludeConstraint{Name, Definition}` — carries
the verbatim DDL text from PG's `pg_get_constraintdef(oid)` as opaque
text. The PG writer re-emits it inline in the CREATE TABLE body,
mirroring the existing `CheckConstraint` precedent. Cross-engine
(MySQL target) refuses loudly — no portable equivalent. Same-engine
PG → PG (and PG-backup → PG-restore via the existing ADR-0047
lineage-marker mechanism) carries the constraint with full fidelity.

### IR addition

```go
// ExcludeConstraint represents a PostgreSQL EXCLUDE constraint
// (pg_constraint.contype = 'x'). PG-only by nature — MySQL has no
// equivalent. Carried verbatim via pg_get_constraintdef as opaque
// text: same shape as ir.VerbatimType (ADR-0051), sibling tier for
// the constraint surface. Cross-engine targets refuse loudly via
// checkCrossEngineSupportable; same-engine PG → PG re-emits the
// Definition string literally. ADR-0053.
type ExcludeConstraint struct {
    // Name is the constraint name (system-generated when not
    // explicitly named at source). Preserved through DDL emit so a
    // target's pg_dump shape stays diffable against the source.
    Name string
    // Definition is the verbatim pg_get_constraintdef output for
    // this constraint, e.g.:
    //   "EXCLUDE USING gist (builds_id_range WITH &&) WHERE ((builds_id_range IS NOT NULL))"
    //   "EXCLUDE USING gist (rotation_id WITH =, tstzrange(starts_at, ends_at, '[)'::text) WITH &&)"
    //   "EXCLUDE USING gist (...) WHERE (...) DEFERRABLE INITIALLY DEFERRED"
    // Includes USING <index_method>, (col WITH op) pairs, optional
    // WHERE predicate, and DEFERRABLE modifiers — everything the PG
    // writer needs to re-emit identically. Empty Definition is a
    // sluice-bug condition (the reader should never populate the
    // slice with an empty entry); writers refuse loudly if seen.
    Definition string
}
```

Added to `Table`:

```go
// ExcludeConstraints declared on this table. PG-only — cross-engine
// targets refuse via checkCrossEngineSupportable (no MySQL equivalent).
// ADR-0053.
ExcludeConstraints []*ExcludeConstraint
```

Standard json marshalling for `Table` round-trips this naturally
(it's a concrete pointer slice; no sealed-interface envelope work);
the ADR-0049 schema-history store via `MarshalTable` / `UnmarshalTable`
inherits the new field for free.

### Reader (PG)

A new method on `SchemaReader`, `populateExcludeConstraints`, mirrors
the shape of `populateCheckConstraints` exactly. Query:

```sql
SELECT  cl.relname,
        con.conname,
        pg_catalog.pg_get_constraintdef(con.oid, true)
FROM    pg_constraint con
JOIN    pg_class      cl ON cl.oid    = con.conrelid
JOIN    pg_namespace  ns ON ns.oid    = cl.relnamespace
WHERE   ns.nspname = $1
  AND   con.contype = 'x'
  AND   cl.relname  = ANY($2)
ORDER BY cl.relname, con.conname;
```

The `true` argument to `pg_get_constraintdef(oid, pretty)` requests
the multi-line / formatted form — same as `psql \d+` shows — but
unlike `psql`'s render, the function omits the `ALTER TABLE … ADD
CONSTRAINT <name>` wrapper, returning just the constraint body. That
body inlines cleanly as `CONSTRAINT <name> <body>` in CREATE TABLE.

### Writer (PG)

`ddl_emit.go` gains an inline-emission loop alongside the existing
`CheckConstraints` emit, in `CreateTablesWithoutConstraints`:

```go
// EXCLUDE constraints (ADR-0053) emit inline like CHECKs. Each
// ir.ExcludeConstraint.Definition is the pg_get_constraintdef body
// (no ALTER TABLE wrapper); prefix with "CONSTRAINT <name>" to
// produce a valid CREATE TABLE clause.
for _, ex := range table.ExcludeConstraints {
    if ex.Definition == "" {
        return "", fmt.Errorf("postgres: ExcludeConstraint %q has empty Definition (sluice bug — reader populated incomplete state)", ex.Name)
    }
    parts = append(parts, fmt.Sprintf("CONSTRAINT %s %s",
        quoteIdent(ex.Name), ex.Definition))
}
```

Inline rather than post-create-ALTER for three reasons: (1) mirrors
the CheckConstraint precedent, (2) keeps the CREATE TABLE complete
in a single statement (no second phase to wire), (3) the constraint
references only the table's own columns and a core-PG-shipped GiST
AM — no cross-table or deferred-resolution concern. The
`CreateConstraints` phase that hosts foreign keys remains
EXCLUDE-free.

### Cross-engine refusal

`internal/pipeline/cross_engine_supportable.go` gains a per-table
check: any `len(table.ExcludeConstraints) > 0` on a non-PG target
refuses loudly with operator-actionable messaging
(`"table %q.%q carries EXCLUDE constraint %q; no MySQL equivalent
exists. EXCLUDE constraints are PG-only — use --exclude-table to skip
this table or migrate to a PG target."`). MySQL → PG is unaffected
(MySQL sources have no EXCLUDE constraints to begin with).

### Schema diff

`schema_diff.go` gains an `excludesByName` indexer + an
`AddExcludeConstraint` / `DropExcludeConstraint` delta variant pair,
parallel to the existing CheckConstraint surface. Definition equality
is byte-exact: PG's `pg_get_constraintdef` is canonicalized server-
side, so two identical constraints produce byte-identical text. Any
divergence (whitespace, predicate normalization, opclass spelling) is
treated as a real change.

### Read-time ordering and idempotency

EXCLUDE constraint reads happen in `populateExcludeConstraints` after
`populateCheckConstraints`. Both queries are independent
(table-keyed); ordering within the schema reader is for readability,
not correctness. Re-running the schema reader against an unchanged
source produces byte-identical IR (the `pg_get_constraintdef` output
is deterministic for a given pg_constraint row).

## Out of scope

- **Other PG-only constraint shapes.** DEFERRABLE/INITIALLY DEFERRED
  on CHECK or FK constraints — modelled today only for the EXCLUDE
  case's verbatim text; the broader DEFERRABLE-everywhere question is
  a separate concern. Tracked as a follow-up if surfaced by corpus.
- **Operator class catalog handling for EXCLUDE's WITH clause.** PG's
  `WITH <operator>` syntax may reference operators owned by extensions
  (e.g. `gist_int_ops` / `btree_gist`). Today's verbatim text
  approach carries the operator name as-is; if the target's matching
  extension isn't installed, PG's own CREATE refuses loudly — the
  loud-failure tenet is preserved by PG's own type system, not by
  sluice's. The Bug-47 verbatim-opclass invariant (ADR-0047) extends
  here for free.
- **MySQL → MySQL EXCLUDE-like emulation.** MySQL has no EXCLUDE
  constraint. Not in scope.
- **Optimizing inline vs post-create emit.** Inline is the chosen
  shape. If future operator workloads surface an ordering concern
  (e.g. EXCLUDE constraints referencing functions from extensions that
  aren't yet installed at CREATE TABLE time), the post-create
  `CreateConstraints` phase is the obvious migration target — but
  speculative until that workload exists.

## Consequences

### Positive

- **Closes a silent fidelity-loss class.** PG → PG and
  PG-backup → PG-restore preserve EXCLUDE constraints with byte-exact
  fidelity. The semantic invariants (no overlapping ranges, etc.)
  transfer cleanly to the target.
- **MySQL targets refuse loudly** instead of silently dropping the
  constraint. Operators discover at migration-plan time, not at
  runtime under a triggering write.
- **Mirrors ADR-0051's pattern** — single verbatim Definition string,
  no internal constraint AST, no per-component round-trip
  vulnerability. Reviewer can audit the verbatim path identically to
  ADR-0051's range-type carry.
- **Corpus harness tightening** — the GitLab leg gains a post-apply
  assertion that `pg_constraint` on the target has the same EXCLUDE
  count + Definition set as the source, closing the DryRun blindness.

### Negative / load-bearing

- **PG-only by definition.** Operators with a multi-source workload
  using EXCLUDE on the PG side and replicating to MySQL must scope
  the EXCLUDE-bearing tables out (`--exclude-table`) or accept the
  loud refusal. No translation path possible; cross-engine refusal
  is the floor.
- **Verbatim text is PG-version-stable but not PG-version-portable.**
  `pg_get_constraintdef`'s output is stable within a PG major version;
  PG-major upgrades on the source between snapshot and resume could
  theoretically change the canonical form, producing spurious diffs.
  Acceptable risk — same as ADR-0051's range-type text-IO; document
  the same-PG-major-version expectation.
- **No structural diffing.** Two functionally-equivalent EXCLUDE
  constraints with different text forms (e.g. `WHERE (x IS NOT NULL)`
  vs `WHERE ((x IS NOT NULL))`) would be treated as different by
  Definition-equality diff. PG's canonicalizer normalizes most cases,
  but a hand-edited target whose `pg_get_constraintdef` output
  differs from the source's would surface as a diff. Acceptable — the
  shipped behaviour is "ship source's canonical form" and operators
  who hand-edit are intentionally diverging.

### Test matrix

**Unit tests** (PG engine package):
- `populateExcludeConstraints` populates correctly from a fixture.
- Empty `Definition` in IR → writer refuses with loud sluice-bug error.
- `MarshalTable` / `UnmarshalTable` round-trip a Table with EXCLUDE
  constraints (the ADR-0049 schema-history store path).

**Integration tests** (PG-only, build-tagged):
- PG → PG migrate of a table with the four observed shapes from the
  GitLab corpus (simple, predicated, multi-key, DEFERRABLE) lands
  with byte-identical `pg_get_constraintdef` on the target.
- PG → MySQL migrate of the same table refuses loudly at
  pre-flight with the operator-actionable message.

**Corpus harness** (tightening):
- `TestCorpus_GitLab_PGToPG_VerbatimCarry` updated to APPLY (not
  DryRun:true) the target schema OR to read the planned target DDL
  back through the PG reader and assert EXCLUDE constraint count
  parity. The exact form depends on harness mechanics — DryRun is
  load-bearing today for the GitLab test's runtime, so the verifier
  may need to be a separate, scope-narrowed assertion on a small
  EXCLUDE-bearing fixture rather than the full GitLab corpus.

**Schema diff**:
- Add EXCLUDE constraint to source → diff detects `AddExcludeConstraint`.
- Drop from source → `DropExcludeConstraint`.
- Definition byte-change → `DropExcludeConstraint` + `AddExcludeConstraint`
  (parallels CheckConstraint's edit-as-drop-then-add shape).

## References

- [ADR-0051](adr-0051-core-pg-type-verbatim-carry.md) — sibling tier
  for the type-side verbatim carry; same `pg_get_constraintdef`-style
  text-IO pattern, same same-engine-PG safety boundary.
- [ADR-0047](adr-0047-verbatim-extension-passthrough.md) — the
  USER-DEFINED uncatalogued type verbatim tier whose lineage-marker
  mechanism this ADR's PG-backup → PG-restore inherits.
- `docs/dev/notes/real-world-corpus-findings.md` §"Iteration 3" — the
  GitLab finding that surfaces the four observed EXCLUDE shapes.
- `internal/engines/postgres/schema_reader.go::populateCheckConstraints`
  — the existing constraint-populator the new
  `populateExcludeConstraints` mirrors.
- PG docs:
  [pg_constraint](https://www.postgresql.org/docs/current/catalog-pg-constraint.html),
  [CREATE TABLE EXCLUDE](https://www.postgresql.org/docs/current/sql-createtable.html),
  [pg_get_constraintdef](https://www.postgresql.org/docs/current/functions-info.html).
