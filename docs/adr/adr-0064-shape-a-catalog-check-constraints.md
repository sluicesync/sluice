# ADR-0064 — Shape A recognized-shape catalog: CHECK constraint changes

**Status:** **Accepted (2026-05-26).** Extends ADR-0054's recognized-shape
catalog (DP-E) with `ShapeKindAddCheck`, `ShapeKindDropCheck`, and
`ShapeKindModifyCheck`. Closes the first of the two `ShapeKindUnrecognized`
sub-shapes the ADR-0054 v0.78.0 amendment explicitly named as v1-deferred
(the second — generated-column expression changes — stays deferred as
task #22's other sub-task; it touches the ADR-0016 expression-translator
surface and would balloon scope).

## Context

ADR-0054 v0.78.0 (RENAME COLUMN sub-task) explicitly listed CHECK
constraint changes as a known follow-up to the v1 catalog:

> "The two remaining task #22 sub-shapes (CHECK constraint changes,
> generated-column changes) still surface as `Unrecognized` with the
> operator-actionable drained-model recovery hint."

Schema evolution in real-world fleets routinely includes constraint
changes — `ALTER TABLE … ADD CONSTRAINT … CHECK (…)`, `DROP CONSTRAINT`,
or expression rewrites — and forcing operators into the drained-model
recovery path for every such change is operationally heavy on N-shard
fleets where DDL coordination is the entire reason ADR-0054 Phase 2
exists. Promoting CHECK to recognized status closes that gap while
preserving the loud-failure tenet for the genuinely-translation-hostile
expressions.

## Decision

The classifier (`pipeline.ClassifyShape`) now recognises three CHECK
sub-shapes via IR-level delta on `ir.Table.CheckConstraints`:

- **`ShapeKindAddCheck`** — exactly one or more named CHECKs appear in
  post that are absent from pre. The shape carries the added constraints
  verbatim (Name, Expr, ExprDialect).
- **`ShapeKindDropCheck`** — exactly one or more named CHECKs from pre
  are absent in post. The shape carries the dropped constraint names so
  the applier can emit `DROP CONSTRAINT <name>`.
- **`ShapeKindModifyCheck`** — exactly one same-named CHECK exists in
  both pre and post but its expression text differs. The shape carries
  the pre-state and post-state constraints; the applier emits as
  DROP + ADD (the only portable approach across PG / MySQL — neither
  engine supports an in-place `ALTER CONSTRAINT … CHECK` expression
  rewrite without dropping and re-adding).

The classifier consumes `Table.CheckConstraints` (already-existing IR
field) and matches by `Name`. Unnamed CHECKs are skipped (the catalog
requires named constraints — the same policy `diffIndexes` applies). A
multi-class boundary that mixes CHECK with any other delta class
remains a combo refusal (loud-failure tenet).

### Engine-side per-shape DDL

`ir.ShapeDeltaApplier` gains three methods (additive on the interface,
so engines that don't implement Shape A live-coordination continue to
inherit the no-op fallback):

- `AlterAddCheck(ctx, table, checks []*ir.CheckConstraint) error` —
  emits `ALTER TABLE <t> ADD CONSTRAINT <name> CHECK (<expr>)` per
  constraint; idempotent on the post-state via detect-then-emit on
  `pg_catalog.pg_constraint` (PG) or `information_schema.CHECK_CONSTRAINTS`
  (MySQL).
- `AlterDropCheck(ctx, table, checks []*ir.CheckConstraint) error` —
  emits `ALTER TABLE <t> DROP CONSTRAINT [IF EXISTS] <name>` per
  constraint; idempotent on the post-state.
- `AlterModifyCheck(ctx, table, oldConstraint, newConstraint *ir.CheckConstraint) error` —
  emits DROP + ADD in sequence. Not transactional on MySQL (catalog
  changes auto-commit per statement); the recovery path is the same
  probe-and-record loop the rest of the v1 catalog already runs through
  on takeover.

### Probe surface

`ir.ShardConsolidationProber` gains:

- `ProbeAddCheck(ctx, table, checks) → (ProbeOutcome, error)` — Applied
  when ALL named constraints exist on the target; NotApplied when NONE;
  Inconsistent on partial.
- `ProbeDropCheck(ctx, table, checks) → (ProbeOutcome, error)` —
  inverts ProbeAddCheck.
- `ProbeModifyCheck(ctx, table, oldName, newConstraint) → (ProbeOutcome, error)` —
  Applied when oldName is absent AND newConstraint.Name is present (the
  DROP+ADD landed); NotApplied when oldName is present AND
  newConstraint.Name is absent (the DROP didn't fire); Inconsistent
  otherwise (both present / both absent / newConstraint exists with the
  wrong expression).

The Inconsistent-on-wrong-expression arm closes the same silent-
divergence class the v0.76.0 `ProbeAlterColumnType` v2 chases on the
type-alter shape: a DROP + ADD where the added constraint has a
different expression than recorded must NOT pass the existence check.
The probe reads the catalog's normalized expression text and compares
against the recorded `Expr` after the same dialect-normalization the
emit-path uses.

### Cross-engine semantics — verbatim carry + refuse-loudly

CHECK expressions are largely SQL-standard but engine-specific dialect
creeps in for the common cases: `JSON_EXTRACT(payload, '$.k')`
(MySQL) vs `payload->>'k'` (PG); `IF(a, b, c)` (MySQL) vs
`CASE WHEN a THEN b ELSE c END` (PG); function-name spellings; cast
syntax. The v1 policy mirrors ADR-0053's EXCLUDE-constraint verbatim
carry:

- Same-dialect emit → pass through with only the read-boundary
  identifier-quoting normalization the existing
  `translateCheckExpr` already runs.
- Cross-dialect emit (PG-source-tagged Expr against a MySQL target, or
  vice versa) → run the existing ADR-0016 / ADR-0045 expression
  translator at the writer boundary. If the translator produces an
  expression containing engine-specific tokens that the target cannot
  parse, the target's DDL parse fails loudly and the error propagates
  through the applier with the standard drained-model recovery hint.
- Operator-supplied translation via `--expr-override` (per ADR-0016) is
  the escape hatch for the rare expressions the translator cannot
  handle. The override mechanism is already wired for generated-column
  expressions; CHECK piggybacks on the same surface (the override key is
  the constraint name).

The CHECK applier additionally runs a **pre-flight refuse-loudly check**
on cross-dialect cases: if the post-translation expression contains a
well-known list of untranslated MySQL→PG / PG→MySQL tokens
(`json_extract`, `->>`, `->`, `IF(`, `CASE` keyword swap edges), the
applier refuses BEFORE issuing the SQL with a structured error that
names the unrecognized clause and the override flag. Refusing
pre-emit (rather than after a partial DROP+ADD on the MODIFY path)
preserves the target's existing state — the operator can recover via
the drained model without first reconstructing a half-modified
constraint.

### What's deferred

- **CHECK constraint name-only rename**
  (`ALTER TABLE … RENAME CONSTRAINT old TO new`). At the IR level a
  rename is byte-identical to a DROP + ADD with the same expression,
  and v1 treats it that way (analogous to the RENAME COLUMN
  drop+add-equals-rename equivalence ADR-0054 v0.78.0 documents). A
  future ADR could split rename out as its own sub-shape if operator
  feedback indicates a need.

- **NOT VALID / not-yet-validated CHECKs (PG-only)**. PG supports
  `ALTER TABLE … ADD CONSTRAINT … CHECK (…) NOT VALID` for adding
  constraints without scanning existing rows. The v1 IR carries no
  `NotValid` field on `ir.CheckConstraint`; v1 always emits the
  validating form. Operators who need NOT VALID for large-table
  rollouts use the drained model (or `--no-coordinate-live-ddl`) and
  apply the NOT VALID variant manually. A future iteration could
  extend `ir.CheckConstraint` with a `NotValid bool` and surface
  through the apply path.

- **Detection through CDC**. ADR-0049's `SchemaSignature` deliberately
  excludes constraints (the comment: *"a change confined to them is not
  a decode-affecting delta"*), so a constraint-only change on the
  source does NOT trigger a `SchemaSnapshot` on the CDC stream. The
  classifier-side support this ADR ships is therefore useful primarily
  for:
  1. Cold-start chain-restore where the full IR is computed from
     SchemaReader output on both sides (constraints present).
  2. A future operator-issued DDL subcommand
     (`sluice schema migrate-shape-a`) that bypasses CDC.
  3. CDC paths once a follow-up widens `SchemaSignature` to include
     CheckConstraints or adds a constraint-only boundary trigger.

  This ADR does not widen `SchemaSignature`; doing so would touch the
  ADR-0049 contract and warrants its own design dialogue (the cost is
  one extra schema-history version per constraint-only change — small
  but non-zero, and the retention-∝-DDL-count invariant deserves
  explicit re-signoff). For now, operators issuing constraint-only
  changes on the source while live-coordination is engaged will see
  the change land via the cold-start side and the CDC stream's row
  apply path will simply observe whatever post-DDL row layout the
  CHECK admits or refuses (PG / MySQL surface the constraint
  violation at INSERT/UPDATE time, loud-failure-by-default).

- **Generated-column expression changes** (the other deferred sub-shape
  the ADR-0054 v0.78.0 amendment named). Touches the
  ADR-0016 expression-translator surface in a different shape (a
  generated column's expression is part of `ir.Column`, not a
  separate constraint slice) and needs its own equality lens — out of
  this PR's scope.

### Test strategy (Bug 74 class-pin matrix)

Per CLAUDE.md's "pin the class, not the representative" rule, the
classifier's `{ADD, DROP, MODIFY}` × `{same-engine, cross-engine}` ×
`{simple-arithmetic, JSON, datetime}` axes are pinned at unit-test
granularity (classifier only — no DB round-trip) AND at integration
granularity (full classify → apply → probe loop, real PG 16 + MySQL
8.0 testcontainers).

| Shape | Engine | Expression family | Pin location |
|---|---|---|---|
| ADD CHECK | same (PG→PG) | simple arithmetic (`qty >= 0`) | unit + integration |
| ADD CHECK | same (PG→PG) | JSON (`(payload->>'k') = 'v'`) | unit + integration |
| ADD CHECK | same (PG→PG) | datetime (`start_date <= end_date`) | unit + integration |
| ADD CHECK | same (MySQL→MySQL) | simple arithmetic | unit + integration |
| ADD CHECK | same (MySQL→MySQL) | JSON (`JSON_EXTRACT(...)`) | unit + integration |
| ADD CHECK | same (MySQL→MySQL) | datetime | unit + integration |
| DROP CHECK | same (both) | n/a (DROP is name-only) | unit + integration |
| MODIFY CHECK | same (both) | each family | unit + integration |
| ADD CHECK (JSON) | cross (PG→MySQL) | JSON | integration — refuse loudly |
| ADD CHECK (JSON) | cross (MySQL→PG) | JSON | integration — refuse loudly |

The cross-engine refusal pins prove the safety floor: a CHECK with a
JSON expression in one dialect refuses loudly on the other rather than
emitting potentially-corrupt SQL. The matrix's two cross-engine cells
are deliberately the most-likely-to-bite case (JSON), not the safe
case (simple arithmetic) — the safe case would silently pass through
the translator and prove nothing about the refuse-loudly path.

## References

- [ADR-0054](adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md) —
  the recognized-shape catalog framework this ADR extends. DP-E names
  the v1 shapes; the v0.78.0 amendment explicitly names CHECK as a
  v1-deferred sub-task this ADR closes.
- [ADR-0053](adr-0053-exclude-constraint-verbatim-carry.md) — sibling
  verbatim-carry policy for EXCLUDE constraints (PG-only). The
  cross-engine refuse-loudly shape this ADR adopts for CHECK is
  modelled on ADR-0053's policy.
- [ADR-0049](adr-0049-cdc-schema-history.md) — the SchemaSignature
  contract the "detection through CDC" deferral references.
- [ADR-0016](adr-0016-expression-translation-policy.md) — the layered
  expression-translation policy this ADR's cross-dialect emit path
  reuses.
- [ADR-0045](adr-0045-expression-identifier-translation-consolidation.md) —
  the consolidated requote+translate composition the cross-dialect
  emit path lands on.
- Task #22 in the project backlog — "extend the recognized-shape catalog";
  this ADR closes one of the two named sub-shapes (CHECK); the other
  (generated-column expression changes) remains a separate sub-task.
