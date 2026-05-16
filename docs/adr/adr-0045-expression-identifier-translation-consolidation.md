# ADR-0045: expression-identifier-translation consolidation

## Status

**Accepted (2026-05-16).** Design signed off; implementation pending,
shipping as **v0.66.0**. Closes the recurring "identifier references
inside opaque dialect-tagged expression strings" defect class (4
confirmed members: v0.65.0 #5, Bug 61, Bug 63, Bug 64) plus the
orthogonal Bug 65 (PG-reader expression-index silent drop). Sign-off
decisions locked: **D2 = MySQL-INDEX cross-dialect brought into the
uniform translate+requote** (closes the latent PG-source-functional-
index→MySQL untranslated gap; a deliberate behaviour change to a
historically requote-only path). **D3 = Bug 65 full-carry** — the PG
reader surfaces expression indexes into the IR (true fidelity,
symmetric with the MySQL-source post-Bug-16 path), not merely a
loud-fail.

## Context

The IR carries four expression positions as **opaque source-dialect
strings** with a dialect tag, never a parsed AST:

- `ir.Column.GeneratedExpr` + `GeneratedExprDialect` (schema.go:161/179)
- `ir.CheckConstraint.Expr` + `ExprDialect` (schema.go:118-143)
- `ir.IndexColumn.Expression` + `ExpressionDialect` (schema.go:327-363)
- `ir.DefaultExpression.Expr` + `Dialect` (schema.go:234-250)

Each target writer must, on the cross-dialect path: strip the source
engine's identifier quoting (done at the *reader* boundary for IR
portability — MySQL backticks can't survive into PG), translate
operator/function spellings (ADR-0016 `translateExprFor{PG,MySQL}`),
and **re-quote identifiers that are reserved in the target** (the
reader strip is lossy for reserved-word column names). This three-leg
policy is ADR-0016's.

**The class has been fixed reactively, one cell of the matrix at a
time** (4 positions × 2 writers × 2 directions): v0.65.0 #5 added the
MySQL-writer requote for generated/CHECK; Bug 61 fixed the PG *reader*
multi-arg cast strip; Bug 63 added the PG-writer requote for
generated/CHECK/index; Bug 64 is the still-open PG- *and* MySQL-writer
DEFAULT cell. The v0.65.2 cycle's recon mapped the whole matrix and
proved the wiring is **inconsistent per cell**:

| Position | PG writer (MySQL→PG) | MySQL writer (PG→MySQL) |
|---|---|---|
| Generated | translate + requote ✓ | translate + requote ✓ |
| CHECK | translate + requote ✓ | translate + requote ✓ |
| Index | translate + requote ✓ | **requote only, NO translate** |
| **DEFAULT** | **translate, NO requote** (Bug 64) | **3-entry lookup only — NO translate, NO requote** (Bug 64, broader) |

Two more facts from the recon:

- The low-level scan primitives `scanStringLiteral`,
  `isIdentifierByte`, `scanParenGroup`, `splitTopLevelArgs` are
  **byte-identical** duplicated in `internal/engines/mysql/expr_walk.go`
  and `internal/engines/postgres/expr_walk.go` (pure duplication).
- The two requote helpers (`requoteMySQLReservedIdents`,
  `requotePGReservedIdents`) diverge **only** in: quote char
  (`` ` `` vs `"`), the reserved-word set, the grammar-keyword
  exclusion set, and a PG-only "skip whitespace before `(`" tweak.
  The *mechanism* (literal-aware byte walk, function-call exclusion,
  numeric/keyword exclusion) is otherwise identical.

A 5th point-fix for Bug 64 would perpetuate exactly the per-cell
duplication that has produced four bugs. Per the clean-code tenet
("named concepts over scattered conditionals; a recurring wart gets
a named, tested mechanism") and validate-before-building, this is a
consolidation, not a patch.

**Bug 65 (orthogonal sibling).** The PG schema reader's
`populateIndexes` (schema_reader.go:656-796) does not query
`pg_index.indexprs`; expression-only index entries (`indkey` sentinel
`0`, empty `attname`) hit `if colName == "" { continue }`
(lines 707-710) and are **silently dropped**. The MySQL reader DOES
carry expression indexes (`information_schema.statistics.expression`,
schema_reader.go:339, post-Bug-16). Silent schema loss violates the
loud-failure tenet. Same family of symptom ("expression fidelity"),
different locus (reader, not writer) — folded here as a sibling.

## Decision

### 1. One shared, engine-parameterized requote mechanism

New shared location (proposed: `internal/translate/exprident`, importable
by both engine packages without an engine→engine import cycle):

- Move the byte-identical scan primitives there **unchanged**
  (`scanStringLiteral`, `isIdentifierByte`, `scanParenGroup`,
  `splitTopLevelArgs`); delete the per-engine copies; engines import
  the shared ones. This is a behavior-preserving move (verified by
  the existing per-engine unit tests, which move with them).
- One `RequoteIdentifiers(expr string, cfg Config) string` where
  `Config{ QuoteByte byte; Reserved map[string]struct{};
  GrammarExclusions map[string]struct{}; SkipWSBeforeParen bool }`.
  The two existing helpers become thin wrappers that pass the
  engine's (reserved set, grammar set, quote byte, ws flag) — the
  **reserved/grammar sets stay per-engine** (they are dialect
  definitions, correctly engine-owned; ~500 lines that must not be
  merged).

### 2. Uniform cross-dialect composition at all four emit sites × both writers

Define one canonical cross-dialect emit composition and apply it at
**all four** positions in **both** `ddl_emit.go` files:

```
cross-dialect:  RequoteIdentifiers( translateExprFor<TARGET>( exprText ) )
same-dialect:   <unchanged per-position short-circuit>
```

- **Bug 64 fix (headline), done via the mechanism, not a point-patch:**
  - PG writer `translateDefaultExpr` cross-dialect arm
    (ddl_emit.go:291) gains the `requotePGReservedIdents` wrap it
    currently lacks.
  - MySQL writer `emitDefault` `DefaultExpression` cross-dialect arm
    (ddl_emit.go:359-374) is rerouted from the 3-entry
    `pgToMySQLDefaultExpr` lookup to
    `requoteMySQLReservedIdents(translateExprForMySQL(expr))`, with
    the existing lookup folded in as translator entries (preserve
    `now()`/`gen_random_uuid()`/`random()` outcomes; the bit-literal
    `bitLiteralDialect` special case stays).
- **D2 — MySQL INDEX cross-dialect (decision required, see below):**
  the index cell is requote-only with **no** `translateExprForMySQL`
  (an intentional historical scoping). The consolidation's default
  is to make it consistent (translate+requote like the other three);
  flagged for sign-off because it is a deliberate behavior change.
- **Same-dialect short-circuits are preserved byte-identical**,
  including the asymmetry that the **MySQL writer requotes even on
  the same-dialect path** (because the MySQL *reader* strips
  backticks for IR portability, so a same-engine MySQL→MySQL
  generated/CHECK col still needs requoting) whereas the PG
  same-dialect path is verbatim (the PG reader's `pg_get_expr`
  already quotes). This asymmetry is real, correct, and documented —
  not "cleaned up".

### 3. Bug 65 — PG reader surfaces expression indexes (sibling)

PG `populateIndexes` extends its query to pull `pg_get_expr(
ix.indexprs, ix.indrelid)` and, for `indkey` expression sentinels,
populate `ir.IndexColumn.Expression` + `ExpressionDialect="postgres"`
instead of `continue`-dropping. This is the PG-source analogue of
the MySQL-source post-Bug-16 behavior; the IR + both writers already
handle index expressions, so this is additive (no IR change). **D3
decision required:** full-carry (recommended — true fidelity,
symmetric with MySQL source, round-trips) vs. minimum loud-fail
(surface "PG expression index unsupported" rather than silent drop).
Recommended: full-carry; it is barely more work than a loud refusal
and removes the asymmetry entirely.

### 4. Proactive corpus sweep

One table-driven unit test over the unified `RequoteIdentifiers`
(reserved/non-reserved/function/operator/keyword/numeric/string-
literal/already-quoted, per engine) **plus** one integration test
that drives a reserved-word-named column (`order`, `user`, `table`,
`column`, and a non-reserved control like `key` for PG / `name` for
MySQL) through **all four positions × both directions** and asserts
migrate success + correct semantics. A 5th cousin cannot appear
without this sweep failing.

### What does not change

- IR shape/contract (all four expression fields already exist;
  Bug 65 only *populates* an existing field for PG source).
- ADR-0016's three-leg policy and the "translate only when
  mechanically derivable, else verbatim → loud target error" tenet
  (we are making the policy *uniformly applied*, not changing it).
- Same-dialect behavior at any position (byte-identical short-circuits).
- The per-engine reserved-word / grammar-keyword sets and quote char
  (dialect definitions; stay engine-owned).
- No CLI/flag/state-format change.

## Gotchas

- **The scan-primitive move must be behavior-preserving.** Move
  verbatim; the existing `expr_walk` callers (e.g. Bug 61's
  `topLevelCastIndex`, `splitTopLevelArgs` users) must keep working
  unchanged. Run the full suite before/after the move as a no-op
  proof.
- **Don't over-translate DEFAULT.** ADR-0016 tenet holds: a default
  expression sluice can't mechanically translate must fall through
  verbatim and let the target parser fail loudly — never guess. The
  Bug 64 fix adds requote + the existing translator; it does not
  invent new default translations beyond folding the 3-entry lookup.
- **MySQL same-dialect requote asymmetry is load-bearing** — a test
  must pin that MySQL→MySQL generated/CHECK reserved-word columns
  still requote (don't "simplify" the short-circuit).
- **Bug 65 + opclass interaction:** an expression index can also
  carry an operator class (Bug 47 / ADR-0032 territory). Populating
  `Expression` must not regress the existing opclass capture; test
  an expression index with a `gin_trgm_ops`-style opclass.
- `gofumpt`/lint: shared package needs its own clean vet; watch the
  `unused` checker when the per-engine primitive copies are deleted.

## Testing

- Behavior-preserving-move proof: full `go test ./...` green before
  and after the primitive relocation, no diff in test outcomes.
- Unified `RequoteIdentifiers` table test (both engine configs).
- 4×2×2 cross-dialect integration sweep (the proactive corpus).
- Bug 64 explicit: MySQL→PG and PG→MySQL DEFAULT with a
  reserved-word column ref + a non-reserved control → migrate
  succeeds, default value correct on a bare INSERT.
- Bug 65 explicit: PG-source expression index (`((lower(name)))`
  and a reserved-word one) → carried to MySQL + PG targets, index
  present and functional; opclass-bearing expression index not
  regressed.
- Regression pins (must stay green, name them): v0.65.0 #5/#6,
  Bug 61 (`stripTypeCast`), Bug 62 (`ir.Bit`), Bug 63
  (gen/CHECK/index PG requote), ADR-0044 Tier-3 guards, same-engine
  PG→PG + MySQL→MySQL generated/CHECK byte-identical.

## Sizing

~600–900 LOC net (a shared package + 8 emit-site rewirings, several
of which shrink; the Bug 65 reader change; the move *deletes*
duplicated primitives so net is smaller than a naive estimate) +
~400–600 LOC tests (the 4×2×2 sweep is the bulk). One focused
release. Replaces ≥4 historical point-fixes with one named mechanism;
forecloses the 5th. Likely **v0.66.0** (minor — substantial
mechanism + the D2 MySQL-INDEX behavior change + Bug 65 fidelity
gain; no API/IR-contract break).

## References

- ADR-0016 — layered expression translation (the three-leg policy
  this makes uniform).
- v0.65.0 #5, Bug 61 (v0.65.1), Bug 63 (v0.65.2) — the reactive
  point-fixes this consolidates; Bug 64, Bug 65 — open, closed here.
- ADR-0032 / Bug 47 — opclass capture (Bug 65 interaction).
- Bug 16 (MySQL-source expression indexes, v0.9.1) — the symmetric
  precedent for Bug 65's PG-source fix.
