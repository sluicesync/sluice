# ADR-0091 — Default-on schema-change forwarding for the single-stream CDC path

## Status

**Proposed (2026-06-14).** Extends ADR-0058 (online ADD COLUMN
forwarding) from an opt-in, ADD-COLUMN-only intercept to a
**default-on, all-unambiguous-shapes** forwarding path on the
single-stream (non-Shape-A) CDC apply loop, controlled by a new
tristate `--schema-changes=forward|refuse` flag (default `forward`).

This **reverses ADR-0058 §1a's deliberate scope split** (which
forwarded ADD COLUMN only and refused DROP / ALTER TYPE / RENAME /
index / CHECK loudly). The reversal is intentional and operator-
driven; the *why* is in §1 below.

Codename F7. Shipped in two parts:

- **F7a (this ADR):** the tristate flag + default flip + forwarding
  of every *unambiguous* shape — ADD / DROP / ALTER TYPE / ALTER
  NULLABILITY / CREATE INDEX / DROP INDEX / ADD CHECK / DROP CHECK /
  MODIFY CHECK. RENAME COLUMN refuses loudly on **both** engines (see
  §3 — it is the one ambiguous shape and needs a stable-identity
  proof neither engine's CDC projection carries yet).
- **F7b (follow-up ADR):** PG attnum-proven RENAME forwarding;
  MySQL RENAME stays refuse (no stable column id).

## Context

### What already exists (the ground truth)

The machinery this ADR needs is **already built and exercised** — by
the Shape A multi-shard path (ADR-0054):

- `ir.ShapeDeltaApplier` (`internal/ir/interfaces.go`) implements, on
  **both** engines, `AlterAddColumn`, `AlterDropColumn`,
  `AlterColumnType`, `AlterColumnNullability`, `AlterRenameColumn`,
  `CreateShapeIndex`, `DropShapeIndex`, `AlterAddCheck`,
  `AlterDropCheck`, `AlterModifyCheck`. Each method is idempotent on
  the post-state (IF [NOT] EXISTS / detect-then-emit) and is
  catalog-only DDL (never touches row data).
- `pipeline.ClassifyShape(pre, post)` already classifies every shape
  from the IR delta, including the single-column RENAME heuristic
  (one drop + one add, type-compatible) and the multi-shape combo →
  `ShapeKindUnrecognized` refusal.
- `pipeline.BoundaryRouter.applyShape` already dispatches each
  `Shape.Kind` to the matching `ShapeDeltaApplier` method on the
  consolidated target — the proven per-shape apply path.

ADR-0058's single-stream intercept (`schema_forward_intercept.go`)
reuses `ClassifyShape` and the ADD-COLUMN branch of that catalog, but
deliberately **refuses every other recognized shape**
(`refuseShapeOutOfV1Scope`). So F7a is not new DDL-emission code: it
is wiring the single-stream intercept to the *same* `applyShape`
dispatch Shape A already uses, plus the default flip and the
cross-engine retarget/scrub the single-stream path needs (which Shape
A's manifest-derived tables don't).

### Why reverse ADR-0058 §1a

ADR-0058 §1a refused DROP / ALTER TYPE / RENAME on the single-stream
path for principled reasons: DROP is destructive, ALTER TYPE has
cross-engine translation hazards, the right place to confirm a
destructive schema change is the operator's explicit `schema migrate`
run. That reasoning treats **loud-refuse as the safe default**.

Operational experience (the PlanetScale long-haul soak, 2026-06-13/14)
inverts the cost calculus:

- **Loud-refuse does not prevent the schema change — it just stops the
  sync.** When a source DROP/ALTER lands, the operator's intent is
  already expressed; sluice refusing only forces them to drain, run
  the DDL on the target by hand, and resume. The schema change happens
  regardless; refuse-loudly buys nothing but downtime.
- **A wedged sync is itself a trust-eroding failure.** The soak's
  non-propagated ADD COLUMN produced a supervisor tight-restart
  crash-loop (NRestarts in the thousands) until the column was added
  on the target by hand. Keeping the sync online through a routine,
  reversible schema change is worth more to operators than the
  conservatism of refuse-loudly.
- **The forwarded DDL is exactly what the operator already did on the
  source.** Forwarding `ALTER TABLE … DROP COLUMN x` to the target is
  carrying the operator's own committed decision forward — the same
  decision sluice carries forward for every row INSERT/UPDATE/DELETE.
  Treating DML as default-forward but DDL as default-refuse is an
  inconsistency operators do not expect.

So the default flips: **keep the sync online by forwarding the
operator's schema changes; let operators who want explicit-only
schema control opt into refuse via `--schema-changes=refuse`.**

This is a behavior change on upgrade (a stream that previously refused
on source DDL now forwards it). It is called out loudly in the release
notes and the flag help; it is the deliberate, operator-requested
tradeoff (uptime > conservative refuse).

## Decision

### 1. Tristate `--schema-changes=forward|refuse`, default `forward`

The new flag is the single control for single-stream schema-change
behavior:

- `--schema-changes=forward` (**default**): forward every unambiguous
  recognized shape to the target via `ShapeDeltaApplier`; log every
  applied DDL at INFO. Refuse loudly only on the cases in §2.
- `--schema-changes=refuse`: the pre-F7 / ADR-0058-flag-off behavior —
  any source DDL surfaces as a loud refuse with the drained-model
  recovery hint. For operators who gate DDL through a separate
  change-management process.

A two-state enum (not a three-state `add-only|forward|refuse`) is
chosen deliberately: the ADD-only mode ADR-0058 shipped was a
scope-limitation artifact, not an operator-desired policy. The
forward-vs-refuse axis is the real decision. A future `add-only` (or
finer per-shape selection) can be added to the enum without breaking
the two existing values if demand surfaces.

#### 1a. `--forward-schema-add-column` is deprecated

The ADR-0058 boolean is kept as a recognized flag for one deprecation
cycle but is now subsumed: with forwarding on by default, setting it
is redundant. When set, the streamer logs a deprecation WARN naming
`--schema-changes` as the replacement and proceeds (forwarding is on
regardless). `--backfill-added-column` is unchanged — it remains a
meaningful modifier of the ADD-COLUMN forward path (source-side
backfill of already-shipped rows).

### 2. Refuse-loudly catalog (the only cases that stop the sync)

Under `--schema-changes=forward`, the sync still refuses loudly on:

- **Multi-shape combo** (`ShapeKindUnrecognized`) — more than one
  structural change in a single boundary. The IR delta cannot
  unambiguously order/separate them; the operator coordinates via the
  drained model.
- **RENAME COLUMN** (`ShapeKindRenameColumn`) — the one ambiguous
  single shape; see §3.
- **ADD COLUMN with a volatile/stateful computed DEFAULT** (ADR-0058
  §2a, Bug 90) — `NOW()` / `nextval` / `random` / unknown function.
  Target-session evaluation diverges from the source's per-row insert
  values. Refuse-on-uncertainty is preserved verbatim from ADR-0058.
- **Target DDL apply fails** — lock contention, permission denied,
  unrecognized type for the target dialect. Refuse loudly, do not
  advance position; retry replays the boundary (DDL is idempotent).

Each refusal names the table, the shape, the per-change drift
(ADR-0060 / F11 rendering), and the drained-model recovery hint —
unchanged from ADR-0058's loud-failure contract.

### 3. RENAME COLUMN refuses loudly on both engines (F7a); the
data-loss reasoning

A column RENAME and a `DROP x + ADD y (same type)` are
**indistinguishable from the IR delta alone**: both present as exactly
one dropped column and one added column with compatible types.
`ClassifyShape`'s `diffRenameColumn` heuristic guesses RENAME for that
pattern, but the guess is unsafe in both directions:

- Truth = DROP+ADD, guess = RENAME → `AlterRenameColumn` keeps the old
  column's data under the new name; the target silently diverges from
  the source (where the new column is fresh and the old data is gone).
- Truth = RENAME, guess = DROP+ADD → the old column's data is
  **dropped** on the target and the new column is empty. Silent data
  loss.

The only safe disambiguation is a **stable column identity** that
survives a rename:

- **Postgres** has it: `pg_attribute.attnum` is stable across RENAME.
  Same attnum + different name = proven RENAME; different attnum =
  proven DROP+ADD. This makes PG RENAME forwarding *safe* — but it
  requires the PG CDC schema projection to carry attnum into the IR,
  which it does **not** today (`attnum` is read only for index/
  constraint column resolution, never as a per-column identity).
- **MySQL** has no equivalent. `INFORMATION_SCHEMA.COLUMNS` exposes
  `ORDINAL_POSITION` (changes on reorder, not stable) and no creation
  id. A MySQL RENAME is **fundamentally unprovable** from catalog
  state; the heuristic is the only signal, and it is not safe enough
  to drive a destructive auto-DROP.

Therefore F7a refuses RENAME loudly on **both** engines: the safe PG
path needs attnum plumbing not yet built, and the MySQL path is
unprovable in principle. The refuse message explains the ambiguity and
the data-loss risk explicitly (not the generic "out of scope" wording)
and points to drained recovery.

**F7b** (separate ADR) adds the PG attnum-into-IR plumbing and an
attnum-aware rename classifier, at which point PG RENAME forwards
safely; MySQL RENAME stays refuse permanently (the operator drains and
renames on both ends explicitly — the one shape where explicit
coordination is genuinely required, not merely conservative).

### 4. REORDER is a no-op (name-based decode)

A pure column reorder (same column set, different ordinal positions)
classifies as `ShapeKindNone` — there is no IR-structural delta,
because the IR column set is identical. sluice's apply path decodes
rows **by column name**, never by position, so the target's physical
column order is irrelevant to correctness. No DDL is emitted; the
SchemaSnapshot forwards so the ADR-0049 schema-history row records.
(Positional decode would be a silent-corruption hazard; name-based
decode is what makes reorder a safe no-op.)

### 5. Cross-engine retarget + Schema scrub for every shape

The single-stream path (unlike Shape A's manifest-derived tables)
receives CDC-emitted IR carrying the **source** engine's column types
and the **source** database name in `ir.Table.Schema`. Before
dispatching to `applyShape`, the intercept must, for every shape:

1. **Retarget types** via `translate.RetargetForEngine` (the same path
   cold-start CREATE TABLE, the broker, chain-restore, and ADR-0058's
   ADD COLUMN already use) so ADD / ALTER TYPE / MySQL-MODIFY-
   NULLABILITY emit the target-dialect type. Same-engine pairs are a
   pass-through.
2. **Scrub `Table.Schema`** so the target SchemaWriter's `qualifyTable`
   falls back to its own DSN-bound database (the source DB name does
   not exist on the target — Bug 89 fix #3 generalized from ADD COLUMN
   to all shapes).
3. **Re-resolve the shape's column pointers** (`AddedColumns`,
   `AlteredColumn`, …) against the retargeted table by name, so the
   applier receives target-dialect column defs.

No new type-mapping code: this reuses `RetargetForEngine` wholesale.
The retarget correctness for ALTER TYPE is the same translation
already proven for cold-start CREATE of the same column.

### 6. Shape A unaffected

`--schema-changes` is a no-op when `--inject-shard-column` is set:
Shape A's boundary router already forwards every recognized shape via
the lease (ADR-0054 DP-E). The engage path skips the single-stream
intercept when Shape A is engaged, exactly as ADR-0058 did.

## Consequences

### Positive

- **Syncs stay online through routine schema evolution.** The soak's
  wedge-on-DDL failure mode is closed for every unambiguous shape.
- **DDL and DML are consistent.** sluice forwards the operator's
  committed schema decisions the same way it forwards their row
  changes.
- **Near-zero new code.** Reuses `ClassifyShape` + `applyShape` +
  `RetargetForEngine`; the net new logic is the flag, the default
  flip, the retarget/scrub generalization, and the RENAME refuse.
- **Loud, not silent.** Every applied DDL logs at INFO; every refusal
  names table + shape + drift + recovery.

### Negative

- **Behavior change on upgrade.** A stream that previously refused on
  source DDL now forwards it. Called out loudly in release notes +
  flag help. Operators wanting the old behavior set
  `--schema-changes=refuse`.
- **DROP COLUMN forwarding is destructive on the target.** This is the
  deliberate tradeoff (§1). It is bounded to *unambiguous* drops (a
  drop with no matching same-type add); the ambiguous drop+add case is
  the RENAME refusal (§3), which protects against the data-loss
  misclassification.
- **ALTER TYPE cross-engine narrowing can fail loudly on the target.**
  A widening type ALTER forwards cleanly; a narrowing/incompatible one
  may be refused by the target engine. That surfaces as the §2
  "target DDL apply fails" refuse — loud, position not advanced,
  retryable after manual reconciliation.
- **RENAME still needs the drained model** until F7b (PG) / forever
  (MySQL).

### Neutral

- `--forward-schema-add-column` deprecated, not removed (one cycle).
- `--backfill-added-column` semantics unchanged.

## Tests

- `internal/pipeline/schema_forward_intercept_test.go` — extend the
  dispatch unit tests: each newly-forwarded shape (DROP / ALTER TYPE /
  NULLABILITY / CREATE INDEX / DROP INDEX / ADD/DROP/MODIFY CHECK)
  reaches its `ShapeDeltaApplier` method on a fake applier; RENAME and
  multi-shape combo refuse loudly with the data-loss / ambiguity
  message; ShapeKindNone (reorder) no-ops; volatile DEFAULT still
  refuses.
- `internal/pipeline/migrate_schema_forward_*_integration_test.go` —
  per-shape end-to-end across all four directions (MySQL→MySQL,
  PG→PG, MySQL→PG, PG→MySQL): cold-start, apply each DDL shape on the
  source, verify the target schema converges and post-DDL DML lands.
  The RENAME cell pins the loud refuse (both engines).
- Flag-model tests: `--schema-changes=refuse` restores loud-refuse;
  `--forward-schema-add-column` logs deprecation and forwards;
  default (no flag) forwards.

The per-shape × per-direction matrix is the F7 analogue of the Bug 74
"pin the class, not the representative" discipline: the apply path
differs by target engine (PG `ALTER COLUMN … TYPE` vs MySQL `MODIFY
COLUMN`), so each shape must be pinned on each target, not one
representative.
