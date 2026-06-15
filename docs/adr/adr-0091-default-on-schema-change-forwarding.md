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

Codename F7. The intercept dispatch can forward every shape, but the
**actual end-to-end reach is bounded by what each source engine's CDC
projection carries on the wire** — pgoutput omits a great deal that the
MySQL information_schema re-read does not. The honest, ground-truthed
matrix is in **§1d**; read it before assuming a shape forwards. Do not
restate "forwards every unambiguous shape" without that qualifier — an
earlier draft of this ADR did, and end-to-end validation proved it
false (the gaps in §1d's footnotes).

Shipped in parts:

- **F7a (this ADR):** the tristate flag + default flip + the
  forwarding that actually works end-to-end per §1d — ADD / DROP
  COLUMN and ALTER COLUMN TYPE on **both** source engines (DROP/ALTER
  on a PG source required relaxing the reader gate, GAP #1; cross-engine
  ALTER TYPE required applier-cache invalidation, GAP #3), plus ALTER
  NULLABILITY on a **MySQL** source (GAP #2). RENAME COLUMN refuses
  loudly on both engines (§3).
- **Documented limitations (not forwarded; §1d footnotes):** all
  PG-source nullability/index/check (pgoutput omits the metadata);
  MySQL-source index/check (would need a new reader-side catalog
  projection — perf-only for indexes, cross-engine-expr-hazardous for
  checks). These need a future catalog-subscription path, not a tweak.
- **F7b (this ADR — SHIPPED):** PG attnum-proven RENAME COLUMN
  forwarding. `ir.Column.StableID` carries `pg_attribute.attnum` (stable
  across RENAME) from the PG CDC reader; the rename intercept forwards
  iff before & after carry the SAME non-zero StableID (proven rename,
  data preserved) and refuses otherwise (a different attnum is a real
  drop+add; a zero attnum is unprovable). MySQL RENAME stays refuse
  permanently (no stable column id). See §3.

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

#### 1d. The real forwarding matrix (what actually forwards, by source engine)

The intercept's per-shape dispatch (§5) can emit any shape's DDL, but a
shape only forwards end-to-end if **(a)** the source CDC reader produces
a boundary for it and **(b)** that boundary's IR carries the shape's
detail. pgoutput (PG logical replication) carries far less than MySQL's
information_schema re-read, which bounds the matrix:

| Shape | MySQL source | PG source |
|---|---|---|
| ADD COLUMN | ✅ forward | ✅ forward |
| DROP COLUMN | ✅ forward | ✅ forward (GAP #1) |
| ALTER COLUMN TYPE (same-engine) | ✅ forward | ✅ forward (GAP #1) |
| ALTER COLUMN TYPE (cross-engine) | ✅ forward (GAP #3) | ✅ forward (GAP #1/#3) |
| ALTER NULLABILITY | ✅ forward (GAP #2)¹ | ❌ refuse² |
| REORDER | ✅ no-op (name-based decode) | ✅ no-op |
| CREATE / DROP INDEX | ❌ refuse³ | ❌ refuse² |
| ADD / DROP / MODIFY CHECK | ❌ refuse³ | ❌ refuse² |
| RENAME COLUMN | ❌ refuse (§3) | ✅ forward via attnum (F7b, §3) |
| RENAME TABLE / multi-shape combo | ❌ refuse | ❌ refuse |

Footnotes (the documented limitations, each a future catalog-poll
subscription rather than a tweak):

1. **MySQL ALTER NULLABILITY (GAP #2):** the reader's emission gate is
   true-delta'd on `SchemaSignatureOf` (name+type), which excludes
   nullability; forward mode widens the gate to also emit on a
   nullability delta (the data is already in `tableSchema.Columns`).
2. **All PG-source nullability / index / check:** pgoutput's
   RelationMessage carries columns (name+type) + the replica-identity
   key-flag and nothing else — no nullability flag, no secondary-index
   metadata, no constraint metadata, no generated columns. The wire
   never signals these, so they produce **no boundary** on a PG source
   and are invisible to forwarding (the §5b normalizer correctly strips
   them from the cold-start seed too, so they don't surface as phantom
   drops). Detecting them would need a separate out-of-band catalog
   subscription (future work, F47-class). This is **not** silent
   corruption: a resulting incompatibility (a source DROP NOT NULL or
   DROP CHECK the target still enforces) surfaces as a **loud apply
   error** on the next affected row, honoring the loud-failure tenet;
   a benign one (CREATE INDEX, an ADD CHECK that the source's
   already-accepted rows satisfy) simply leaves the target without that
   object.
3. **MySQL-source index / check:** the MySQL CDC reader's `tableSchema`
   projects only `{Schema, Name, Columns, PrimaryKey}` — it does not
   read secondary indexes or CHECK constraints on a DDL boundary.
   Forwarding them would need a new reader-side catalog projection
   (and, for CHECK, a cross-engine expression-translation path). The
   value is perf-only for indexes and cross-engine-hazardous for
   checks, so both are deferred, not built.

This matrix is the **source of truth** for operator docs and for any
"does shape X forward?" question. The Consequences section's "behavior
change on upgrade" applies only to the ✅ rows.

### 2. Refuse-loudly catalog (the only cases that stop the sync)

Under `--schema-changes=forward`, the sync still refuses loudly on:

- **Multi-shape combo** (`ShapeKindUnrecognized`) — more than one
  structural change in a single boundary. The IR delta cannot
  unambiguously order/separate them; the operator coordinates via the
  drained model.
- **RENAME COLUMN that cannot be PROVEN** (`ShapeKindRenameColumn`
  without a stable column id) — a MySQL-source rename, or any rename
  whose before/after columns lack a matching non-zero
  `ir.Column.StableID`. A PG-source rename IS proven via attnum and
  forwards (F7b); see §3.
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

### 3. RENAME COLUMN: PG forwards via attnum (F7b); MySQL refuses; the
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
  proven DROP+ADD. **F7b plumbs this through:** the PG CDC reader
  resolves each column's attnum on the relation boundary (the same
  off-hot-path round-trip that resolves identity-key columns) and
  carries it into `ir.Column.StableID`; the rename intercept forwards
  iff `before.StableID == after.StableID && before.StableID != 0`.
  Ground-truthed on a live PG 16: a real `RENAME old_label TO
  new_label` arrives as before/after attnum `3/3` (proven → forward,
  data preserved); a `DROP gone, ADD fresh (same type)` arrives as
  `3/5` (different attnum → refuse). The proof is definitive, so a bug
  can only ever REFUSE (safe), never mis-forward.
- **MySQL** has no equivalent. `INFORMATION_SCHEMA.COLUMNS` exposes
  `ORDINAL_POSITION` (changes on reorder, not stable) and no creation
  id. A MySQL RENAME is **fundamentally unprovable** from catalog
  state; the heuristic is the only signal, and it is not safe enough
  to drive a destructive auto-DROP. MySQL columns carry
  `StableID == 0` (unknown), so a MySQL-source rename stays unprovable
  → refuse, permanently.

**StableID is METADATA, not a schema attribute.** It does NOT
participate in the decode contract (`ir.SchemaSignatureOf` /
`SchemaSignature.Equal` — name + ordered type only) nor in
alter-detection (`pipeline.diffAlteredColumn`), and it is deliberately
NOT serialized by the schema-history / backup codec (a persisted
attnum would be meaningless on resume — it is only ever compared
between two live CDC projections within one stream). A seed
(StableID=0) vs the first CDC snapshot (StableID=attnum) for an
unchanged column therefore does NOT diff as altered and shares a
signature.

**Seed-guard interaction (§5b):** RENAME is treated as a
destructive/mutating shape by the seed-guard, so a rename classified
against the cold-start SEED (no attnum on the seed side → never
provable at that boundary) is SKIPPED, not forwarded — a safe
non-destructive divergence (the column keeps its old name on the
target). A real PG rename only forwards on a genuine CDC→CDC boundary
where both sides carry attnum. For PG the first-touch RelationMessage
primes the cache before any DDL, so a real rename is always a CDC→CDC
boundary.

The PG forward reuses the same per-shape dispatch
(`applyShapeForward` → `applyShapeDelta` → `AlterRenameColumn`) and
cross-engine retarget/scrub every other shape uses, so a PG-source
rename also forwards to a **MySQL** target — the proof is the
PG-source attnum, independent of the target engine. MySQL RENAME stays
refuse permanently (the operator drains and renames on both ends
explicitly — the one shape where explicit coordination is genuinely
required, not merely conservative).

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

### 5b. Projection-fidelity hazard and the seed-guard (the critical safety mechanism)

The cold-start **seed** (a full `SchemaReader` read) and a **CDC
SchemaSnapshot** are *not* the same fidelity. A CDC projection carries
only what the wire protocol delivers:

- **pgoutput** (`projectRelation`) carries columns (name+type) + the
  replica-identity key-flag. It **omits** generated columns (pre-PG18
  they're unpublished), secondary indexes, CHECK constraints,
  nullability, defaults, comments.
- MySQL's binlog path re-reads `information_schema` on a DDL boundary,
  so its CDC projection is full-fidelity (matches the cold-start read).

`pipeline.ClassifyShape(seed, firstCDCSnapshot)` therefore sees the
fields the CDC projection drops as a **phantom delta**: a PG generated
column present in the seed but absent from pgoutput diffs as a phantom
`DropColumn`; a secondary index as a phantom `DropIndex`; a residual
type-precision asymmetry as a phantom `AlterColumnType`. Under
ADR-0058's ADD-only path these phantoms were *refused* (harmless noise);
under ADR-0091's default-on forwarding a phantom drop/alter would
**forward destructive DDL and silently corrupt the target** — caught by
the `Generated` and `PGToMySQL` convergence integration tests on the
first CI run of this change.

Two layers close this:

1. **`CDCSchemaSnapshotNormalizer` (the PG normalizer).** The seed is
   normalized to match pgoutput's fidelity before comparison — Bug
   84/86/ADR-0065 already strip type-detail / nullability / default /
   comment / CHECK constraints; ADR-0091 extends it to also drop
   **generated columns** and **secondary indexes**. This makes the
   steady-state seed→firstCDC diff `ShapeKindNone`.
2. **The seed-guard (defense-in-depth).** The normalizer cannot be
   *proven* complete (Bug 84/86 were found incrementally). So a
   **destructive/mutating** shape (DROP / ALTER TYPE / ALTER
   NULLABILITY / DROP INDEX / DROP+MODIFY CHECK) is **never forwarded
   when classified against a seed-sourced pre** — only against a genuine
   CDC→CDC boundary, where both sides share projection fidelity and a
   phantom cannot arise. Additive shapes (ADD COLUMN / CREATE INDEX /
   ADD CHECK) pass, since a phantom of them cannot occur against the
   seed (the CDC projection is a subset of the seed's fidelity).

The seed-guard's cost: a real DROP/ALTER that lands as the *very first*
post-cold-start boundary won't forward at that one boundary (the target
keeps the column — a safe, non-destructive divergence; subsequent
CDC→CDC boundaries forward normally). The benefit: no residual fidelity
gap can ever forward a phantom destructive DDL. This is the
value-fidelity discipline (CLAUDE.md "loud failure / no silent loss")
applied to schema forwarding: when in doubt, do **not** destroy.

**Engine limitation that follows:** because pgoutput carries no
secondary-index / generated-column / CHECK metadata, those shapes
**cannot be forwarded on a PG source via CDC at all** (the wire never
signals them). They forward on a **MySQL source** (full-fidelity
`information_schema` re-read). This is documented, not a bug — the wire
doesn't carry the signal.

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
