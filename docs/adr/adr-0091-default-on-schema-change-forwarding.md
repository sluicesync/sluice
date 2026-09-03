# ADR-0091 — Default-on schema-change forwarding for the single-stream CDC path

## Status

**Accepted — shipped v0.99.45 (F7a) → v0.99.46 (F7b) → v0.99.48 (F7c).**
Proposed 2026-06-14. `--schema-changes=forward|refuse` is the live flag
in `cmd/sluice/cli.go` (default `forward`), and
`--forward-schema-add-column` is deprecated in its favour. This header
said Proposed for ~40 releases after the flag shipped: the index row
declared "Accepted" *unbolded*, and the G-17 status-parity gate read
only bold tokens, so the DOC-3 lag it exists to catch sailed straight
through it. The gate now reads unbolded declarations on both sides.

Extends ADR-0058 (online ADD COLUMN
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
- **The drained-model recovery must actually run on a warm resume
  (impl note, audit 2026-09-01 A2-1).** Every refusal in this ADR
  hands the operator "drain, apply the change on the target, restart
  with the same stream-id". On a Postgres source that restart
  re-refused, four runs out of four: the reader's `TxCommit` carried
  `CommitLSN` — the commit record's START — and logical decoding skips
  a transaction on resume only when its commit record starts strictly
  before the requested LSN, so a resume from a cleanly-persisted
  boundary replayed the whole last applied transaction. Its
  `RelationMessage` is rendered from the historic catalog (the pre-DDL
  shape), it re-seeded the relation cache, and the first post-DDL
  transaction classified against it exactly as it had the first time.
  The same replay routed a renamed table's rows to the old name into
  `sluice_cdc_skipped_tables`, and its historic column names resolved
  to `StableID` 0 against the live catalog, so the runbook's own PG
  RENAME COLUMN path (a proven rename) reached this ADR's §3 refusal
  as unprovable. Fixed at the convention, not at the doors: the
  `TxCommit` now carries `TransactionEndLSN` — the post-commit point,
  the Postgres sibling of item 132's MySQL fold — so a warm resume
  after a clean boundary starts at the NEXT transaction (ADR-0027,
  corrected). Rows and `TxBegin` keep the pre-transaction point, so a
  position persisted mid-transaction still replays whole. Pinned on a
  real server both directions (`TestPGCDC_TxCommitPositionIsPostCommit`)
  and per shape through the reader (`TestPGCDC_DrainedModelRecoveryResumes`)
  and the real applier + ledger (`TestStreamer_PGToPG_DrainedModelRecovery`).
  *Stated consequence:* the replayed transaction was also the only
  witness of the pre-DDL shape at resume, and only when it happened to
  touch the altered table. Without it, a schema change applied on the
  source while the stream was STOPPED is not classified at resume by
  either mode — the first post-resume `RelationMessage` is the anchor,
  which is what the runbook documents ("the post-restart CDC schema
  cache rebuilds from the first event"). The drained model applies the
  change on both sides before restarting and is unaffected; a
  warm-resume source↔target shape reconciliation (target-derived seed,
  same-engine exact) is the follow-up that would make a one-sided
  change loud again, and the §3 seed-guard is the shape it would reuse.
- **Table-rewrite VALUE changes are never streamed (impl note,
  capture-completeness G3, 2026-08-26).** PostgreSQL does not logically
  decode the contents of an `ALTER COLUMN TYPE` table rewrite — the
  rewritten rows produce ZERO pgoutput messages (observed on the wire).
  What forwarding can and cannot do about that, by shape:
  - *Typmod-only ALTER (e.g. `numeric(10,4)→(10,1)`, `varchar(n)`
    shrink):* detected — `classifyRelationChange` compares
    `TypeMod` as well as name/OID, so the shape rides this ADR's
    standard door. Under `forward` (the default), **for families whose
    projected IR carries the modifier** (numeric, varchar/char,
    timestamp/time precision outside the temporal-collapse class below,
    bit), the target receives the same USING-less ALTER and its own
    rewrite applies the identical deterministic cast, so pre-rewrite
    target rows **converge** with the source's rewritten values
    (integration-pinned: `TestStreamer_SchemaForward_AlterType_TypmodOnly_PG`).
    Under `refuse` it refuses loudly for EVERY typmod-carrying family —
    the compare is on the raw wire typmod, projection-independent —
    and pre-fix, refuse mode had NO door for this shape and diverged
    silently.
  - *Detected-but-unforwardable class (VF review 2026-08-26; refusal
    shipped, audit 2026-08-27 A2): a typmod delta visible to the raw
    compare but INVISIBLE to the projected IR.* For these members the
    raw compare classifies `AlterColumnType`, but the projected schema
    signature does not move, so `forward` could never emit a boundary
    or forward an ALTER for them. They now **refuse loudly under BOTH
    modes** — the reader's TYPMOD-PROJECTION-GATE
    (`unforwardableTypmodColumn` in `checkSchemaRace`, per changed
    COLUMN so a mixed multi-column ALTER cannot ride a sibling's moved
    signature) fires the standard drained-model refusal instead of
    silently diverging pre-existing target rows at exit 0. Enumerated
    members: **`interval(p)` / interval field restrictions** (projects
    to the empty `ir.Interval{}`; `ALTER … TYPE interval(3)` rounds
    every stored fractional second with zero decoded messages),
    **every ARRAY element typmod** (`numeric(10,4)[] → numeric(10,1)[]`
    rounds every element; the element resolves with typmod −1 by
    design), and **the session-TZ cast swaps — `time ↔ timetz` and
    `timestamp ↔ timestamptz`, either direction, scalar AND array**
    (`unforwardableSessionTZCast`). Both swaps MOVE their projected
    signatures (`time ↔ timetz` since the TIMETZ-PROJECTION fix,
    2026-08-28, which caught it as projection-identical before that;
    the timestamp pair always has — `ir.DateTime` vs `ir.Timestamp`),
    so the intercept COULD see them, but they refuse for their own
    reason: the source's ALTER resolved every stored value against the
    SOURCE session's TimeZone, and a forwarded ALTER would re-cast the
    target's pre-existing rows against the TARGET session's TimeZone —
    differing settings diverge silently. The timestamp pair FORWARDED
    until the TIMESTAMPTZ-SWAP-FORWARD operator decision (2026-08-28)
    aligned it with its sibling — a deliberate behavior change: the
    convergence-when-both-session-TZs-happen-to-match case is given up
    to refuse the silent-divergence case, consistent posture across
    the class. Impl note on the hazard's reach: the wire does not
    carry the source ALTER session's TimeZone, and sluice's PG applier
    sessions do not pin one (`afterConnectSessionPins` pins
    `extra_float_digits` and `bytea_output` only — grep-verified, no
    `SET TIME ZONE` anywhere in the engine) — a target-side pin would
    only narrow the hazard (making the target's cast deterministic),
    never close it, because the source setting stays unknowable.
    *ARRAY variants — RESTORED, audit 2026-08-31 SL-3.* The 08-28
    filing left `time[] ↔ timetz[]` and `timestamp[] ↔ timestamptz[]`
    forwarding, and the first of those was a **regression opened
    inside that same delta**: `time[] ↔ timetz[]` had refused since
    the A2 gate (v0.132.1) because both element types resolved at
    typmod −1 to a flag-less `ir.Time` and the pair was
    projection-IDENTICAL; the TIMETZ-PROJECTION fix made the two
    projections differ, the projection-equality gate stopped firing,
    and the swap forwarded from v0.134.0 with its per-element hazard
    intact. `sessionTZSwapPair` now unwraps BOTH sides through
    `pgArrayElementOID` and runs the scalar arms on the element OIDs,
    so the class is matched by PAIR rather than by projection accident
    — stable under any future projection change, and automatically
    covering a zone-aware element family added to that map later. A
    scalar↔array dimension change (`time → timetz[]`) is deliberately
    NOT a swap: PG needs an explicit `USING` to express it, so a
    forwarded bare ALTER fails loudly rather than diverging.
    *The other lane — FIXED, audit 2026-08-31 SL-2.* MySQL's
    `TIMESTAMP ↔ DATETIME` `MODIFY` is the same class resolved by the
    executing session's `time_zone` (observed on 8.0.46: the same
    stored value becomes `21:00:00` when the ALTER runs at `+09:00`
    and stays `12:00:00` at UTC), and the audit judged it the MORE
    common member — sluice pins `time_zone='+00:00'` on its own
    connections, so a non-UTC source host makes the divergence the
    DEFAULT outcome rather than a coincidence. It refuses in the MySQL
    engine's own boundary paths (`mysql.sessionTZSwapPair`, wired into
    all three of that engine's `ir.SchemaSnapshot` emitters — binlog,
    VStream standalone, VStream snapshot-stream). Cross-lane
    enumeration is now mechanical:
    `TestSessionGUCCastRoster_EveryCDCLane` (`internal/docsync`)
    derives each lane's pairs from that lane's own declaration, fails
    a lane that declares pairs without a refusal path, and fails a
    newly-registered engine that classifies itself as neither.
    *The first-boundary window — FIXED, audit 2026-09-01 SLM-1.* The
    SL-2 refusal compared each boundary against a memo only the
    lane's own emitter wrote, so a table's FIRST DDL after any start
    was never checked — and that is the boundary Shape A's router
    forwards (its cache is seeded from the cold-start handoff, so the
    first CDC snapshot is a real boundary to it). Observed on a
    `--default-time-zone=+09:00` source: under `--inject-shard-column`
    the first `MODIFY c TIMESTAMP` forwarded and every pre-existing
    target row read 9 h off at exit 0; under the default
    `--schema-changes=forward` it was §3 seed-skipped and later rows
    landed in a zone-mismatched column. The reader now has a prior at
    its first boundary: the streamer hands every MySQL lane a seed
    through `schemaSeedSetter` (`SetSchemaSeed`) — the RAW source IR
    on cold start (captured before mappings, which rewrite types for
    the target), the retained schema-history version resolved at the
    persisted position on warm resume (`loadRetainedSchemaSeed`) — and
    the binlog lane additionally keeps the decode cache's last shape
    across a DDL clear (`retainPriorShapes`). The pipeline carries its
    own door as the belt behind the readers' braces:
    `refuseSessionZoneSwap` runs in `BoundaryRouter.RouteBoundary`
    before the lease and in `routeForwardBoundary` after the §3 seed
    guard (deliberately after it, so an operator's `--type-override`
    onto a zone sibling is not reported as a swap they never made);
    `TestSessionZoneSwapDoorRoster_EveryShapeApplyCallSite` derives
    every `applyShapeDelta` caller and its callers from the AST and
    holds each behind a named door. The residual window, stated: a
    table with no prior at all — outside the cold-start scope with no
    retained version, and never decoded before the DDL — still cannot
    refuse at its first boundary, and the intercepts treat that
    boundary as a cache prime rather than an ALTER.

    *The class was wider than the pair — FIXED at the pipeline door,
    audit 2026-09-01 SLM-5, v0.139.0.* The predicate required BOTH
    sides of the ALTER to be in a zone family, so only the sibling
    swap matched. Measured 2026-09-03 on mysql:8.0.46 and postgres:16,
    a value stored at 2026-06-15 20:00:00 UTC altered under +09:00 /
    Asia/Tokyo against a UTC control: MySQL `TIMESTAMP` to VARCHAR gave
    `2026-06-16 05:00:00`, to DATE `2026-06-16` (crossing midnight), to
    TIME `05:00:00`, to BIGINT `20260616050000`; the reverse casts into
    `TIMESTAMP` shifted the stored instant by the same nine hours; PG
    `timestamptz` to text/varchar/date behaved identically. All of them
    forwarded unrefused. `ir.SessionZoneCast` now keys on
    SESSION-NORMALISED — stored UTC, so rendering must pick a zone
    (`timestamptz`, MySQL `TIMESTAMP`) — rather than on carries-a-zone,
    because the same measurement showed `timetz` is NOT affected
    (`timetz` to text/time was byte-identical under both zones: the
    offset travels with each value) while `time` to `timetz` IS (an
    offset is invented from the session). The widened predicate
    CONTAINS the sibling one by construction, pinned, so no shipped
    refusal was dropped; the scalar-to-array dimension carve-out is
    inherited deliberately, since PG needs an explicit USING and a bare
    forwarded ALTER therefore fails loudly rather than diverging. Scope,
    stated: this is the PIPELINE door, reached when a boundary is
    FORWARDED — the mode that re-casts the target rows. The two
    reader-side first-boundary arms still carry only the sibling pair
    (filed SLM-5c), and `ZoneSiblingSwap` still refuses `timetz` to
    `time`, which measured NOT session-dependent (filed SLM-5b:
    narrowing a shipped refusal is its own reviewed change).
    The temporal-collapse members below
    are NOT in this class — their raw projection moves, so they emit a
    boundary and keep the normalizer posture described in the caveat.
    Use the drained model for these ALTERs, as the refusal instructs.
    The class-enumeration gate is BUILT:
    `TestTypmodProjectionGate_EveryTypmodFamily` asserts every
    typmod-consuming family in `oidToType` (pg_type.typmodin-derived
    scalar roster + the derived array roster) either moves the
    projected signature or sits on this documented refuse list, and
    that the reader actually refuses the listed members under forward.
    *Message honesty (A2-VARCHAR-TEXT-MSG, operator decision
    2026-08-27):* two members of this class are catalog-only — the
    **unbounded `varchar⇄text` OID swap** (both project
    `ir.Text{TextLong}`; PG treats the pair as binary-coercible, no
    rewrite) and **interval same-range precision WIDENING** (existing
    values fit; no rewrite) — so the silent-divergence sentence is
    factually wrong for them. The refusal is shape-aware: those two
    shapes get an honest "no table rewrite occurred on the source; the
    change is projection-invisible — apply the same ALTER on the
    target via the drained model" text, every other member keeps the
    divergence text, and the verdict is identical for all (pinned both
    ways by `TestCheckSchemaRace_UnforwardableTypmod`). Ground truth:
    bounded `varchar(n)→text` moves the projection and already
    forwards — only the exotic unbounded shape reaches the gate.
    *Demand-gated allowlist sketch (NOT built — build only if an
    operator hits the refusal):* the two lossless shapes could
    pass-without-forward — values are byte-identical on both sides and
    the target catalog simply keeps its old, projection-equivalent
    type (its `text` vs the source's new unbounded `varchar`, or its
    `interval(3)` vs the source's widened `interval(6)`), so no target
    DDL is needed for value fidelity; the residue is a declared-type
    cosmetic drift the operator reconciles at leisure. The predicate
    would be exactly `losslessNoRewriteDelta` (directional interval
    widening on identical range bits; unbounded-varchar⇄text swap),
    which already exists as the message selector.
  - *Same-type, same-typmod `ALTER … USING <expr>`:* **undetectable,
    permanently** — the post-rewrite RelationMessage is byte-identical
    to the pre-rewrite one and no catalog artifact survives; the source
    values changed and nothing streams. Use the drained model for any
    value-rewriting ALTER.
  - *Cross-type ALTER with a value-changing `USING`:* detected and
    forwarded, but §5's apply deliberately emits **no USING clause**,
    so the target applies the default cast — a value-changing source
    expression diverges on pre-existing rows. Same drained-model
    guidance.
  - *Temporal caveat:* the Bug 84/86 comparison lens collapses
    timestamp/time `bare ≡ (0) ≡ (6)` into one class, so an ALTER
    between those class members is refused under `refuse` (the reader
    compares raw typmods) but NOT forwarded under `forward` (the
    intercept's normalized diff sees no delta) — the documented
    normalizer false-negative (`cdc_normalize.go`).
  The `pg_class.relfilenode` rewrite-detection WARN (a rewrite
  allocates a new relfilenode; so do VACUUM FULL/CLUSTER, hence WARN
  not refuse) is a filed follow-up, not built.

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
