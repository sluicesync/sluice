# ADR-0058 — Online schema-change forwarding in the CDC apply path

## Status

**Accepted (2026-05-24), shipped v0.79.0.** Lifts the long-standing
"non-Shape-A streams refuse loudly on any source DDL during CDC" gap
identified by the F12 (online schema evolution) and F16 (backfill of
already-shipped rows) Reddit-research findings (task #40,
`C:\code\sluice-reddit-research-2026-05-23.md`). Sluice now optionally
forwards `ALTER TABLE … ADD COLUMN` from source to target through the
live CDC apply path, with an opt-in source-side backfill of rows that
shipped to the target before the ALTER landed.

ADD COLUMN is the **only** shape v0.79.0 forwards on the live single-
stream path. DROP COLUMN, ALTER COLUMN TYPE, RENAME COLUMN, CHECK
constraint changes, and generated-column changes continue to refuse
loudly per the existing classifier — the drained model
(`sync stop --wait` → schema migrate → `sync start --resume`) remains
the recovery path for those shapes. The Shape A (`--inject-shard-column`)
multi-shard coordination path is unaffected — it already handles every
recognized shape via the lease + boundary-router catalog
(ADR-0054 DP-E); this ADR only fills in the single-stream gap.

## Context

### What competitors do (the F12 finding)

The Reddit research (task #40, F12) classified four failure modes
operators run into across competing CDC tools when source DDL lands
mid-stream:

1. **Silently ignored.** SQL Server native CDC: the capture job
   continues against the pre-DDL projection until manually
   reconfigured.
2. **Requires manual restart.** AWS DMS / Qlik Replicate: the task
   errors on the first post-DDL row event; operator stops the task,
   reloads the table, restarts.
3. **Propagated but not backfilled.** Debezium / Striim: ADD COLUMN
   forwards to the sink, but rows shipped to the sink BEFORE the
   ALTER carry the column's default (or NULL) rather than the
   source's actual values for those rows.
4. **Propagated and backfilled.** Fivetran's HVR (the gold standard
   on this dimension) — but only on a paid tier, and only on
   homogeneous engine pairs.

The F12 finding rates this severity-A because every CDC tool's
operator-trust narrative breaks the moment a source schema change
silently leaves the target stale. Operators expressing the same
concern across three different vendor communities (Debezium-users
mailing list, Fivetran subreddit, DMS forum) is what tipped the
finding to severity-A.

### What sluice does today (pre-v0.79.0)

The non-Shape-A CDC path:

1. The CDC reader (`internal/engines/{postgres,mysql}/cdc_reader.go`)
   detects a schema change in its protocol projection (PG: pgoutput
   RelationMessage; MySQL: TableMapEvent's information_schema re-read)
   and emits `ir.SchemaSnapshot{IR: postIR, …}`.
2. The streamer's per-stream change channel routes the snapshot
   through `interceptSchemaSnapshotsForCoordination` — a pass-through
   for non-Shape-A streams (`s.boundaryRouter == nil`).
3. The applier's per-event dispatch (`change_applier.go`'s
   `SchemaSnapshot` case) persists the snapshot's IR into the
   `sluice_cdc_schema_history` table on the same transaction as the
   ADR-0007 position write — purely a record-keeping action.
4. The next row event references a column the target doesn't have
   yet. The applier raises `column "<new>" does not exist`. The
   stream exits loudly via the standard retriable-error path.

This is **failure mode (2)** from the competitor list. Sluice's
opinion is that mode (2) is acceptable as a **default** (operators
who want explicit-only schema control get exactly what they expect),
but the absence of an opt-in forwarding path is what closes off
trust for operators on F12.

The Shape A multi-shard path is already correct: the
`BoundaryRouter` (`shard_consolidation_router.go`) classifies the
delta via `ClassifyShape`, runs the lease + per-shape applier on
recognized shapes (including ADD COLUMN), and refuses loudly on
unrecognized shapes. That code path is reused mechanically here —
this ADR only wires it for the single-stream case.

### What this ADR ships

A new opt-in intercept that activates when:

- `--forward-schema-add-column` is set, AND
- `--inject-shard-column` is **not** set (Shape A has its own
  intercept already covering this).

The intercept tracks per-table IR state across `ir.SchemaSnapshot`
events, classifies the delta via the same `ClassifyShape` the
Shape A router uses, and:

- On `ShapeKindAddColumn` → calls
  `ir.SchemaDeltaApplier.AlterAddColumn` on the target. Optionally
  triggers a bounded source-side backfill of already-shipped rows
  for the new column when `--backfill-added-column` is also set.
- On any other recognized shape (DROP, ALTER TYPE, RENAME, CREATE/
  DROP INDEX) → refuses loudly with the operator-actionable drained-
  model recovery hint. **Same refusal shape as Shape A's
  unrecognized-combo path** — operators see the same recovery hint
  whether they're on Shape A or single-stream.
- `ShapeKindNone` → no-op pass-through (forwards the snapshot to
  the applier so the ADR-0049 schema-history row still records).
- `ShapeKindUnrecognized` (multi-shape combo) → refuses loudly.

## Decision

### 1. Scope: ADD COLUMN only, opt-in, target-side ALTER + optional source-backfill

#### 1a. ADD COLUMN is the only shape v0.79.0 forwards on the single-stream path

Why ADD COLUMN gets shipped and the other shapes do not:

- **Additive semantics.** A new column on the target can carry a
  sensible default (NULL or the column's DEFAULT) for rows that
  shipped pre-ALTER. Sluice's target write succeeds for existing
  rows without operator intervention.
- **Both engines support `IF NOT EXISTS`.** PG 9.6+ and MySQL 8.0+
  both have `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, so the
  forwarded ALTER is idempotent at the engine level. Re-running
  after a partial failure is safe.
- **Cross-engine ground truth: the same `AlterAddColumn` method
  already exists.** Both engines implement
  `ir.SchemaDeltaApplier.AlterAddColumn` for the Phase 3.2 chain-
  restore orchestrator (`internal/pipeline/chain_restore.go:580`)
  and for the broker's incremental-restart apply
  (`internal/pipeline/broker.go:1003`). Wiring the live CDC apply
  path is mechanical — no new engine surface.
- **What competitors do better than us TODAY on ADD COLUMN is the
  positioning lever.** Every other CDC tool we surveyed handles
  ADD COLUMN partially wrong (silent-drop / restart-required /
  no-backfill); sluice gets exactly this shape right in v0.79.0.

Why the other shapes stay refuse-loudly on the single-stream path:

- **DROP COLUMN** is destructive on the target. Forwarding silently
  has data-loss semantics; the operator's intent ("drop the column
  on both ends") is genuine, but the right time to confirm it is on
  the operator's schema-migrate run, not as a side effect of a CDC
  apply. The drained-model recovery already supports this cleanly.
- **ALTER COLUMN TYPE / NULLABILITY** has cross-engine translation
  hazards that warrant explicit operator review. A PG `text` →
  `int` ALTER on the source has no single right behavior on a
  MySQL target. Refuse-loudly + drained recovery is the safe path.
- **RENAME COLUMN** is handled by the Shape A catalog (ADR-0054
  DP-E, RENAME-column heuristic). The single-stream path could
  add it later; v0.79.0 scope is ADD COLUMN only to keep the
  surface area tight.
- **CHECK constraints / generated columns** are out of v1 scope for
  the broader classifier; they're refused loudly today and remain
  so.
- **CREATE / DROP INDEX** is index-only DDL; forwarding it is
  defensible but adds operational complexity (lock duration on
  large tables, online-index-build options that differ across
  engines). Out of v0.79.0 scope.

The scope split is intentional and operator-facing: **ADD COLUMN
is the additive case where forwarding has a clean default; every
other shape benefits from explicit operator coordination via the
existing drained model.** This ADR makes that explicit so future
proposals to extend the shape catalog inherit the same justification
discipline.

#### 1b. Opt-in flag, not default-on

Default behavior is unchanged: operators who don't set
`--forward-schema-add-column` see exactly the pre-v0.79.0 path
(SchemaSnapshot recorded, next row event errors with `column does
not exist`).

Why opt-in and not default-on:

- **Some operators want explicit-only schema control.** A staging
  environment where DDL is gated through a separate change-
  management process must continue to refuse loudly on any source
  DDL.
- **Behavior change in defaults is a compatibility hazard.** The
  conservative path is "operators opt into the new behavior";
  flipping the default-on could happen in a major-version bump
  after the feature has soaked in production.
- **Symmetry with `CoordinateLiveDDL`.** Shape A's live-coordination
  defaults ON because it's only active when Shape A itself is
  engaged (operator already opted into multi-shard coordination by
  setting `--inject-shard-column`). The single-stream path has no
  such opt-in upstream of itself, so the flag has to default OFF.

The flag is named `--forward-schema-add-column` (not
`--forward-schema-changes`) to make the scope explicit: the
flag's name itself documents that **ADD COLUMN is the only thing
that gets forwarded**; any future shape expansion will need a
separate flag with its own justification.

#### 1c. Backfill is a second opt-in flag

`--backfill-added-column` (off by default; ignored when
`--forward-schema-add-column` is off):

- When set, after the target ALTER lands, the streamer issues a
  bounded source-side SELECT (PK-keyed iteration via
  `ir.BatchedRowReader.ReadRowsBatch`) projecting `(pk, new_col)`
  for rows already on the target.
- For each batch, emits one `ir.Update` per row through the
  applier's standard change-event dispatch. The update's `After`
  row carries the PK columns + the new column's value;
  unchanged columns are absent. The applier's existing
  column-list-tolerant UPDATE path consumes this directly — no
  new applier method.
- Throttling: reuses the existing `ApplyBatchSize` / AIMD
  controller. Each batch is bounded by `BulkBatchSize` rows on
  the source-read side and dispatched via the applier's normal
  batched apply (so the AIMD controller's rate-limiter shapes
  throughput).
- Position semantics: backfill `Update` events carry the
  SchemaSnapshot's source-side `Position` (the same position the
  ALTER landed at on the source). They're idempotent under the
  applier's standard position-write — re-running the backfill
  after a crash re-issues the same UPDATEs against the same PK
  range, and the applier's existing UPSERT-into-UPDATE semantics
  no-op identical writes.

Why backfill is a second flag rather than always-on with
forwarding:

- **Backfill SELECTs cost source database I/O.** On a 1B-row table,
  the cost is significant; operators must opt in knowingly.
- **The trivial-default case doesn't need backfill.** If the source
  ALTER carried `DEFAULT NULL` or `DEFAULT <constant>`, the target
  already has the right value on every existing row (PG and MySQL
  both populate existing rows with the column's DEFAULT on ADD
  COLUMN). Backfill is only useful when the operator's intent is
  "carry the source's actual per-row values forward."
- **A future "backfill on NOT NULL DEFAULT" auto-mode** could be
  added later as a v0.x enhancement; v0.79.0 keeps the surface
  explicit (operator says YES to backfill or accepts the
  default-only target state).

### 2. Refuse-loudly cases

Beyond the shape-restriction refusals (DROP / ALTER TYPE / RENAME /
index / multi-shape combo), the forwarding path itself refuses
loudly on three additional conditions:

#### 2a. ADD COLUMN with computed default / sequence-from-source / identity-respecify

If the new column's `ir.Column.Default` is `ir.DefaultExpression`
(an engine-dialect expression — `NOW()`, a sequence reference, a
function call), the forwarding path refuses loudly. The reasoning:

- Cross-engine: a PG sequence reference (`nextval('foo_seq')`) on
  the source has no equivalent on a MySQL target.
- Same-engine: even on PG → PG, the default expression's evaluation
  context is the **target's session** at ALTER time, not the
  source's. A `DEFAULT NOW()` populates every existing target row
  with the target-side ALTER timestamp, not the source's per-row
  insert time — silently wrong.

Recovery: operator drains the stream, runs the schema migrate with
the right expression on both sides explicitly, resumes.

#### 2b. Target-side ALTER fails

If `applier.AlterAddColumn` returns an error (lock contention with
concurrent target traffic, permission denied, unrecognized type
shape for the target dialect), the intercept refuses loudly and
**does not advance the streamer's position**. The next runOnce
iteration on retry will replay the SchemaSnapshot and re-attempt
the ALTER (idempotent via `IF NOT EXISTS`). Operator can also
manually drain and apply the ALTER via schema migrate.

#### 2c. Backfill SELECT fails / UPDATE fails

If the source backfill SELECT errors (connection drop, table no
longer exists), the intercept refuses loudly with the table name
and the read error. The streamer's standard retry path picks up the
next iteration; the backfill resumes from the last successfully-
applied PK cursor (the cursor is persisted via the applier's
position-write, same as bulk-copy resume in ADR-0018).

If the target backfill UPDATE fails (transactional collision,
target-side constraint refusal), the intercept refuses loudly with
the table name and the write error. **Critically, the streamer
does NOT claim the column is backfilled** — the
operator-visible state reflects "ALTER landed, backfill incomplete"
and the next attempt will resume the backfill from the cursor.

The three refuse-loudly cases share the loud-failure tenet from
CLAUDE.md: every refusal names the table, the failure-shape, and
the operator-actionable recovery hint.

### 3. Failure modes (summarized)

| Source event | Forwarding flag | Backfill flag | Outcome |
|---|---|---|---|
| ADD COLUMN with literal/none default | off | n/a | refuse loudly on next row event (pre-v0.79.0 behavior) |
| ADD COLUMN with literal/none default | on | off | target ALTER lands; existing target rows carry the column's DEFAULT (NULL if none) |
| ADD COLUMN with literal/none default | on | on | target ALTER lands; existing target rows backfilled from source values |
| ADD COLUMN with `DEFAULT NOW()` / sequence / function | on | * | refuse loudly (DP-2a) |
| DROP COLUMN / ALTER TYPE / RENAME / CHECK / generated | * | * | refuse loudly (shape-restriction; drained recovery) |
| Multi-shape combo (rename + index) | * | * | refuse loudly (existing classifier behavior) |
| Target ALTER fails | on | * | refuse loudly; position not advanced; retry replays |
| Source backfill SELECT fails | on | on | refuse loudly; cursor persisted; retry resumes |
| Target backfill UPDATE fails | on | on | refuse loudly; cursor at last successful PK; retry resumes |

### 4. Why no MySQL INSTANT ADD COLUMN tradeoff

MySQL 8.0.12+ supports `ALTER TABLE … ADD COLUMN … ALGORITHM=INSTANT`
which avoids the full-table rewrite. Sluice's `AlterAddColumn`
emits the standard `ADD COLUMN` without the algorithm hint —
**cross-engine parity** is the deciding tenet. PG has no INSTANT
equivalent (every PG `ADD COLUMN … DEFAULT <constant>` since 11 is
already metadata-only); emitting `ALGORITHM=INSTANT` on MySQL would
make the two engines' forwarded ALTER behave differently for the
operator.

Operators who specifically want INSTANT on MySQL can:
1. Drain the stream.
2. Manually run `ALTER TABLE … ADD COLUMN … ALGORITHM=INSTANT` on
   the target.
3. Resume the stream — the target schema already matches the
   source, so the SchemaSnapshot's `AlterAddColumn` no-ops via
   `IF NOT EXISTS`.

A future ADR could thread an `--alter-algorithm=instant` flag if
operator demand surfaces; v0.79.0 keeps it out to ship the
positioning win cleanly.

### 5. Same-engine vs cross-engine

Both same-engine and cross-engine pairs benefit. Cross-engine
specifics:

- **MySQL → PG.** The source's CDC schema snapshot carries the
  column with the MySQL-dialect type. The intercept calls
  `ir.SchemaDeltaApplier.AlterAddColumn` on the PG target's
  SchemaWriter; the existing translation pass (which already runs
  on every cold-start CREATE TABLE) handles the MySQL → PG type
  rewrite via `translate.RetargetForEngine` — same call the broker
  already makes (`broker.go:997`). The forwarded ALTER lands on PG
  with the PG-dialect column definition.
- **PG → MySQL.** Mirror image; the chain-restore tests
  (`chain_restore_cross_test.go:126`) already pin the
  `VARCHAR(36)` shape preservation through `AlterAddColumn` for
  the cross-engine UUID → CHAR(36) case. The live CDC path reuses
  the same call.
- **Same-engine.** No translation; the source IR column is the
  target IR column.

### 6. Difference from the chain-restore caller

The Phase 3.2 chain-restore caller of `AlterAddColumn`
(`chain_restore.go:580`) operates in a **drained world**: there are
no live writes against the source or target during chain restore.
The live CDC caller (this ADR) operates with **live writes against
the target**: the streamer's applier may be mid-batch when the
ALTER fires.

The single-stream forwarding path mitigates this by:

- **Apply at the boundary, not asynchronously.** The intercept
  blocks the SchemaSnapshot's forward progress until the
  `AlterAddColumn` returns. No row event after the SchemaSnapshot
  is dispatched until the ALTER has landed. This is the same
  ordering the Shape A boundary router uses.
- **Engine-level locking.** PG's `ALTER TABLE … ADD COLUMN` takes
  an `ACCESS EXCLUSIVE` lock briefly; MySQL's takes a metadata
  lock. The streamer's applier is the only writer against the
  target table by construction (single-stream), so contention is
  with the operator's own application writes — out of sluice's
  scope to coordinate.
- **Backfill is incremental, not bulk-locking.** Backfill UPDATEs
  flow through the applier's standard batched-apply path; each
  batch is one transaction. No table-level locks; only row-level.

No new engine-side surface is needed. The chain-restore path's
`AlterAddColumn` implementation handles both cases correctly.

### 7. Forward-compat with F11 (CDC-stream schema-drift detection)

F11 (Reddit research finding, task #47) is the sibling feature:
detecting schema drift on the CDC stream and surfacing it to the
operator (or refusing loudly with a precise classification). The
two features divide responsibility cleanly:

- **F11 answers** "the source schema changed — what does the
  operator need to know?"
- **F16 / this ADR answers** "the source schema changed — what
  should sluice DO about it?"

Both close half of the "Debezium-class schema-evolution" story.
F11 is targeted for a later v0.x release; the two are designed to
compose without changes to either's CLI surface (F11 adds a
reporting flag; F16 adds a forwarding flag).

## Consequences

### Positive

- **F12 positioning closed.** Sluice is the first CDC tool the
  Reddit-research survey identified that does ADD COLUMN
  forwarding + optional backfill in a single tool, on the OSS
  tier, cross-engine. This is the marquee task #45 outcome.
- **Operator-trust narrative complete on the additive case.**
  Operators who set `--forward-schema-add-column` get a
  Debezium-class behavior with stronger correctness guarantees
  (no silent forwarding; explicit refuse-loudly on every shape
  that can go subtly wrong; backfill is opt-in and bounded).
- **Reuses existing primitives.** No new engine surface, no new
  control table. The intercept is ~300 LOC and reuses the same
  `ClassifyShape` + `AlterAddColumn` + `BatchedRowReader` +
  `ChangeApplier` already exercised by Shape A and chain
  restore.
- **Default behavior unchanged.** Operators who don't opt in see
  exactly the v0.78.x behavior. No silent migration of existing
  deployments to the new path.

### Negative

- **Backfill cost on large tables.** A 1B-row table backfill is
  hours of source-side SELECT load. Operators must opt in
  knowingly. Documented in the flag's help text.
- **Refuse-loudly on every other shape feels narrow.** The
  operator wanting full schema-evolution must still use the
  drained model for DROP / ALTER TYPE / RENAME. The scope-split
  justification (§1a) explains why this is the right v1 boundary.
- **Two flags, not one.** `--forward-schema-add-column` and
  `--backfill-added-column` could collapse to one tristate flag
  (`--schema-add-column=off|forward|forward-and-backfill`). The
  two-flag form was chosen for grep-ability in CI/deployment
  manifests and for forward-compat with future shape flags;
  operators reading the flag set can see exactly which features
  are enabled per stream.

### Neutral

- **Shape A multi-shard streams are unaffected.** Shape A's
  intercept (`shard_consolidation_intercept.go`) already handles
  ADD COLUMN via the lease + boundary router; the
  `--forward-schema-add-column` flag is a no-op when
  `--inject-shard-column` is set (the new intercept is engaged
  only when Shape A is NOT engaged). The CLI surfaces a clear
  log line when the operator sets both flags ("ignoring
  --forward-schema-add-column — Shape A live coordination already
  forwards every recognized shape via the lease").

## Tests

Pinned by:

- `internal/pipeline/schema_forward_intercept_test.go` — unit
  tests of the intercept's dispatch, refuse-loudly classifications,
  and backfill batch-size resolution.
- `internal/pipeline/migrate_add_column_forward_pg_integration_test.go`
  — PG → PG live CDC; cold-start, ALTER on source, verify column
  appears on target and INSERTs after the ALTER land.
- `internal/pipeline/migrate_add_column_forward_mysql_integration_test.go`
  — MySQL → MySQL.
- `internal/pipeline/migrate_add_column_forward_cross_integration_test.go`
  — MySQL → PG and PG → MySQL.

The integration tests' negative-pin case exercises the flag-off
path (refuse-loudly behavior preserved) so future contributors who
broaden the default behavior have a regression-guard.

### Bug 89 closure (v0.79.0, 2026-05-24)

The four MySQL-side integration tests
(`TestAddColumnForward_MySQL_FlagOn_ForwardsALTER`,
`TestAddColumnForward_MySQL_Backfill`,
`TestAddColumnForward_Cross_MySQLToPG`,
`TestAddColumnForward_Cross_PGToMySQL`) failed in main CI after the
initial feature commit (`7ca4b5a`) and were tracked as Bug 89. Three
distinct fold points surfaced; each is a representative of a class
the original implementation didn't account for, so the fix locus for
each lives where the class is decided rather than where the failure
surfaced.

1. **Intercept cache seeding (cross-engine asymmetry).** The intercept
   tracks per-table IR state in a `cache` map and classifies the
   second-and-later SchemaSnapshot's delta via `ClassifyShape`. PG's
   pgoutput emits a `RelationMessage` on first-touch of every
   relation (with `KeyColumn` flags), so the very first row event
   surfaces a `SchemaSnapshot` with the **pre-DDL** schema — seeding
   the cache. The MySQL binlog has no first-touch equivalent;
   `maybeSnapshotSchemaB1` only fires when `pendingDDLActive` is
   true, which is set on the first observed DDL `QUERY_EVENT`. So on
   MySQL the FIRST `SchemaSnapshot` the intercept sees is the
   **post-ALTER** schema, the cache is empty (`hadPre=false`), the
   intercept seeds the cache and forwards the snapshot as the
   anchor, and the ADD COLUMN is never classified. The fix
   pre-populates the intercept's cache from the cold-start source
   IR (the same `synthesizeColdStartSeedSnapshots` mechanism Shape A
   uses for the ADR-0054 Bug 83 fix), promoted to also feed the
   ADR-0058 intercept when `--forward-schema-add-column` is set and
   Shape A is not engaged. The fix locus is
   `internal/pipeline/streamer.go` (extend the seed gate) +
   `internal/pipeline/schema_forward_intercept.go` (accept the seed
   parameter; reuse `lookupSeedCache` for the MySQL bare-name
   fallback). Pinned by
   `TestForwardAddColumn_SeededPre_ClassifiesFirstCDCSnapshot` and
   `TestForwardAddColumn_SeededPre_BareName_FallbackResolves`.

2. **PrimaryKey absent from CDC-emitted SchemaSnapshot IR.**
   `runBackfillForAddedColumn` requires the post-ALTER table's
   `ir.Table.PrimaryKey` to drive the cursor-paginated source SELECT.
   Both engines' CDC-side projections produced `*ir.Table` values
   with `PrimaryKey` left unset: PG's `projectRelation` only copies
   name and type; MySQL's `maybeSnapshotSchemaB1` only copies
   Schema/Name/Columns. Backfill then refused with "table has no
   primary key" on both engines as soon as the seed fix above let
   the intercept actually reach the backfill branch. The fix is at
   the projection locus (not the backfill consumer): PG projects
   `KeyColumn`-flagged columns into `ir.Index{Columns: ...}`; MySQL
   projects its pre-existing `tableSchema.PrimaryKey []string` field
   into the same shape. The class here is "CDC-emitted SchemaSnapshot
   IR must carry every identity-bearing field downstream consumers
   reasonably expect," so the projection — not the consumer — is the
   right locus.

3. **Cross-engine target schema name leakage.** The CDC-emitted IR
   carries the SOURCE database name in `ir.Table.Schema` (MySQL:
   `source_db`; PG: `public`). PG's `SchemaWriter.qualifyTable`
   honors a non-empty `table.Schema` over its own bound schema, so
   forwarding `ALTER TABLE source_db.widgets ADD COLUMN price` to a
   PG target raised `ERROR: schema "source_db" does not exist`
   (SQLSTATE 3F000). MySQL's writer doesn't qualify by schema in its
   ALTER (just uses `w.schema` for the probe), so MySQL targets
   weren't affected. The chain-restore caller of `AlterAddColumn`
   never hit this because manifest-derived tables carry
   `Schema=""`. The fix is in `retargetAddedColumns`: clear the
   retargeted table's `Schema` field so the target SchemaWriter's
   `qualifyTable` falls back to its own DSN-derived bound schema.
   The cross-engine class — source identifiers must not leak through
   into target DDL — is the deciding principle.

All 7 cells (3 PG + 2 MySQL + 2 cross-engine) now pass. The Bug 89
shape is **not** a single bug at a single locus — it's three
sibling classes that all happened to surface as the same symptom
("post-ALTER INSERT never lands"). Pinning each class at its own
locus keeps the fix readable and gives future contributors three
independent regression guards rather than one bundled patch.

## Glossary anchors

- "ADD COLUMN forwarding": the new behavior this ADR ships.
- "Single-stream path": a sluice sync stream where
  `--inject-shard-column` is unset (one source → one target). The
  Shape A multi-shard path has its own intercept.
- "Backfill": the source-side bounded SELECT that populates
  already-shipped target rows with the source's values for the
  newly-added column. Distinct from the column's DEFAULT
  expression (which the engine applies automatically).
- "Drained recovery": the existing operator workflow — `sync stop
  --wait` → schema migrate → `sync start --resume` — that remains
  the fallback for every shape this ADR does NOT forward.
