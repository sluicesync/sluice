# Prep — Multi-source aggregation Shape A (sharded → consolidated), roadmap §4

Design-first SPIKE evidence. This doc is the research pass that feeds
[ADR-0048](../../adr/adr-0048-multi-source-aggregation-shape-a.md)
(Status: Proposed — sign-off pending). It does **not** propose code to
land; it records what a real sharded-Vitess → consolidated-target
harness *actually surfaced* for each of roadmap §4's three pieces, so
the human sign-off on ADR-0048 is grounded in observed pain rather than
theory.

Companion references (read alongside, not duplicated here):
- Proto-design: [`design-multi-source-aggregation.md`](../design-multi-source-aggregation.md) — Shape A is its explicitly-deferred case.
- [ADR-0031](../../adr/adr-0031-multi-source-aggregation-target-schema.md) — shipped Shape B (v0.25.0); §"Why Shape B (not A)" enumerates exactly the three pieces this spike exercises.
- Roadmap §4 — the spec (why / what / 3 gotchas).

## What the spike did

A new build-tagged harness:
`internal/pipeline/shapea_spike_vstream_integration_test.go`
(`//go:build integration && vstream`).

- **Source.** A sharded Vitess keyspace via `vitess/vttestserver:mysql80`,
  `NUM_SHARDS=2` (shards `-80` / `80-`), keyspace `commerce`, table
  `customer` sharded by `customer_id` with a `hash` vindex, seeded with
  8 rows that Vitess scatters across both shards. Bootstrapped by
  re-declaring the proven pattern from
  `internal/engines/mysql/cdc_vstream_integration_test.go`
  (`startVTTestServerWithShards`) inside the pipeline package, because
  that helper is package-private to the mysql engine and the spike
  drives the *pipeline* package.
- **Target.** ONE consolidated `customer` table, exercised into BOTH a
  stock `postgres:16` testcontainer (cross-engine — the most
  informative case) AND a stock `mysql:8.0` testcontainer (same-engine
  sanity), table-driven over the two.
- **Topology.** Two per-shard consolidation passes (the proto-design's
  N-process model) into the single target table, each stamping a
  distinct `source_shard_id` discriminator value, exercising the
  composite-PK consolidation and the populated-target second pass.
- **Throwaway prototypes (clearly banner-marked, uncommitted-as-prod).**
  `injectShardColumnIntoSchema` (simulates the IR-stage transform) and
  `shardValueRowReader` (simulates value population). These are
  deliberately crude — no column-origin marker, no CDC path, no
  IdempotentRowWriter PK plumbing — *so the gaps are visible*. The real
  design must not promote them; see ADR-0048 Decision sections.

### Build-tag choice (justified)

Reuse the existing **`vstream`** tag, not a new `shapea` tag. The
spike's defining cost is the `vitess/vttestserver` image (~700 MB) —
exactly the cost `vstream` already gates per CLAUDE.md's build-tag
layering. A separate tag would fragment the heavy-image gate for zero
benefit. The harness is shaped (table-driven over targets, parameterised
per-shard pass) so it can become the permanent Shape-A integration
artifact under this same tag if the design is accepted: drop the
throwaway helpers, point at the real `internal/translate` transform.

### Reproduction

```
# Windows (Rancher Desktop env per CLAUDE.md):
$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
$env:TESTCONTAINERS_RYUK_DISABLED = "true"
go test -tags='integration vstream' -v -count=1 -timeout=30m `
  -run 'TestSpikeShapeA' ./internal/pipeline/...

# Compile-only validation (no Docker needed; what was run this session):
go vet -tags='integration vstream' ./internal/pipeline/
go build ./...                                  # bare build unaffected
gofumpt -l internal/pipeline/shapea_spike_vstream_integration_test.go
```

This session validated the harness **compiles and vets clean** under
`integration vstream`, that the bare build and the standard
`integration` tag are unaffected, and that gofumpt is clean. The
container-backed run requires a Docker host with the ~700 MB
vttestserver image; the harness is the deliverable artifact, the design
evidence below is from harness construction + code-reading the exact
production intervention points (preflight.go, migrate.go, redact.go as
the IR-stage analog, translate/mappings.go, add_table.go, schema_diff.go,
interfaces.go).

## Harness fidelity caveats (be honest about what the spike models)

1. **Two physical shard DSNs vs one source scattered by hash.**
   vttestserver scatters seeded rows across both shards via the hash
   vindex; the spike drives a single source DSN per pass and stamps a
   distinct discriminator per pass to *simulate* two physically-distinct
   shard sources landing in one target. The real topology is two
   physical shard endpoints, two `sluice sync start` processes. The
   consolidation pain (shared target, discriminator, populated-target
   preflight, composite PK) is faithfully reproduced; the per-shard
   *source isolation* is simulated. The ADR's design holds for both —
   the simulation is a harness-cost simplification, not a design gap.
2. **Bulk-copy path, not the snapshot→CDC handoff.** The spike drives
   the Migrator-equivalent bulk path because the per-shard VStream COPY
   + handoff is already covered by the engine-level vstream suite, and
   the Shape-A-specific pain is in *consolidation*, which the bulk path
   surfaces directly. CDC-path discriminator injection is analysed
   below (Piece 1) by code-reading the applier path; it is called out
   as the single largest residual design surface.

---

# Piece 1 — Discriminator-column injection (`--inject-shard-column NAME=VALUE`)

### Observed design pain

**Where injection belongs: the IR translation stage — confirmed.** The
codebase already has the exact precedent: `internal/translate` is a
"pure schema-rewrite pass that sits between the source-side SchemaReader
and the target-side SchemaWriter" (`mappings.go` package doc). It is
I/O-free, engine-agnostic, copy-on-write, validates against the schema,
and is called in `Migrator.Run` at step 1.5 (`translate.ApplyMappings`)
*before* schema-apply. PII redaction's value-side analog
(`pipeline/redact.go`, composed before per-engine `prepareValue`)
confirms the two-part shape: a **schema-stage** transform (add the
column to the IR `Table`) plus a **value-stage** transform (populate the
column on every row, on *all four* row-emitting codepaths). The spike's
throwaway `injectShardColumnIntoSchema` proves the schema half is a
clean ~15-line `internal/translate` pass; `shardValueRowReader` proves
the value half is a row-channel wrap structurally identical to
`redactRows`. **Recommendation: a new `internal/translate` pass
`InjectShardColumn(schema, name)` for the schema half + a value-stage
wrap mirroring `redactRows`'s four-codepath contract (bulk-copy, CDC
`Apply`, CDC `ApplyBatch`, backup-stream) for the population half.** No
regex, no DDL string-munging — IR-first tenet satisfied.

**Composite PK is mandatory and the spike confirms why.** Without
folding `source_shard_id` into the PK as the leading column, shard 2's
`customer_id=1` collides with shard 1's `customer_id=1` on the target's
primary key. The spike's `injectShardColumnIntoSchema` rewrites
`PrimaryKey.Columns` to `[shard, source_pk...]`; with that, the second
pass's 8 rows land cleanly alongside the first's (16 total, asserted).
**The consolidated PK MUST become `(shard, source_pk)`** — this is not
optional and not operator-configurable without breaking the disjointness
guarantee that the populated-target bypass (Piece 2) depends on.
Downstream consequence the spike surfaced: every secondary index and FK
that referenced the old PK shape now has a shard-prefixed PK to reason
about; `IdempotentRowWriter`'s `INSERT ... ON CONFLICT (pk)` clause
(used by add-table and the snapshot/CDC overlap absorber) must emit the
*composite* conflict target or the idempotent upsert silently
mis-targets. This is a real, named plumbing surface, not hand-waving.

**The IR column-origin marker is the load-bearing gap — confirmed by
code-reading the diff path.** `ir.Column` (schema.go) has no field
distinguishing a sluice-injected column from a source-derived one. The
closest precedent is `Column.SourceColumnType` (records pre-override
type for `--type-override` disambiguation) — a narrow, single-purpose
provenance field. `ir.SchemaDiff` (schema_diff.go) computes
`ColumnsExtra` purely by set-difference of column names between source
and target; `Summary()` renders it as "extra column(s)". **Without an
origin marker, `sluice schema diff` and `verify` against a consolidated
Shape-A target will ALWAYS report `source_shard_id` as an extra column
— permanent false drift on every diff/verify run.** That is precisely
roadmap §4 gotcha 1, and the spike confirms it is real (the spike's
schema-injection produces exactly the `ColumnsExtra` shape diff keys
on). **Recommendation: add `ir.Column.Origin` (a small sealed enum or a
`SluiceInjected bool` — the codebase prefers typed sums, see
`DefaultValue`; a bool is the lighter precedent matching
`GeneratedStored`). The diff/verify comparison must skip
sluice-injected columns on the target side when the source lacks them
(and assert they are present + NOT NULL, the inverse check).** This
field is the single highest-leverage IR change; the rest of Piece 1 is
mechanical given it.

### Open design question for sign-off (Piece 1)

- **CDC-path injection point.** Bulk-copy injection is a clean
  `redactRows`-shaped wrap. The CDC path is harder: `ChangeApplier.Apply`
  / `BatchedChangeApplier.ApplyBatch` consume `ir.Change` (Insert/
  Update/Delete carry `ir.Row` maps; Update carries Before+After;
  Delete carries Before). The discriminator must be stamped into
  *every* row-bearing change *and* into the Before/After PK-identity
  used to locate the target row for Update/Delete — otherwise an
  UPDATE from shard 2 with `customer_id=1` would match shard 1's row
  (composite-PK WHERE clause must include the shard value). Is the
  injection a `RedactorSetter`-style optional applier surface
  (`ShardColumnSetter`), or an orchestrator-side change-stream wrap
  before the applier? The redaction precedent (`applyRedactor` +
  per-engine `SetRedactor`) leans toward the engine-surface answer for
  CDC, orchestrator-wrap for bulk. **Decision needed: confirm the
  two-surface split (translate-pass for schema, value-wrap for bulk,
  optional applier-surface for CDC) or pick a single unified surface.**

---

# Piece 2 — Populated-target bulk-copy bypass

### Observed design pain

**The existing preflight refuses exactly as designed — and that's the
evidence.** `preflightColdStart` (preflight.go) probes every table via
`ir.TableEmptyChecker.IsTableEmpty` and refuses on the first non-empty
table with the Bug 9 recovery hint. The spike's second pass
(`expectPopulatedTarget: true`) hits this: shard 1 already populated
`customer`, so shard 2's cold-start is refused. The *only* existing
escape is `--force-cold-start`, which `preflightColdStart` honours by
**skipping the check entirely and silently** (`if force { ...; return
nil }`). The recovery hint for that flag literally says "will collide on
PRIMARY KEY in most cases — use with extreme caution". **This is the
silent-corruption hazard roadmap §4 gotcha 2 warns about**: an operator
who reaches for `--force-cold-start` to land shard 2 gets *no check*
that the discriminator actually guarantees PK disjointness. If the
shard-column value is wrong (operator typos `--inject-shard-column
source_shard_id=1` on the shard-2 stream too), shard 2's rows silently
overwrite shard 1's via the composite-PK upsert, or collide and abort
mid-copy — both are Bug-9-class failures the loud-fail tenet forbids.

**The bypass is correct IF AND ONLY IF the discriminator guarantees
disjointness — that conditional is the entire design.** The spike makes
this concrete: the second pass lands cleanly *because* the composite PK
`(source_shard_id, customer_id)` with a *distinct* `source_shard_id`
makes shard 2's `(2, 1)` disjoint from shard 1's `(1, 1)`. The
production design must not route around the preflight with the existing
silent `--force-cold-start`; it needs a **new discriminator-aware
preflight** that, when `--inject-shard-column` is set on a stream into a
populated target, *positively verifies* the safety conditions rather
than skipping the check.

### Recommended approach (Piece 2)

A new preflight branch — `preflightShardConsolidation` — that fires
when `--inject-shard-column NAME=VALUE` is set AND the target is
non-empty. It must **loudly assert**, before any data moves:

1. The target table already has the discriminator column (it was
   created by shard 1's stream) and it is **NOT NULL** for every
   existing row. A NULL discriminator on a pre-existing row means a
   prior non-shard-aware load contaminated the table → refuse loudly
   with an operator-actionable message (drop + reload, or pick a clean
   target).
2. The incoming stream's `VALUE` does **not already exist** among the
   target's `DISTINCT discriminator` values. If it does, this shard's
   data is already (partially) loaded, or the operator reused a shard
   value → refuse loudly ("shard value N already present on target;
   this stream would collide/double-load — verify --inject-shard-column
   VALUE is unique per shard").
3. The composite PK on the target includes the discriminator as a key
   column (structural check against the target catalog) — otherwise the
   disjointness guarantee doesn't hold and the bypass is unsafe.

Only when all three pass does bulk-copy proceed into the populated
target. The spike asserts the *positive* shape of these (16 rows, 2
distinct non-NULL shard values, zero NULL discriminators); the
production preflight asserts the same invariants *before* the load as a
refusal gate. This replaces `--force-cold-start`'s silent skip with a
loud, operator-actionable contract — loud-fail tenet satisfied. The
spike's harness captures both the refusal observation and the
clean-landing observation in `t.Logf` SPIKE OBSERVATION lines so a
greenlit implementation has the assertion shapes ready.

### Open design question for sign-off (Piece 2)

- **First-shard detection.** Shard 1's stream IS a legitimate cold-start
  into an empty target — it must NOT trip the new preflight. Shard
  2..N's streams trip it and must pass the three-point check. The
  natural discriminator is "target table empty → first shard, allowed;
  non-empty + `--inject-shard-column` set → run the three-point check".
  But a *resumed* shard-1 stream after a crash sees a non-empty target
  too. Does the preflight need to consult `sluice_cdc_state` /
  `sluice_migrate_state` to distinguish "I am shard 1 resuming" from "I
  am shard 2 starting"? **Decision needed: is the empty/non-empty +
  discriminator-value-presence check sufficient, or must the preflight
  also key on the per-stream state row (stream-id ∈ recorded shard
  values)?** The spike leans toward "discriminator-value-presence is
  sufficient and simplest" (if value N is absent, this stream is safe
  to load regardless of resume status, because the composite PK makes
  it disjoint), but this is a genuine correctness call for the human.

---

# Piece 3 — Cross-shard schema-migration coordination

### Observed design pain

**ADR-0030's add-table is structurally single-source — confirmed by
reading add_table.go.** `AddTable` takes one `SourceDSN` / one
`StreamID`, isolates one table from one source schema, and (PG path)
issues one `ALTER PUBLICATION ... ADD TABLE` against that one source.
Its target-side coordination (`preflightStream`, the Bug 46
`resolveAddTableTargetSchema` reconciliation, the cdc-state
`live_added_tables` filter-flip for MySQL) is all keyed on a single
`StreamID`. For Shape A, when the operator runs `ALTER TABLE customer
ADD COLUMN loyalty_tier INT` on the *source* keyspace, **every shard's
source sees the same DDL** (Vitess propagates schema across shards), so
**every shard's stream independently tries to apply the same ALTER to
the single shared consolidated target**. The spike's topology (two
streams, one target) makes the failure mode concrete: the first stream's
applier runs `ALTER TABLE customer ADD COLUMN loyalty_tier INT` on the
target and succeeds; the second stream's applier runs the *same* ALTER
and fails (column already exists) — or, worse, on engines/paths where
the ALTER is not idempotent, the second stream wedges. ADR-0030's
machinery has no cross-stream consensus; it assumes one stream owns the
target's schema.

**The N-process-per-shard model is right for data, but schema-DDL needs
exactly-once coordination.** ADR-0031 already decided N independent
processes (failure isolation, resource isolation, OS-level lifecycle —
all correct and the spike doesn't challenge that for the *data* path).
But schema migration is the one place where N independent processes
into a *shared* target collide: a CREATE/ALTER/DROP on the shared
consolidated table must happen **exactly once**, not N times. This is
the Shape-B-vs-Shape-A difference ADR-0031 §"Failure modes" flagged
("Target table created by source A is dropped on source B. Shape A only
— schema migrations need coordination across shards").

### Recommended approach (Piece 3)

**Single-writer schema coordination over the existing per-target control
table; N processes for data, 1 elected DDL-applier per schema change.**
Concretely: extend `sluice_cdc_state` (the per-target control table that
already keys on stream-id and already grew additive columns —
`source_dsn_fingerprint` ADR-0031, `slot_name` ADR-0030,
`live_added_tables` ADR-0034, `target_schema` Bug 46) with a
schema-migration coordination row/column such that the first shard
stream to observe a source schema change acquires a **DDL lease** (a
conditional UPDATE on a `schema_migration_token` column scoped to the
consolidated table identity, not the stream-id), applies the DDL to the
shared target exactly once, and records the applied schema version; the
other N-1 shard streams observe the recorded version, **skip the DDL
apply** (it's already done), and resume CDC against the migrated target.
This reuses the codebase's established pattern (additive control-table
column, idempotent migration, conditional-update coordination — the
same shape as ADR-0034's filter-flip poll) rather than introducing a
new coordination substrate (no etcd, no separate lock service — the
target database IS the coordination point, consistent with sluice's
"the target's control table is the source of truth" idiom). The DDL
lease is the minimal cross-stream consensus roadmap §4 piece 3 asks
for; it does not pull the N processes into one process (ADR-0031's
N-process decision stands for the data path).

### Open design question for sign-off (Piece 3)

- **Lease holder failure mid-DDL.** If the elected DDL-applier shard
  stream crashes *after* acquiring the lease but *before* the target
  ALTER commits (or before recording the applied version), the other
  N-1 streams block waiting for a version that never gets recorded.
  ADR-0007's position-and-data atomicity (DDL + version-record in one
  target tx) closes the "applied but not recorded" gap on engines with
  transactional DDL (PG); MySQL's implicit-commit DDL does NOT (the
  ALTER commits, then the crash, then no version recorded → next
  stream re-attempts the ALTER → "column exists" if idempotent-guarded,
  wedge if not). **Decision needed: is a lease-timeout + idempotent-DDL
  re-attempt (CREATE/ALTER ... IF NOT EXISTS, guarded) the contract, or
  does Shape-A schema coordination require a drained-stream model
  (ADR-0030 Strategy A: `sync stop --wait` all shards, run one
  cross-shard `schema add-table`/migrate, `sync start --resume` all)
  for v1, deferring live cross-shard schema migration to a Phase 2?**
  The spike's evidence leans toward **drained model for v1** (simplest,
  matches ADR-0030's own Phase-1 conservative-refusal precedent; live
  cross-shard schema migration is genuinely Phase 2) — but this is the
  single biggest scope decision for the human and the ADR surfaces it
  as a decision point rather than silently picking.

---

## Roadmap §4's five gotchas — confirmed / refuted

Roadmap §4 lists three gotchas explicitly; the proto-design + ADR-0031
contribute the rest of the "five pieces of machinery" framing. Status
against the spike:

1. **"Discriminator column needs an IR origin marker so diff doesn't
   flag an extra column."** — **CONFIRMED, and it is the single
   highest-leverage IR change.** Code-reading `schema_diff.go` proves
   `ColumnsExtra` is pure name set-difference; the spike's injected
   column produces exactly that shape. No marker → permanent false
   drift on every diff/verify. (Piece 1.)
2. **"Cold-start populated-target bypass is a sharp tool; getting it
   wrong = silent corruption."** — **CONFIRMED.** The only existing
   bypass (`--force-cold-start`) is a *silent skip*; the spike's
   second-pass observation shows the refusal fires correctly and that
   routing around it silently is the corruption hazard. The fix is a
   loud discriminator-aware preflight, not the silent flag. (Piece 2.)
3. **"Shape A is heavier than Shape B; waits for a real operator
   request."** — **CONFIRMED as a scoping fact, not refuted.** The
   spike quantifies the weight: one new `internal/translate` pass + one
   IR field + value-wrap on 4 codepaths + a new preflight branch + a
   control-table DDL-lease coordination column. Heavier than Shape B's
   `--target-schema`, lighter than a new orchestrator. The "wait for
   demand" gate is a product call, unaffected by the spike; the spike's
   job is to make the design *ready* when demand lands.
4. **(ADR-0031 framing) "ADR-0030 add-table is single-source; Shape A
   needs cross-stream consensus."** — **CONFIRMED.** `add_table.go` is
   structurally single-`StreamID`; the shared-target N-stream DDL
   collision is real. Minimal consensus = a control-table DDL lease
   (Piece 3); open question = lease-holder-failure contract.
5. **(proto-design framing) "Stream-id-aware bulk copy: shards can't
   conflict on PK because the discriminator makes the composite PK
   unique."** — **CONFIRMED, with a sharpened condition.** The spike
   shows this is true *only if* the composite PK actually leads with
   the discriminator AND the per-stream discriminator value is unique
   AND `IdempotentRowWriter`'s ON CONFLICT target is the composite key.
   The proto-design's one-liner is correct but understates the three
   plumbing preconditions, each of which the spike named.

None of the five were refuted. The spike *sharpened* 3 of them (added
the precise production intervention point or the precondition the
original framing understated) and confirmed the other 2 as stated.

## Net recommendation

Shape A is a **coherent, bounded design** with three genuine open
decision points (CDC injection surface; populated-target first-shard
detection; cross-shard DDL-lease failure contract / drained-vs-live for
v1). Each is surfaced explicitly in ADR-0048 as a decision point for
sign-off — none silently picked. The harness is ready to become the
permanent integration artifact under the existing `vstream` tag if
greenlit. No production code, no release/in-flight-v0.68.0 files, and
no git history were touched by this spike.
