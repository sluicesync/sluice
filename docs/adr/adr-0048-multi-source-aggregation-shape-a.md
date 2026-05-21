# ADR-0048 — Multi-source aggregation Shape A (sharded → consolidated)

**Status:** **Accepted (design-only; implementation demand-gated per
roadmap §4).** This ADR is the design pass *before* code. It was
produced from a sharded-Vitess → consolidated-target spike (harness:
`internal/pipeline/shapea_spike_vstream_integration_test.go`, build tag
`integration vstream`; design evidence:
[`docs/dev/notes/prep-multi-source-shape-a.md`](../dev/notes/prep-multi-source-shape-a.md)).
All three decision points in §"Decision points requiring sign-off"
RESOLVED: DP-2 (discriminator-value-presence) and DP-3 (drained model
for v1) 2026-05-16; DP-1 (two-surface split — (a)) 2026-05-21.
Acceptance commits to the design; implementation waits for a concrete
operator workload per roadmap §4 demand-gate. Roadmap §4. Extends
[ADR-0031](adr-0031-multi-source-aggregation-target-schema.md) (shipped
Shape B, v0.25.0, which explicitly deferred Shape A) and builds on the
control-table additive-column pattern of
[ADR-0030](adr-0030-mid-stream-live-add-table.md) /
[ADR-0034](adr-0034-mysql-phase-2-live-add-table.md) and the
position-and-data atomicity of [ADR-0007](adr-0007-position-persistence.md).

## Context

Sluice shipped Shape B (microservices → analytics, per-source
`--target-schema` namespacing) in v0.25.0. Shape A — N
**functionally-identical** sources (same schema, sharded by key/hash —
e.g. PlanetScale Vitess shards) consolidated into **one** target table
— was explicitly deferred by ADR-0031 §"Why Shape B (not A)" because it
needs three pieces of machinery sluice does not have: discriminator-
column injection, a populated-target bulk-copy bypass, and cross-shard
schema-migration coordination. Roadmap §4 records the deferral and its
three gotchas.

A spike built a real sharded-Vitess (`vttestserver`, shards `-80`/`80-`,
table `customer` sharded by `customer_id`) → consolidated-target
(cross-engine PG **and** same-engine MySQL) harness to surface the
*actual* design pain rather than theorize. This ADR is the design that
spike evidence supports. The full per-piece observed pain, the harness
fidelity caveats, and the five-gotcha confirmed/refuted table are in the
prep doc; this ADR carries the decision.

Tenet alignment, established up front:
- **IR-first.** Discriminator injection is an `internal/translate` pure
  IR pass (the established home for schema-rewrite passes — same place
  as `ApplyMappings`) plus a value-stage wrap mirroring `redactRows`.
  No regex over DDL, no engine-specific imports in the orchestrator.
- **Loud-fail beats silent corruption.** The populated-target bypass
  replaces `--force-cold-start`'s *silent skip* with a *loud,
  operator-actionable, three-point preflight*. This is the sharpest
  tool in Shape A and the tenet is paramount here.
- **Contain Postgres complexity / clean named concepts.** Cross-shard
  schema coordination reuses the existing per-target control table as
  the coordination point (no new lock substrate); the IR origin marker
  is one small, named field.

## Decision

### 1. Discriminator-column injection — an IR-stage transform, two halves

A new CLI flag `--inject-shard-column NAME=VALUE`, mirrored on each
shard's `sluice sync start` / `sluice migrate`, drives two transforms:

- **Schema half — `internal/translate.InjectShardColumn(schema, name)`.**
  A pure, I/O-free, copy-on-write IR pass (structurally identical to
  `ApplyMappings`) that appends an `ir.Integer{Width:64}` NOT NULL
  column `name` to every table and **folds it into the primary key as
  the leading key column**, so the consolidated PK becomes
  `(shard, source_pk...)`. Called in the orchestrator immediately after
  `ApplyMappings` / `ApplyExpressionOverrides`, before schema-apply and
  before the cross-engine supportability preflight.
- **Value half — a row-channel wrap.** The discriminator `VALUE` is
  stamped onto every row on **all four** row-emitting codepaths, exactly
  the contract PII redaction established (`docs/dev/notes/prep-pii-
  redaction-phase-1.md`): bulk-copy (`copyTable` / parallel),
  CDC `Apply`, CDC `ApplyBatch`, and backup-stream. For bulk-copy the
  wrap is orchestrator-side (mirrors `redactRows`); for CDC the wrap is
  an optional engine applier surface (mirrors `RedactorSetter` /
  `applyRedactor`) — see decision point DP-1.

**The composite PK `(shard, source_pk)` is mandatory and not
operator-configurable.** It is the disjointness guarantee the
populated-target bypass (Decision 2) depends on. Consequent plumbing
(explicitly in scope, not hand-waved): `IdempotentRowWriter`'s
`INSERT ... ON CONFLICT (pk) DO UPDATE` conflict target and the
Update/Delete WHERE-clause PK identity must use the **composite** key,
or the idempotent upsert and CDC row-location silently mis-target across
shards.

### 2. The IR column-origin marker (the load-bearing IR change)

Add a provenance marker to `ir.Column` distinguishing a
**sluice-injected** column from a **source-derived** one. Precedent:
`Column.SourceColumnType` (a narrow single-purpose provenance field) and
the bool `GeneratedStored`. The proposed shape is a single bool
`Column.SluiceInjected` (lighter than a sealed enum; the codebase
prefers typed sums only where multiple variants exist —
`DefaultValue` — and here there are exactly two states).

`ir.SchemaDiff` (and `verify`) compute `ColumnsExtra` by pure
column-name set-difference; **without this marker, `sluice schema diff`
/ `sluice verify` against a consolidated Shape-A target report the
discriminator as a permanent "extra column" — false drift on every
run.** The diff/verify comparison must:
- **Skip** a target column from the `ColumnsExtra` set when it is
  `SluiceInjected` and absent on the source.
- **Inversely assert** the injected column *is* present on the target
  and NOT NULL — its absence/nullability becomes the real drift signal.

This single field is the highest-leverage change; the rest of Piece 1
is mechanical given it.

### 3. Populated-target bulk-copy — a loud discriminator-aware preflight, NOT `--force-cold-start`

Shard 1's stream is a normal cold-start into an empty target. Shard
2..N must bulk-copy *into a target shard 1 already populated*. Today's
only escape is `--force-cold-start`, which `preflightColdStart`
honours by **silently skipping the check entirely** — precisely the
silent-corruption hazard roadmap §4 gotcha 2 names (a mis-set shard
value → cross-shard overwrite or mid-copy abort, Bug-9 class).

Introduce `preflightShardConsolidation`, fired when
`--inject-shard-column NAME=VALUE` is set AND the target is non-empty.
It **loudly refuses** unless all three hold, *before any data moves*:

1. The target table has the discriminator column and it is **NOT NULL
   for every existing row** (a NULL ⇒ a prior non-shard-aware load
   contaminated the table).
2. The incoming stream's `VALUE` is **not already present** among the
   target's `DISTINCT discriminator` values (presence ⇒ this shard is
   already loaded or the operator reused a shard value → collide/double-
   load).
3. The target's composite PK structurally **leads with the
   discriminator** (otherwise the disjointness guarantee is void and the
   bypass is unsafe).

Only then does bulk-copy proceed. This is the bypass — but it is a
*positive, loud, three-point assertion*, not a silent skip. The spike
asserts the positive shape (16 rows, 2 distinct non-NULL shard values,
zero NULL discriminators); the production preflight asserts the same
invariants as a refusal gate.

### 4. Cross-shard schema-migration coordination — a control-table DDL lease

ADR-0030's add-table is structurally single-`StreamID` (single source
DSN, single publication ALTER). When the operator alters the source
keyspace schema, **every shard's stream sees the same DDL and would
independently apply it to the single shared target** — the first
succeeds, the rest fail ("column exists") or wedge (non-idempotent DDL).

Coordinate via the **existing per-target control table**
(`sluice_cdc_state`) — the same additive-column / idempotent-migration /
conditional-update pattern ADR-0030/0034/0031 and Bug 46 already use.
The first shard stream to observe a source schema change acquires a
**DDL lease** (conditional UPDATE on a coordination column scoped to the
*consolidated-table identity*, not the stream-id), applies the DDL to
the shared target **exactly once**, and records the applied schema
version; the other N-1 streams observe the recorded version, **skip the
DDL apply**, and resume CDC against the migrated target. The target
database remains the coordination point (consistent with sluice's "the
target control table is the source of truth" idiom — no etcd, no
separate lock service, N-process data path from ADR-0031 unchanged).

## Decision points requiring sign-off (Proposed → Accepted gate)

The spike surfaced three genuine design questions. They are **not
silently picked**; each needs an explicit owner decision before
implementation.

- **DP-1 — CDC-path injection surface.** Bulk-copy injection is a clean
  orchestrator-side `redactRows`-shaped wrap. CDC is harder: the
  discriminator must be stamped into every row-bearing `ir.Change`
  *and* into the Before/After PK-identity used to locate the target row
  for Update/Delete (composite-PK WHERE must include the shard value).
  Options: (a) an optional engine applier surface
  (`ShardColumnSetter`, mirroring `RedactorSetter`/`applyRedactor`) —
  the redaction precedent's answer for CDC; (b) an orchestrator-side
  change-stream wrap before the applier; (c) a single unified surface
  for both bulk and CDC. **Spike lean:** the two-surface split
  (translate-pass for schema, value-wrap for bulk, optional
  applier-surface for CDC) — it matches the redaction precedent exactly.
  **Owner to confirm or override.** — **RESOLVED 2026-05-21: option
  (a), the two-surface split** (translate-pass schema + value-wrap bulk
  + optional `ShardColumnSetter` applier surface CDC). Owner-confirmed
  after a code-grounded re-examination of the (a)/(b)/(c) tradeoff —
  see [`docs/dev/notes/adr-0048-dialogue-prep.md`](../dev/notes/adr-0048-dialogue-prep.md)
  for the dialogue summary. Three findings sharpened the call: (i) the
  current `ir.Update`/`ir.Delete` carry FULL-row `Before`/`After` maps
  (`map[string]any`) and both engines build WHERE from every column via
  `buildWhereClause` (mysql `change_applier.go:1068`+, postgres
  `change_applier.go:1398`+) — so there is no engine-specific identity
  tuple today; (b)'s "layering inversion" objection is weaker than the
  ADR originally implied (orchestrator-side mutation of the maps would
  work), and the real argument for (a) is the redaction precedent's
  already-shipped, already-pinned shape. (ii) (c)'s real cost is bigger
  than first presented: introducing `ir.Change.Key` only pays off if
  WHERE construction also migrates from full-row predicates to
  key-only — a deliberate refactor of the most correctness-critical
  surface, and the current full-row WHERE carries hard-won Bug-6-class
  robustness (cf. the `logZeroRowsAffected` infrastructure). (iii) Per
  the CLAUDE.md zero-users tenet, demand-gating is real: confirming (a)
  commits to the design only; no implementation until a concrete
  operator workload forces Shape A. If/when an independent forcing
  function appears for unified change-identity (an upsert/dedup
  workload that demands key-only WHERE, an ADR-0050 implementation
  finding, multi-source consolidation evidence), the unified-`Key`
  refactor becomes its own co-equal ADR at that point — not pre-built
  on speculation.

- **DP-2 — populated-target first-shard detection.** Shard 1's
  legitimate cold-start into an empty target must NOT trip
  `preflightShardConsolidation`; shard 2..N must. "Target empty ⇒ first
  shard; non-empty + `--inject-shard-column` ⇒ run the three-point
  check" works — but a *resumed* shard-1 after a crash also sees a
  non-empty target. Must the preflight also consult
  `sluice_cdc_state` / `sluice_migrate_state` (stream-id ∈ recorded
  shard values) to distinguish "shard 1 resuming" from "shard 2
  starting"? **Spike lean:** discriminator-value-presence is sufficient
  and simplest — if `VALUE` is absent on the target, the stream is safe
  to load regardless of resume status (composite PK makes it disjoint),
  so no state-row consultation is needed. **This is a correctness call
  for the owner.** — **RESOLVED 2026-05-16: discriminator-value-presence
  only** (owner-confirmed; correctness rests on the mandatory composite
  PK making each shard disjoint regardless of resume status — no
  `sluice_cdc_state`/`migrate_state` consultation).

- **DP-3 — cross-shard DDL coordination: live vs drained for v1, and
  lease-holder-failure contract.** If the elected DDL-applier crashes
  after acquiring the lease but before the target ALTER + version-record
  commit, the other N-1 streams block. ADR-0007's atomicity closes the
  "applied-but-not-recorded" gap on transactional-DDL engines (PG);
  MySQL's implicit-commit DDL does not. Options: (a) lease-timeout +
  idempotent-DDL re-attempt (guarded `... IF NOT EXISTS`) as the v1
  contract; (b) **drained model for v1** (ADR-0030 Strategy A:
  `sync stop --wait` all shards → one cross-shard schema migrate →
  `sync start --resume` all), deferring *live* cross-shard schema
  migration to a Phase 2. **Spike lean:** drained model for v1 — it is
  the simplest, mirrors ADR-0030's own Phase-1 conservative-refusal
  precedent, and live cross-shard schema migration is genuinely a
  Phase 2 problem. **This is the single biggest scope decision; owner
  to set the v1 boundary.** — **RESOLVED 2026-05-16: drained model for
  v1** (owner-confirmed; ADR-0030 Strategy-A precedent — `sync stop
  --wait` all shards → one cross-shard schema migrate → `sync start
  --resume`; *live* cross-shard schema migration + the lease-holder-
  failure contract on non-transactional-DDL MySQL is deferred to a
  Phase 2). Decision 4's control-table DDL-lease design is retained as
  the Phase-2 target, not v1.

## Consequences

**Positive.** Closes the last outstanding multi-source shape with a
bounded, IR-first, loud-by-default design. Discriminator injection
reuses the `translate` + `redactRows` precedents (no new architecture);
the origin marker is one small field; the populated-target bypass turns
a silent footgun into a loud contract; cross-shard coordination reuses
the control-table substrate (no new lock service, N-process data path
preserved).

**Costs / residual edges (each gets an explicit loud branch per the
tenet).**
- The composite-PK rewrite ripples into `IdempotentRowWriter` ON
  CONFLICT targets and CDC Update/Delete WHERE identities — named,
  in-scope plumbing, not silent.
- `verify`/`diff` must learn the origin marker or every consolidated
  diff is false drift — addressed by Decision 2; a regression here is
  high-severity (operator can't trust diff) and gets a pinned test.
- Cross-shard DDL coordination's failure contract (DP-3) is genuinely
  hard on non-transactional-DDL engines; the drained-v1 option exists
  precisely to keep v1 loud and simple.

**Neutral.** Shape B (`--target-schema`) is untouched (no regression).
Single-source migrate/sync is untouched (the flag is opt-in; nil/empty
is a zero-cost passthrough exactly like `Redactor`). MySQL and PG
targets both supported (the spike exercised both); the discriminator is
engine-neutral IR.

## Alternatives considered

- **Regex/DDL-string injection of the shard column.** Rejected — violates
  the IR-first tenet outright; `internal/translate` is the established
  pure-IR home and the spike proved a ~15-line pass suffices.
- **`--force-cold-start` as the populated-target answer.** Rejected — it
  is a *silent skip*; the spike's second-pass observation is the
  evidence that this is the corruption hazard the loud-fail tenet
  forbids. The discriminator-aware three-point preflight is the
  loud replacement.
- **Single-process multi-source orchestrator for cross-shard schema
  coordination.** Rejected — ADR-0031 already decided N processes for
  data (failure/resource isolation); the spike does not challenge that.
  Only the DDL step needs exactly-once coordination, and a control-table
  lease delivers it without collapsing the process model.
- **No IR origin marker; teach `diff`/`verify` to special-case the flag
  value.** Rejected — the marker is provenance that belongs in the IR
  (precedent: `SourceColumnType`); threading a CLI flag value into the
  diff engine leaks orchestrator config into `internal/ir` and breaks
  on backup/restore where the flag isn't present but the column is.
- **A new external lock service (etcd/consul) for cross-shard DDL.**
  Rejected — the target control table is already sluice's coordination
  point everywhere else; a new substrate contradicts "contain
  complexity" and adds an operational dependency.

## Scope / non-goals

**In (v1, pending sign-off):** `--inject-shard-column NAME=VALUE`; the
`internal/translate.InjectShardColumn` schema pass; the value-stage wrap
on the four codepaths (CDC surface per DP-1); `ir.Column.SluiceInjected`
+ diff/verify origin awareness; composite-PK rewrite + the
`IdempotentRowWriter`/CDC-identity plumbing; `preflightShardConsolidation`
(the loud three-point bypass); cross-shard DDL coordination (live or
drained per DP-3).

**Out:** Shape C multi-master (ADR-0031 / proto-design — different
product, requires conflict resolution); cross-source temporal ordering
(per-source ordering only, unchanged — Lamport/vector clock is a
different topology); single-process multi-source orchestrator (ADR-0031
non-goal stands); per-table renaming (roadmap §9, demand-gated);
operator-configurable non-composite PK (the composite PK is the
disjointness guarantee — not optional). Any chunk-format/serialization
change (orthogonal).

## Testing

The spike harness
(`internal/pipeline/shapea_spike_vstream_integration_test.go`,
`integration vstream`) becomes the permanent Shape-A integration
artifact if greenlit — drop the throwaway `injectShardColumnIntoSchema`
/ `shardValueRowReader` helpers, point at the real `internal/translate`
pass. Test surface on implementation:

- Sharded Vitess (`vttestserver`, 2 shards) → consolidated PG **and**
  consolidated MySQL: N per-shard streams land disjoint via the
  composite PK; consolidated count + distinct non-NULL discriminator
  values asserted (the spike's existing assertions).
- `preflightShardConsolidation`: empty target + first shard → allowed;
  non-empty + absent shard value → allowed; non-empty + **present**
  shard value → **loud refusal**; existing NULL discriminator → **loud
  refusal**; composite-PK-missing → **loud refusal**.
- `sluice schema diff` / `verify` against a consolidated target reports
  **in sync** (the injected column is NOT flagged as extra) — the
  regression pin for Decision 2; high severity per the tenet.
- CDC path (DP-1 dependent): an Update/Delete from shard 2 with a PK
  value that also exists on shard 1 modifies **only** shard 2's row
  (composite-PK identity correctness).
- Cross-shard schema migration (DP-3 dependent): source ALTER applied
  exactly once to the shared target; N-1 streams skip cleanly; (drained
  variant) `sync stop --wait`→migrate→`sync start --resume` round-trips.
- Regression guards: single-source migrate/sync unaffected (flag
  opt-in, zero-cost when unset); Shape B `--target-schema` unaffected.

## Sequencing

Design-first: **Proposed → owner resolves DP-1/DP-2/DP-3 → Accepted →
implementation.** Do not implement before sign-off. Demand-gated per
roadmap §4 (waits for a concrete operator workload) — this ADR + the
prep doc + the ready harness make the design *implementable on demand*,
not *implemented now*. Estimated on greenlight: one `internal/translate`
pass + one `ir.Column` field + value-wrap on 4 codepaths + one preflight
branch + one control-table coordination column ≈ 600–1000 LOC incl.
tests + this ADR moved to Accepted. Touches the CDC apply path and a
control-table migration → the push-first / CI-Integration-green-before-
tag release discipline applies. Pairs with no in-flight ADR; independent
of the v0.68.0 ADR-0047 work.
