# ADR-0060 — CDC apply-side schema-drift diff in refuse-loudly messages

## Status

**Accepted (2026-05-25), shipped v0.81.0 (target).** Closes Reddit-
research finding F11 (severity-A): when sluice's CDC apply-side
refuses to forward a source DDL, the refusal message now includes a
structured per-table drift report naming the specific columns,
indexes, and constraints that changed. Previously the message said
only "schema change detected on table X" and operators had to
manually `pg_dump --schema-only` both sides to figure out the
specific change.

This is the **operator-side** half of the schema-evolution story.
ADR-0058 (#45) is the **automation** half: when an operator opts in
via `--forward-schema-add-column`, sluice forwards the ALTER itself.
ADR-0060 covers the cases where sluice refuses — but does so
loudly enough that an operator can do the right thing without
debugging the diff themselves.

## Context

### The F11 finding (Reddit research)

The Reddit research catalog (task #40,
`C:\code\sluice-reddit-research-2026-05-23.md`) classified F11 as a
severity-A operator-pain class: across Debezium-users, Striim
forum, and AWS DMS community, operators reported the same
diagnostic gap. When a CDC tool refuses a source DDL, the operator
sees "stream stopped: schema change on table X" — but not WHAT
changed. The operator then has to:

1. Stop the stream cleanly (or accept it's already stopped).
2. Run `pg_dump --schema-only` on source.
3. Run `pg_dump --schema-only` on target.
4. `diff` the two outputs.
5. Find the specific column / type / constraint that drifted.
6. Decide what to do (manual migrate, alter target, drop stream, etc.).

Steps 2–5 are pure operator-time burn that the CDC tool could have
spent for them at refuse-time, since the tool already has both
schema snapshots in memory (the cached pre-DDL one and the post-DDL
one observed from the CDC reader's projection).

The trust failure mode F11 describes: an operator under outage
pressure, looking at "schema change detected" with no further
detail, will reach for "restart sluice and hope" rather than
diagnose. Even one silent-loss event from that path ends sluice's
credibility — per the tenets, **zero users is the current reality,
not a problem to rush past.**

### What sluice does today (pre-v0.81.0)

The pre-v0.79.0 refuse-loudly path (`internal/pipeline/
schema_forward_intercept.go`) surfaces messages of the form:

```
pipeline: forward schema add-column: shape drop-column on
"public.users" is out of --forward-schema-add-column scope
(v0.79.0 forwards ADD COLUMN only; ADR-0058 §1a documents the
scope split). recovery: drained model — run 'sluice sync stop
--wait', then run schema migrate, then resume.
```

The shape (`drop-column`, `rename-column`, etc.) is named, but the
specific column / index / constraint is not. Operators still have
to diff schemas to identify the drift.

The same applies to multi-shape combo refusals from
`ClassifyShape`: the error message says "multi-shape combo delta
(added=1 dropped=1 created-idx=1 dropped-idx=1 altered-col=false)"
— informative on the shape *counts* but silent on the column/index
*names*.

ADR-0058 (#45) shipped the auto-forward half for ADD COLUMN only.
For every other shape, refuse-loudly remained the right answer per
the loud-failure tenet — but the refusal didn't carry enough
information to act on.

## Decision

### 1. Add a structured per-table drift report to every refuse-loudly path

The pure-function diff evaluator `ir.DiffTable(pre, post *ir.Table)`
returns a `SchemaDriftReport` value naming the specific changes:

- `ColumnsAdded` / `ColumnsDropped` (name, type, nullability, default).
- `ColumnsAltered` (before/after type, nullable, default,
  generated-expr, with `AlterKinds` listing which aspects fired).
- `ColumnsRenamed` (old name, new name) — detected via the same
  full-attribute-match heuristic as the pipeline shape classifier
  (ADR-0054 v0.78.0); ambiguous cases (multi-add/drop, attribute
  mismatch) fall through to add/drop entries.
- `IndexesAdded` / `IndexesDropped` (name, columns, unique flag).
- `ChecksAdded` / `ChecksDropped` / `ChecksAltered` (name, expr).
- `ForeignKeysAdded` / `Dropped` / `Altered` (name, columns,
  parent reference).

All slices are deterministically ordered (alphabetical by
identifying name) so the rendered message is stable across runs —
operators paste these into tickets/chat, so determinism is
load-bearing.

### 2. Render with operator-action wording inline

The pipeline package owns the rendering pass via
`pipeline.RenderSchemaDriftReport`. Each drift entry produces a
single line of the form:

```
[<category>] <name> <details> — <operator action hint>
```

Examples:

```
[column-added] created_at Timestamp NULL — drained schema migrate
to add this column on the target before resuming; OR restart with
--forward-schema-add-column to auto-forward future ADD COLUMN
events (ADR-0058)

[column-dropped] legacy (was Varchar(100) NULL) — drained schema
migrate to drop this column on the target before resuming; DROP
COLUMN is destructive, no auto-forwarding

[column-altered] score (type Int32 → Int64, nullability NULL →
NOT NULL) — drained schema migrate to apply the change on the
target before resuming; ALTER COLUMN is not auto-forwarded

[index-added] unique index ix_users_email on (email) — drained
schema migrate to add the index on the target before resuming;
CREATE INDEX is not auto-forwarded (concurrent rebuild needs
operator scheduling)
```

The `[<category>]` prefix is greppable — operators in incident
triage filter on category. The per-line operator-action hint maps
each drift category to the safe recovery the operator should take:

| Category          | Hint kernel                                  |
| ----------------- | -------------------------------------------- |
| column-added      | drained migrate OR opt into --forward-schema-add-column |
| column-dropped    | drained migrate (destructive, no auto)       |
| column-renamed    | drained migrate (rename out of v1 scope)     |
| column-altered    | drained migrate (alter out of scope)         |
| index-added       | drained migrate (concurrent rebuild scheduling) |
| index-dropped     | drained migrate                              |
| check-added       | drained migrate + validate existing rows     |
| check-dropped     | drained migrate                              |
| check-altered     | drained migrate                              |
| fk-added          | drained migrate, NOT VALID + VALIDATE for big tables |
| fk-dropped        | drained migrate                              |
| fk-altered        | drained migrate                              |

### 3. Surface the rendering in BOTH refuse paths

`routeForwardBoundary` (the intercept's single-stream refuse path)
fold the rendered drift into the error message on two sites:

1. `ClassifyShape` returns an error (multi-shape combo) — the
   classify error itself names the class counts (`added=1
   dropped=1 …`); the drift rendering names the specific
   columns/indexes. Both go into the same error.
2. A recognized-but-refused shape (drop-column, rename-column,
   alter-column-*, create-index, drop-index) — the existing
   "shape X on Y is out of scope" message gets the drift rendering
   appended.

The drained-model recovery hint (`forwardRecoveryHint`) remains at
the END of the message so the rendered drift sits between the
shape-level summary and the recovery instructions. The recovery
hint is unchanged.

### 4. Engine neutrality + the projection-gap class

The `ir.DiffTable` function is engine-neutral: it compares IR
struct fields only, no engine-specific knowledge. When a source's
CDC projection drops information that schema diffs would normally
have, the drift entry's "after" field is empty or the comparison
trivially equates — examples:

- **pgoutput RelationMessage doesn't carry `attdefault`.** Post-DDL
  CDC SchemaSnapshots arrive with `Default = nil` for every column.
  A column with a DEFAULT in pre and `Default = nil` in post would
  surface as a spurious `ColumnAlterDefault`. We handle this in
  one place: the pipeline-side default prober (the same one
  ADR-0058 §2a introduced for the volatility refusal) is the
  source of truth when the in-band CDC default is nil. F11's drift
  evaluator currently doesn't have that prober wired in (it runs
  on the cached pre vs the in-band post and doesn't issue
  source-side queries); when projection-gap fields are nil on
  both sides, the diff doesn't fire. When they differ in actual
  IR-carried data, the diff surfaces correctly. The gap is
  documented and is a known v1 limitation — a follow-up can wire
  the default prober through if operators report false negatives.
- **MySQL TableMapEvent drops generated-column expressions.**
  Same shape as the default case. The cached pre side carries the
  full expression from the cold-start SchemaReader read; the post
  side from the CDC projection carries an empty string. Surfaced
  as a `ColumnAlterGeneratedExpr` on first boundary — but the
  rendered hint covers the operator action regardless. False-
  positive risk is acceptable because the hint is "ALTER
  generated-expr changed on `<column>` — drained migrate" and
  drained migrate is the right action even if the alleged change
  is actually a projection gap.

The ADR documents the projection-gap class explicitly so operators
who see a `[column-altered] X (generated-expr changed)` and find
no actual change in their source DDL can attribute it to the
projection gap rather than think they have a bug.

### 5. What this ADR is NOT

Out of scope deliberately:

- **No Prometheus drift counter.** The drift surfaces via the
  refusal message + slog at error level. A scrape-level
  `sluice_cdc_drift_total{table,kind}` is reasonable future work
  but the v1 deliverable focuses on operator-readability at the
  error path.
- **No per-cell auto-remediation.** F11 makes the refusal more
  useful; it does not add new auto-forwarding flags. ADR-0058's
  one-flag-per-shape rule still governs — every additional auto-
  forward shape goes through its own ADR with its own safety
  analysis.
- **No `sluice diagnose` integration.** A future enhancement could
  embed the drift report in `sluice diagnose --schema-drift`
  output; v1 just covers the live refuse path.
- **No rewrite of the existing refuse-loudly catalog.** F11
  augments the refusal text additively; the shape names, the
  scope-split language, and the recovery hint are unchanged.

## Consequences

### Positive

- **Operator action latency drops.** A refusal message that names
  the specific column means operators can act in one step
  (drained migrate the specific column) instead of five (stop,
  pg_dump source, pg_dump target, diff, identify, act).
- **Slack/ticket paste-friendliness.** The per-line greppable
  format with category brackets is the same shape that worked
  well for the Shape A holder-fenced log lines (ADR-0054 §6).
- **Bug 74 class-pin coverage.** The drift evaluator's test matrix
  exercises every drift category (Column add/drop/alter/rename;
  Index add/drop; Check add/drop/alter; FK add/drop/alter) so a
  future change to the diff logic can't silently miss a category.
- **No happy-path footprint.** ADR-0058 (#45) auto-forwarding
  behavior is unchanged — the existing 11+ tests still pass. The
  drift rendering only fires on refuse paths.

### Negative

- **Refusal messages get longer.** A multi-shape combo with N
  drift entries surfaces N+2 lines. Acceptable trade-off:
  operators in incident response WANT detail.
- **Projection-gap false positives possible on generated-expr and
  some default cases** (see §4). Mitigation: the hint is
  drained-migrate either way, so the action remains correct; the
  ADR documents the class.
- **Diff structure duplicates some logic with `ir.DiffSchemas`.**
  Two pure-function diff implementations now exist on the IR:
  `DiffSchemas` (schema-wide, `sluice schema diff` use case) and
  `DiffTable` (single-table, CDC refuse use case). They have
  different output shapes and different consumer requirements;
  collapsing them would force one consumer to compromise. The
  duplication is documented and constrained.

### Compatibility

- **Wire compatibility: unchanged.** No new IR struct exposed
  on-wire; `SchemaDriftReport` is an in-process value type used by
  the refuse-loudly error path.
- **Public API: additive.**  `ir.DiffTable` /
  `ir.SchemaDriftReport` and `pipeline.RenderSchemaDriftReport`
  are new exports; nothing existing changed signature.
- **Configuration: no new flags.**  The feature is always on —
  refuse-loudly messages always include the drift report. No
  feature flag, no behavior toggle. The output is purely
  informational; downstream programmatic error parsers should
  match on the existing shape-name prefix, not the new drift
  block.

## Implementation notes

- Pure-function diff lives in `internal/ir/schema_drift.go`
  (mirrors the existing `internal/ir/schema_diff.go` for
  `DiffSchemas`). The IR package stays self-contained — no
  pipeline imports.
- Rendering lives in `internal/pipeline/schema_drift_render.go`.
  Operator-action wording is pipeline-specific (references
  `--forward-schema-add-column`, `sluice sync start --resume`,
  etc.) so it can't move down into IR without circular layering.
- The intercept wiring is in
  `internal/pipeline/schema_forward_intercept.go` —
  `renderDriftForRefusal` is the single helper used by both refuse
  paths (`ClassifyShape` error and `refuseShapeOutOfV1Scope`).
- The unit-test matrix covers each drift category and the Bug 74
  multi-kind case (column with simultaneous type + nullability
  changes). The integration test exercises real source-side
  ALTERs against testcontainers PG and asserts the surfaced error
  contains the specific column names.

## Relationship to other ADRs

- **ADR-0058 (online schema-change forwarding, #45)**. Companion.
  ADR-0058 is "do it automatically when opted-in"; ADR-0060 is
  "tell the operator what changed when sluice refuses." Together
  they cover both forwarding gradients (auto + manual). The
  intercept's wiring uses the SAME `ClassifyShape` boundary as
  ADR-0058; the difference is what happens after classification.
- **ADR-0054 (Shape A live cross-shard DDL coordination)**.
  Source of the `ClassifyShape` catalog and the RENAME COLUMN
  detection heuristic ADR-0060's rename pairing mirrors.
- **ADR-0029 (sluice schema diff)**. The `ir.DiffSchemas` cousin.
  Different consumer, different output shape, same engine-neutral
  diff design principle.
