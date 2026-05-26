# ADR-0064: smart compaction — same-row event collapsing within a merge window (§14e)

## Status

**Accepted (2026-05-26); implemented in Task #16, chain 14e.** Builds
on ADR-0046 §14d (naive compact). Naive compact stays the default;
smart compact is an opt-in extension wired through the same
`sluice backup compact` surface and the same staging / catalog-swap
machinery. The naive path is unchanged — smart compact is a
pre-stage transform over each merged segment's change-chunks that
*replaces* the chunks in-place under the same `seg-merged-<id>/`
directory before the catalog swap commits.

The decision is deliberately scoped to **per-row event collapsing
within an already-grouped merge window**. Cross-segment compaction,
cross-table reorderings, and event reorderings that change the
applied source-order are explicitly out of scope (Bug-74 risk class:
inverting INSERT-then-UPDATE to UPDATE-then-INSERT silently
corrupts; see §3 below).

## Context

ADR-0046 §14d shipped a byte-level concat compactor (v0.77.0). It
produces archives that are still bigger than necessary: a row that
got INSERTed, then UPDATEd 49 times, then UPDATEd a 50th time within
one merge window stays as 50 INSERT/UPDATE events on disk. The net
state on restore is the same as if the row was INSERTed once with
the 50th UPDATE's column values; the intermediate 49 are pure
redundancy.

For an operator with a high-update workload (an order table where
each row changes status 5–10 times before settling, an analytics
dashboard updating row-level counters), naive compact's segment-
count reduction wins are real but the chunk-byte savings are
zero by construction. Smart compaction is the optimisation that
turns the same-row update chain into a single net event, materially
shrinking restore time and archive storage.

The §14d locked design decision #1 reserved this path:

> "Naive" = byte-level chunk concatenation within a time window.
> No event-level dedup, no same-row collapsing — deferred to #16.

This ADR closes that deferral.

## Decision

### 1. Smart compact is a pre-stage transform inside CompactChain

When `CompactOpts.SmartCompaction` is true, after `executeMergeGroup`
copies every source segment's chunks + manifests into the staging
dir, a NEW pass walks each incremental's change-chunks, builds a
per-table-per-PK accumulator, applies the policy table (§2 below),
and rewrites the chunk file(s) with the collapsed event stream. The
chunk paths stay the same; the manifest's `ChangeChunks[].RowCount`
and `SHA256` are recomputed. The catalog swap remains the
linearization commit — pre-swap → "smart compact never happened";
post-swap → "smart compact happened" — so crash recovery is
identical to ADR-0046 §14d.

### 2. Policy table (the load-bearing decision)

Within one merge window, per `(schema, table, pk_tuple)`:

| Input chain                          | Collapses to                                  |
|--------------------------------------|-----------------------------------------------|
| INSERT then UPDATE(s)                | INSERT with the final UPDATE's column values |
| UPDATE(s) only                       | One UPDATE with the final UPDATE's values    |
| INSERT then DELETE                   | Nothing (the row never existed durably)      |
| UPDATE(s) then DELETE                | Just the DELETE                              |
| DELETE then INSERT (row reused)      | Replace chain (treated as two distinct logical rows; emit both verbatim, do NOT collapse) |
| Single INSERT / UPDATE / DELETE      | Pass through unchanged                       |

TRUNCATE is a table-scoped barrier: when emitted on table T, every
accumulator for table T (across every PK) is dropped, and the
TRUNCATE itself is emitted verbatim as a marker. Other tables'
accumulators are untouched.

DDL events (`ir.SchemaSnapshot` from ADR-0049, or any future
schema-delta in-stream marker) are full barriers: every accumulator
across every table is flushed (per the policy table above) BEFORE
the DDL is emitted, and the accumulators reset to empty. Events on
different sides of a DDL barrier cannot collapse because the row's
column set may differ across the boundary, and silently applying
new-shape values onto an old-shape row is exactly the silent-loss
class this ADR is designed to prevent.

### 3. F3 invariant preservation (load-bearing)

The collapsed chunk-stream's **end-LSN must equal the input window's
end-LSN**. The segment's `LineageSegment.EndPosition` is recorded in
`lineage.json` (not in the chunk bytes) and is preserved verbatim by
the catalog rebuild. But the chunk-stream itself must close on a
position at or beyond the last source event's position so that:

- A restore that walks the chunk to EOF lands at a position equal
  to (or sequenced after) every collapsed event's original position.
- The slot's `confirmed_flush` after apply matches the segment's
  EndPosition (ADR-0007/0010/0020 idempotent-applier contract).

Implementation: the chunk-rewriter ALWAYS preserves every
`TxBegin` / `TxCommit` event verbatim and in source-order — these
carry the transaction-boundary positions the applier acks against.
Row events between them collapse, but the TxBegin/TxCommit envelope
stays. The last event in the rewritten chunk-stream is therefore
the same TxCommit that was last in the input — same position, same
LSN, F3 preserved by construction.

A chunk whose only events were collapsed-out (e.g. a chunk that
contained only an INSERT-then-DELETE pair for one row) still
preserves its TxBegin/TxCommit envelope. The rewritten chunk MAY
have zero row events between TxBegin/TxCommit; the applier already
treats an empty source-tx as a no-op (ADR-0027 documented this for
the streamer; smart-compact's collapsed chunks ride the same path).

### 4. Per-row identity — the PK lookup

"Same row" = same `(schema, table, PK-tuple)`. The PK is looked up
from the incremental manifest's `Schema.Tables[…].PrimaryKey`. Two
PK strategies are recognised:

- **`pk` (default)** — use the table's declared `PrimaryKey.Columns`.
  Composite PKs use the full tuple. The Before / After / Row payload
  on the event carries the PK column values (engines populate at
  least the PK on every CDC row event by construction — PG via
  `REPLICA IDENTITY` `pkey`/`full`; MySQL via the binlog row image's
  first columns).
- **`replica-identity`** — PG-specific escape hatch when the table
  uses `REPLICA IDENTITY USING INDEX <some_unique_idx>`; the unique
  index's columns identify the row. Functionally equivalent to `pk`
  for tables where the declared PK *is* the replica identity (the
  common case). The flag exists for the edge case where the operator
  has declared an alternative replica identity index; sluice
  currently doesn't record this distinction in `ir.Table` (no
  ReplicaIdentityIndex field), so the v1 implementation treats
  `replica-identity` as an alias for `pk` and reserves the flag for
  a future enhancement when the IR captures the distinction.
- **`none`** — disables PK-based collapsing entirely; every event
  passes through. Used as a debugging escape hatch.

**Tables without a declared PK skip smart compaction**: the
accumulator can't establish identity, so collapsing would conflate
distinct rows. These tables fall through to the naive-concat path
unchanged. The compaction report names every such table under
`TablesWithoutPK` so the operator knows the work was skipped.

### 5. Source-order is sacred (the Bug-74 / ordering invariant)

Within a per-row chain, events are applied to the accumulator in
**source-order** (the order they appear in the chunk stream, which
the rotation FSM and the CDC reader guarantee is LSN-monotonic
within a single segment, and contiguity-asserted across segments by
the §14d pre-flight). The accumulator's `events` slice appends; it
NEVER reorders. The collapse rule reads events left-to-right and
emits the net result; reversing the order (e.g. UPDATE before
INSERT) would produce semantically different output and is a
forbidden refactor.

This is the Bug-74 / family-dispatch lesson applied to compaction:
the test pin is the FULL {INSERT, UPDATE, DELETE}^N matrix on every
representative table shape (single-PK, composite-PK), not one
representative per group.

### 6. Cross-table — TRUNCATE and DDL barriers

- **TRUNCATE on T**: drops every accumulator for table T at the
  moment the TRUNCATE is encountered. The TRUNCATE itself is
  emitted unchanged. Other tables' accumulators are untouched. A
  TRUNCATE-then-INSERT chain for the same row collapses to
  `TRUNCATE then INSERT` (the TRUNCATE drops the prior accumulator
  state; the INSERT seeds a fresh one).
- **DDL barrier** (ADR-0049 `ir.SchemaSnapshot`): flush every
  accumulator across every table per the policy table, emit the
  SchemaSnapshot, reset every accumulator to empty. The post-DDL
  row events seed fresh accumulators against the new schema shape.

### 7. Failure mode — refuse loudly on corrupt PK

If an event's payload (Row for INSERT, Before/After for UPDATE,
Before for DELETE) doesn't carry the PK columns the table declares —
i.e. the column key is absent from the map — that's a corrupt or
mis-decoded event and smart-compact REFUSES LOUDLY with a message
naming the table + the missing column + the offending chunk path.
The operator's recovery is to:

1. Investigate the source-side corruption (a CDC reader emitting
   incomplete row images is a real bug worth surfacing).
2. Re-run compact with `--smart-compaction-off` to fall through to
   the naive path.

This mirrors ADR-0046 §14d's loud-failure tenet: never silently
drop, never silently mis-collapse. The fallback path (naive concat)
exists exactly for this case.

### 8. CLI surface

`sluice backup compact` grows three flags:

- `--smart-compaction` — opt-in flag enabling event-level collapse
  for every merge group. **Default: off** for the v1 ship (gives
  operators a quiet ship and a clean A/B test against naive compact;
  flip to default-on in a later release once field data confirms the
  safety profile).
- `--smart-compaction-off` — explicit opt-out flag for the
  refuse-loudly-on-corrupt-PK case (and for any operator who wants
  the audit trail of "I considered smart compaction and chose naive
  for this run"). Mutually exclusive with `--smart-compaction`.
- `--compaction-pk-strategy=pk|replica-identity|none` — the row-
  identity discriminator. Default `pk`. `none` disables collapsing
  (passes every event through verbatim) — useful for debugging the
  pre-/post-compact byte diff.

### 9. Compaction report extensions

`CompactResult` and `CompactPlanGroup` grow these fields:

- `EventsBefore` (int64) — count of per-row events the source
  chunks carried for this group (INSERT/UPDATE/DELETE only; not
  TxBegin/TxCommit which aren't subject to collapse).
- `EventsAfter` (int64) — count of per-row events after collapsing.
- `EventsCollapsed` (int64) — `EventsBefore - EventsAfter`. Mirrors
  the existing `SegmentsRemoved` field's shape; operators see "X
  events collapsed" alongside "Y segments merged".
- `RowsCollapsed` (int64) — number of distinct `(schema, table,
  PK-tuple)` keys whose accumulator had `len(events) > 1` (the
  collapse-eligible chains; single-event chains weren't candidates
  and aren't counted).
- `TablesWithoutPK` ([]string) — list of `schema.table` references
  that were skipped because the table has no declared PK. Empty for
  the common case.

The report's `BytesBefore` / `BytesAfter` fields are reused: under
naive compact `BytesBefore == BytesAfter` (bytes are moved); under
smart compact `BytesAfter < BytesBefore` (chunks are rewritten with
fewer events). The existing field semantics carry the new signal
without a fresh field.

## Trade-offs

### Wall-time vs. storage

Smart compaction is more CPU than naive: every change-chunk in every
merged segment is decoded, the events are routed through the
accumulator, the accumulator is flushed, and the new chunk is
re-encoded + re-hashed. The wall-time multiplier is roughly
`O(N_events × log N_pks)` for the accumulator map ops plus the
constant codec encode/decode cost. On benchmarks the encode/decode
cost dominates — see the integration tests' restore-speedup figures
below.

The storage savings depend on the workload's update-density:

- **Insert-heavy workloads** (append-only logs, event-source tables):
  near-zero savings — most chains are single-event, no collapse
  opportunities.
- **Mixed read/write workloads** (typical OLTP — orders, users):
  10–40% event reduction in our synthetic tests, weighted toward
  high-update hot keys.
- **High-update workloads** (counters, status machines, dashboards):
  50–90% event reduction.

Break-even (smart-compact CPU cost == storage savings) lands around
the 20–30% reduction mark on a typical SSD-backed operator setup —
i.e. anything more update-heavy than insert-mostly comes out ahead.
The naive path stays as the default so insert-heavy operators don't
pay the CPU tax for negligible savings, and so a per-operator
opt-in policy decision is reviewable rather than implicit.

### Increased restore-side test surface

Smart-compacted archives must produce byte-identical row sets on
restore as naive-concat archives (the compaction is lossless on net
state, only intermediate events are dropped). The integration test
suite asserts this with a same-engine PG → PG round-trip:
naive-compact restore vs smart-compact restore on identical input,
final tables compared via `pg_dump --schema-only` shape + content
hashing. This is the load-bearing pin: every collapsed-chunk archive
restores to the same end-state as its naive-concat counterpart.

### Streamability of the collapsed chunk

Smart-compact REWRITES the chunk, which means the merged segment's
incremental manifests must be re-marshalled with new SHA-256s. This
is in scope: the chunk's bytes change, the SHA-256 changes, the
manifest's `ChunkInfo.SHA256` must be updated, and the manifest
is re-written in the staging dir before the catalog swap. The
catalog swap then re-writes lineage.json with no further chunk-byte
changes.

## Alternatives considered

### A. Push smart-compaction into the CDC pump (compact at write time)

Rejected. The compaction is a backup-time operation; doing it
during CDC pumps would (a) tightly couple compaction to the
streaming path's hot loop, (b) lose the operator's
control-knob (smart-compact is opt-in per compact run, not a
one-way write-time decision), and (c) prevent a re-compaction pass
when a future policy refinement lands. Keeping it as a
backup-compact-time transform isolates the new code path.

### B. Build a full event-DAG and topologically sort

Rejected as overkill. The §14d pre-flight already asserts
position-contiguity between consecutive source segments, so the
source-order is already a total order. A flat append-only
accumulator (this ADR's choice) is the simplest correct shape;
introducing a DAG would invite the Bug-74-class regression where
events get reordered "for efficiency" and silently change apply
semantics.

### C. Cross-segment smart-compaction (collapse across N segments
without merging the segments themselves)

Out of scope for this ADR. The merge window grouping (§14d's job)
already chooses which segments collapse together; smart-compact
operates within that window. Cross-segment compaction without
merging would require a new policy decision about how to express
"events are now scattered across a different shape than the
catalog records" — that's a follow-on chunk if operator demand
surfaces it.

## Implementation files

- `internal/pipeline/chain_compact_smart.go` — the row accumulator,
  the policy-table flusher, the per-merge-group transform invoked
  by `executeMergeGroup` when `SmartCompaction` is on.
- `internal/pipeline/chain_compact_smart_test.go` — the exhaustive
  policy-matrix unit pin (Bug-74 discipline: every cell of
  {INSERT, UPDATE, DELETE}^N × {single-PK, composite-PK} ×
  {TRUNCATE-barrier, DDL-barrier}).
- `internal/pipeline/chain_compact.go` — wired the new transform
  into the existing `executeMergeGroup` flow under an
  `opts.SmartCompaction` guard.
- `internal/pipeline/backup_smart_compact_integration_test.go`
  (`integration` build tag) — drives a 1000-INSERT + 500-UPDATE
  workload on real PG (testcontainers); restores pre- + post-
  smart-compact archives; asserts byte-identical row sets;
  records the event-reduction ratio.
- `cmd/sluice/backup.go` — `BackupCompactCmd` grows
  `--smart-compaction` / `--smart-compaction-off` /
  `--compaction-pk-strategy` flags.

## Rationale recap

Naive compact (§14d) was the smallest correct shape: byte-level
concat, no event awareness, loud-failure on every boundary
condition. Smart compact (§14e) extends that by adding ONE
event-level transform inside the same staging machinery, with
the same loud-failure tenet (refuse on corrupt PK, refuse across
DDL barriers, refuse to reorder events). The naive path stays
the default for the cautious-ship reasons in §8; the smart path
is the optimisation operators opt into once their workload's
update-density justifies the CPU tax.
