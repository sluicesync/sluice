# ADR-0178: An analytical target class — `ir.ChangeSink` as the sibling of `ChangeApplier`

## Status

**Proposed (Discovery — design sketch, no adoption).** Filed 2026-07-21 from a question about whether sluice should offer warehouse destinations (BigQuery et al.), prompted by the Supabase Pipelines public alpha. **Nothing here is built and none of it should be built on current demand** — see the roadmap entry, which keeps warehouse targets demand-gated with the parquet path as the interim answer.

This ADR exists because "add a BigQuery engine" is the wrong mental model and would be an expensive thing to discover mid-implementation. It records what the *right* model looks like, so that if demand arrives the design starts from here rather than from a `RowWriter` implementation that cannot work.

BigQuery primitives cited below were verified against Google's documentation (Storage Write API stream types/offsets; table-schema modification limits), not assumed.

## Context

### Why `ChangeApplier` cannot be implemented for a warehouse

sluice's CDC target contract is `ir.ChangeApplier` (`internal/ir/interfaces.go:1544`). Three of its properties are load-bearing for OLTP targets and unimplementable on an analytical one:

**1. Idempotent upsert.** ADR-0010 makes the applier `INSERT … ON CONFLICT`-shaped. BigQuery has no primary keys, no unique enforcement, and no `ON CONFLICT`. There is nothing to conflict *on*.

**2. Per-change DML.** `Apply` consumes a change channel and writes changes. On a warehouse, `UPDATE`/`DELETE`/`MERGE` are DML — quota-limited, high-latency, and priced per operation. The high-throughput ingest path (Storage Write API) is **append-only**. A per-change applier would be both incorrect and economically absurd.

**3. Position durability inside the data transaction.** The contract's sharpest requirement (ADR-0007, and stated in the interface doc): *"The position write happens inside the same transaction as the data write — atomicity guarantees that progress and data move together."* A warehouse has no transaction spanning an ingest append and a control-table update. This is the one that cannot be worked around by writing a cleverer applier — the guarantee has to be **rebuilt on a different primitive**.

Additionally, most of `SchemaWriter` is meaningless: `CreateIndexes`, `CreateConstraints`, and `SyncIdentitySequences` (interface lines 88–103) have no warehouse analogue. So this is not "one more engine" — it is a **different phase set** in the orchestrator, which is what makes it a target *class*.

### What the warehouse actually offers instead

- **Storage Write API** with four stream types. The **default stream** is at-least-once. A **committed-type** stream with **client-provided offsets** gives genuine exactly-once: *"the Storage Write API never writes two messages that have the same offset within a stream, if the client provides stream offsets when appending records."* Retrying an append at the same offset is recognized as a duplicate and dropped.
- **Schema evolution is narrow.** `ALTER TABLE ADD COLUMN` is supported, and added columns **must be `NULLABLE` or `REPEATED`**. Renames, type changes, and drops are not directly supported; the documented path is export-to-GCS and reload into a new table.

## Decision (sketch)

Introduce a second target contract, `ir.ChangeSink`, alongside `ir.ChangeApplier`. Engines implement one or the other; the orchestrator dispatches on which is present, exactly as it already probes optional surfaces by type assertion.

### 1. The contract — split materialization from position durability

```go
// ChangeSink is the analytical-target sibling of ChangeApplier.
// Where ChangeApplier upserts per change inside a transaction that
// also carries the position, a ChangeSink appends batches to a
// landing table and DERIVES its position from what landed.
type ChangeSink interface {
    // EnsureLanding creates the landing table(s) for the schema:
    // source columns plus sluice's change metadata.
    EnsureLanding(ctx context.Context, s *Schema) error

    // AppendChanges appends one batch. Batches are large by design
    // (warehouse ingest is batch-economical); the orchestrator sizes
    // them via the existing MaxBufferBytesSetter surface.
    AppendChanges(ctx context.Context, streamID string, batch []Change) error

    // Frontier returns the highest durably-landed position for the
    // stream, or ok=false for a cold start. This REPLACES
    // ReadPosition + the transactional position write.
    Frontier(ctx context.Context, streamID string) (Position, bool, error)
}
```

**The load-bearing idea is that the position is *derived*, not separately stored.** Each landed row carries `_sluice_position`; `Frontier` is `SELECT MAX(_sluice_position) WHERE _sluice_stream_id = ?`. That is self-consistent by construction — if the append landed, the frontier advanced; if it did not, it did not. There is no two-phase problem to solve because there is no second thing to write.

**Constraint this imposes:** appends must be serialized per stream (one appender), or a gap could hide under `MAX`. That must be an enforced invariant of the sink, not an assumption — a parallel-append optimization would require tracking a contiguous frontier instead, and should be out of scope for v1.

### 2. Exactly-once in two tiers

**Baseline (portable, no special primitive): at-least-once append + dedup at merge.** Every change carries `(_sluice_position, _sluice_seq)`. A crash mid-batch may re-append rows already landed, so the landing table can contain duplicates — which is harmless, because the merge step takes the **latest `(position, seq)` per primary key**. The merged output is identical either way. This is what most warehouse CDC pipelines do and it works on any append-only target.

**Optional capability: `OffsetedChangeSink`.** An engine that can commit at a client-supplied offset (BigQuery committed-type streams) implements an extra method and gets true exactly-once *append*, eliminating landing-table duplicate bloat. Declared as a capability, probed by type assertion — the same pattern as every other optional surface in `ir`.

### 3. Two materialization modes, operator-chosen

- **`append` — the landing table is the deliverable.** Full change history, no DML at all, so the quota/cost problem simply does not arise. For a lot of analytical consumers this is *better* than a mirror: it is the raw material for slowly-changing-dimension modelling, and downstream tools (dbt et al.) expect exactly this shape.
- **`merged` — landing table plus a periodic `MERGE`** into a current-state table keyed by PK. Cadence-driven (`--merge-interval`), never per-change. This is the "mirror the source" mode, and it is where the DML cost lives.

Making `append` the default is a deliberate inversion of the OLTP instinct to mirror. It is cheaper, simpler, has no DML quota exposure, and is closer to what the destination is for.

### 4. Constrained schema evolution, declared not discovered

Map `ShapeKindAddColumn` → `ALTER TABLE ADD COLUMN` (emitted `NULLABLE`, per BigQuery's requirement). **Refuse every other shape loudly** — rename, type change, drop — with the drained-model recovery hint the existing intercept already produces. This is the correct outcome rather than a limitation to apologize for: the documented BigQuery path for those changes is export-and-reload, which is an operator decision about a warehouse table, not something a sync should perform silently.

(The column-DEFAULT forwarding gap filed separately on the roadmap is moot here — warehouse tables have no meaningful column defaults.)

### 5. Type fidelity — the tenets transfer directly

Named lossy cells that must refuse rather than coerce: no `UNSIGNED` integers; `NUMERIC` is bounded (38,9) with `BIGNUMERIC` beyond it, so a wider source decimal must refuse or be explicitly widened; `GEOGRAPHY` is **WGS84-only**, so a PostGIS geometry with `SRID != 4326` must refuse (sluice already has SRID machinery from the geometry work); nested/multi-dimensional arrays have no direct analogue. This is exactly the Bug-74 family-matrix discipline applied to a new target — and the reason a warehouse engine is *not* a quick win.

### 6. It composes with the parquet path rather than replacing it

The initial load should **reuse `backup export-as-parquet`** (ADR-0164) → GCS → a BigQuery load job, then hand off to `ChangeSink` for the CDC leg. That is the snapshot→CDC handoff, warehouse edition: it reuses a surface that already exists and already has an independent-reader CI gate (DuckDB), instead of building a second bulk path.

So the two ideas are complementary, not alternatives:

| | parquet export | `ChangeSink` |
|---|---|---|
| Shape | one-shot / periodic batch | continuous incremental |
| Exists today | **yes** | no |
| New code | none | a target class |
| Best for | backfill, initial load, ad-hoc extract | keeping a warehouse current |

## Consequences

- **A second target contract is a genuine architectural commitment** — a new interface in `ir`, a phase-set branch in the orchestrator, a capability declaration, and its own testing discipline. It is the largest structural addition since the engine registry itself.
- **The IR is vindicated, and that is the cheap part.** Nothing above requires changing `ir.Schema`, `ir.Row`, or `ir.Change`. The engine-neutral IR is exactly what makes this tractable; the cost is all in the contract and orchestration layers.
- **`append` mode is a product position, not just an implementation detail.** It sidesteps DML economics entirely and is arguably the more useful artifact — worth deciding deliberately rather than defaulting to mirror-the-source.
- **Testing needs an independent reader**, per the new-surface checklist: the landing table and the merge output must be verified by something that is not sluice's own writer. The parquet/DuckDB precedent applies directly.
- **`sluice verify` needs a story** — its depth ladder assumes a mirror. Against an `append`-mode target the source-vs-target row comparison is meaningless without merge-aware semantics. This is an open question, not a solved one.

## Alternatives considered

- **Implement `ChangeApplier` for BigQuery with per-change `MERGE`.** Rejected: correct-looking, quota-limited, latency-bound, and expensive — it would work in a demo and fail in production, which is the worst failure mode for this project's credibility.
- **Emit into GCS as parquet/JSON and let the operator own loading.** This is the *interim* answer and it is a good one — it is the current recommendation. It stops being sufficient only when someone wants continuous low-latency freshness, which is precisely the demand signal that should gate this ADR.
- **A generic "append-only target" abstraction covering warehouses, object stores, and queues at once.** Rejected as premature: one implementation is not enough to find the right abstraction, and sluice's own history (the flavors pattern, the trigger engines) suggests building the second concrete case before generalizing.
- **Do nothing and stay OLTP-focused.** Entirely defensible, and the status quo. sluice's differentiator is correctness in heterogeneous OLTP migration; warehouse CDC is a crowded space (Fivetran, Airbyte, Debezium, and now Supabase Pipelines) where sluice would be a late entrant without a clear correctness edge.

## Open questions (unresolved by design)

1. **`verify` against an append-mode target** — what does "the data matches" even mean when the target holds history? Merge-aware verification, a materialized comparison view, or an explicit "unverifiable in this mode" refusal.
2. **Which engine second?** The contract should not be designed against BigQuery alone — Snowflake, ClickHouse, and DuckDB have materially different ingest and merge primitives. Designing against exactly one is how a "general" interface ends up shaped like its first implementation.
3. **Cost visibility.** A merged-mode sync spends real money per merge cycle. sluice has no notion of target cost anywhere; a warehouse target is the first place operators would reasonably expect one.
4. **Where the merge runs.** In-warehouse SQL (`MERGE`) is the obvious answer, but it makes sluice the author of DML it does not otherwise emit, with its own failure and retry semantics distinct from the apply path.
