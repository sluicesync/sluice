# ADR-0077: Overlap secondary-index builds with the bulk copy in `sluice migrate`

## Status

Accepted. Builds on [ADR-0076](adr-0076-cross-table-copy-worker-pool.md) (the cross-table copy worker pool — this extends its per-table completion point), [ADR-0019](adr-0019-parallel-within-table-bulk-copy.md) (within-table chunking), [ADR-0018](adr-0018-per-batch-bulk-copy-checkpointing.md) / [ADR-0015](adr-0015-migration-resume.md) (resume state), and the PG index-build-parallelism machinery (the `IndexBuildTuner` worker pool + connection-budget probe). This is **phase (a)** of roadmap item 3b ("PG→PG copy throughput: index-build overlap + identity passthrough"); phase (b) (PG→PG identity byte-pipe passthrough) is a separate, later chunk.

This is a **concurrency chunk** — the `-race` integration gate must pass before any tag (the development box is `CGO_ENABLED=0` and cannot run `-race` locally; the gate is CI-only).

### Implementation notes (what landed)

- **The overlap mechanism — completion channel + separate index pool under one errgroup.** `runBulkCopyPhases` previously ran Phase 2 (`runBulkCopyTablePool`) to completion, closing every copy connection, THEN Phase 4 (`sw.CreateIndexes` over the whole schema). The new `runOverlappedCopyAndIndexPhase` (`internal/pipeline/migrate_index_overlap.go`) runs both as two cooperating goroutines under one `errgroup`:
  - `runBulkCopyTablePool` gained an `onTableCopied func(*ir.Table)` callback, invoked **only** after `bulkCopyOneTable` returns nil for a table (the per-table success point); an error short-circuits via the errgroup ctx exactly as before.
  - The producer goroutine runs the copy pool; its `onTableCopied` forwards each just-copied table onto a buffered (`len=#tables`) `chan *ir.Table`, then closes the channel when the pool finishes.
  - The consumer goroutine calls `ir.IncrementalIndexBuilder.BuildTableIndexesFromChannel`, which drains the channel with the engine's OWN bounded worker pool. A copy error cancels the index pool (shared ctx); an index-build error cancels the copy pool. **Index builds are NOT enqueued on the copy goroutines** — that would starve copy slots; the callback only hands off.
- **Engine-neutral optional surface.** `ir.IncrementalIndexBuilder` (type-asserted, like `IndexBuildTuner` / `DegradedFKReporter` — NOT on the base `ir.SchemaWriter`):
  `BuildTableIndexesFromChannel(ctx, s *Schema, completedTables <-chan *Table) error`.
  PG implements it (reusing ~90% of `CreateIndexes`: the worker body, `buildOneIndex`'s `CREATE INDEX IF NOT EXISTS`, and the mem/concurrency resolution are verbatim; only the feeder differs — instead of flattening the whole schema up front, `indexBuildJobs` was factored into `indexBuildJobsForTables([]*Table)` so the feeder expands one completed table at a time). Two companion optional surfaces: `ir.IndexBuildBudgetSetter` (`SetIndexBuildBudget(int)`) for the reserved budget, and `ir.TableIndexedNotifier` (`SetTableIndexedCallback(func(*Table))`) so the builder reports per-table completion back to the orchestrator. **Engines without `IncrementalIndexBuilder` (MySQL)** take the fallback: the orchestrator runs the copy pool serially, then identity-sync, then the whole-schema `CreateIndexes` — exactly the pre-ADR-0077 behaviour.
- **Combined connection budget (the load-bearing change).** Copy and index connections are now open **simultaneously**, so the single measured `CopyBudget` must cover both. `splitCopyAndIndexBudget(copyBudget, withinParallelism)` (`internal/pipeline/connection_budget.go`) carves it once at the single chokepoint, immediately after `resolveTargetCopyParallelism`:
  - `copyBudget < 1` (no measured ceiling / MySQL / degraded probe) → `(0, 0)`: both axes stay unclamped (pre-ADR-0077; MySQL overlaps via the post-copy fallback anyway).
  - else: `indexBudget = clamp(round(0.25 × copyBudget), 1, 8)` (the index pool's auto concurrency self-caps at 8, so reserving more is wasted); `copyBudget' = max(copyBudget − indexBudget, withinParallelism)`, then `indexBudget` is trimmed to `copyBudget − copyBudget'` so the **INVARIANT** holds: `indexBudget + copyBudget' <= copyBudget`. If the trim would drop `indexBudget` below 1 (the copy axis alone needs the whole budget for one table), the split returns `(0, copyBudget)` — no overlap, indexes build in the post-copy phase rather than starving copy.
  `copyBudget'` is fed into `resolveCopyParallelismBudget` (so the copy axes see only their slice); `indexBudget` is threaded to the SchemaWriter via `SetIndexBuildBudget` and used as the `connBudget` for `computeIndexBuildConcurrency` **instead of** the self-probe (a fresh self-probe would count the still-open copy connections as spare and double-allocate). When `indexBudget == 0` (no overlap), the writer keeps its self-probe.
- **Resumability.** `ir.TableProgress` gained `IndexesBuilt bool` (additive, `omitempty` on the JSON wire — old tokens decode to `false`). A `complete` table with `IndexesBuilt=true` promotes from the compact bare-string `"complete"` to the object form `{"state":"complete","indexes_built":true}` so the flag survives; `IndexesBuilt=false` stays the bare string. The index pool flips it true (via the existing clone-under-`stateMu` write helper) once a table's last secondary index lands. On resume: `State=complete && !IndexesBuilt` → copy is skipped (`resumeActionSkip`) but the table is re-fed to the index pool (`CREATE INDEX IF NOT EXISTS` guards a crash mid-index-build); `IndexesBuilt=true` → the copy pool's `onTableCopied` fires for the skipped table but the orchestrator filters it out (`alreadyIndexed`), so it is fully skipped.
- **Phase ordering unchanged for the rest.** Constraints/FKs + identity-sequence sync stay AFTER the combined copy+index phase (FK validation needs all data + indexes; identity-sync depends on copied rows, not indexes, so its position relative to index builds is immaterial). Scoped to `migrate`; the `sync` cold-start path (`runBulkCopyWithOpts`) stays serial by design (commented there).
- **Observability seam.** Two test-only package vars (`onTableCopiedObserver` in pipeline, `onIndexBuildStartObserver` in postgres, plus a build-tagged `SetIndexBuildStartObserverForTest`) let the integration test assert `min(indexBuildStart) < max(copyComplete)` — proving the overlap genuinely happens, so the chunk can't silently regress to sequential and still pass a zero-loss test.

## Context

The 110 GB / 43-table at-scale comparison (`docs/comparison-pgcopydb.md`, "At scale") showed pgcopydb ~1.75× faster end-to-end (895 s vs 1564 s) on a realistic mixed corpus, *after* roadmap item 3 closed the cross-table copy gap. The run is disk-bound (~100–122 MB/s) — more parallelism does nothing or hurts. Two **structural** differences remain; this ADR addresses the broad one:

> **Overlapped index builds.** pgcopydb builds each table's indexes as soon as its data lands, concurrently with the still-copying tables. sluice ran a full bulk-copy phase **then** a separate index phase — a sequential ~457 s tail (29% of sluice's total) that pgcopydb hides.

The PG index-build machinery already existed (a deferred, idle-target, tuned, bounded worker pool — `CreateIndexes` + `IndexBuildTuner`). What was missing was running it *during* the copy instead of after. The hard part is correctness, not throughput: copy and index connections now coexist, so the budget that previously governed each phase sequentially must govern their **simultaneous** sum.

## Decision

Overlap the copy and secondary-index-build phases on engines that expose an incremental index builder, via a completion channel + a separate index pool under one errgroup, with the combined connection budget split once at the single chokepoint. Refuse to overlap (fall back to the post-copy phase) when no slot can be spared for the index pool without starving copy.

### Combined-budget policy: static reserve at the chokepoint (mirrors ADR-0076)

ADR-0076 established that the right place to bound simultaneously-open connections is the SINGLE existing chokepoint, *by construction*, with no runtime semaphore. This ADR extends that: reserve a small static slice of `CopyBudget` for the index pool (`clamp(0.25 × budget, 1, 8)`), hand the rest to the copy axes, and enforce `indexBudget + copyBudget' <= CopyBudget` at the same chokepoint. The 0.25 fraction mirrors pgcopydb's default 4-table-jobs / 4-index-jobs balance; the `[1, 8]` clamp matches the index pool's own auto hard cap (`indexBuildConcurrencyHardCap`), above which extra reserved connections are wasted.

The within-table copy factor is the **floor** for the copy slice: the copy axis always keeps at least one table's worth of connections. When the budget is so tight that reserving even one index slot would push copy below that floor, the split declines to overlap and the index phase runs after the copy — the conservative choice (never starve the load-bearing copy to chase the index-overlap win).

We rejected a shared runtime semaphore counting all open copy+index connections (the same option ADR-0076 rejected for the two copy axes): it turns a construction-time invariant into a runtime one that must be re-proven under `-race`, for no benefit the static reserve doesn't already provide.

### Why a self-probe in the index pool would be a bug here

`CreateIndexes` (the non-overlapped path) self-probes the target's spare connection budget to size its worker pool. In the overlap path the copy pool is *still running* with connections open, so a self-probe would see those slots as "in use by someone else" — or, worse on a generous target, see headroom that the copy pool is about to consume — and either way mis-size the pool against the combined ceiling. So the overlap path uses the **reserved** `indexBudget` verbatim (via `SetIndexBuildBudget`) instead of self-probing; the self-probe is kept only for the non-overlapped fallback.

### Resume: `IndexesBuilt`, additive and fail-safe

The additive `IndexesBuilt` flag defaults to `false` for any state row written before this change. The safe interpretation of "absent flag" is "copy done, indexes NOT yet built" → re-feed to the index pool, which is a no-op under `CREATE INDEX IF NOT EXISTS`. The inverse default (treating absent as "built") would silently skip a never-built index — a silent-loss-class outcome, refused. A crash mid-index-build leaves `IndexesBuilt=false`; the resume re-feeds the table and `IF NOT EXISTS` absorbs whatever indexes already landed.

## Consequences

- PG→PG many-table / large-corpus migrates overlap the index phase with the copy, closing most of the ~457 s sequential index tail the at-scale benchmark measured. Single-table migrates are largely unaffected (one table's indexes still build after its copy, which is most of the wall there anyway).
- The connection budget now governs copy + index connections held simultaneously, at one chokepoint, by construction — a wide schema can't exhaust the target's slots by running the two phases at once.
- Resume is correct and order-independent under the new overlap: `IndexesBuilt` short-circuits fully-indexed tables, re-feeds copied-but-unindexed ones, and `IF NOT EXISTS` guards a crash mid-build.
- MySQL (and any engine without `IncrementalIndexBuilder`) is unchanged: serial copy → identity-sync → whole-schema `CreateIndexes`.
- The sync cold-start path is unchanged (still serial).
- New optional IR surfaces (`IncrementalIndexBuilder`, `IndexBuildBudgetSetter`, `TableIndexedNotifier`) — additive, type-asserted, no change to the base interface or to engines that don't implement them.

## Alternatives considered

- **A shared runtime semaphore over all open copy + index connections.** Rejected for the same reason ADR-0076 rejected it for the copy axes: a new runtime invariant to prove under `-race`, no benefit over the static reserve.
- **Enqueue each table's index build on its copy goroutine.** Rejected — index work would occupy copy slots and starve the copy pool; the separate index pool with a hand-off channel keeps the two resource pools distinct.
- **Self-probe the index pool's budget even on the overlap path.** Rejected — double-counts the copy pool's open connections (see above); the reserved slice is the correct input.
- **Make `IncrementalIndexBuilder` a required method on `ir.SchemaWriter`.** Rejected — MySQL has no comparable index-build-parallelism contract, and the loud-failure-safe fallback (post-copy whole-schema `CreateIndexes`) is exactly the pre-existing behaviour; an optional type-asserted surface keeps the base interface small (the established pattern).
- **Overlap the `sync` cold-start path too.** Deferred — its snapshot-pinning + durable-watermark interplay is a separate correctness surface (same deferral ADR-0076 made).
