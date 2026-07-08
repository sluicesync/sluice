# ADR-0114: Post-copy DDL-phase reparent retry

- Status: Accepted
- Date: 2026-06-23
- Deciders: sluice maintainers
- Relates: ADR-0108 (cold-copy write reparent-retry), ADR-0109 (cold-copy source-read resume), ADR-0110 (coordinated grow-gate), ADR-0113 (restore reparent-reconciliation)

## Context

The cold-copy **data** phase rides a PlanetScale storage-grow / reparent fine: the row writers are wrapped in `flushWithReparentRetry` (ADR-0108), the source reads in the reconnect-resume loop (ADR-0109), all coordinated by the grow-gate (ADR-0110), and restore additionally reconciles any reparent-touched table from its immutable chunks (ADR-0113).

But the **post-copy DDL phases** — `CreateIndexes`, `CreateConstraints` (foreign keys), `CreateViews`, and `SyncIdentitySequences` — exec their DDL **directly** against the target and returned the raw driver error with **no retry**. A grow/reparent that lands on a DDL phase therefore aborted the *whole* migrate / restore, even though the data was already fully and correctly copied.

**Live finding (Track-C cross-engine MySQL→PG restore, 2026-06-23):** all 29 tables' rows landed byte-perfect against the manifest (manifest = target = 312,970,106 rows, 0 mismatches) riding a live PG reparent — then `create index idx_lc_k on live_churn` died with `FATAL: terminating connection due to administrator command (SQLSTATE 57P01)` because PG entered a *second* reparent exactly when the index phase began. Loud failure, data intact, indexes incomplete: `rc=1` after a ~32-min successful copy.

This is **not** Postgres-specific. The same `sw.CreateIndexes` / `CreateConstraints` call sites are shared by both engines and live in **both** orchestrators (`restore.go` *and* `migrate.go` — three call-site groups across `runBulkCopy`, `runBulkCopyForAddTable`, `runBulkCopyPhases`). MySQL/Vitess reparents on storage grows too; a `CREATE INDEX` / `ALTER` whose connection is terminated mid-build surfaces the MySQL analog (Error 1105 "not serving" / connection reset / `--read-only`). We saw it on PG first only because a from-zero-growing cross-engine PG target reparents more during the run, and PG `CREATE INDEX` holds a long connection (a wide collision window).

## Decision

**A single engine-general DDL-phase retry helper** wrapping every post-copy DDL phase in both orchestrators.

1. **Engine-neutral classifier seam.** New optional `ir.TransientClassifier` interface (`IsTransientError(err) bool`), implemented by both engine `SchemaWriter`s by delegating to the **same** internal `classifyApplierError` the applier / cold-copy paths use. The DDL path returns the raw driver error (it never ran through the applier classifier), so this exposes the identical verdict to the orchestrator — no second classifier to drift. A writer that doesn't implement it ⇒ no retry (pre-ADR-0114 behaviour, byte-for-byte).

2. **One retry helper.** `runDDLPhaseWithReparentRetry(ctx, phase, classifierSrc, do)` in `internal/pipeline` runs the phase, and on a **classified transient** backs off (exponential 100ms→30s cap, ~30-min wall-clock bound — the same envelope as ADR-0108/0109) and re-runs `do`. A **non-transient** (a real DDL fault) returns unchanged after one attempt — fails loudly, exactly as before. Budget exhaustion surfaces a loud terminal error wrapping the last transient (never silent, never infinite). It is the DDL-phase analog of `flushWithReparentRetry` / `copyTableWithSourceReadRetry`.

3. **Wire all post-copy DDL phases** in `restore.go` and every `migrate.go` variant: identity-sequence sync, indexes, constraints, views (the per-view DDL inside `runViewsPhase`, atop its existing dependency-pass retry).

**No grow-gate threading.** The DDL phase runs serially *after* all data copy completes — there are no concurrent cold-copy lanes to coordinate, so the gate's job (quiesce-many-lanes-together) doesn't apply. The bounded per-phase retry alone is the fix.

## Correctness — idempotency of each wrapped phase

The helper **relies on** (does not add) each phase being idempotent on re-run, which both engines already guarantee:

- **CreateIndexes** — PG `CREATE INDEX IF NOT EXISTS` (non-CONCURRENTLY: an interrupted build is atomic, leaves nothing; IF NOT EXISTS skips ones already done). MySQL pre-checks existing indexes and skips them (no Error 1061 "duplicate key name"). No half-built index to clean.
- **CreateConstraints** — both engines detect-then-skip an already-present FK/constraint via the catalog (Bug 131 idempotent-resume).
- **CreateViews** — `CREATE OR REPLACE` for regular views; matviews are detect-as-success; `runViewsPhase`'s pass-retry already tolerates "already exists".
- **SyncIdentitySequences** — `setval` / AUTO_INCREMENT reset writes the same high-water value on re-run.

The classifier is a pure verdict over the error: it never mutates state, advances a position, or swallows a terminal error.

## Consequences

- A migrate / restore whose post-copy DDL phase collides with a storage-grow reparent now **rides it out** instead of aborting after a correct data copy. Loud-failure-safe: the data was never at risk (the failure was always loud); this just removes a needless abort.
- The fix is engine-general (one helper, one interface) and covers both orchestrators — no per-engine duplication.
- Zero behaviour change on the happy path: `do` succeeds first try, the classifier is never consulted.

### Follow-up — Bug #114 (v0.99.122): catalog-race on the concurrent PG index build

The v0.99.118 fresh-DB re-validation found that a reparent during the **concurrent** PG `CreateIndexes` pool, combined with this whole-phase retry, could abort with a `pg_class` `23505` (`CREATE INDEX IF NOT EXISTS` is not atomic against an overlapping same-name build; the retry's CREATE overlaps the prior attempt's just-committed-and-replicated build). That `23505` is correctly non-transient (a user-table dup-key must stay loud), so the retry layers here didn't catch it — it aborted loudly with all data intact (no silent loss; the cross-engine index-name-collision hypothesis was ruled out). v0.99.122 wraps the single index-build chokepoint (`buildOneIndex`) in the existing narrow `retryOnCatalogRace` (the CDC schema-apply helper) as the INNER layer, scoped to the `pg_class`/`pg_type` constraint-name `23505` shape only. See CHANGELOG v0.99.122.

### Residual / follow-up — CLOSED (v0.99.120, roadmap item 42)

The PG `migrate` **overlapped copy+index** path (ADR-0077, `runOverlappedCopyAndIndexPhase` → `ir.IncrementalIndexBuilder.BuildTableIndexesFromChannel`) builds per-table indexes *interleaved with copy* inside the engine method, so the pipeline-level wrap above can't reach it. **v0.99.120 closes this** by wrapping the overlap path's per-table `buildOneIndex` in an in-engine bounded reparent-retry (`buildOneIndexWithReparentRetry` → the pure `retryIndexBuildWithReparent` policy) that reuses the SAME envelope + classifier as the cold-copy chunk retry (`copyChunkWithRetry`): on a classified transient it re-acquires a FRESH connection (the reparented primary), re-tunes it, and replays the idempotent `CREATE INDEX IF NOT EXISTS`, instead of cancelling the errgroup. A real DDL fault stays terminal; budget exhaustion is loud. Pinned by unit tests of the pure policy (rides-then-succeeds / terminal-no-retry / reacquire-transient-ridden / loud-exhaustion / ctx-cancel).

### Residual / follow-up — MySQL overlap parity (audit N-15b, 2026-07-08)

The 2026-07-08 audit cross-check found the MySQL overlap paths (ADR-0080) never got the v0.99.120 treatment: both `indexBuildWorkerTracked` (vanilla concurrent workers) and `buildEachAsCopiedSerial` (the VStream serial build-as-copied branch — the one every production PlanetScale/Vitess target takes since v0.99.199/200) called `buildTableIndexes` bare, so the writer's own `IsTransientError` went unused exactly where reparents happen. Closed by mirroring the PG shape in-engine (`mysql/schema_writer_index_reparent_retry.go`): the pure `retryIndexBuildWithReparent` policy reuses the ADR-0108 cold-copy envelope (`coldCopyReparent*Var` + `coldCopyReparentBackoff`) and `classifyApplierError`; the worker path re-acquires a fresh pinned `*sql.Conn` on retry (nil-guarded deferred close, as PG), the serial path rides the pooled `*sql.DB`'s implicit reacquire. MySQL has no `CREATE INDEX IF NOT EXISTS`, so replay idempotency rests on `buildTableIndexes`' detect-then-skip probe (Bug 131) — a partially-applied or committed-but-unacked combined ALTER is skipped on replay, never double-created (pinned by `TestBuildTableIndexesFromChannel_VStreamNoDoubleCreateAfterMidAlterRetry`). Same unit-pin set as PG plus wiring pins through `BuildTableIndexesFromChannel` on both flavors/paths; perf-parity-matrix row 17 declares the per-mode coverage.
