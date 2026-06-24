# ADR-0105: Concurrent key-hash CDC apply for the Postgres target

## Status

**Accepted (2026-06-20) — implemented, `-race`/PostGIS CI-green, and live-validated; shipped in v0.99.82.** Brings the [ADR-0104](adr-0104-mysql-pipelined-cdc-apply.md) concurrent key-hash CDC apply to the **Postgres** `ChangeApplier`, closing the cross-region apply-throughput gap that ADR-0104 closed for MySQL targets but left open for Postgres. Realizes roadmap [item 26](../dev/roadmap.md). Delivered in two steps: STEP 1 extracted the engine-neutral lane router + checkpoint frontier + lane orchestration into the shared `internal/laneapply` package, with the GA MySQL path re-wired behind the `LaneApplier` seam and pinned byte-identical (the existing MySQL concurrent unit + `-race` integration suites stayed green across the extraction — the central risk, contained by pin-first). STEP 2 implemented the PG `LaneApplier` (per-lane `ON CONFLICT` UPSERT backends, `classifyApplierError` 40001/40P01 → in-lane split-retry, separate-tx checkpoint, `applyOne` barriers, full `cacheMu`-guarded metadata caches, lane pool registering the PostGIS geometry codec). The serial-vs-`--apply-concurrency=4` byte-identical differential covers the full value-family matrix incl. `numeric[][]` + geometry (the Bug-74 corollary). **Live-validated on the Track-B2 cross-region 2-shard Vitess→PlanetScale-Postgres link: `--apply-concurrency=4` cleared a deliberately-built ~16,000-GTID backlog to caught-up (~245) within ~2 minutes and then sustained pace with the source (≈42 GTID/s applied vs ≈25/s produced), where serial PG was measured at single-digit rows/s and could not keep up.** `--apply-concurrency W` is now engine-general (the CLI flag + the streamer's `applyApplyConcurrency` plumbing were already engine-agnostic; only the help text needed updating). **Concurrency chunk** → the `-race` integration gate passed on CI before the tag (both engines green). **Follow-up fix (Bug 158, v0.99.83):** the post-release regression cycle found a CRITICAL silent-loss bug in this path — the concurrent orchestrator's barrier *unconditionally* invalidated the per-table metadata caches on every SchemaSnapshot (bypassing the engine's first-touch guard), which on a PG target marked the first-post-cold-start baseline schema-dirty → forced lane DML onto the `pgx.QueryExecModeExec` text path → json/jsonb (decoded to `[]byte`) failed `22P02` → lane aborted → position pinned at `0/0` (silent loss). Fixed by deferring invalidation entirely to the engine's guarded apply-then-invalidate (byte-identical to serial), making the concurrent barrier position-free (the frontier owns the resume position), and excluding a SchemaSnapshot's metadata-anchored token from boundary tracking. The orchestrator's `LaneApplier.InvalidateMetadataCaches` seam method was removed as redundant-and-harmful.

## Context

The cross-region CDC apply wedge ([item 23](../dev/roadmap.md)) is, at root, an apply-throughput deficit: when apply lags a high-write multi-shard source, `MinimizeSkew` holds the ahead shard and the lag compounds. ADR-0104 closed this for **MySQL** targets with `--apply-concurrency W` — a merged change stream fanned across W in-order lanes by primary-key hash, each lane committing concurrently on a dedicated backend, with a contiguous seq-frontier that advances the resume position only to a source-tx boundary durable across all lanes. Live-validated ~4×.

**Postgres targets have no equivalent lever.** The PG path uses [ADR-0092](adr-0092-pipelined-cdc-apply.md) *within-transaction* pipelining (one `pgx.Batch` per target tx, sent in one round trip): it overlaps the commit RTT but does **not** parallelize across keys. On a contended cross-region link this is apply-bound. Live measurement (2026-06-20, Track B vs B2 against the same 2-shard Vitess source): the MySQL target with `--apply-concurrency=4` applied ~66 rows/s while the PG target (ADR-0092 only) applied ~6.5 rows/s — ~10× slower, making PG the slower cross-region target. The asymmetry is structural, not a bug: PG simply has no concurrency lever for the apply.

### Why the ADR-0104 design transfers cleanly to Postgres

The correctness core of ADR-0104 is **engine-neutral**:

- The **key-hash lane router** (`hash(qualified table, ordered PK values) mod W`) gives the same-key-closed guarantee — all changes to one row land on one lane, applied in source order — so the dependent-row hazard (INSERT then DELETE/UPDATE of the same key racing on two transactions) is structurally impossible. This is pure hashing over `ir.Change`; nothing MySQL-specific.
- The **contiguous checkpoint frontier** (`markCommitted` / `recordTxBoundary` / `checkpointPosition` / `waitForFrontier`) maintains `persisted_position ≤ all-durably-committed-data` across out-of-order lane commits. Pure lock-disciplined state; zero database contact.

Postgres already satisfies the contracts these rely on: idempotent `INSERT ... ON CONFLICT (key) DO UPDATE` ([ADR-0010](adr-0010-idempotent-change-apply.md)) so replay-from-a-lagging-position is exactly-once for keyed tables; the [ADR-0089](adr-0089-default-adaptive-apply-batch-size.md) keyless at-least-once guard; and the shared `appliershared.RunBatchLoop` + `appliercontrol` AIMD machinery ([ADR-0081](adr-0081-applier-control-plane-extraction.md), [ADR-0052](adr-0052-aimd-apply-batch-size-controller.md)) both engines already use.

What differs is small and well-contained: the per-lane dispatch (PG `ON CONFLICT` UPSERT on a dedicated backend), the position-checkpoint write (PG control-table upsert), and the **error classification** for the in-lane retry — Postgres has no PlanetScale tx-killer (Error 1105); its transient, retry-worthy aborts are **serialization failures (SQLSTATE 40001)** and **deadlocks (40P01)**.

## Decision

**Extract the engine-neutral lane+frontier core into a shared package, then wire both engines to it** (roadmap option A, not replicate). Rationale: the frontier+router are the exactly-once correctness landmark; a single shared implementation avoids two divergent copies (the Bug-74 class risk of a fix landing in only one engine), is unit-testable independent of any database, and lets future engines inherit it cost-free. The cost — a careful refactor of GA MySQL code — is bounded by pinning MySQL byte-identical first.

### 1. The shared core (`internal/laneapply`, new package)

Moves, verbatim in behavior, from `internal/engines/mysql/change_applier_concurrent.go`:

- `laneRouter` + `laneFor` + `writeCanonicalKeyValue` (the key-hash router).
- `checkpointFrontier` + `newCheckpointFrontier` + `markCommitted` / `recordTxBoundary` / `checkpointPosition` / `waitForFrontier` / `frontierSeq` (the seq-frontier).
- The lane-orchestration skeleton (`run` / route / barrier / `laneApplyLoop` / `readLaneBatch` / the recursive shrink-and-retry `applyLaneBatch` / the lane-local committable-size read cap from v0.99.81), generalized over a small seam interface.

The pure logic (router + frontier) is already database-free; it moves with its existing unit tests. The orchestration generalizes over:

```go
// LaneApplier is the minimal per-engine surface the shared concurrent
// key-hash orchestrator drives. One method family applies a batch on a
// dedicated backend; the rest is PK routing, error classification, and
// the position checkpoint write.
type LaneApplier interface {
    // PKValuesForRouting returns the ordered PK values of a row change
    // for lane hashing, or ok=false for a keyless change (→ barrier path).
    PKValuesForRouting(ctx context.Context, c ir.Change) (qualified string, pkVals []any, ok bool, err error)

    // ApplyLaneBatch applies changes on lane `lane`'s dedicated backend in
    // one transaction (idempotent UPSERT per ADR-0010) and commits, returning
    // the number of rows durably committed. The shared orchestrator handles
    // the recursive split-on-retriable-error; this method applies one
    // (sub-)batch atomically.
    ApplyLaneBatch(ctx context.Context, lane int, batch []ir.Change) (committed int, err error)

    // ClassifyError maps a raw driver error to a classified error exposing
    // Retriable() (split-and-retry) vs fatal. MySQL: tx-killer (1105) +
    // lock-wait. Postgres: serialization (40001) + deadlock (40P01).
    ClassifyError(error) error

    // WriteCheckpoint persists the merged position at a durable frontier
    // boundary in its own transaction (the ADR-0007 relaxation; see below).
    WriteCheckpoint(ctx context.Context, pos ir.Position) error

    // ApplyBarrierChange applies one keyless change after all lanes drain
    // (ADR-0089 at-least-once path), and the metadata-cache invalidation hook
    // on a schema change.
    ApplyBarrierChange(ctx context.Context, c ir.Change) error
    InvalidateMetadataCaches(qualified string)
}
```

The per-lane AIMD controllers (`[]ir.BatchSizeController` via `LaneAIMDSetter`) are passed into the orchestrator; that interface is already engine-agnostic ([ADR-0052](adr-0052-aimd-apply-batch-size-controller.md)/ADR-0104).

### 2. MySQL re-wire (pin-first)

Before moving anything: pin the MySQL concurrent path's observable behavior — the existing `change_applier_concurrent_test.go` units (router, frontier, lane apply, split-convergence) and the `change_applier_concurrent_integration_test.go` exactly-once / serial-vs-W differential / boundaryless-position / tx-killer integration tests. The refactor must leave **all of these byte-identical green**, and the live-validated MySQL behavior unchanged. MySQL's `ChangeApplier` becomes a `laneapply.LaneApplier` implementation (its dispatch, `classifyApplierError`, `writePositionTx`, cache accessors wired into the seam). This is mechanical re-wrapping, not a logic change.

### 3. Postgres implementation

Implement `laneapply.LaneApplier` on the PG `ChangeApplier`:

- **`ApplyLaneBatch`** — a dedicated `*sql.DB`/pgx backend per lane (respecting the ADR-0092 conn-budget fix, v0.99.47, extended to W lanes), applying the batch's changes as idempotent `ON CONFLICT` UPSERTs in one tx and committing. Reuses the existing serial dispatch arm (`dispatch` on a tx) — NOT the ADR-0092 `pgx.Batch` pipelining, which is an orthogonal intra-tx optimization that can be layered later. W=1 ≡ today's serial path (byte-identical).
- **`ClassifyError`** — map `40001` (serialization_failure) and `40P01` (deadlock_detected) to retriable (split-and-retry); everything else fatal. (PG serialization aborts on a contended target are the analog of MySQL's tx-killer; splitting + idempotent retry converges them exactly as on MySQL.)
- **`WriteCheckpoint`** — upsert the merged position into the PG control table in its own tx (the ADR-0007 relaxation: position written separately, only to a fully-durable frontier boundary).
- **`PKValuesForRouting`** — reuse the existing PG PK metadata lookup (`loadPrimaryKey` / `pkForRedact`); keyless → barrier path.

### 4. Wiring

`SetApplyConcurrency` on the PG applier (already an `ir` interface, MySQL-only adoption today → now both). The pipeline streamer's `maybeAttachAIMDController` / `attachLaneAIMDControllers` path is already engine-agnostic and engages the moment a PG applier implements `LaneAIMDSetter`. `--apply-concurrency W` therefore works against a PG target with no CLI change.

## The position relaxation (inherited verbatim from ADR-0104)

`persisted_position ≤ all-durably-committed-data, always` — the position can lag the data (a crash loses only the checkpoint, replayed idempotently on resume) but never leads it (the frontier never passes an uncommitted change). Source-transaction cohesion ([ADR-0027](adr-0027-source-transaction-cohesion.md)) is relaxed identically: a source tx's rows scatter across lanes and commit separately; the final state is correct because the frontier only checkpoints at a fully-committed tx boundary. For keyed tables this is exactly-once across crash+resume (ADR-0010 UPSERT); keyless tables keep at-least-once (ADR-0089). **No new relaxation is introduced — PG inherits exactly the ADR-0104 contract.**

## Consequences

**Positive.** PG targets get the same ~N× cross-region apply lever as MySQL; one shared, independently-unit-tested exactly-once core instead of two; future engines inherit it. `--apply-concurrency` becomes engine-general.

**Negative / risks.** (1) Refactoring GA MySQL code — mitigated by pin-first + the full `-race` integration gate on both engines before tag. (2) W dedicated PG backends multiply connection use — must compose with the conn-budget accounting (cap W so total connections stay within the target's limit; document the interaction with `--apply-concurrency`). (3) The shared package must not become a dumping ground — only the router+frontier+orchestration skeleton move; engine specifics stay behind the seam.

**Default-off.** `--apply-concurrency` defaults to serial (W≤1), byte-identical to today on both engines; opt-in `W>1` engages lanes.

## Testing

- **Pin (pre-refactor):** the existing MySQL concurrent unit + integration suites stay green byte-for-byte across the extraction.
- **PG units:** router/frontier already covered by the moved shared tests; add PG `ClassifyError` (40001/40P01 → retriable; others fatal) and the PG lane-apply split-convergence pin.
- **PG integration (`-race`):** exactly-once + same-key INSERT→UPDATE→DELETE ordering; the **serial-vs-`--apply-concurrency=4` byte-identical differential** (the Bug-74-class check) on a real PG target; warm-resume under the knob; W=1 ≡ serial byte-identical. Mirrors the MySQL ADR-0104 integration matrix.
- **Live A/B (Track B2):** measure PG apply throughput serial vs W on the cross-region link, the analog of the MySQL ~4× result.

## Alternatives considered

- **Replicate the pattern in `internal/engines/postgres` (no extraction).** Faster to a first cut but doubles the critical exactly-once surface — a future fix (another Bug-74-class subtlety) would have to land in two places or silently diverge. Rejected for a correctness-critical landmark.
- **Extend ADR-0092 pipelining instead of lanes.** Intra-tx pipelining overlaps only the commit RTT (the same ceiling ADR-0104 Phase-1 hit on MySQL); it cannot parallelize across keys. Orthogonal and insufficient as the throughput lever; it can still be layered *inside* a lane later.

## Addendum — broker chain-replay reuses the lane path (2026-06-24)

The `sync from-backup` broker replays each incremental's change chunks through the engine applier's `ApplyBatch`, identical to the live streamer. Until this addendum it opened that applier and never wired the lane count, so every incremental replayed through the single-stream ADR-0092 pipelined path — RTT-bound on a cross-region link, the same wedge this ADR lifts but on the broker-replay path. The broker now resolves `--apply-concurrency` (`SyncFromBackup.ApplyConcurrency`, the `sync from-backup run --apply-concurrency W` flag) through the same `applyApplyConcurrency` helper the streamer uses, so incremental replay fans across W in-order PK-hash lanes. The exactly-once contract is inherited verbatim: every change in an incremental carries the same broker chain-position token, so the lanes persist the identical position the serial path did — the frontier still only checkpoints at a fully-durable boundary, and the broker's idempotent re-replay-from-parent recovery is unchanged. The broker resolver (`resolveBrokerApplyConcurrency`) does **not** run a connection-budget probe (the broker opens a single applier up front; per-lane backpressure handles a tight target), so auto resolves to the fixed conservative default (4) rather than the streamer's probe-bounded count.
