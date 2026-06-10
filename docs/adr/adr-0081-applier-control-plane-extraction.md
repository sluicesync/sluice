# ADR-0081: Extract the engines' applier control plane into a shared, dialect-seamed package

## Status

Accepted — repo-audit task M2.2 (operator-authorized 2026-06-10). Tier (a) merged via PR #170 (`internal/appliershared` helpers); tier (b) — the batched-apply loop extraction this ADR primarily documents — implemented alongside this ADR. Tiers (c) and (d) remain open follow-ups. Builds on the metadata convergence in repo-audit M2.1 (PR #169: both engines' appliers use `map[string]*ir.Column` caches and `(string, []any, error)` SQL builders), which made the two engines' batch loops structurally diffable in the first place.

### Implementation notes (what landed in tier b)

- `internal/appliershared/batch_loop.go` — `BatchConfig` (the dialect seam), `RunBatchLoop` (ApplyBatch outer loop: AIMD consult → one batch → log), `RunOneBatch` (one accumulation cycle: pre-tx wait → dispatch loop → flush), `commitBatch` (position-write-then-commit), `DefaultIdleFlushPeriod`.
- Each engine's `change_applier_batch.go` shrank to: the engine-invariants file header, a `defaultIdleFlushPeriod` alias (keeps the per-engine item-18 Fix B pins guarding the constant), the thin `ApplyBatch` (streamID check + `maxBatchSize <= 1` fall-through + `RunBatchLoop`), a one-line `applyOneBatch` delegate (the item-18 AIMD unit pins drive a single cycle directly), and `batchConfig()` wiring the dialect.
- All pre-existing behaviour pins — the item-18 AIMD unit pins, both engines' `ApplyBatch` integration suites, the AIMD streamer integration tests, pgtrigger (which composes the PG applier) — passed **unchanged**; new seam-level unit pins in `batch_loop_test.go` cover commit ordering, every flush trigger, and the schema-event family (Truncate AND SchemaSnapshot) × {first-change, mid-batch} × both `TransactionalDDL` modes.

## Context

The two engines' `ChangeApplier`s were written as deliberate mirrors: the same batched-apply control flow — batch accumulation, the ADR-0052 AIMD controller interaction, the item-18 100ms idle-grace flush timer, the ADR-0028 byte cap, ADR-0027 source-tx boundary alignment, and the ADR-0007/ADR-0010 position-write-then-commit ordering — maintained twice, with dialect differences interleaved line-by-line. The repo audit measured the cost precisely:

- `change_applier_batch.go`: MySQL 507 vs Postgres 497 lines, **~71 differing non-comment lines after normalizing engine-name strings (≥85 % identical)**.
- **16 of 19 commits** that touched either file had to touch both. The item-18 latency fix (the 100ms idle-flush grace + AIMD `batchStart` placement) was applied twice, line for line — the canonical two-file tax.

The mirror discipline worked (no divergence bug shipped), but it is a standing silent-loss risk: a future fix applied to one engine and subtly mis-ported to the other would diverge behaviour that three ADR contracts (0007, 0010, 0052) depend on, with no compiler signal. Tier (a)'s near-miss inventory found the same pattern in the smaller helpers (`logZeroRowsAffected` differing only by log prefix; `defaultIdleFlushPeriod` byte-identical), and the SQL builders (`buildInsertSQL` etc.) as identical skeletons with dialect-divergent leaves.

The mechanical diff of the two batch loops reduced the ~71 divergent lines to exactly five clusters:

1. MySQL: a schema event (Truncate / SchemaSnapshot) as the **first** change applies alone via `applyOne` before any tx opens (~12 lines).
2. PG: `forceSynchronousCommitOn` (F7) pinned after `BeginTx` (~4 lines).
3. PG: a schema event as the first change dispatches **onto** the batch tx, then flushes as a 1-change batch (+ the ADR-0049 cache-after-commit hook for snapshots) (~10 lines).
4. The same divergence **mid-batch**: MySQL flushes the in-flight batch then `applyOne`s the event (~30 lines); PG dispatches it then flushes (~10 lines).
5. `commitBatch` leaves: PG's `writePositionTx` takes `controlSchema`, and PG reports the applied LSN to the slot-ack tracker after commit (~2 lines).

Clusters 1, 3, and 4 are a single underlying fact wearing two shapes: **whether the target's DDL is transactional**. MySQL DDL implicit-commits the open transaction (mixing it into a batch silently breaks the batch's atomicity), so events must flush-then-apply-alone; PG DDL is transactional, so events ride the batch tx and the position write lands atomically with them (ADR-0049 locked decision #4a). Everything else — the entire accumulation/flush/observe state machine — is engine-neutral control flow.

## Decision

Extract the batch loop ONCE into `internal/appliershared`, behind a flat config-of-closures dialect seam (`BatchConfig`), following the `exprident.Config` precedent (ADR-0045): a shared skeleton owns the control flow; each engine fills a small struct of the genuinely divergent leaves and keeps all dialect knowledge in its own package. The seam, field by field:

- `EngineName` — log/error prefix, so operator-facing output stays byte-identical ("mysql: applier: …" / "postgres: applier: …").
- `TransactionalDDL` — the **named flag** for clusters 1/3/4. Both handling shapes live in the shared loop, each commented with the engine rationale; the flag picks. A hook-per-shape alternative was rejected (below).
- `ByteCap` — the resolved ADR-0028 soft cap (engines resolve their `defaultMaxBufferBytes` fallback).
- `BatchSizeProvider` / `BatchObserver` — the ADR-0052 AIMD surfaces, passed through.
- `BeginTx` — opens the batch tx with the error already in final shape; PG's hook owns F7 `SET LOCAL synchronous_commit = on` (cluster 2) so the durability pin stays engine-side.
- `Dispatch`, `ApplyOne`, `Redact`, `StampShard`, `Classify` — the engines' existing methods, passed as method values.
- `WritePosition` — wraps the engine's `writePositionTx` + per-exec timeout; absorbs the `controlSchema` arity difference (cluster 5).
- `Commit` — the engine's Bug-56 `commitWithTimeout`.
- `AfterCommit` (optional) — PG's slot-ack report (Bug 15 / ADR-0020); nil on MySQL.
- `CacheSchemaSnapshot` (optional) — PG's ADR-0049 cache-after-commit hook; nil on MySQL (its snapshots route through `applyOne`, which owns the cache update).

**What stays engine-side, deliberately:** value codecs and `prepareValue`, the SQL builders (`buildInsertSQL`/`buildUpdateSQL`/`buildDeleteSQL`/`buildTruncateSQL` — identical skeletons but dialect-divergent leaves: quoting, placeholders, ON DUPLICATE KEY vs ON CONFLICT; they are what the seam's `Dispatch` calls into, not extraction candidates), error classification, the readers, control-table SQL, the per-change `Apply`/`applyOne` path, and the column-type/PK/conflict-key caches. The IR stays the only cross-engine contract; `appliershared` depends on `ir` and nothing engine-specific.

### Invariants the extraction must preserve (and how that was verified)

- **Item-18 timing.** `batchStart` is assigned after the pre-tx wait loop, immediately before `BeginTx` (apply-only latency for the AIMD observer, with the IsZero guard on pre-tx cancellation); the idle timer is created after the first dispatched change and reset only after each subsequent successful dispatch (and on mid-batch TxBegin). Timer ownership and sequencing moved into the shared loop **positionally unchanged**; the engine unit pins (`change_applier_aimd_test.go` ×2) and integration pins (`…IdleFlushCommitsPartial` ×2) are the behaviour oracle and passed without edits.
- **ADR-0007/ADR-0010 ordering.** `commitBatch` is data dispatches → position write on the SAME tx → commit → post-commit hooks, asserted by the existing idempotency integration pins plus a new seam-level ordering pin.
- **Per-call config assembly.** `batchConfig()` builds the seam per `ApplyBatch` call (cheap closures), so unit tests constructing `ChangeApplier` literals see current field values. The AIMD provider/observer are captured at call start rather than re-read per outer iteration — a non-observable change (the streamer wires them before `Run`; the applier is single-goroutine by contract).

## Consequences

- The two-file tax is gone for the control plane: the next batch-loop fix lands once. The engines' `change_applier_batch.go` files are now ~150-line dialect declarations instead of ~500-line mirrored state machines.
- The dialect seam makes the engine differences **legible**: `TransactionalDDL`, F7, slot-ack, cache-after-commit are now named fields with doc comments instead of interleaved diff noise.
- A third engine's batched applier becomes "fill the config" — consistent with the engine-registry tenet.
- Cost: one more indirection layer when reading the apply path (engine file → shared loop). Mitigated by the seam being flat closures (no interface dispatch chains) and the shared file carrying the merged, de-duplicated comments.
- Risk accepted: the shared loop is concurrency-bearing (timer + channel select + AIMD interaction), so this chunk lands only through CI's `-race` Integration gate per the release-process rule for concurrency chunks.

### Tier plan (the whole extraction arc)

- **(a) DONE (PR #170):** byte-identical helpers → `appliershared` (`Schema`, `RunWithDeadline`, `NonGeneratedRowKeys`, `TruncateToken`).
- **(b) DONE (this ADR's implementation):** the batched-apply loop behind `BatchConfig`.
- **(c) OPEN:** control-table CRUD (`readPosition`/`listStreams`/`requestStop`/… are near-identical skeletons over dialect-divergent SQL) — same seam style, lower risk than (b).
- **(d) OPEN:** lease/keyset/heartbeat helpers (`shard_consolidation_lease.go`, `keyset_store.go`, `heartbeat_writer.go` pairs) — the largest remaining mirrored surface.

## Alternatives considered

- **Keep the mirrors + rely on review discipline.** The status quo; rejected on the audit's evidence — 16/19 co-changes is a standing mis-port risk on idempotency-critical code, and the discipline cost recurs forever.
- **An interface (`BatchDialect`) instead of a config struct.** Method sets invite engines to embed/override partial behaviour and hide which leaves diverge; the flat struct makes the divergence inventory exhaustive and greppable. exprident's `Config` set the precedent and has aged well.
- **Hook funcs for the schema-event shapes instead of `TransactionalDDL`.** e.g. `HandleSchemaEventFirst`/`HandleSchemaEventMidBatch` closures per engine. Rejected: it splits the loop's control flow across packages (the flush-before-DDL ordering — the part that protects the position write — would live in engine closures where a future edit could reorder it), and both shapes exist only because of one boolean fact about the target. A named flag with both branches in one file, each commented, is the more legible wart.
- **Generics / a state-machine type per engine.** Overweight for two engines and zero polymorphic types; the divergence is values-and-closures shaped, not type-shaped.
- **Extract the SQL builders too.** Rejected (tier-a finding): their skeletons are identical but every leaf is dialect (quoting, placeholder style, conflict clause). Sharing them would force a quoting/placeholder abstraction — exactly the engine knowledge the IR-first tenet keeps out of shared code.
