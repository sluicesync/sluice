# Task #65 Phase A diagnostic report

**Status:** Phase A complete. Phase B (the fix) is a separate task — do not implement here.

**PR:** https://github.com/orware/sluice/pull/69 (do not merge — diagnostic only, to be reverted after Phase B lands).

**Date:** 2026-05-26.

## Scope

Two failing tests in PR #67 (#24, PG streamer harness) and PR #68 (#23, MySQL multi-shard):

1. `TestPhase2e_MySQL_Takeover_ProbeAndRecord_Applied` — fails with `RouteBoundary takeover: pipeline: observe lease "takeover_target" timed out after 1m0s (last state expired, holder "shard-b")`.
2. `TestPhase2e_PG_StreamerHarness_3SourcesToTarget_ExactlyOnceApply` and `TestPhase2e_PG_StreamerHarness_RestartResumeAfterBoundary` — fail with `target users.active column never landed`.

The same `BoundaryRouter` routing/observe-flow class (`internal/pipeline/shard_consolidation_router.go` + `internal/pipeline/shard_consolidation_lease.go`) is suspect for both.

## Method

Per CLAUDE.md three-phase debugging protocol: **Phase A is instrumentation only — no fix code**. Added DEBUG-level `slog.DebugContext` at every decision point on the routing/observe-flow path:

- `RouteBoundary` entry, ClassifyShape verdict, Acquire branch dispatch, handleHeldLease entry, DispatchProbe outcome, observeUntilApplied entry, every observe-loop iteration.
- `LeaseManager.Acquire`: TryAcquireLease result row state, takeover decision + `ddl_text_equal` boolean (prior == new ddl_text), RecordDDLText recorded/err result.
- `LeaseManager.Observe`: entry, exit with state + holder + checksum.
- `shard_consolidation_intercept.go`: SchemaSnapshot arrival on the streamer path.

Test-side: each failing test installs a DEBUG-level slog handler for the duration of the test + dumps the lease row state (holder, expires_at, applied_at, ddl_text, ddl_checksum, version, NOW(), seconds_to_expiry) before/after the failing RouteBoundary call.

## MySQL ground truth (confirmed via local Docker repro)

`TestPhase2e_MySQL_Takeover_ProbeAndRecord_Applied` ran locally with DEBUG instrumentation. Key log entries (timestamps removed for brevity):

```
phase-a: lease Acquire entry table=takeover_target stream_id=shard-a ddl_text=ir-schema:takeover_target:add-x
phase-a: TryAcquireLease result acquired=true current_holder=shard-a current_ddl_text="" current_has_applied_at=false
phase-a: takeover decision takeover=false prior_ddl_text="" new_ddl_text=ir-schema:takeover_target:add-x ddl_text_equal=false
phase-a: RecordDDLText result recorded=true err=<nil>
shard consolidation lease acquired takeover=false
# test SQL-expires the lease
phase-a dump (pre-shard-b-RouteBoundary) row: holder="shard-a" applied_at={false} ddl_text="ir-schema:takeover_target:add-x" seconds_to_expiry={-60 true}
phase-a: RouteBoundary entry stream_id=shard-b ddl_text=ir-schema:takeover_target:add-x
phase-a: ClassifyShape ok shape_kind=add-column
phase-a: lease Acquire entry stream_id=shard-b ddl_text=ir-schema:takeover_target:add-x
phase-a: TryAcquireLease result acquired=true current_holder=shard-b current_ddl_text=ir-schema:takeover_target:add-x current_has_applied_at=false
phase-a: takeover decision takeover=true prior_ddl_text=ir-schema:takeover_target:add-x new_ddl_text=ir-schema:takeover_target:add-x ddl_text_equal=true
phase-a: RecordDDLText result recorded=false err=<nil>
phase-a: Acquire returned ErrLeaseContended err="pipeline: shard consolidation lease contended: lease for \"takeover_target\" taken over between acquire and ddl-record"
phase-a: observeUntilApplied entry observe_timeout=1m0s
# 30 iterations of observe-loop, each showing state=held/expired holder=shard-b — never reaches applied
phase-a: observe loop iteration obs_state=expired obs_holder=shard-b time_remaining=-140.8794ms
RouteBoundary takeover: pipeline: observe lease "takeover_target" timed out after 1m0s
```

### Conclusion (MySQL, hypothesis (a) — specific form)

Sequence:

1. Shard-a's first `Acquire` INSERTs (lease row absent). `RecordDDLText` succeeds because the row's `ddl_text` was empty (changed `""` → `"ir-schema..."`).
2. Test simulates a crash by `Release`-ing without `Apply`, then SQL-expiring the lease.
3. Shard-b's `TryAcquireLease` correctly hits the EXPIRED takeover branch: UPDATEs holder, returns `acquired=true` with the prior holder's `ddl_text` preserved.
4. `LeaseManager.Acquire` correctly sets `takeover=true`.
5. **Bug:** `LeaseManager.Acquire` then calls `RecordDDLText` again with the **same** `ddl_text` (`"ir-schema:takeover_target:add-x"` — both shards derive the same identity-checksum from the same post-IR table). The MySQL driver's default **changed-rows** `RowsAffected()` semantics (`CLIENT_FOUND_ROWS` flag off) returns 0 because the value didn't change → `recorded=false`.
6. `LeaseManager.Acquire` interprets `recorded=false` as `ErrLeaseContended` → caller falls into `observeUntilApplied`, which polls a row that shard-b itself just took over → never reaches APPLIED → 60s timeout.

### Load-bearing lines

- **Decision site (incorrect "contention" interpretation):** `internal/pipeline/shard_consolidation_lease.go:464-471` (the original; shifted after instrumentation):
  ```go
  if ddlText != "" {
      if recorded, err := m.store.RecordDDLText(ctx, tableName, m.streamID, ddlText); err != nil {
          return nil, fmt.Errorf("pipeline: lease record ddl %q: %w", tableName, err)
      } else if !recorded {
          return nil, fmt.Errorf("%w: lease for %q taken over between acquire and ddl-record",
              ErrLeaseContended, tableName)
      }
  }
  ```
- **MySQL changed-rows semantics:** `internal/engines/mysql/control_table.go:246-260` `recordShardLeaseDDLText`. The companion `finalizeShardLeaseApply` (lines 266-292) already documents this exact MySQL trap in a long comment, but the same trap applies to RecordDDLText.

## PG ground truth (CI confirmed)

CI run on PR #69 (`Integration (pipeline-rest-other)`, job 77956190071) surfaced the following for both PG streamer tests:

```
phase-a dump (phase-C-fail): no lease row for table="public.users"
phase-a schema dump (phase-C-fail): users column "id" (bigint)
phase-a schema dump (phase-C-fail): users column "email" (character varying)
phase-a schema dump (phase-C-fail): users column "source_shard_id" (character varying)
```

And from the intercept instrumentation:

- **5 occurrences of `shard consolidation intercept: seeded from cold-start handoff` (one per streamer cold-start across the two failing tests)** — the cold-start seed correctly primes the per-table IR cache.
- **0 occurrences of `phase-a: intercept received SchemaSnapshot`** — the intercept's `slog.DebugContext` on every snapshot arrival NEVER fired across the whole test.

Between the cold-start logs (21:08:27.318Z) and the test's failure point (21:10:04 — 98s later), the log is otherwise silent: no row events, no SchemaSnapshots, no warnings, no errors. The 3 streamers' CDC readers are running but emitting nothing.

### Conclusion (PG, hypothesis: test design, not router bug)

The test applies `ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT TRUE` to all 3 sources (line 615) and waits 90s for the column to land on the target. **The test does not perform any DML on the source after the ALTER**.

PG's `pgoutput` logical-replication plugin **does not decode DDL** — `ALTER TABLE` does not produce a `RelationMessage` on the wire. The plugin emits a `RelationMessage` only when the next DML referencing that relation crosses the slot. Without an `INSERT`/`UPDATE`/`DELETE` after the ALTER, the streamer's PG CDC reader has nothing to emit; the `SchemaSnapshot` that would drive `RouteBoundary` is never produced.

This is **NOT a BoundaryRouter or LeaseManager bug** — it is a test-design issue. The MySQL test (which uses binlog, where DDL is a first-class event class) is unaffected by this distinction.

### Load-bearing lines

- The test issues only `ALTER TABLE` then waits for the target column: `internal/pipeline/shard_consolidation_phase2e_streamer_pg_integration_test.go:613-621`.
- PG's CDC reader emits `SchemaSnapshot` via RelationMessage propagation, which requires post-DDL DML to fire.

### Phase B fix shape (PG side)

Two options:

**Option PG-1: Update the test to issue a post-ALTER DML on every source before checking for the target column.**

```go
const altSQL = `ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT TRUE;`
for i := 0; i < 3; i++ {
    phase2eApplyDDL(t, h.sourceDSNs[i], altSQL)
}
// New: trigger PG's RelationMessage emission via a small DML on each source.
for i := 0; i < 3; i++ {
    phase2eApplyDDL(t, h.sourceDSNs[i], `INSERT INTO users (email) VALUES ('`+phase2eShardLabels()[i]+`_postddl_trigger@example.com');`)
}
if !waitForPhase2eTargetColumn(t, h.targetDSN, "active", 90*time.Second) { ... }
```

This matches operational reality: any real workload will produce DML after a DDL, so the column will arrive via the existing intercept-then-RouteBoundary path. (Side effect: phase D's row count changes from 6 → 9; the test's later assertions need adjusting accordingly.)

**Option PG-2: Force a sluice-side post-DDL flush that drives a SchemaSnapshot synthesis without source DML.**

Cleaner from a test-coverage perspective (pins the "ALTER-only, no DML" case) but requires a new IR/streamer surface — well outside the scope of "fix the failing tests." This would belong to a separate ADR (force-flush-on-coordinated-DDL).

**Recommendation: Option PG-1.** Smallest test change, correct behaviour assertion (a production deploy would have DML after a DDL), no production code change needed.

### Reviewer corollary (PG)

The PG-1 fix interacts with the MySQL fix (Option 1 — skip RecordDDLText on takeover when prior == new). Both fixes together pin the full ADR-0054 Phase 2e cross-engine matrix. A reviewer should confirm:

- MySQL takeover with same-DDL → fixed by Option 1, integration test passes.
- PG streamer 3-source contention with post-ALTER DML → fixed by Option PG-1, integration test passes.
- PG streamer restart-resume → fixed by Option PG-1, integration test passes (with the same DML-after-ALTER pattern).

## Proposed Phase B fix shapes (do not implement in this PR)

Three candidates, ordered by preference:

### Option 1: Skip RecordDDLText on takeover when prior == new

In `LeaseManager.Acquire` (line ~464), skip the `RecordDDLText` call when `takeover && priorDDLText == ddlText` — the takeover branch already preserved the right ddl_text (TryAcquireLease's row update kept the prior text intact and the new holder will record the same value). This is the smallest diff and the most surgical:

```go
needsRecord := ddlText != "" && !(takeover && priorDDLText == ddlText)
if needsRecord {
    ...
}
```

Tradeoff: slightly weakens the "did I really still hold the lease?" check on the takeover path, but the takeover UPDATE itself in TryAcquireLease already established ownership atomically (gated on `applied_at IS NULL`), so this isn't actually a regression in the lease-ownership contract.

### Option 2: Use matched-rows semantics in RecordDDLText

Add `CLIENT_FOUND_ROWS` (or its DSN equivalent `clientFoundRows=true`) to the MySQL engine's DSN — or directly change `recordShardLeaseDDLText`'s UPDATE to detect matched-rows differently (e.g. an `AND lease_holder_stream_id = ? AND applied_at IS NULL AND <some-always-changing-clause>`).

Tradeoff: invasive (touches the engine layer + DSN config) and could ripple to other UPDATEs that depend on changed-rows semantics. Risk of unintended behaviour change elsewhere.

### Option 3: Treat recorded=false on takeover-confirmed-by-row as a no-op success

In `LeaseManager.Acquire`, if `takeover && !recorded`, re-Observe the row to confirm we still hold it. If yes (still `lease_holder_stream_id == m.streamID` AND `applied_at IS NULL`), accept the no-op and continue. If we lost the lease between acquire and now, surface `ErrLeaseLost` (a different error class than `ErrLeaseContended`).

Tradeoff: an extra round-trip; more complexity than Option 1.

**Recommendation:** Option 1 — smallest diff, correct semantics, easy to pin with a unit test (mock-based) plus the existing integration test as the regression assertion.

## Reviewer corollary (per Bug 74 lesson + CLAUDE.md)

When Phase B lands, the reviewer should re-derive the family matrix for the changed-rows MySQL behaviour and confirm:

- The pin covers `priorDDLText == ddlText` takeover (the failing test).
- The pin covers `priorDDLText != ddlText` takeover (operator manually adjusted DDL between shards — Option 1 still records the new text, RecordDDLText returns recorded=true).
- The pin covers `priorDDLText == ""` takeover (defensive: shard-a crashed before RecordDDLText ran — Option 1 takes the record path because the predicate's `priorDDLText == ddlText` is false when prior is "" and new is non-empty).
- A PG counterpart pin (already covered by `TestPhase2e_PG_*`).

## Cleanup (Phase C, after Phase B)

- Revert this PR (#69) entirely — the instrumentation is verbose and was tooling only.
- The two test files cherry-picked from `origin/task-23` and `origin/task-24` should be brought in via their own PR merges (#67, #68) after Phase B fixes them.
