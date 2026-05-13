# sluice v0.45.0 — Safe Migrations operator workflow + batch-latency telemetry

**Closes GitHub issue #17 (Option B — proper structural fix) + lands GitHub #18 Phase 1+2** (batch-latency telemetry + cross-region safety rail).

Operators bootstrapping `sluice sync start` against a PlanetScale-MySQL branch with Safe Migrations enabled (the recommended production configuration) previously hit:

```
sluice: error: pipeline: create tables: mysql: create table "audit_log":
  Error 1105 (HY000): direct DDL is disabled
```

… after sluice had already captured the snapshot position and reached the schema-apply phase. No actionable hint, no recovery path that didn't involve toggling Safe Migrations off and on around the run.

v0.45.0 adds:

1. **`--schema-already-applied` flag** — the recommended workflow for Safe Migrations operators. Pre-create schema via PlanetScale deploy request, then run sluice with this flag to skip every DDL phase.
2. **Focused Safe Migrations error wrap** — when sluice does hit the refusal, the message names the new flag and walks the operator through both recovery paths.
3. **Three DSN-parser papercut fixes** — no more doubled `"invalid DSN: invalid DSN:"` prefix; clearer hint for the `/db/branch` shape; suppression of the retry-policy fallback WARN for parse/credential errors.
4. **DEBUG batch-latency telemetry** — foundation for the v0.46.0+ AIMD auto-tuner.
5. **Cross-region safety rail** — startup WARN when target is PlanetScale AND `--apply-batch-size > 50`, naming the Vitess 20s tx-killer constraint.

## Added — #17 main fix

- **`--schema-already-applied` flag on `sluice sync start`.** When set, sluice skips:
  - `CREATE TABLE` / `CREATE INDEX` / `ADD FOREIGN KEY` / `CREATE VIEW`
  - `SyncIdentitySequences`
  - `EnsureControlTable` (the `sluice_cdc_state` table)
  - The cold-start preflight refusal (Bug 9)

  Operator promise: every source table exists on the target with a compatible schema, AND the `sluice_cdc_state` control table has been pre-created. Bulk-copy runs into operator-prepared empty tables. Sluice does NOT validate the schema match.

  Operators on PlanetScale Safe Migrations branches push schema changes via deploy requests (including the `sluice_cdc_state` DDL — see `internal/engines/mysql/control_table.go` for the exact shape), then run sluice with this flag.

## Fixed — #17 papercuts

- **Safe Migrations DDL refusal — operator-friendly error.** When MySQL/Vitess returns `Error 1105 (HY000): direct DDL is disabled`, the new `wrapDDLError` helper produces a multi-line error pointing at both recovery paths:
  - (a) pre-create schema via a PlanetScale deploy request, then re-run with `--schema-already-applied`
  - (b) temporarily disable Safe Migrations, run sluice to bootstrap, then re-enable

  Wired at every DDL exec site in `internal/engines/mysql/schema_writer.go` and `control_table.go`.

- **`parseDSN` no longer produces a doubled `"invalid DSN:"` prefix.** Driver and sluice each added an `"invalid DSN: ..."` prefix; sluice now strips the redundant inner copy.

- **`/db/branch` DSN shape produces an actionable hint.** PlanetScale credentials are branch-scoped (the branch is implicit in the user/password); operators sometimes try to encode the branch in the DSN path. The driver's generic `"did you forget to escape a param value?"` sent operators down the wrong rabbit hole. New `dsnShapeHint` detects path-with-extra-slash (correctly skipping the parenthesised `protocol(address)` block so unix sockets like `/tmp/mysql.sock` don't false-positive) and prepends a clearer hint.

- **Retry-policy WARN suppression on non-retriable startup failures.** The ADR-0038 retry loop's `"applier: retry policy disabled (cannot open position reader); falling through to single-attempt run"` WARN fired even when the underlying error was a parse failure or bad credentials (where the inner `runOnce` is about to surface the same error and exit). New `isTransientOpenError` classifier downgrades to DEBUG for `invalid DSN` / `parseDSN failure` / `Access denied` / `Unknown database` shapes; legitimate network transients still WARN.

## Added — #18 Phase 1 + 2

- **DEBUG-level batch-latency telemetry on every applier-committed batch.** Both MySQL and PG appliers emit:

  ```
  level=DEBUG msg="applier: batch latency" stream_id=foo rows=N millis=M
  ```

  per non-empty batch, measuring wall-clock from "batch start" through "position write + tx commit returns." Operators running `--log-level=debug` (and the future AIMD auto-tuner) get per-target per-batch cost visibility. Empty / idle-flush batches are elided to avoid noise during quiet periods.

- **Cross-region safety rail at startup.** When target engine name is `planetscale` AND `--apply-batch-size > 50`, sluice WARNs at startup:

  ```
  level=WARN msg="apply-batch-size > 50 against a planetscale target may exceed Vitess's 20s transaction-killer timeout under sustained CDC load"
       apply_batch_size=100 safe_threshold=50
       hint="if you see frequent 'mysql: applier: batch rollback on error' with 'code = Aborted ... for tx killer rollback', reduce --apply-batch-size to 25-50. See GitHub #18 for the auto-tuning controller planned for a future release."
  ```

  Conservative classification — false positives on same-region PS-MySQL are accepted to avoid missing the cross-region foot-gun. Phase 3 (AIMD controller, v0.46.0+) will replace this static rail with an auto-discovered per-target ceiling informed by the new batch-latency telemetry.

## Migration / Compatibility

- **Drop-in upgrade from v0.44.x** for vanilla MySQL / PG operators not using PlanetScale.
- **PlanetScale operators**: drop-in. The new `--apply-batch-size > 50` warning is informational; existing values continue to work.
- **Safe Migrations operators (new workflow)**: pre-create schema via PlanetScale deploy request → run `sluice sync start --schema-already-applied`. The `sluice_cdc_state` DDL must be included in the deploy request.
- **Operators pattern-matching on the exact DSN-error text** (rare): the doubled `"invalid DSN:"` prefix is gone; update scripts if relying on the old text.

## Who needs this release

- **Anyone running `sluice sync start` against a PlanetScale-MySQL branch with Safe Migrations enabled**: **upgrade**. The `--schema-already-applied` workflow is the supported path forward.
- **Anyone running cross-region PS-MySQL with `--apply-batch-size > 50`**: drop-in benefit. The new safety-rail WARN surfaces the tx-killer foot-gun at startup.
- **Anyone with telemetry pipelines scraping DEBUG logs**: opt-in benefit; the new `applier: batch latency` line is the foundation for v0.46.0+'s AIMD auto-tuner.

## Verification surface

- **8 new unit tests + subtests** across `internal/engines/mysql/{schema_writer_errors,connect_dsn}_test.go` + `internal/pipeline/streamer_warn_test.go`:
  - `TestWrapDDLError_SafeMigrationsBlockedIsWrapped` — 1105 wrap recognises Safe Migrations message + names both recovery paths
  - `TestWrapDDLError_OtherErrorsUnchanged` — default-pass invariant
  - `TestParseDSN_NoDoubleInvalidPrefix` — no more `"invalid DSN: invalid DSN:"`
  - `TestDSNShapeHint_BranchPathDetected` — `/db/branch` → actionable hint
  - `TestDSNShapeHint_PlainPathNoHint` — well-formed DSNs + unix sockets get no false-positive
  - `TestMaybeWarnApplyBatchSizeRisky_*` (3 subtests) — Phase 2 warn-policy correctness
  - `TestIsTransientOpenError_*` (5 subtests) — papercut WARN suppression classification
- **End-to-end Safe Migrations validation** deferred to operator re-test against a real Safe-Migrations-enabled PlanetScale branch.

## Issue tracker after v0.45.0

| # | State | Resolution |
|---|---|---|
| 12 | ✅ Closed | v0.40.0 — CDC generated-column filter |
| 13 | ✅ Closed | v0.42.0 — bounded retry on transient applier errors |
| 14 | ✅ Closed | v0.43.0 — VStream COPY-phase dedup |
| 15 | ✅ Closed | v0.41.0 — pre-CDC anchor write |
| 16 | ✅ Closed | v0.44.0 — PlanetScale backup chain-resume via VStream COPY |
| 17 | ✅ Closed | v0.45.0 — **Safe Migrations preflight + --schema-already-applied** |
| 18 | 🟡 Open (in progress) | v0.45.0 Phase 1+2 shipped; Phase 3 (AIMD controller) deferred to v0.46.0+ pending telemetry collection |
