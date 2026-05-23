# sluice v0.42.0 — bounded retry on transient applier errors (closes #13)

**Closes GitHub issue #13.** Implements [ADR-0038](docs/adr/adr-0038-applier-retry-on-transient-errors.md). Before v0.42.0, the first transient applier error during CDC exited the entire `sluice sync start` process:

- **PlanetScale-MySQL / Vitess**: `Error 1105 (HY000): vttablet: rpc error: code = Aborted desc = transaction <id>: in use: for tx killer rollback` — routine on managed Vitess deployments under throttling / vtgate restarts / failover.
- **Postgres**: `SQLSTATE 40001` (serialization_failure), `40P01` (deadlock_detected) — routine under heavy concurrent write load.

Operators were wrapping the binary in a supervisor (systemd `Restart=on-failure`, k8s pod restart, `while :; do sluice ...; done`) and relying on warm-resume to retry. v0.42.0 brings retry inside the streamer: each per-engine classifier categorises errors as retriable or terminal, and `Streamer.Run` wraps the apply pipeline with exponential backoff that resets on observed CDC-position progress.

## Added

### Error classification

- **`ir.RetriableError` interface** (`internal/ir/applier_retry.go`) — the optional surface an applier error can implement to signal "retry me". Preserves the original error via `Unwrap`, so `errors.Is` / `errors.As` chains keep working.
- **MySQL/Vitess classifier** (`internal/engines/mysql/applier_errors.go::classifyApplierError`):
  - Retriable: `Error 1213` (InnoDB deadlock), `Error 1105` with `code = Aborted` / `Unavailable` / `ResourceExhausted` from vttablet, `driver.ErrBadConn`, `io.EOF`, connection-reset / refused / broken-pipe / i/o-timeout.
  - **NOT retriable**: `Error 1062` (duplicate key) — masks data bugs and sluice idempotency gaps (e.g. GitHub issue #14, which intentionally stays terminal). Non-transient Vitess gRPC codes (`InvalidArgument`, `FailedPrecondition`, `NotFound`, `PermissionDenied`) also stay terminal.
- **Postgres classifier** (`internal/engines/postgres/applier_errors.go::classifyApplierError`):
  - Retriable: `SQLSTATE 40001` (serialization_failure), `40P01` (deadlock_detected), `57P01` / `57P02` / `57P03` (admin/crash/cannot-connect-now shutdown), the entire `08*` connection_exception class, driver-level transients.
  - **NOT retriable**: `23505` (unique_violation) — same rationale as MySQL `1062`.

### Pipeline retry loop

`Streamer.Run` now dispatches to `runWithRetry` when `ApplyRetryAttempts > 1`. The loop:

1. Opens a side-channel applier to read the persisted CDC position between attempts.
2. Runs `runOnce` (the pre-v0.42.0 `Run` logic, renamed).
3. On `errors.As(... &ir.RetriableError{})` match: reads the post-attempt position and compares to the pre-attempt position.
4. If progressed → resets the consecutive-failure counter to 1 (a successful batch landed; that retry made forward progress).
5. If no progress → increments the counter.
6. Beyond the attempts budget → wraps the underlying error with `pipeline: apply retry budget exhausted after N consecutive failures at position "..."` and returns.
7. Otherwise sleeps the computed exponential-backoff interval and loops.

`ResetTargetData` is cleared after iteration 1 so a transient applier failure during retry doesn't re-trigger the destructive `--reset-target-data` flow.

### CLI flags on `sluice sync start`

| Flag | Default | Notes |
|---|---|---|
| `--apply-retry-attempts N` | 8 | Maximum consecutive retriable failures before exit. `1` = no retry (pre-v0.42.0 behaviour). |
| `--apply-retry-backoff-base DUR` | `100ms` | Base interval for exponential doubling between retries. |
| `--apply-retry-backoff-cap DUR` | `30s` | Per-attempt upper bound on backoff. |

Default 8-attempt schedule: 100ms → 200ms → 400ms → 800ms → 1.6s → 3.2s → 6.4s → 12.8s (≈25.5s total wait across 8 attempts). Bounded at ~4 minutes worst-case if the cap is hit.

## Migration / Compatibility

- **Drop-in upgrade from v0.41.x.** No IR changes that break callers; the applier's `Apply` / `ApplyBatch` signatures are unchanged. Engines that don't implement the classifier behave identically to v0.41.x.
- **Behaviour change worth flagging**: `sluice sync start` no longer exits on the first Vitess `Error 1105` or PG `40001`. Operators expecting fail-fast can set `--apply-retry-attempts=1`.
- **Stream-supervisor integrations** keep working — they're the outer recovery layer for `retry budget exhausted` exits.
- **GitHub issue #14** (PS-MySQL cold-start duplicate-PK) is explicitly NOT addressed by this release. `Error 1062` stays non-retriable per ADR-0038 — masking it would hide the underlying issue. The Phase A instrumentation for #14 lives on branch `phase-a-issue-14`; the fix lands in v0.42.x or v0.43.0 once the repro produces ground truth.

## Who needs this release

- **Anyone running `sluice sync start` against PlanetScale-MySQL / Vitess**: **upgrade immediately**. This is the operationally load-bearing fix.
- **Operators on PG → PG under concurrent write load**: drop-in benefit on `40001` / `40P01`.
- **Operators with supervisor wrappers**: still supported. v0.42.0 absorbs the noise; supervisor handles "genuinely stuck".
- **Operators not using continuous-sync (just `sluice migrate`)**: drop-in; no behaviour change.

## Verification surface

- **22 new unit tests** across `internal/engines/{mysql,postgres}/applier_errors_test.go` covering each retriable shape, the explicit non-retriable shapes (`1062` / `23505`), the default-deny invariant (unknown errors stay non-retriable), Vitess non-transient gRPC codes correctly staying terminal, and the leaf `classifyVitessMessage` helper.
- **New `internal/pipeline/streamer_retry_test.go`** pins the exponential-doubling schedule, hint-floor semantics, the cap, and the 8-attempt total-budget promise (asserts total wait < 4 minutes per ADR-0038).
- **CI integration tests pass on tag v0.42.0** — the retry loop's outer-Run shape preserves test compatibility (stub appliers in tests don't implement `RetriableError`, so retry never engages and behaviour is identical to v0.41.x).

## Companion — GitHub issue #14 Phase A on `phase-a-issue-14`

While v0.42.0 was in flight, a separate Phase A instrumentation chunk landed on branch `phase-a-issue-14` (commit `b995afb`). It adds temporary DEBUG-level slog probes at:

- `internal/engines/mysql/row_writer.go::writeBatched` — PS-MySQL bulk-write batch boundaries (hypothesis a — writer-side).
- `internal/engines/mysql/cdc_vstream_snapshot.go::bufferCopyRow` — VStream COPY chunk emit boundaries (hypothesis b — VStream-side).

Both gated at `slog.LevelDebug` with `phase_a_probe="github_issue_14"`. Operators reproducing #14 with `--log-level=debug` can grep for that key to read the ground truth: do consecutive VStream chunks overlap by PK range (hypothesis b), or does a single batch contain duplicate PKs (hypothesis a)? The fix lands in v0.42.x or v0.43.0 once Phase A reads true.
