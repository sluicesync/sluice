# sluice v0.46.0 — Source-side retry on transient CDC reader errors

**Closes GitHub issue #19.**

Before v0.46.0, when the source CDC reader's pump hit a transient — `read tcp: ... read: connection reset by peer`, `EOF`, vttablet `code = Aborted/Unavailable/ResourceExhausted`, PG SQLSTATE `57P01` admin-shutdown, etc. — the pump stashed the error via `setErr` and closed the changes channel. The applier saw the close as a normal EOF, committed any pending batch, and returned `nil`. The streamer returned `nil` from `runOnce` — a clean exit code 0 — even though the operator's overnight `sluice sync start` had silently dropped its CDC connection mid-stream.

The ADR-0038 retry loop (v0.42.0) catches *applier-side* transients via the `ir.RetriableError` interface, but never saw a source-side error to classify. v0.46.0 closes that gap.

## Fixed

- **Source-pump errors now classified through `ir.RetriableError`.** New `classifyReaderError` per engine (`internal/engines/mysql/reader_errors.go`, `internal/engines/postgres/reader_errors.go`) wraps every pump-side `setErr` site. The classifier shapes are identical to the applier-side classifier (Vitess 1105 transients, PG 40001 / 40P01 / 57P0x / 08*, `driver.ErrBadConn`, network-text shapes), so reader and applier retry surfaces stay in lockstep.

- **Streamer surfaces source-side errors into the existing retry loop.** Both `coldStart` and `warmResume` now capture the reader's `Err()` method (via an optional-interface probe — every shipping CDC reader exposes one). After `dispatchApply` returns, `runOnce` calls `surfaceSourceError` and, if the source stored a non-cancellation error, returns it wrapped as `pipeline: source cdc reader: <err>`. The wrap satisfies `errors.As(&ir.RetriableError{})` for the classified transients, so `runWithRetry`'s existing exponential-backoff loop handles the retry transparently.

- **Context-cancellation errors filtered.** The pump's check for `context.Canceled` / `context.DeadlineExceeded` is best-effort, and an outer-ctx-driven shutdown must not surface as a retriable error. `surfaceSourceError` filters these to avoid spurious retries during normal shutdown.

## Migration / Compatibility

- **Drop-in upgrade from v0.45.x.** No flag changes, no behaviour change for non-transient source errors.
- **The previously-silent exit-0 failure mode (#19) is closed.** Source-pump transients now flow through the same retry loop applier transients already used. Operators on the default `--apply-retry-attempts 8` see automatic recovery; operators on `--apply-retry-attempts 1` see the previously-silent error surface as a non-zero exit code — strict improvement for the single-attempt opt-out path.

## Who needs this release

- **Anyone running `sluice sync start` overnight or in long-running mode against any source**: **upgrade**. The #19 silent-exit failure was the highest-risk known issue between v0.42.0's applier-retry landing and today — overnight test cycles produced clean exit codes on what were actually mid-stream transient connection resets, with no operator-visible signal that the stream had dropped.
- **Anyone with monitoring on sluice exit codes**: opt-in benefit. Source-side transients now surface as non-zero terminal errors (after retry exhaustion) instead of exit 0.
- **Operators on `--apply-retry-attempts 1`**: drop-in benefit. Previously-silent source transients now surface immediately as terminal errors.

## Verification surface

- **3 new test files + 23 subtests**:
  - `internal/engines/mysql/reader_errors_test.go` — 9 subtests confirming MySQL reader/applier classifier shape parity.
  - `internal/engines/postgres/reader_errors_test.go` — 10 subtests confirming PG reader/applier classifier shape parity (40001, 40P01, 57P01, 08006 retriable; 23505 non-retriable).
  - `internal/pipeline/streamer_source_error_test.go` — 4 funcs covering `surfaceSourceError`: nil-fn passthrough, nil-return passthrough, 4 context-cancellation filter cases, and the GitHub #19 happy path returning a transient verbatim for the retry loop.
- **End-to-end source-transient retry validation deferred to operator re-test** with `--apply-retry-attempts 8` running through a forced TCP reset on the source connection.

## Issue tracker after v0.46.0

| # | State | Resolution |
|---|---|---|
| 12 | ✅ Closed | v0.40.0 — CDC generated-column filter |
| 13 | ✅ Closed | v0.42.0 — bounded retry on transient applier errors |
| 14 | ✅ Closed | v0.43.0 — VStream COPY-phase dedup |
| 15 | ✅ Closed | v0.41.0 — pre-CDC anchor write |
| 16 | ✅ Closed | v0.44.0 — PlanetScale backup chain-resume via VStream COPY |
| 17 | ✅ Closed | v0.45.0 — Safe Migrations preflight + `--schema-already-applied` |
| 18 | 🟡 Open (in progress) | v0.45.0 Phase 1+2 shipped; Phase 3 (AIMD controller) deferred pending operator-collected telemetry from v0.45.0 runs |
| 19 | ✅ Closed | v0.46.0 — **source-side retry on transient CDC reader errors** |
