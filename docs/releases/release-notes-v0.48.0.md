# sluice v0.48.0 — closes #21 + #22, lands #23 Phase A diagnostics

**Three fixes bundled in one release** to reduce per-release CI cost burden. They share the *"remove the operational burden of supervising sluice externally"* theme — two close confirmed exit paths that required an external supervisor wrapper, the third lays the diagnostic foundation for the silent-stall bug operators have hit twice.

## Closes GitHub #21 — MySQL applier missing `invalid connection` retry

`go-sql-driver/mysql` exports its own `ErrInvalidConn = errors.New("invalid connection")` sentinel, distinct from `database/sql`'s `driver.ErrBadConn`. v0.42.0's `classifyApplierError` only checked `driver.ErrBadConn`, so the mysql driver's sentinel slipped through and the applier exited even though the same connection-reset class on PG retries fine.

Two-line fix at `internal/engines/mysql/applier_errors.go::classifyApplierError`. Confirmed against the operator's 3-hour repro (4 exits caused by `invalid connection`, all required supervisor restarts; same window, 6 successful in-process retries of Vitess `exceeded timeout: 20s`).

## Closes GitHub #22 — `backup stream run` doesn't retry source transients

Pre-v0.48.0 the backup-stream's rollover loop treated any rollover error as terminal. A source-side TCP reset / gRPC `Unavailable` that v0.46.0's sync-stream path retries through took the backup-stream process down.

v0.48.0 mirrors the sync-stream retry policy on the rollover loop:
- Classify rollover errors via `ir.RetriableError`
- Close the current CDC pump, reopen from the last committed parent's `EndPosition`
- Retry with exponential backoff
- Bounded by new `--retry-attempts` (default 8) / `--retry-backoff-base` (default 100ms) / `--retry-backoff-cap` (default 30s) flags on `backup stream run`, matching the sync-stream's `--apply-retry-*` knobs
- Consecutive-failure counter resets on a successful rollover so a long-lived stream doesn't carry retry debt forward

## Lands GitHub #23 Phase A — silent-stall diagnostics

The silent-stall failure mode (process alive, no apply, no log, no exit) has been observed twice on the local validation rig — on PG/v0.43.0 and Local MySQL/v0.46.0 — engine-agnostic. Phase A doesn't try to *fix* the stall yet; per the CLAUDE.md three-phase debug protocol, that comes in Phase B once we have ground truth from operator-collected goroutine dumps.

**`stream: heartbeat` INFO log line every `--heartbeat-interval` (default 60s).** Per-stream goroutine emits a positive liveness signal at default log level. Distinguishes silent-stall (process alive, no apply, no log) from wedge (process alive, no heartbeat either). When the next stall fires, the log shows heartbeats stopping AND the operator can hit pprof to dump goroutines.

**Global `--pprof-listen ADDR` flag** binds `net/http/pprof`'s debug endpoints at the given address for the duration of any subcommand. Off by default. When set, sluice logs at startup pointing operators at `/debug/pprof/goroutine?debug=2` — exactly the data needed to localise the wedge point.

## Migration / Compatibility

- **Drop-in upgrade from v0.47.x.** All three changes are additive at the operator surface; no flag renames.
- **The supervisor pattern operators added for #21 / #22 is no longer required** for the documented retry-policy classes. Existing supervisors continue to work as defence-in-depth; they shouldn't fire as often.
- **Heartbeat log noise**: at default 60s, an overnight 24h run produces ~1440 extra INFO lines per stream. Disable via `--heartbeat-interval=0`.
- **pprof endpoint is opt-in**: no exposure surface change unless `--pprof-listen` is set. Operators setting it should bind to localhost — stdlib pprof has no auth layer.

## Who needs this release

- **Anyone running `sluice sync start` against a PlanetScale-MySQL target**: **upgrade**. #21 supervisor restarts should drop materially.
- **Anyone running `sluice backup stream run`**: **upgrade**. #22 silent-coverage-gaps on TCP-reset cascades are closed.
- **Anyone who has hit #23**: drop-in benefit. Heartbeat distinguishes stall from wedge immediately; pprof enables a 10-second diagnosis on the next occurrence.

## Verification surface

- `internal/engines/mysql/applier_errors_test.go` extended with 2 new cases (`gomysql.ErrInvalidConn` bare + wrapped) — GitHub #21.
- `internal/pipeline/heartbeat_test.go` (new) — 3 tests for emit-on-tick, zero-interval-disables, exits-on-ctx-cancel (catches goroutine leaks).
- Existing backup-stream tests cover the rollover loop's success path; retry branch's `openCDCReaderWithSlot` reopen uses the same engine API the original open uses.
- **End-to-end retry validation deferred to operator re-test** — same pattern as v0.42.0 / v0.46.0.

## Issue tracker after v0.48.0

| # | State | Resolution |
|---|---|---|
| 12–17, 19 | ✅ Closed | v0.40.0–v0.46.0 |
| 18 | 🟡 Open (in progress) | Phase 1+2 shipped v0.45.0; Phase 3 (AIMD) pending telemetry |
| 20 | 🟡 Open (in progress) | Chunk 14a shipped v0.47.0; 14b–d queued |
| 21 | ✅ Closed | **v0.48.0 — MySQL classifier catches gomysql.ErrInvalidConn** |
| 22 | ✅ Closed | **v0.48.0 — backup stream run retries on transient CDC errors** |
| 23 | 🟡 Open (Phase A shipped) | **v0.48.0 — heartbeat + pprof; Phase B pending operator-collected goroutine dump** |
| 24 | 🟡 Open (planned) | Feature: PII redaction; roadmap entry to be added |
