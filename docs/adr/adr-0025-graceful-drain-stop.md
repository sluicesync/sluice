# ADR-0025: Graceful-drain `sync stop` + ackLSN floor on warm-resume

## Status

Accepted. Implemented in `internal/engines/postgres/cdc_reader.go::ackLSN` (the load-bearing data-correctness fix), `internal/pipeline/stop_signal.go::pollStopSignal` (graceful-drain shape with hard-timeout watchdog), and `internal/pipeline/streamer.go::Run` (separates `streamCtx` from `applyCtx` so stop-signal cancels the reader for clean drain rather than the applier for rollback).

## Context

ADR-0020 introduced the `lsnTracker` SPSC channel to enforce slot-ack-after-apply for the Postgres CDC path: the keepalive routine reads `applied` from the tracker and sends that as `confirmed_flush_lsn`, so the slot can't advance past durably-applied work. Bug 15 in the v0.4.0 night soak surfaced as a permanent row gap from a `sync stop` mid-batch; ADR-0020 was the canonical fix, and the integration test (`TestStreamer_PostgresToPostgres_StopRestartNoLoss`) confirmed the post-restart wedge was eliminated.

The v0.6.0 real-world testing report flagged Bug 15 as **partially fixed**: the integration test passed (because it timed `RequestStop` to fire well after the first apply commit, by which point `applied > 0`), but a CLI `sync stop` issued shortly after stream start still dropped 8-21 events. Empirical confirmation against a sustained-writer repro showed the slot's `confirmed_flush_lsn` advancing **past** the persisted `source_position` by ~16 KB — events between those two LSNs got skipped on warm-resume.

The slot acks past applied. The `applied` invariant ADR-0020 was supposed to enforce was being broken.

### Root cause

`ackLSN` (in `cdc_reader.go`) had this branch:

```go
applied := r.appliedLSN.LoadApplied()
if applied == 0 {
    // No applied feedback yet — keep the slot alive by sending
    // startLSN-equivalent (the streamedLSN at this point is
    // startLSN until the first Commit comes through).
    return streamedLSN
}
return applied
```

The comment claimed `streamedLSN == startLSN until the first Commit comes through`. That's wrong. The pump updates `streamedLSN` on every `CommitMessage` parsed off the WAL stream:

```go
case *pglogrepl.CommitMessage:
    *streamedLSN = m.CommitLSN
    return nil
```

So `streamedLSN` advances as soon as the pump *parses* an event off the wire — well before the applier durably commits it to the target. The tracker stays at `applied=0` until the first batch finishes; during that window every keepalive ackes `streamedLSN`, advancing `confirmed_flush_lsn` past events that haven't been applied.

This is silent for 99% of streams — the first batch commits within seconds and `applied` takes over before any meaningful WAL window is acked off. But on warm-resume from a populated stream, where the writer is firing immediately and `--apply-batch-size > 1` means the applier holds in-flight work for tens of seconds, the window is wide enough to lose entire batches. The integration test happened to wait for the first batch to commit before issuing `RequestStop`, masking the bug; real workloads hit the window directly.

The integration test passed because the test waits for `dst row count > seed + 50` before issuing `RequestStop` — that wait condition implies at least one batch has committed (the bulk-copy phase delivers the seed but doesn't update the tracker; the wait-past-seed condition implicitly waits for the first CDC apply commit).

## Decision

Two layered fixes:

### 1. `ackLSN` anchors at `startLSN` until the first applied report

```go
func (r *CDCReader) ackLSN(streamedLSN, startLSN pglogrepl.LSN) pglogrepl.LSN {
    if r.appliedLSN == nil {
        return streamedLSN
    }
    applied := r.appliedLSN.LoadApplied()
    if applied == 0 {
        return startLSN  // <-- the fix
    }
    return applied
}
```

`startLSN` is the LSN the pump started streaming from (cold-start: snapshot LSN; warm-resume: persisted_position's LSN). It's the safe floor — the stream's previous instance already had work past `startLSN` durably applied at startup, so reporting it as `confirmed_flush_lsn` is at-worst a no-op (no slot advance). Once the applier commits its first batch on the new instance, the tracker takes over and the keepalive sends `applied` going forward.

This is a one-line, one-parameter change. The callsite in `pump` adds `startLSN` to the `ackLSN` call. No other plumbing.

### 2. Graceful-drain shape for `sync stop`

The pre-fix `pollStopSignal` cancelled `applyCtx`, which made the applier's `applyOneBatch` see `<-ctx.Done()` and roll back the open transaction — dropping any rows already dispatched into the in-flight batch. With the ackLSN fix, those rolled-back rows are still safely retained on the source slot (because `applied` never advanced past them), so warm-resume redelivers and the rows land. So the data-loss bug is closed by fix #1 alone.

But the rollback-and-redeliver shape has a UX cost: every `sync stop` causes a brief storm of redelivered events on the next start. The cleaner semantics is to **drain** the in-flight batch — let the applier's existing `channelClosed` branch commit the partial batch, after which the position write captures everything that was already in the tx.

Implementation:

- Streamer's `Run` now derives **two** cancellable contexts from the parent: `streamCtx` (scopes the CDC reader's pump) and `applyCtx` (scopes the apply loop).
- `pollStopSignal` cancels `streamCtx` on stop-flag observation. The pump exits, `defer close(out)` closes the change channel, and the applier's `applyOneBatch` hits the `case c, ok := <-changes; if !ok` branch which calls `commitBatch(...)` with whatever events were already dispatched. Clean commit; position written; tracker reports applied.
- A watchdog goroutine waits for either `pollCtx.Done()` (= apply finished naturally) or `time.After(stopDrainTimeout)` (default 30s); on timeout it cancels `applyCtx` as a hard fallback.

Parent ctx cancel (Ctrl-C) still cancels both children automatically — that path remains the abort shape (rollback + return). `sync stop` is the graceful path.

### Why not just fix #2 alone

Without fix #1, a `sync stop` that fires *during the warm-up window* (before the first apply commit) still has a problem: the pump may have parsed events past startLSN and the keepalive may have ackd them. The graceful drain commits the partial batch, but the slot is already past those events, and on a subsequent stop, those events would be skipped. Fix #2 is the UX layer; fix #1 is the correctness floor.

## Consequences

- **Bug 15 closed for the warm-up window.** The repro that previously dropped 25-42 rows now drops 0; warm-resume catches up cleanly.
- **`sync stop` is a clean commit boundary, not a rollback boundary.** Fewer events to redeliver on next start; cleaner position semantics for operators correlating stop-restart pairs in dashboards.
- **One-shot stops are now safe under any timing.** The previous mental model "wait long enough after start before stopping" is no longer load-bearing.
- **`--apply-batch-size` no longer trades correctness for throughput.** Larger batches still amortise commit cost; they no longer expand the loss window.
- **Operator-facing: the stop-info log line changes from "draining and exiting" to "draining stream and exiting"** — small wording tweak that reflects the actual mechanism (CDC reader stops, applier drains, exits).

## Migration notes

No state-format or interface changes visible to operators. Engines without applier-LSN feedback (legacy non-streamer callers) keep the v0.4.0 behaviour (`return streamedLSN`) — fix #1 only kicks in when a tracker is wired, which is the streamer's standard plumbing today.

The `pollStopSignal` signature changed (added `cancelStream` parameter); this is a pipeline-package internal symbol so no external callers exist.

## Verification

- Unit test `TestAckLSN_AnchorsAtStartLSNUntilFirstApply` (`internal/engines/postgres/lsn_tracker_test.go`) pins the contract: when `applied=0`, ack returns `startLSN` even when streamedLSN has advanced past it.
- Unit test `TestPollStopSignal_HardCancelsApplyOnDrainTimeout` (`internal/pipeline/stop_signal_test.go`) verifies the watchdog escalates to `cancelApply` after `drainTimeoutForTest` elapses.
- Unit test `TestPollStopSignal_WatchdogExitsCleanlyOnApplyDone` verifies the watchdog exits without firing `cancelApply` when apply finishes naturally.
- Real-world repro (sustained writer, `--apply-batch-size=50`, mid-stream `sync stop`): pre-fix dropped 25-42 rows; post-fix drops 0.

## Added in v0.9.0: `sync stop --wait`

Operator feedback from v0.8.0 stretch testing: when coordinating an ALTER window, operators have no clean handle that says "drain has completed; safe to ALTER now". Today's `sync stop` is fire-and-forget — it writes `stop_requested_at`, the streamer drains within ~30s, and the operator either polls `sync status` or `pgrep`s the streamer process to know when it's safe to issue the ALTER.

v0.9.0 closes that gap with a `--wait` flag on `sluice sync stop` that blocks the CLI until the streamer confirms graceful-drain completion.

### Mechanism

The flag-clearing convention. The streamer already calls `applier.ClearStopRequested(streamID)` at startup (so a stale flag from a previous run doesn't immediately exit the next `sync start` — Bug 11 fix from v0.3.2). v0.9.0 adds a second clear point: after a stop-signal-driven graceful drain, the streamer clears the flag again as the very last step of `Streamer.Run`. The CLI's `--wait` polls `ReadStopRequested` until it returns `false` and exits success.

Two pieces had to land together:

1. **`pollStopSignal` exposes whether it observed the flag.** Added an optional `*atomic.Bool` parameter; the poll goroutine sets it to true the moment it first sees `stop_requested_at IS NOT NULL`. The streamer reads it after `dispatchApply` returns and clears the flag *only* when the observed bit is set — so a Ctrl-C / outer-ctx cancel mid-stream doesn't masquerade as a graceful drain.
2. **CLI `waitForStopComplete` polls the cleared-flag signal.** 1s polling cadence (the streamer-side poll is the rate-limiting factor at 5s), bounded by `--timeout` (default 5 minutes), exits non-zero with a clear message on timeout. The stop request itself stays written, so the streamer continues draining in the background after CLI timeout — re-running `sync stop --wait` keeps watching the same flag.

### Why not a new column

A new `stopped_at` column on `sluice_cdc_state` would carry the same signal more explicitly, but it's a schema migration on every existing target and the cleared-flag pattern is already well-formed (idempotent: re-issuing `sync stop` re-sets the flag; tolerant of restart: the streamer always clears at startup). The cleared-flag approach reuses the lifecycle the existing code already manages.

### Backwards compatibility

A `sync stop --wait` against a streamer running an older sluice version (one that doesn't clear the flag on graceful exit) blocks until `--timeout` fires. Acceptable: `--wait` is opt-in, the timeout puts a bound, and the operator gets a clear "did not complete drain" message that points them at `sluice sync status`. Without `--wait` the behaviour is unchanged.

### Verification

- Unit test `TestPollStopSignal_SetsObservedOnFlag` (`internal/pipeline/stop_signal_test.go`) pins the observed-flag contract.
- Unit tests `TestWaitForStopComplete_FlagClears` / `_Timeout` / `_ContextCancel` / `_NonPollingApplier` (`cmd/sluice/sync_stop_test.go`) cover the four CLI poll paths: success, timeout, outer-ctx cancel, and graceful degradation when the applier doesn't implement `ReadStopRequested`.
