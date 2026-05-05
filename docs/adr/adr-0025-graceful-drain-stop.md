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
- Real-world repro at `C:\code\sluice-testing\workspace\bug15_repro_dev.sh` (sustained writer, `--apply-batch-size=50`, mid-stream `sync stop`): pre-fix dropped 25-42 rows; post-fix drops 0.
