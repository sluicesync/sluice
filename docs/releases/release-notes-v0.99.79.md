# sluice v0.99.79

**Fixes a clean-shutdown hang in the cross-table concurrent VStream cold-copy: cancelling a sync (Ctrl-C / graceful drain) during the copy phase could hang when every concurrent stream was parked in backpressure.** Affects only the concurrent VStream cold-copy (`vstream_copy_table_parallelism` / auto-shard with more than one stream); serial and single-stream copies were never exposed.

## Fixed

**Concurrent VStream cold-copy (ADR-0099) no longer hangs on cancellation under full backpressure.** Each of the K concurrent copy streams parks its producer in a condition-variable backpressure wait when its per-stream byte budget is full and the consumer isn't draining. That wait wakes only on a broadcast (a peer stream erroring, or copy completion) — it does not observe context cancellation directly. Normally a cancel is noticed by some stream still in a cancellation-observing select, which trips the shared failure path and broadcasts, freeing the parked streams. But if *all* K streams happened to be parked at the instant a bare context cancel fired — an operator Ctrl-C or graceful drain during the cold-copy phase, with nothing draining the buffered rows — no goroutine observed the cancel, nothing broadcast, and the copy's wait-group join blocked indefinitely: the shutdown hung.

The fix adds a small cancel-waker that, on context cancellation, trips the shared failure path (setting the error and broadcasting) so every parked stream wakes and unwinds promptly. It has no effect on the normal-completion path (it watches the copy-done signal and exits cleanly, with a re-check so a cancel/completion race can never retroactively fail a copy that already finished). Throughput, the happy path, and the exactly-once / position-recording contract are all unchanged — a cancelled copy still records no position, exactly as before for a cancel a stream detected itself.

This was found as an intermittent race-detector test timeout under heavy parallel load and pinned with a regression test that deterministically reaches the all-streams-parked state (it hangs to the timeout on the unpatched code and passes with the fix).

## Compatibility

Fully backward-compatible. No flags, no config, no data/schema changes, and no change to how a healthy copy or sync behaves. The fix only affects what happens when a concurrent VStream cold-copy is cancelled while fully backpressured. Operators not using the concurrent VStream copy (`vstream_copy_table_parallelism`>1 / multi-stream auto-shard) are unaffected.

## Who needs this

Operators running `sluice sync` against a **Vitess/PlanetScale source with the concurrent cold-copy enabled** (multi-shard auto-shard copy, or `vstream_copy_table_parallelism`>1) who may cancel or restart a sync during the initial copy phase — before this fix, a cancel issued while all copy streams were backpressured could hang the shutdown. Everyone else can upgrade at their convenience.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.79
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.79
```
