# sluice v0.99.80

**`--apply-concurrency` graduates to full GA: each concurrent CDC-apply lane now adapts its own batch size and survives a PlanetScale transaction-killer in place — no stream restart.** This closes the v0.99.77 preview caveat. The live cross-region validation (which deliberately forced a transaction-killer storm) also caught and fixed a real convergence gap before this shipped.

## Added

**Per-lane AIMD + in-lane transaction-killer recovery (ADR-0104, item 23(c)).** The concurrent key-hash apply path (introduced in v0.99.77) fans the change stream across W in-order lanes by primary-key hash. Each lane now has its **own** AIMD apply-batch-size controller, so a lane adapts its batch size to the link latency independently — the concurrent path is W parallel *adaptive* appliers, not W static ones. And when a lane hits a PlanetScale transaction-killer abort (Error 1105, "tx killer rollback"), it recovers **in place**: the lane's controller shrinks and the failed batch is **re-chunked** — split in half recursively and re-applied idempotently — until the sub-batches are small enough to commit, with **no whole-run restart**. A transient transaction-killer clears on a quick retry; a persistent one converges by splitting; a single change that still can't commit fails the run loudly after a bounded retry (so warm-resume can recover). Exactly-once is preserved throughout — a sub-batch's changes advance the resume position only after that sub-batch durably commits, in source order across the splits.

Live-validated on a cross-region 2-shard Vitess→PlanetScale-MySQL link: steady-state apply ran at ~233 rows/s on the default configuration (the per-lane AIMD keeps batches small enough that no transaction-killer fires at all), and a deliberately-triggered transaction-killer storm split-and-converged with **zero fatal errors and zero stream restarts**.

This graduates `--apply-concurrency` out of preview: in v0.99.77 the lanes used static batch sizing, so a transaction-killer stopped the stream and warm-resumed (correct, but disruptive) rather than recovering in place. Now they adapt and recover per lane.

## Fixed

**In-lane recovery splits a persistently-killed batch instead of re-applying it whole.** Caught by the GA live validation: the first cut re-applied the same buffered batch on a retriable failure, but the controller's shrink only sized the *next* read — so a batch too large to commit under the transaction-killer timeout could never succeed when re-applied unchanged, exhausting the retry budget and going fatal (a restart loop at a high batch-size ceiling). The fix re-chunks (split-in-half-and-recurse) so the batch converges to committable sub-batches. Pinned by a new test that forces a persistent transaction-killer above a size threshold and proves the input lands exactly-once via splitting.

## Compatibility

Fully backward-compatible and opt-in. `--apply-concurrency` defaults to `0` (serial, byte-identical); set `W>1` (e.g. 4) on a MySQL target to engage. No data, schema, or default-behavior changes. Postgres targets are unaffected (they use the ADR-0092 within-transaction pipelining instead).

**One operating note:** keep `--apply-batch-size` at a sane value (the default is fine). An absurdly high ceiling on a transaction-killer-heavy target can make the controller lag the committable size, causing safe-but-slow split churn (no data loss, no crash, no restart). The controller adapts from a sane default; a follow-up will make an over-large ceiling self-correct faster.

## Who needs this

Operators running `sluice sync` against a **cross-region PlanetScale-MySQL target** whose CDC apply lags the source (the per-shard wedge): `--apply-concurrency=W` (start with 4) lifts apply throughput toward W× and now adapts per lane and rides through transaction-killers without restarting the stream. If you tried `--apply-concurrency` on v0.99.77 against a loaded PlanetScale target and saw stream restarts on transaction-killers, this is the release that fixes that.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.80
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.80
```
