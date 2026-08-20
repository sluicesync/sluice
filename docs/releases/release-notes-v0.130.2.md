# sluice v0.130.2

**An audit-hardening patch.** A fresh independent audit of the v0.130.x line found three internal concurrency / resource-lifecycle weaknesses and a handful of smaller ones. None is a data-loss or a user-hit failure — they are robustness fixes for teardown, memory bounds, and connection lifecycle, surfaced by the audit before anyone ran into them. **Drop-in from v0.130.1 — no new flags, no format change, and no change to any value path; a migrate or sync that worked before is byte-identical after.**

## Fixed

**The VStream snapshot→CDC stream now joins its CDC pump on close.** When a PlanetScale/Vitess cold-started sync tore down (a clean stop, or a stream teardown), `close()` could return while the post-COPY CDC pump goroutine was still finishing its last dispatch — a bounded goroutine leak past the close, and a race window on the raw gRPC stream that the `-race` detector grades. `close()` now cancels the pump on its own derived context (so a pump parked on a full output channel is freed, which cancelling the stream alone could not do) and waits for it to exit before releasing the connection. This is the fourth and last implementor of the CDC pump-join contract the other readers already carried; teardown is now race-free and leak-free.

**The concurrent CDC apply coordinator now bounds its look-ahead.** In the concurrent (multi-lane) apply path, if one lane stalled — for example a hot table pinned to a single lane while the other lanes kept committing — the coordinator's pending-commit set could grow without bound at stream-rate for the duration of the stall (a long storm could reach hundreds of MB to gigabytes). It was never a deadlock or a loss, but the memory growth was real and the code's own comment claimed a bound that lane backpressure did not actually provide. The coordinator now waits for the durable frontier to catch up before running more than a fixed distance ahead, capping the pending set. The cap is a generous safety bound (~1M in-flight sequences) that a healthy stream never reaches, so throughput is unaffected.

**The Postgres pipelined apply path no longer orphans a goroutine on a raw connection at exec-timeout.** On the high-throughput pipelined apply path, a per-statement timeout (the Bug-56 watchdog that bounds a stalled flush against a half-closed destination) ran the batch flush / commit under a wall-clock timer that could not cancel the operation — so on a timeout the flush goroutine was abandoned while still holding the raw pgx connection, which the same code then rolled back and returned to the pool. A pgx connection is not safe for concurrent use, so that was a `-race`-class conflict, and a connection returned to the pool mid-operation could corrupt the next user's protocol (surfacing loudly as a busy-connection / broken-protocol error, never as wrong data — the frontier advances only on a reported commit, so a timed-out batch simply retries and re-applies idempotently). The pipelined path now bounds the flush and commit with the context those pgx operations already accept — the same deadline mechanism the serial exec path uses — so a timeout cancels the operation cleanly and closes the connection with no orphaned goroutine. (The serial path is unchanged and was already safe; only the raw-connection pipelined lane had the exposure.)

**Smaller robustness + documentation fixes.** The standalone VStream CDC reader now refuses a second `StreamChanges` and resets a stale error on start (matching the snapshot reader's guard); the Postgres pgoutput dispatch loop now treats a deliberate-stop cancellation as a clean stop rather than a retriable fault (matching the MySQL binlog reader); and two stale code comments were corrected (the transaction-killer accumulator note, and a note that the `TINYINT(1)` preflight's `MAX_EXECUTION_TIME` cap is inert on MariaDB / MySQL < 5.7.7).

## Compatibility

Drop-in from v0.130.1 — no schema, format, flag, or error-code change, and no change to any value path. The changes are teardown, memory-bound, connection-lifecycle, and error-classification only; a fresh independent value-fidelity review confirmed the delta is value-neutral. Nothing an operator does differently; a run that was correct before is byte-identical after.

**Who needs this:** anyone running the concurrent CDC apply path, the Postgres pipelined apply path, or long-running syncs where one lane can fall behind (route skew) benefits from the memory-bound and connection-lifecycle hardening. Everyone else: no action.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.130.2
```

Container images: `ghcr.io/sluicesync/sluice:0.130.2` (multi-arch; the image tag carries no `v` prefix).
