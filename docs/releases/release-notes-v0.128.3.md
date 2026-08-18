# sluice v0.128.3

**A concurrency fix in the Vitess/PlanetScale VStream cold-start path.** Closing a snapshot stream now waits for its COPY pump goroutines to exit before tearing down the connection, closing a shutdown-time data race. **Drop-in upgrade from v0.128.2 — no schema/format/flag change.**

## Fixed

**Closing a VStream snapshot stream no longer races its COPY pump goroutines against the connection teardown (the last open-finding on the pump-join roster).** `vstreamSnapshotStream.close()` cancelled the gRPC context, broadcast its condition variable, and closed the connection — but never waited for the COPY pump goroutines (and, on the concurrent auto-shard path, their per-shard sub-goroutines) to actually exit. A straggling pump could then touch the just-closed connection: an unsynchronised access the `-race` detector flags, and a use-after-close in the worst ordering.

`close()` now joins the pumps before closing the connection. It cancels the context (unblocking a pump in `Recv` or interruptible reconnect backoff), flips the pumps' terminal state under the lock and broadcasts — the load-bearing step, because a pump parked in enqueue backpressure waits on the condition variable, which does not observe context cancellation, so it needs the explicit terminal-state flip to wake and exit rather than re-park — then waits on a `WaitGroup` that tracks every COPY goroutine (outside the lock, so a pump blocked on the lock cannot deadlock the wait), and only then closes the connection. A clean end-of-COPY close stays a no-op that never overwrites the finished position; the post-COPY CDC pump is a separate lifecycle, already joined elsewhere.

Pinned by three deterministic unit tests plus a real-vttestserver integration test that closes mid-copy 50 times over on both the single-stream and concurrent (2-shard) configurations, and mutation-verified in both directions (removing the join fails the roster gate and the join pin; a bare broadcast without the terminal-state flip deadlocks the backpressure-wake pin). The shutdown data race itself is confirmed by the CI `-race` integration run.

## Compatibility

Drop-in from v0.128.2 — no schema, format, or flag change. This only affects the shutdown ordering of a VStream (Vitess/PlanetScale) cold-start snapshot stream; steady-state copy and CDC behavior is unchanged.

**Anyone running migrate or sync from a Vitess/PlanetScale source** benefits from the tightened shutdown, though the race window was narrow and shutdown-only. **Everyone else: no action — this is a drop-in upgrade.**

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.128.3
```

Container images: `ghcr.io/sluicesync/sluice:0.128.3` (multi-arch; the image tag carries no `v` prefix).
