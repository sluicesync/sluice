# sluice v0.99.84

**Resilience fix: a MySQL CDC stream could hang forever on warm-resume against an unresponsive source connection.** No data loss, but a stalled-but-healthy-looking stream is a serious shape; this makes the resume preflight fail loudly and reconnect instead of hanging.

## Fixed

**Warm-resume position-verify no longer hangs the stream on a wedged source connection (HIGH resilience).** Found by a three-phase root-cause investigation of a live stall: after a PlanetScale transaction-killer caused a stream restart, a half-dead connection was left in the source connection pool; the warm-resume preflight that confirms the persisted position is still resumable (`SHOW BINARY LOGS` in file/pos mode, a `GTID_SUBSET` check in GTID mode) ran under the stream's *unbounded* context, so the verify query blocked on the TCP read forever. The whole stream wedged — the main goroutine was captured stuck **302 minutes** in the binlog-file check in `IO wait`, the apply position frozen, the process alive but making zero progress (a "looks healthy but dead" stall, not a crash and not silent data loss).

The fix bounds the verify queries with a 30-second timeout. On expiry — when sluice's own deadline fired and the stream is not shutting down — it returns a **retriable** error, so the existing reconnect-and-retry loop draws a fresh connection from the pool and continues, leaving the wedged connection behind. Critically, a verify timeout is **never** treated as an invalid/purged position: that distinction matters because an invalid-position result intentionally triggers a full cold-start re-snapshot, and a transient source hiccup must not cause that. A genuine shutdown (the parent context being cancelled) is likewise not mistaken for the reconnect path.

Pinned by unit tests driven by a fake blocking database driver: the timeout surfaces as retriable (and not as an invalid-position error) for both file/pos and GTID resume modes, returns within the bound rather than hanging, and a parent-context cancel is not misclassified as a source-unresponsive reconnect.

## Compatibility

No interface, flag, or default-behavior changes. The bounded verify timeout (30s) applies to every MySQL/Vitess CDC warm-resume; a healthy source answers these metadata queries in milliseconds, so the only behavior change is that an *unresponsive* source now fails loudly and reconnects instead of hanging. No effect on Postgres sources or on the cold-start path.

## Who needs this

Anyone running `sluice sync` against a MySQL or Vitess/PlanetScale source over a long-lived continuous stream — especially against targets that can induce stream restarts (e.g. a PlanetScale transaction-killer under load). If you have ever seen a sync that stopped advancing while the process stayed up and logged nothing, this closes that class of stall.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.84
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.84
```
