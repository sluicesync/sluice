# sluice v0.99.88

**CRITICAL fix: a large compressed MySQL transaction could silently drop rows on warm-resume (a regression in v0.99.87's new `binlog_transaction_compression` support).** If you use binlog transaction compression, upgrade from v0.99.87. Sources without compression are unaffected.

## Fixed

**Large compressed transactions are now resume-safe (item 28 follow-up).** v0.99.87 added `binlog_transaction_compression=ON` support by unpacking each `TRANSACTION_PAYLOAD_EVENT` (the ZSTD container that wraps an entire transaction) and applying its inner row events. To track the resume position, it stamped every inner event with the payload's end position — the transaction boundary. That is correct for a transaction small enough to apply in a single batch, but it is **not** correct for a large one:

- The batched applier commits a transaction larger than `--apply-batch-size` (default 1000 changes) in *several* target batches as it works through the payload.
- Each of those partial, mid-payload commits persisted a resume position that was already *past the entire payload*.
- So if the stream was interrupted partway through applying a large compressed transaction — a PlanetScale transaction-killer restart, a watchdog bounce, a crash — and then warm-resumed, it restarted *after* the payload and **silently skipped the rows it hadn't applied yet**. An exactly-once violation: the target was permanently missing rows, with no error.

Small compressed transactions and uninterrupted applies were never affected (a single-batch transaction commits once, at the payload boundary), which is exactly why the original single-row regression pin didn't catch it. A large multi-row compressed-transaction resume test surfaced it: a 20000-row compressed `INSERT`, killed mid-apply and resumed, came back with roughly 5000 rows permanently missing and the position sitting past the payload.

The fix recognizes the underlying constraint: a compressed payload's inner events have **no independent on-wire positions** (the server zeroes them when compressing), so the payload is atomic for resume — a resume can only re-read from before the payload or skip to after it, never partway in. So every inner row / table-map / BEGIN event now anchors at the payload's **start** position — a mid-payload interruption therefore re-reads the whole payload and idempotently re-applies it (ADR-0010) — and only the **final inner event, the commit**, advances the resume position to the payload's end, once the whole transaction is durably applied. The resume position thus advances past a compressed transaction exclusively when that transaction is fully, durably applied.

Validated end-to-end against a real compression-enabled MySQL 8.0 source: a 20000-row compressed `INSERT` killed mid-CDC-apply (target at ~3000 rows) then warm-resumed re-reads the whole payload and converges exactly (source checksum == target checksum, no loss). Pinned by an integration test that drives a multi-inner-event compressed transaction and asserts the row changes anchor at the payload's start while the commit advances to the end — verified to fail without the fix (row position == commit position) and pass with it. GTID-mode resume is unaffected (it tracks the GTID set carried by the GTID event preceding each payload, not the inner positions).

## Compatibility

No interface, flag, or default-behavior changes. This is strictly a correctness fix for the compression path added in v0.99.87. Sources without `binlog_transaction_compression` never exercise the changed code. Both file/pos and GTID resume modes are covered.

## Who needs this

Anyone running `sluice sync` against a **MySQL/Vitess source with `binlog_transaction_compression=ON`** (MySQL 8.0.20+), especially with large transactions and a target that can trigger stream restarts (e.g. a PlanetScale transaction-killer under load). v0.99.87 shipped compression support with this resume-time silent-loss edge; v0.99.88 closes it. If you don't use binlog compression, nothing changes for you.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.88
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.88
```
