# sluice v0.99.87

> ⚠️ **KNOWN ISSUE — upgrade to v0.99.88.** This version's compression support has a CRITICAL edge case: a **large** compressed transaction (more rows than `--apply-batch-size`, default 1000) that is **interrupted mid-apply and warm-resumed** can silently drop the un-applied rows (the resume position could advance past the whole payload). Small compressed transactions and uninterrupted applies are unaffected, but **anyone using `binlog_transaction_compression=ON` should use v0.99.88**, which fixes the resume-position anchoring. Sources without binlog compression are not affected at all.

**MySQL CDC now supports `binlog_transaction_compression=ON`.** Previously, a source with binlog transaction compression enabled would have its compressed transactions *silently* not applied — continuous sync looked healthy but replicated nothing. This fixes that.

## Fixed

**Compressed MySQL transactions are now applied (item 28; previously a silent no-op).** MySQL 8.0.20+ can compress each transaction into a single `TRANSACTION_PAYLOAD_EVENT` (a ZSTD container wrapping the transaction's `TABLE_MAP`, row events, and commit) when `binlog_transaction_compression=ON` — a setting operators commonly enable for WAN replication or disk savings. sluice's binlog dispatch had no handler for that event type, so a compressed transaction fell through to the ignored-events default:

- nothing was applied to the target,
- the resume position never advanced, and
- there was **no error** — only a soft "no row events received during startup grace period" warning, while heartbeats kept the stream looking alive.

So against a compression-enabled source, continuous sync silently replicated nothing — the worst kind of failure for a data-movement tool, and a violation of sluice's loud-failure discipline.

The fix unpacks the payload event and dispatches each decoded inner event (TABLE_MAP, INSERT/UPDATE/DELETE rows, commit) through the normal apply path. The load-bearing subtlety is **position tracking**: the inner events carry synthetic binlog positions (the server zeroes their end-of-event positions when compressing, since they are not independently addressable inside the compressed payload), so sluice stamps each inner event with the **outer** payload event's position — the real transaction boundary. That keeps the persisted file/pos resume point payload-aligned: a warm-resume restarts at the *next* transaction, never partway into a compressed payload (mid-payload misalignment is what surfaces downstream as the `no corresponding table map event` error). GTID-mode resume is unaffected — it tracks the GTID set, which rides the separate GTID event that precedes each payload.

Validated end-to-end against a real compression-enabled MySQL 8.0 source: before the fix, nothing applied and the position stayed frozen; after, compressed `INSERT`/`UPDATE`/`DELETE` apply exactly-once in steady state (source checksum == target checksum), and a warm-resume from a compressed-transaction position drains the accumulated backlog correctly. Pinned by a reader-level integration test (steady-state emit + warm-resume) that was verified to fail without the fix ("got 0 row changes; want 3") and pass with it.

This was found while investigating a large-scale-program resume incident; the earlier `no corresponding table map event` symptom turned out to be the same missing-compression-support gap surfacing under a disk-full-confounded source, not a separate resume bug.

## Compatibility

No interface, flag, or default-behavior changes. Sources *without* binlog transaction compression are completely unaffected (the new code path only runs for `TRANSACTION_PAYLOAD_EVENT`s, which such sources never emit). Both file/pos and GTID resume modes are covered. Postgres sources are unaffected.

## Who needs this

Anyone running `sluice sync` against a **MySQL/Vitess source with `binlog_transaction_compression=ON`** (MySQL 8.0.20+). If you had compression enabled, continuous CDC was silently applying nothing on prior versions — upgrade to v0.99.87. If you don't use binlog compression, this release changes nothing for you.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.87
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.87
```
