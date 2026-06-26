# sluice v0.99.131

**New: delayed-replica CDC apply — `--apply-delay DURATION` keeps the target a configurable interval behind the source (the MySQL `MASTER_DELAY` "oops window" disaster-recovery pattern, ADR-0121, roadmap item 46). Opt-in and off by default; fully drop-in over v0.99.130.**

## Features

**Delayed-replica CDC apply: `--apply-delay DURATION` (ADR-0121).** A new opt-in steady-state CDC apply mode that holds each change until its **source commit timestamp plus the configured delay** has elapsed before applying it. A target deliberately held N minutes (or hours) behind the source gives you a window to **stop sluice before an accidental `DROP TABLE`, a bad migration, or a runaway `DELETE` on the source replicates** — then recover from the still-intact target. It's cheap, comprehensible point-in-time protection that complements the logical-backups track.

It is **engine-neutral** — works on MySQL and PostgreSQL alike, using the source commit timestamp each CDC reader already carries (`ir.Change.SourceCommitTime()`). Only the steady-state CDC apply is delayed; the cold-start / bulk-copy phase is unaffected, so the initial seed copy still lands at full speed and only the live tail is held back.

**Resume stays exactly-once across a crash mid-delay-window** — the load-bearing invariant: the delay gate sits strictly *upstream* of the applier, which is the only thing that advances the durable resume position (in the same transaction as the data). So a held-but-unapplied change never advances the position and is simply re-read on restart and re-applied idempotently — the delay window is never the sole home of an un-applied change, the source is. **Memory stays bounded by backpressure**, not a large in-heap buffer: the gate holds at most one change and blocks before reading the next, so the source read naturally backpressures behind the delay (the cost is throughput — a delayed replica reads no faster than it applies, which is exactly the intended DR semantics). A whole source transaction releases together (all its row events share the commit timestamp), so a transaction is never split across the delay. The configured delay is subtracted from the `sluice_sync_lag_seconds` metric, so a correctly-running delayed replica reads ~0 lag rather than `delay` seconds — and the sync-lag alert still catches genuine backlog accruing *on top of* the intended delay.

## Compatibility

Off by default (`--apply-delay 0` = no delay = the prior behavior, byte-identical apply path); the zero value is safe for every construction. No other behavior changes. One operational note, documented in the flag help and ADR-0121: for delays approaching the source's replication idle timeout (PostgreSQL `wal_sender_timeout`, default 60s; MySQL `net_write_timeout` / `slave_net_timeout`), the backpressured reader connection may be reaped and sluice will reconnect-and-replay (correct via the resume invariant, but churny) — raise that server-side timeout for large delays. Assumes the source and sluice clocks are roughly aligned (a large skew just shifts the effective delay). Fully drop-in over v0.99.130.

## Who needs this

Operators who want a **delayed-replica DR safety net** — a target kept deliberately behind the source so a destructive mistake can be caught before it propagates. Everyone else is unaffected (the feature is inert unless `--apply-delay` is set). Note: this builds on the PostgreSQL slot-ack-after-apply fix shipped in v0.99.130 — if you intend to run `--apply-delay` on a PostgreSQL source, upgrade past v0.99.130 (the fix is what makes a far-ahead reader resume-safe).

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.131 · **Container:** ghcr.io/sluicesync/sluice:0.99.131
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
