# Cross-region migration

Moving a database between regions — AWS to GCP, or two regions of one provider — is not just a slower version of a same-region migration. The round trip between sluice and the target becomes the dominant cost, and several defaults that are right at low latency are wrong at high latency.

This page is written from a completed AWS to GCP `us-east4` migration of a PlanetScale MySQL database (reported 2026-09-08), which reached a caught-up tailing stream with a clean parity check. Everything below is a hoop that migration actually had to jump through.

## Run sluice close to the TARGET, not the source

The copy is read-once, write-many-round-trips. Reads stream: one request pulls a long sequence of rows, so read latency is paid roughly once per stream. Writes do not: every INSERT batch is a separate round trip that must complete before the next one on that connection begins, so write latency is paid *per batch*, thousands of times.

Put the machine running sluice in the target's region. If you have to choose, target proximity beats source proximity, and the difference is measured in hours on a large copy.

## Which bound are you on? (`COPY-BOTTLENECK`)

When the bulk-write path engages against a PlanetScale target, sluice logs one `COPY-BOTTLENECK` line. It does not tell you what to change, because there are two different bounds here with **opposite fixes**, and the discriminator is one number you can read directly:

**Read the target's CPU before changing anything.**

| Target CPU | You are bound by | The lever |
|---|---|---|
| High | The tier | A larger tier, or Metal. More copy parallelism will not scale — a PS-10 pins at 100% under a 2-wide copy (ADR-0116). |
| Low, with flat throughput | Round trips in flight | `--copy-fanout-degree` and `--vstream-copy-table-parallelism`. The server is idle; you are not asking it for enough at once. |

`--planetscale-metrics-db` and `--planetscale-metrics-branch` surface that CPU number.

Cross-region copies are usually the second row, and it is worth knowing why the shape is so lopsided: **vtgate blocks `LOAD DATA`, so the snapshot lands through a single INSERT connection**, and a single connection at 30ms RTT has a hard ceiling no instance size can raise. `--copy-fanout-degree`'s own help says it exists "to beat the single cross-region-RTT-bound INSERT connection vtgate forces" — that is the flag's entire purpose.

The reported migration scaled the target from M-160 (2 vCPU) to M-640 (8 vCPU) and throughput moved 14k to 16k rows/s, with target CPU at **11%**. The tier was never the bound. Adding `--copy-fanout-degree 16 --vstream-copy-table-parallelism 4` took the bulk copy from an estimated 18-36 hours to about 4.

### The two axes, and their defaults

They are siblings and you usually want both; raising one alone moves you onto the other.

- **`--copy-fanout-degree N`** — the WRITE axis. The incoming snapshot row stream is PK-hash-partitioned across N concurrent batched-INSERT writers, each on its own connection. Default is auto: **4**. Bounded by the target connection budget and `--max-target-connections`, and capped at 2 on a target whose plan tier the buffer-pool probe cannot read (which is what a DEV branch looks like) — the cold start logs that cap when it binds.
- **`--vstream-copy-table-parallelism N`** — the READ axis: how many single-table COPY streams run concurrently. The engine default is **1, serial**. On a cross-region copy this is often the first thing to raise.

Both are cold-start only. Start from the reported 16 and 4, watch target CPU, and stop raising when CPU climbs or the connection budget caps you.

## Index builds under PlanetScale safe migrations

Safe migrations refuse direct DDL, which collides with index creation. sluice's automatic answer is the deploy-request fallback (ADR-0148): when a deferred `ADD INDEX` is refused, sluice opens a PlanetScale deploy request and drives it, which also sidesteps the 15-minute direct-DDL timeout.

The route the reported migration settled on, after `--upfront-indexes` did not work for them:

1. Turn safe migrations **off** on the target branch.
2. Start `sluice sync start`, letting it create the schema.
3. Turn safe migrations **on** once the schema exists.
4. Let the copy run; the post-copy index build then goes through the deploy-request fallback.

This is a field-reported recipe, not yet a sluice-verified one. **The `--upfront-indexes` failure under safe migrations is an open question** — ADR-0148 positions that flag as the safe-migrations answer precisely because it avoids attempting the doomed direct `ALTER`, so a failure there is either a gap in that reasoning or a different error wearing the same clothes. If you hit it, the exact error text is the useful thing to capture.

## The quiet window after the copy: finalization

After the bulk copy finishes and every index deploy request completes, there is a **silent stretch** before CDC begins. During it:

- `sync status`, `sync health` and `verify` report that the stream is **not found on target**.
- sluice logs nothing at all until the phase completes.

Both are expected, and neither means the run has died. What is happening is the VStream FLOAT exact re-read: vtgate's row streamer renders single-precision `FLOAT` through mysqld's float-to-text formatter, so a stored `8388608` arrives as `8388610` — a real float32-level loss — and sluice re-reads those columns exactly from the source and repairs them by primary key. Only after that does it persist the CDC anchor.

The ordering is deliberate and load-bearing. CDC replays from the copy anchor, so anything that changed between the copy and the re-read is re-applied to its final value; and because the anchor is *not yet written*, a crash mid-repair re-cold-starts cleanly instead of warm-resuming onto a half-repaired target. Writing the anchor earlier to make the status surfaces happier would trade a cosmetic problem for a correctness one.

**What ends it** is a single INFO line:

```
pipeline: float repair complete — single-precision FLOAT columns re-read exactly from source
```

That line, and the `sluice_cdc_state` row appearing, are the two signals that finalization is done. The window scales with the number of rows in FLOAT-bearing tables, so it is longer on a bigger database. `--no-float-exact-reread` skips the phase entirely and keeps the rounding — a real trade, not a speedup, and sluice WARNs when you take it.

## See also

- [`throughput-tuning.md`](../throughput-tuning.md) — every throughput flag, including the two above
- [`cdc-streaming.md`](cdc-streaming.md) — the applier retry policy and CDC-phase signals
- [ADR-0097](../adr/adr-0097-parallel-writer-fanout-vstream-snapshot-copy.md) — the write-side fan-out design
- [ADR-0148](../adr/adr-0148-planetscale-deploy-request-index-build.md) — the deploy-request index build
