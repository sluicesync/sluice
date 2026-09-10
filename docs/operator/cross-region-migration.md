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

A `sync` cold start does not register its stream on the target until the very end — after the copy, the index build **and** the FLOAT exact re-read have all finished. The ordering is deliberate (see below), so for the whole of a multi-hour cold start there is no stream row to look up.

> **Correction (v0.148.1).** v0.148.0 shipped this section claiming the cold-start block generally. It was not: on a target that had never run `migrate` the progress tables were never created, so the feature was a no-op in its most common case, and it reached only the PostgreSQL fast-path copy. v0.148.1 fixes the table creation and the phantom "in progress" row that outlived a finished copy. **The remaining limitation is real and not yet fixed: the block appears for PostgreSQL sources only.** A MySQL, MariaDB, PlanetScale or Vitess source takes the serial copy path, which still records nothing — so on those sources the blackout described above persists, and the log line below is your signal. That is tracked as audit A0909-P2b.

For a **PostgreSQL source**, `sync status` reports the cold start from the progress rows the copy writes as it goes:

```
cold start in progress (no CDC anchor yet — this is expected, not a stall):
STREAM        PHASE      STARTED               LAST PROGRESS WRITE
prod-cutover  bulk_copy  2026-09-08T10:31:02Z  4s ago
```

**The PHASE advancing is the liveness signal.** `LAST PROGRESS WRITE` moves when the phase does — per-table progress is written to a separate table that `sync status` does not read — so a long copy of one large table can hold that age still for a long time while the run is perfectly healthy. **A climbing age is evidence the run is gone only once the PHASE has also stopped moving**, and on a big table that can take a while. Before killing a run on this signal, compare the target's row counts against the source. (Until v0.149.0 this section said a climbing age alone meant the run was gone; that was wrong for the whole copy phase.) `sync health` says the same thing in its error, and still exits non-zero — a cold start is not a healthy stream, and a cron probe that treated it as one would go quiet exactly when a stuck cold start needed attention.

Before v0.148.0 all three commands reported only **not found on target**, with no way to tell that from a dead process. If you are on an older build, that is what the silence means.

What is happening is the VStream FLOAT exact re-read, logged under `FLOAT-EXACT-REREAD`: vtgate's row streamer renders single-precision `FLOAT` through mysqld's float-to-text formatter, so a stored `8388608` arrives as `8388610` — a real float32-level loss — and sluice re-reads those columns exactly from the source and repairs them by primary key. Only after that does it persist the CDC anchor, which is what those three commands look for.

The phase announces itself, says how many tables it will touch, and logs each one as it completes:

```
pipeline: FLOAT-EXACT-REREAD: re-reading single-precision FLOAT columns exactly from the source
  before CDC starts. The stream is NOT yet registered on the target, so `sync status` and
  `sync health` will report it as not found until this finishes — that is expected, not a stall
  tables=12
pipeline: FLOAT-EXACT-REREAD: table repaired table=orders done=1 of=12
```

Before v0.148.0 it logged nothing at all until it finished, which — together with the status surfaces reporting the stream as absent — is what sent the operator who reported this to `strace`.

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
