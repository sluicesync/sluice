# Cross-engine initial-copy throughput (and sluice vs. pgloader for MySQL → PG)

Companion to the PG → PG pgcopydb comparison. Same 1.5M-row `events` table, same
two-PG-container Docker network, plus a `mysql:8.0` container and a
`vitess/vttestserver:mysql80` single-shard cluster — all on one network so no path
pays a host→container tax. Same correctness bar: **every run zero-loss, verified by
aggregate checksums** across numeric / text / boolean columns. The four engines
(PG, MySQL, Vitess) all held byte-identical aggregates to the original PG source
after a full PG → Vitess → PG round-trip.

## The competitive number: sluice vs. pgloader, MySQL → PG

pgloader is the canonical open-source MySQL → PG migrator — the cross-engine analog
of pgcopydb. Head-to-head, same source and target, median of 3:

| Tool | Median | Throughput | Notes |
|---|---|---|---|
| **sluice** (`migrate`, default 8-way) | **5.09 s** | ~295k rows/s | parallel PK-range COPY into PG |
| pgloader | 8.72 s | ~172k rows/s | default `pgloader mysql://… postgresql://…` |

**sluice is ~1.7× faster than pgloader on MySQL → PG**, out of the box, both
zero-loss with all three indexes rebuilt. (Setup note: pgloader's bundled MySQL
client can't speak MySQL 8's default `caching_sha2_password` — the server had to be
started with `--default-authentication-plugin=mysql_native_password` for pgloader to
connect at all. sluice's driver handled either auth plugin with no flags. That's a
small but real operational papercut in pgloader's column.)

## Full cross-engine matrix (sluice, 1.5M rows, median of 3)

| Direction | Median | Throughput | Path |
|---|---|---|---|
| PG → PG | 5.05 s | ~297k rows/s | native COPY both ends |
| MySQL → PG | 5.09 s | ~295k rows/s | MySQL read → PG COPY |
| Vitess → PG | 5.35 s | ~280k rows/s | VStream/vtgate read → PG COPY |
| **PG → Vitess** | 27.77 s | ~54k rows/s | vtgate write (batched INSERT) |
| **PG → MySQL** | 44.82 s | ~33k rows/s | LOAD DATA + 2 secondary-index builds |

### The dominant finding: the *target* engine sets the pace, not the source

Anything copying **into Postgres** lands at ~5 s (~290k rows/s) regardless of
whether the source is PG, MySQL, or Vitess — because PG's `COPY` is fast and sluice
parallelizes it. Anything copying **into MySQL** is ~9× slower (~33k rows/s), even
on the fast `LOAD DATA LOCAL INFILE` path, because MySQL's bulk ingest plus
secondary-index construction on 1.5M rows is simply heavier than PG's COPY +
deferred index build.

Operator takeaway: **for a migration, the write side is the bottleneck.** "Moving
*off* MySQL/Vitess *to* Postgres" is the fast, cheap direction; "moving *into*
MySQL" is where you budget time and reach for `--bulk-parallelism`, bigger batches,
and `local_infile=ON`.

### Vitess specifics

> ⚠️ **Version note (Bug 132 — silent-loss regression, fixed in v0.99.18).** The
> Vitess → PG numbers below were measured on v0.99.13 and re-verified on v0.99.18. In
> the interim releases **v0.99.14–v0.99.17**, a `workload=olap` setting interacted with
> the parallel chunked reader so a Vitess/PlanetScale `migrate` of a table above the
> ~80k chunk threshold silently copied only a fraction of the rows and reported success
> (exit 0). **v0.99.18 fixes it** by scoping that setting to the no-PK full scan alone.
> If you ran a PlanetScale/Vitess `migrate` of a ≥100k-row table at default parallelism
> on v0.99.14–v0.99.17, re-verify row counts and re-run on v0.99.18 — the source data
> was never touched; only the target received a partial copy. See the CHANGELOG.

- **Vitess → PG (off-Vitess)** is fast and clean — 5.35 s, indistinguishable from
  MySQL → PG (measured on **0.99.13**, the last version before the regression above).
  This is the "migrating off PlanetScale/Vitess to Postgres" path, and it's a strong
  showing. sluice read via the `planetscale` flavor (vtgate SQL, chunked by the bigint
  PK, which sidesteps the no-PK vtgate row-cap).
- **PG → Vitess (into-Vitess)** works and is zero-loss, but loses the `LOAD DATA`
  fast path: vtgate's embedded MySQL had `local_infile=OFF` (not operator-tunable
  through vtgate), so sluice correctly **fell back to batched INSERT** and said so
  in a WARN. It still finished in 27.8 s.

> ⚠️ **Do not read PG→Vitess (27.8 s) vs PG→MySQL (44.8 s) as "Vitess is faster
> than MySQL."** The vttestserver embedded MySQL runs with **test-tuned durability**
> (relaxed `innodb_flush_log_at_trx_commit`-class settings), while the stock
> `mysql:8.0` container runs full durability. The two MySQL-protocol write numbers
> reflect *different server configs*, not a Vitess-vs-MySQL engine delta. The only
> sound cross-target comparison in this matrix is **PG-target vs MySQL-protocol-
> target** (the ~9× gap), and that holds for both.

## Caveats

- Same host/image/methodology caveats as the pgcopydb doc (8-CPU containers,
  wall-clock includes ~0.5–1 s container start, single-table shape, 3-run medians).
- The MySQL-target numbers were the only noisy ones (42–58 s across runs; the
  reported 44.82 s median is from a *clean serial* re-run after an early run was
  contaminated by a concurrent migration — a reminder these are wall-clock, not
  isolated-CPU, measurements).
- Vitess here is a single-shard `vttestserver`, not a production multi-shard cluster
  with vindexes; sharded-keyspace copy (per-shard fan-out) is a separate benchmark.
- pgloader was run with defaults; it has tuning knobs (`workers`, `concurrency`,
  batch rows) that could narrow the gap, just as sluice has `--bulk-parallelism`.
- Software: sluice v0.99.13 / v0.99.18 (GHCR runtime image), pgloader 3.6.7, MySQL 8.0,
  Postgres 16, `vitess/vttestserver:mysql80`, all containerized on one Docker network.
