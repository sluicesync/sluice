# Throughput tuning

A short reference for the knobs operators reach for when sluice's
defaults don't match the workload. The defaults are tuned for
correctness and the median local-database case; production multi-TB
or cross-host workloads usually want at least one of the items below.

## Per-batch CDC throughput: `--apply-batch-size`

Default: `1` (conservative, one source change per target transaction).
Production tuning: `100`–`500`.

The applier amortises per-tx commit overhead by batching CDC changes.
v0.3.0 testing measured the per-change applier at ~6.5 rows/sec on
PG→MySQL CDC for a 5000-row source transaction. With
`--apply-batch-size=100` the same workload reaches ~600 rows/sec on
local Docker; production hardware sees 3–100× improvements depending
on source transaction shape and network latency. See
[ADR-0017](adr/adr-0017-batched-cdc-apply.md).

## Concurrent CDC apply: `--apply-concurrency`

Default: `auto:N` — fast out of the box as of ADR-0106. The merged CDC change stream is fanned across N in-order lanes by primary-key hash (same key → same lane → applied in source order, so dependent INSERT→UPDATE→DELETE on a row never reorder), each lane committing concurrently on its own dedicated backend with its own AIMD batch-size controller. On a high-latency cross-region link a serial applier is RTT-bound and falls below the source write rate; concurrent lanes lift aggregate apply throughput toward N× (live-validated ~4× on a 2-shard Vitess→PlanetScale-MySQL link).

`N` is conservative and connection-budget-bounded, matching the cold-copy axes' `auto:4` so the whole pipeline has one mental model — sluice fans out ~4-wide by default, bounded by your target:

- **Postgres target:** `min(4, budget)`, where `budget` comes from the same connection-slot probe `--max-target-connections` drives. A constrained instance yields fewer lanes automatically; if the budget is exhausted or the probe is unavailable, apply degrades to serial (the cold-start preflight still owns the loud connection-budget refusal).
- **MySQL / PlanetScale-MySQL target:** a fixed ceiling of `4` — there is no connection-slot probe (`--max-target-connections` is inert against engines without a slot model), and PlanetScale per-branch connection limits are generous relative to 4 lanes + 4 dedicated backends across every tier.

The contract mirrors `--table-parallelism` (`0 = auto:N`, `1 = disable`):

- `--apply-concurrency 0` (the default, unset) → `auto:N`.
- `--apply-concurrency 1` → the explicit **serial opt-out**, byte-identical to the pre-ADR-0106 default. Reach for it if you want strictly serial apply (e.g. a tiny target you'd rather not fan out against).
- `--apply-concurrency W` (W > 1) → honored verbatim — you own your target's connection budget. Raise it for a beefy target.

Correctness is unchanged by the default flip: the resume position advances only to a source boundary durable across all lanes (exactly-once for keyed tables; keyless tables keep their at-least-once baseline via the keyless guard), per-lane AIMD self-throttles on a slow/weak target, and a transient in-lane abort (a PlanetScale tx-killer on MySQL, a serialization/deadlock on Postgres) is handled in-lane (controller shrink + idempotent split-retry) without restarting the stream. See [ADR-0104](adr/adr-0104-mysql-pipelined-cdc-apply.md) (MySQL), [ADR-0105](adr/adr-0105-postgres-concurrent-cdc-apply.md) (Postgres), and [ADR-0106](adr/adr-0106-default-adaptive-apply-concurrency.md) (the fast-by-default decision).

## Parallel within-table bulk copy: `--bulk-parallelism` + `--bulk-parallel-min-rows`

Default: `min(8, NumCPU)` parallel readers per table; tables under
`--bulk-parallel-min-rows` (default `80000` as of v0.62.0; previously
`100000`) stay on the single-reader path.

Tables above the threshold split into N PK ranges and copy
concurrently. The pgcopydb-class signature feature for multi-TB
migrations — 4–8× wall-clock improvement on a 16-vCPU host with a
500 GB single table. See
[ADR-0019](adr/adr-0019-parallel-within-table-bulk-copy.md).

**Threshold-tuning note (v0.62.0+).** Sluice consults
`information_schema.tables.table_rows` (InnoDB) when deciding which
path a table takes. That catalog row-count is an *estimate* that
commonly undershoots actuals by 0.1–5%. The default 80,000 sits
below 100k specifically to absorb that undershoot — a 100k-actual
table reporting as ~95-99k via the catalog still crosses the
threshold and engages parallel copy. Operators wanting the
pre-v0.62.0 behaviour pass `--bulk-parallel-min-rows=100000`
explicitly.

Empirical baseline (local benchmarking rig, 25-table-100k-row
medium fixture, Win11 + Rancher Desktop):

| Configuration | Rows/sec | Wall (2.5M rows) |
|---|---|---|
| v0.61.0 defaults, `local_infile=OFF` | ~28k | 88s |
| v0.61.0 defaults, `local_infile=ON` | ~33k | 75s |
| v0.61.0 `--bulk-parallel-min-rows=50000`, `local_infile=ON` | ~54k | 46s |
| v0.62.0 defaults, `local_infile=ON` | (expected ~50-55k) | (~45-50s) |
| v0.61.0 PG → PG defaults | ~125k | 20s |

Cross-engine note: PG → PG runs ~4× faster than MySQL → MySQL on
the same fixture / same host. The delta is reader-side
(PG's COPY-binary protocol + parallel chunks vs MySQL's per-table
LOAD DATA INFILE). Worth investigating in a future throughput pass.

For local-machine measurement of your own workload, a local
benchmarking rig (bootstrap + throughput-run + record-baseline
scripts) covers the workflow.

## Network compression for cross-host copies

For sluice runs that traverse a real network (across data centres,
across regions, across cloud providers), enabling compression on the
client connections often beats throwing more parallelism at the wire.
Sluice doesn't enable compression by default — the local-database
case it's tuned for has no measurable compression win and the CPU
cost is real.

### Postgres

pgx supports compression negotiation. Set on the DSN:

```
postgresql://user:pass@host/db?sslmode=require&gssencmode=disable
```

Note that compression rides on top of the encrypted channel —
`sslmode=require` (or `sslmode=verify-full`) is the right baseline
for cross-host work. PG 17+ includes a streaming-replication
compression knob (`SET wal_compression = 'lz4'` on the source) that
shrinks WAL volume itself, which is independently useful for
high-write CDC workloads.

### MySQL

The `compress=true` DSN parameter enables MySQL's wire-level
compression:

```
user:pass@tcp(host:3306)/db?compress=true&parseTime=true
```

For PlanetScale-MySQL paths via VStream (gRPC), compression is
negotiated automatically by the gRPC layer — no DSN tuning needed.

### When to tune

Compression hurts on local docker (CPU dominates over already-fast
loopback bandwidth). Worth measuring for any workload where the
sluice host and database host are on different physical machines.
Measured PlanetScale-vs-local throughput tables show a 70-85× gap
that is mostly network latency, not bandwidth, so compression won't
recover most of it. Compression is the right knob for cross-region
high-bandwidth workloads, not for cross-region high-latency ones.

## Memory-bounded streaming: `--max-buffer-bytes`

For workloads with huge rows (TEXT columns at MB scale, BYTEA blobs,
JSON documents) the per-batch memory accumulation can grow into the
hundreds of MB at typical row-count batches. `--max-buffer-bytes`
caps each batch's accumulated byte size, flushing whichever cap
hits first. See [ADR-0028](adr/adr-0028-memory-bounded-streaming.md)
for the full rationale and the audit of where memory accumulates.

## See also

- [ADR-0017 — Batched CDC apply](adr/adr-0017-batched-cdc-apply.md)
- [ADR-0019 — Parallel within-table bulk copy](adr/adr-0019-parallel-within-table-bulk-copy.md)
- [ADR-0027 — Source-transaction-boundary CDC batching](adr/adr-0027-source-transaction-boundary-cdc-batching.md)
- [ADR-0028 — Memory-bounded streaming](adr/adr-0028-memory-bounded-streaming.md)
- [ADR-0104 — MySQL pipelined / concurrent CDC apply](adr/adr-0104-mysql-pipelined-cdc-apply.md)
- [ADR-0105 — Postgres concurrent key-hash CDC apply](adr/adr-0105-postgres-concurrent-cdc-apply.md)
- [ADR-0106 — Fast-by-default adaptive `--apply-concurrency`](adr/adr-0106-default-adaptive-apply-concurrency.md)
