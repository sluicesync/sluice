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

## MySQL CDC apply coalescing over WAN (automatic, no flag)

This one needs no tuning — it is on by default — but it is the dominant
factor in MySQL-target CDC throughput on a high-latency link, so it is
worth understanding.

Postgres has a pipelining primitive (`pgx.Batch`), so ADR-0092/0138 made
PG apply send a whole batch in one round trip regardless of size. MySQL
has **no** pipelining primitive: both the single-lane batch loop and the
concurrent apply lanes dispatch one `tx.ExecContext` per change, so a
batch of N changes was N network round trips. On a LAN that is invisible;
over WAN (cross-region, PlanetScale-MySQL) it caps apply at roughly
`lanes / RTT` and stalls behind Vitess's 20-second transaction killer —
measured around ~20 rows/sec to PlanetScale-MySQL versus PG's ~5,000/sec.

sluice now **coalesces** consecutive changes of the same kind and shape
into one statement ([ADR-0139](adr/adr-0139-mysql-multirow-insert-apply.md)
/ [ADR-0140](adr/adr-0140-mysql-coalesce-update-delete-apply.md)):

- Same-table, same-column-shape, keyed **INSERTs** → one multi-row
  `INSERT … VALUES (…),(…),… AS new ON DUPLICATE KEY UPDATE …`.
- A keyed, non-PK-changing **UPDATE** applies as the same keyed upsert,
  so it coalesces alongside inserts to the same table+shape.
- Consecutive keyed **DELETEs** → one `DELETE … WHERE pk IN (?,…)`
  (single-col PK) / `WHERE (a,b) IN ((?,?),…)` (composite PK).

Every value still binds to a `?` through the identical per-value codec
the serial path uses — the wire encoding is byte-for-byte unchanged,
only the number of placeholder groups grows — so this is a round-trip
optimisation, not a value-path change. A run flushes before any
non-coalescable change so apply order matches source order; keyless-table
U/D and PK-changing UPDATEs stay on the serial full-before path.

**Observability.** A rate-limited INFO line (at most once per 30s across
all lanes) reports the running coalescing ratio:

```
mysql: applier: coalescing ratio  rows_per_stmt=12.4 coalesced_rows=… coalesced_statements=… assessment="good — same-kind runs coalescing well"
```

`rows_per_stmt` is the average rows folded into each coalesced statement.
A high ratio means long same-kind runs (one round trip absorbs many
rows); a value near `1` means the workload alternates kinds or has no
runs to coalesce, so apply stays RTT-bound — in that case widen
`--apply-concurrency` instead.

## MySQL bulk-load INSERT batching (automatic, no flag)

The bulk-copy counterpart to the coalescing above
([ADR-0150](adr/adr-0150-mysql-byte-targeted-insert-batching.md)). On
MySQL-flavor targets that can't take `LOAD DATA` — **PlanetScale/Vitess
always** (no `LOAD DATA` support), vanilla MySQL when `local_infile=OFF`
or a geometry column forces the fallback, and every idempotent write
(resume, chunked copy, sync cold-start COPY) — bulk rows land via
multi-row INSERT statements. Those statements are composed to a
**~1 MiB estimated statement body** (the pscale-dumper's battle-tested
size) rather than the old fixed 500 rows, which amortizes 10–20× more
payload per WAN round trip on narrow-row tables. As with coalescing,
every value still binds through the identical per-value codec — only the
number of placeholder groups grows. `--max-buffer-bytes` below 1 MiB
lowers the per-statement size; there is no knob to raise it.

**PlanetScale tier ceiling.** When bulk-loading a PlanetScale target,
sluice logs one INFO noting that writes there are **tier-CPU-bound, not
connection-bound** (a PS-10 pins at 100% CPU under a two-wide copy;
ADR-0116). Raising copy parallelism past the automatic budget will not
scale throughput linearly — a larger tier (or Metal) is the real lever.

## MySQL write-path statement protocol: `interpolateParams` (automatic on PlanetScale/Vitess; DSN opt-in elsewhere)

Every parameterized statement on MySQL's default binary protocol is two
round trips — a hidden `COM_STMT_PREPARE` plus the execute — and sluice
prepares fresh per statement. go-sql-driver's `interpolateParams=true`
collapses that to **one `COM_QUERY` round trip** by rendering values
into the SQL text client-side. Since
[ADR-0153](adr/adr-0153-mysql-flavor-interpolation-default.md), the
**`planetscale` and `vitess` drivers default this ON** (their
deployments are WAN-RTT-shaped) for every connection EXCEPT row-data
read sessions — reads keep the prepared/binary protocol because
MySQL's FLOAT→text conversion does not round-trip float32 values
(ADR-0153's read-fidelity exemption); putting `interpolateParams=false`
in the DSN restores the binary protocol everywhere.

**Vanilla MySQL keeps the binary protocol by default, but the opt-in
works on any flavor:** append `interpolateParams=true` to the DSN (it
survives sluice's DSN parsing untouched). The benefit scales directly
with your round-trip time — roughly one saved RTT per bulk INSERT
statement and per CDC flush; the ADR-0153 real-PlanetScale bench
measured **−33% bulk copy and −26% CDC burst drain at ~100 ms client
RTT**. At LAN RTTs the saving is negligible and can be marginally
negative — interpolated SQL text is fatter on the wire than placeholder
text plus binary args, especially for binary-heavy rows — which is
exactly why vanilla stays conservative by default. Bandwidth note:
interpolated statements typically run ~1.0–1.3× the binary-protocol
bytes (temporals ~2.3×, doubles ~2–3×, ordinary text/binary ~par; worst
case ~2× for zero-padded or escape-dense blobs, since the driver
escapes raw bytes rather than hex-encoding) — this only matters on thin
uplinks where transfer time competes with RTT, and the PlanetScale
bench's −33%/−26% figures are net of that inflation.

Fidelity is not a trade-off: the interpolation encoder is pinned
byte-exact against the binary protocol across the full value-family
matrix (microsecond temporals, DECIMAL/DOUBLE extremes, binary with
every octet, 4-byte UTF-8, JSON/BIT/SET/ENUM/GEOMETRY, NULLs) on
**both** write paths — bulk batched INSERT and CDC coalesced apply —
for all flavors, including under `NO_BACKSLASH_ESCAPES` sessions. Two
caveats sluice handles for you: DSNs pinning a
big5/cp932/gb2312/gbk/sjis/gb18030 collation are unsafe for
interpolation (the PlanetScale/Vitess default steps aside with a WARN;
an explicit `interpolateParams=true` there is refused loudly by the
driver), and a statement whose interpolated text would exceed the
driver's `maxAllowedPacket` transparently falls back to the prepared
path for that statement.

## Sync cold-start cross-table parallelism (per source flavor)

`sluice sync start`'s initial cold-start copy parallelizes across tables like `sluice migrate` does, but the mechanism — and the default — depends on the source flavor, because each flavor has a different consistency story for "N readers, one CDC handoff position":

- **Postgres source:** parallel by default. The exported-snapshot fast path (ADR-0079) reuses migrate's full cross-table × within-table pool, every reader pinned to the one exported snapshot, budget-clamped by the target's connection-slot probe. `--table-parallelism` / `--bulk-parallelism` apply.
- **Self-managed (non-Vitess) MySQL source:** parallel by default — `--copy-table-parallelism` auto-resolves to `min(4, table count)` FTWRL-coordinated pinned-snapshot readers (ADR-0101; the same cross-table auto:4 migrate uses). Consistency is identical to the serial path: one FTWRL cut, one binlog position, no stitch. Sources without the RELOAD privilege (RDS, Aurora, restricted users) fall back to the serial single-snapshot copy with a loud WARN — consistency preserved, concurrency lost; grant RELOAD to restore it. `--copy-table-parallelism=1` (or DSN `copy_table_parallelism=1`) is the serial opt-out. Target-side write concurrency is `readers × --copy-fanout-degree`; the operator owns `W × D ≤ --max-target-connections` (MySQL has no connection-slot probe).
- **Vitess / PlanetScale (VStream) source:** sequential single-stream by default, DELIBERATELY — the cold-copy INFO log names the knob. N concurrent COPY streams are one flag away (`--vstream-copy-table-parallelism`, ADR-0099), but the default stays 1 because the stream count K is not persisted in the resume token: a changed default would silently re-derive a different table→stream partition for an interrupted copy resumed across a version upgrade (ADR-0099 §5). If you set K > 1, resume with the same K.
- **Trigger-CDC flavors (pgtrigger / sqlite-trigger / d1-trigger):** serial by design — the snapshot/anchor consistency argument is bound to a single connection; there is no parallel knob. The cold-start INFO log says so.

## VStream FLOAT exact re-read: `--no-float-exact-reread` (PlanetScale/Vitess source)

vttablet's rowstreamer streams a VStream cold-start COPY over the text protocol, and MySQL's `FLOAT`→text conversion display-rounds single-precision floats to 6 significant digits (a stored `8388608` arrives as `8388610`). sluice's SQL read projection fixes this with a `(col * 1E0)` DOUBLE promotion, but that projection can't be injected into vttablet's server-side SELECT — so the COPY value itself is rounded.

By **default**, `sluice sync start` (and `sluice backup full`) on a PlanetScale/Vitess source **repairs** this: after the bulk copy completes and before CDC begins, sluice re-reads each single-precision `FLOAT` column exactly from the source over a separate SQL connection and UPDATEs the copied rows by primary key (on backup, it patches the archived rows). `DOUBLE` columns and the CDC leg are already exact and untouched.

The cost is one extra source read pass over each table that has a single-precision `FLOAT` column and a primary key — bounded to the PK + FLOAT columns, cursor-paginated on the sync path (bounded memory), and buffered one table at a time on the backup path. On a schema with no single-precision `FLOAT` columns, or a non-VStream source, there is zero cost.

`--no-float-exact-reread` skips the re-read: the `FLOAT` columns retain the 6-significant-digit rounding, and a loud WARN names each affected column. Use it only if you don't care about sub-6-significant-digit `FLOAT` precision (or specifically want the backup's within-row consistency — see `docs/managed-services.md`). A **keyless** table (no primary key to target the re-read) can't be repaired regardless — it WARNs and retains the rounding.

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

## Post-load statistics refresh: `--analyze-after`

A freshly bulk-loaded table has stale planner statistics, so the
first post-cutover queries plan badly until autovacuum or a
background ANALYZE catches up. `migrate --analyze-after` closes
that window at cutover time: once constraints and views are in
place, sluice runs one per-table statistics refresh on the target
(Postgres `ANALYZE`, MySQL `ANALYZE TABLE`, SQLite `ANALYZE`) —
the same reason pgcopydb runs a per-table `VACUUM ANALYZE` by
default. Advisory: a per-table failure WARNs and never fails the
migration. Default off. Migrate-only — a sync target's statistics
churn under continuous CDC apply, so the flag doesn't apply there.

## See also

- [ADR-0017 — Batched CDC apply](adr/adr-0017-batched-cdc-apply.md)
- [ADR-0019 — Parallel within-table bulk copy](adr/adr-0019-parallel-within-table-bulk-copy.md)
- [ADR-0027 — Source-transaction-boundary CDC batching](adr/adr-0027-source-transaction-boundary-cdc-batching.md)
- [ADR-0028 — Memory-bounded streaming](adr/adr-0028-memory-bounded-streaming.md)
- [ADR-0104 — MySQL pipelined / concurrent CDC apply](adr/adr-0104-mysql-pipelined-cdc-apply.md)
- [ADR-0105 — Postgres concurrent key-hash CDC apply](adr/adr-0105-postgres-concurrent-cdc-apply.md)
- [ADR-0106 — Fast-by-default adaptive `--apply-concurrency`](adr/adr-0106-default-adaptive-apply-concurrency.md)
- [ADR-0138 — Pipeline the concurrent PG apply lanes](adr/adr-0138-pipeline-concurrent-apply-lanes.md)
- [ADR-0139 — MySQL multi-row INSERT coalescing](adr/adr-0139-mysql-multirow-insert-apply.md)
- [ADR-0150 — Byte-targeted ~1 MiB INSERT batching for the MySQL bulk-load path](adr/adr-0150-mysql-byte-targeted-insert-batching.md)
- [ADR-0140 — MySQL UPDATE/DELETE apply coalescing](adr/adr-0140-mysql-coalesce-update-delete-apply.md)
