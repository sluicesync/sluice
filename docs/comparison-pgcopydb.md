# sluice vs. pgcopydb — the open-source PG → PG bulk-copy comparison

pgcopydb is the fast-path reference for Postgres → Postgres copy. It's a single
static binary, BSD-licensed, snapshot-only (no CDC), and it's the tool whose
tactics — parallel `COPY`, deferred index/constraint creation, snapshot-based
consistency — directly inspired sluice's bulk-copy implementation (`CLAUDE.md`,
`docs/dev/notes/pgcopydb-planetscale-fork-review.md`). So the honest question an
evaluator asks is: *"for the PG → PG initial copy, how close did sluice actually
get to the thing it was modeled on?"*

**Short answer.** On a single large table sluice's default auto-parallel COPY edges
pgcopydb's default. On a realistic **large mixed corpus (110 GB, 43 tables)** pgcopydb
was originally ~1.75× faster end-to-end (895 s vs 1564 s), from two structural
advantages: it overlaps index builds with the copy (sluice ran them as a separate
phase), and its raw byte-pipe COPY is ~23% faster per stream than sluice's
decode-through-the-IR path. **Both gaps are now shipped optimizations** — index-build
overlap (ADR-0077) and a same-engine PG→PG identity passthrough (ADR-0078) — which
**close sluice's gap from 1.75× to 1.42×** (1269 s vs pgcopydb's 895 s) on the *same*
corpus, all zero-loss. The run is disk-bound at this scale, so the bigger lever turned
out to be the passthrough (−235 s, removing the per-value IR CPU) over the overlap
(−60 s, since overlapped index builds still contend for the saturated disk). sluice's
pitch over pgcopydb stays **coverage** (cross-engine + CDC); the PG→PG speed gap is now
mostly architectural headroom, not a missing tactic.

---

## TL;DR

- **You're PG → PG, one-shot, forever, and you want the leanest possible snapshot
  copy:** pgcopydb is excellent and purpose-built — use it. Its single-stream COPY
  is ~2.5× faster than sluice's, and at matched parallelism it edges ahead.
- **You want one tool that also does MySQL ↔ PG, Vitess/PlanetScale, and
  continuous CDC after the copy — and you want competitive (sometimes better)
  out-of-the-box copy speed:** sluice. On a single large table its default
  auto-parallel COPY beat pgcopydb's default.

The choice isn't "fast vs slow." Both are fast and both are correct. It's
"PG-only snapshot specialist" vs "cross-engine migrate-and-sync generalist whose
copy is in the same league."

---

## Both are correct

Every run was **zero-loss**, verified by aggregate checksums (not just row counts)
across `count`, `sum(id)`, `sum(user_id)`, `sum(amount)` (numeric),
`sum(length(event_type))` (text), and the boolean true-count. 1,500,000 rows
landed identically each time, with all three indexes rebuilt. So this comparison
is purely about **speed and coverage**, not correctness.

---

## Headline numbers (single 1.5M-row table, both with defaults and tuned)

Measured against a 1,500,000-row PG-16 source table (~300 MB), mixed types
(`bigint` PK, `varchar(32)`, `jsonb`, `numeric(12,2)`, `timestamptz`, `boolean`)
+ two secondary indexes. **Both tools run as containers on the same Docker
network** against two PG-16 containers, so neither pays a host→container network
tax the other doesn't. Median of 3 runs; lower is faster.

| Configuration | Median | Throughput | Notes |
|---|---|---|---|
| **sluice — default (8-way)** | **5.05 s** | ~297k rows/s | auto-splits the table into 8 PK-range COPY chunks |
| pgcopydb — split 4-way | 4.86 s | ~309k rows/s | `--split-tables-larger-than 10MB --table-jobs 4 --index-jobs 4` |
| sluice — 4-way (`--bulk-parallelism 4`) | 5.42 s | ~277k rows/s | matched to pgcopydb's 4-way |
| pgcopydb — default (single stream) | 6.14 s | ~244k rows/s | default does **not** split one table |
| sluice — single stream (`--bulk-parallelism 1`) | 15.08 s | ~99k rows/s | per-stream floor |

Three things to read out of that table.

### 1. Out-of-the-box on a single big table, sluice's default won

5.05 s vs 6.14 s (~18% faster). The reason is a **default-behavior difference**,
not a deep throughput edge: for one large table, pgcopydb's `--table-jobs` default
parallelizes *across* tables, so a single table gets a single COPY process unless
you add `--split-tables-larger-than`. sluice's default (`--bulk-parallelism=0` →
`min(8, NumCPU)`) splits a table above 80k rows into PK ranges automatically. So
the out-of-box winner flips with table shape:

- **One (or few) very large tables:** sluice's default auto-split wins — you'd have
  to know to pass pgcopydb's split flags to match it.
- **Many medium tables:** pgcopydb's default cross-table parallelism is already
  doing the right thing; this single-table benchmark doesn't reward it. A
  multi-table benchmark would narrow or reverse the out-of-box gap.

### 2. At matched within-table parallelism, pgcopydb is ~10% faster

pgcopydb split-4 (4.86 s) vs sluice bp4 (5.42 s). Apples-to-apples on splitting,
pgcopydb is ahead. Which leads to the real engineering gap:

### 3. pgcopydb's *per-stream* COPY is ~2.5× faster

Single stream vs single stream: pgcopydb 6.14 s vs sluice 15.08 s. This is the
honest "where pgcopydb is strictly better." pgcopydb drives libpq's binary `COPY`
protocol directly; sluice's per-stream writer path carries more per-row overhead.
sluice closes the gap by leaning on parallelism (its default 8-way ≈ pgcopydb's
tuned 4-way), but if you pinned both to one stream, pgcopydb wins decisively. On a
box with few cores — where you can't parallelize your way out — that per-stream gap
would show through.

---

## Multi-table: the cross-table gap (found here, now closed)

The single-table table above is the *least* favorable shape for pgcopydb's default.
The opposite shape — **many tables** — is where pgcopydb's cross-table parallelism
shines. The original measurement that surfaced the gap: 1.5M total rows across
**30 tables × 50k rows** (each below sluice's 80k within-table-split threshold, so it
isolates the *cross-table* axis). Median of 3, sluice 0.99.17:

| Configuration | Median | Read-out |
|---|---|---|
| **pgcopydb — default (4 table-jobs)** | **6.13 s** | copies 4 tables concurrently |
| pgcopydb — table-jobs 8 | 6.47 s | flattens past 4 (slight contention) |
| **sluice 0.99.17 — default** | **16.12 s** | copied tables *serially* (within-table parallel only) |
| sluice 0.99.17 — `--bulk-parallel-min-rows 10000` | 6.97 s | forced 8-way within-table split per table |

At the time, sluice's `migrate` had **no cross-table concurrency** — the per-table loop
was serial and `--bulk-parallelism` only split *within* a table, so 30 medium tables
were single-streamed *and* serial (~2.6× behind pgcopydb out of the box; tunable to
within ~14% by hand-lowering the split threshold).

**That gap is now closed (roadmap item 3, both phases):**

- **(a) cross-table copy worker pool** ([ADR-0076](adr/adr-0076-cross-table-copy-worker-pool.md), `--table-parallelism`, default 4 — pgcopydb's `--table-jobs` model) copies N tables concurrently, composed with the within-table axis under one combined connection budget.
- **(b) adaptive `--bulk-parallel-min-rows`** (`0 = auto`, scales the threshold down as the table count rises) so a many-medium-table schema auto-engages within-table parallelism — no hand-tuning.

The at-scale run below confirms the pool carries a full 43-table corpus zero-loss; the
remaining gap vs pgcopydb is **no longer cross-table scheduling** — it's per-stream
copy rate and a sequential index phase (both characterized next).

---

## At scale: 110 GB mixed corpus (the honest end-to-end picture)

A larger, fairer corpus than the 1.5M-row micro-benchmarks: **107 GB heap / 133 GB
with indexes, 422M rows across 43 tables** — 3 huge tables (~30 GB each, exercising
within-table PK-range splitting) + 40 medium tables (exercising the cross-table pool)
+ realistic mixed columns (bigint PK, varchar, jsonb, numeric, timestamptz, boolean)
+ 3 secondary indexes per table. Both tools containerized on one Docker network
against two tuned `postgres:16` containers (`shared_buffers=2GB`, `max_wal_size=16GB`,
`maintenance_work_mem=1GB`, `max_connections=300`, `/dev/shm=8g`). Single Windows +
Rancher-Desktop host, shared NVMe. Reproduce via `benchmarks/pgcopydb/` (seed.sql +
gen_fn.sql + bench.sh). All runs zero-loss (aggregate-checksum verified).

| Configuration | Total wall | Notes |
|---|---|---|
| **pgcopydb — default (4 table/index jobs)** | **895 s (~15 min)** | overlaps COPY + CREATE INDEX + VACUUM (12-way) |
| pgcopydb — tuned (`--split-tables-larger-than 2GB --table-jobs 8 --index-jobs 4`) | 985 s | *slower* — over-splitting contends on a saturated disk |
| sluice — original (no index-overlap, IR copy) | 1564 s (~26 min) | bulk-copy 1103 s (99.5 MB/s) **then** a separate 457 s index phase — 1.75× pgcopydb |
| sluice — + index-overlap only (ADR-0077) | 1504 s | index tail collapses into the copy; **−60 s** (disk-bound, so little of the 457 s tail hides) — 1.68× |
| **sluice — + index-overlap + PG→PG passthrough (ADR-0077 + ADR-0078, default today)** | **1269 s (~21 min)** | raw `COPY→COPY` byte-pipe removes the IR per-value CPU; **−235 s** on top — **1.42× pgcopydb** |
| sluice — tuned (`--table-parallelism 8 --bulk-parallelism 8`) | ≈ default | identical to default — more parallelism does nothing (disk-bound) |

Each sluice row was measured on the **same** still-seeded source volume, target reset
between runs, by swapping only the binary (commits `e6ce956` → `22e96fb` → `2a9eace`);
all three landed identical aggregate checksums. The passthrough lane engaged
automatically (logged `raw-copy passthrough lane eligible (ADR-0078)`); index-overlap
is confirmed by the `indexes` phase completing within ~2 ms of `bulk_copy` instead of a
separate ~457 s tail.

Three honest read-outs:

### 1. It's disk-bound — parallelism is maxed, structure is the lever

sluice tuned (64-way) ties sluice default (32-way); pgcopydb tuned (split) is *slower*
than pgcopydb default. The shared NVMe saturates at ~100–122 MB/s and extra workers /
table-splitting only add contention. So the wins are **not** more parallelism — they
are the two structural differences below. (Absolute MB/s is host-specific and far
below pgcopydb's published cloud figures of 1.5–2 TB/hr / 400–500 MB/s, which assume
provisioned-IOPS disks + a fat NIC; *those* need an in-region cloud rig. Here the
portable takeaway is the **ratio**, not the absolute.)

### 2. The original 1.75× gap had two separable causes — both now closed to 1.42×

pgcopydb 895 s vs sluice's *original* 1564 s. From pgcopydb's own step summary (COPY
cumulative 38.9 min + CREATE INDEX cumulative 33.6 min, overlapped into ~14 min wall,
12-way concurrency), the two causes — and what each is worth now that both are shipped
(measured above: index-overlap −60 s, passthrough −235 s):

- **Overlapped index builds.** pgcopydb builds each table's indexes *as soon as its
  data lands*, concurrently with the still-copying tables. sluice runs a full
  bulk-copy phase **then** a separate index phase — a sequential ~457 s tail (29% of
  sluice's total) that pgcopydb hides.
- **~23% higher copy rate** (122.6 vs 99.5 MB/s). pgcopydb byte-pipes the raw COPY
  stream (`COPY … TO STDOUT` → `COPY … FROM STDIN`) with zero per-value work; sluice
  decodes every row into its typed IR and re-encodes it via `pgx.CopyFrom` (binary
  COPY — already the fast pgx path, `row_writer.go`). That decode→IR→re-encode is the
  price of IR-first generality (cross-engine, redaction, type-overrides,
  value-fidelity); even at disk saturation the extra per-byte CPU keeps the disk
  slightly less full.

### 3. Both are correct

Every run landed all 422M rows with matching aggregate checksums (count + sum(id) +
sum(amount) + sum(length(event_type)) + true-count per table) and all indexes. The
comparison is speed + coverage, not correctness.

**Two optimizations this surfaced — both now shipped and measured (above):**

1. **Overlap index builds with the copy** (ADR-0077, broadest win — every engine pair,
   not just PG→PG): build a table's indexes as soon as its copy completes, concurrently
   with ongoing copies, under the same combined connection budget; constraints/FKs stay
   a final phase. The ~457 s sequential tail collapses — but at 110 GB on a saturated
   disk the overlapped builds contend for the same I/O, so the net was a modest **−60 s**.
   The win is larger when the disk is *not* the bottleneck (smaller corpora, faster
   storage, in-region provisioned IOPS).
2. **PG→PG identity passthrough** (ADR-0078, closes the per-stream rate gap): for a
   same-engine, no-transform copy (no redaction / type-override / shard-injection /
   cross-engine), bypass the IR and byte-pipe `COPY … TO STDOUT` → `COPY … FROM STDIN`
   via pgx's raw `pgconn` — pgcopydb's exact tactic — falling back to the IR path the
   moment any transform is present. This removes the per-value decode/re-encode CPU and
   was the **bigger lever at scale: −235 s** (1504 → 1269 s), lifting sluice's per-stream
   rate toward pgcopydb's. A single auditable value-fidelity gate guarantees the lane is
   taken only when there is provably no transform to skip.

## RAM-resident: the software-limited gap (disk ceiling removed)

The 110 GB runs above are **disk-bound** (~100–122 MB/s), so they measure the
storage more than the software past a point. To see where the *software* gap
actually sits post-ADR-0077/0078, we re-ran the same harness on a RAM-resident
corpus (2026-06-10, sluice main @ v0.99.33-tag commit): both `postgres:16`
containers with `PGDATA` on tmpfs (13 GB caps, 31 GB Docker VM), an 8.9 GB
total / 7.0 GB heap mixed corpus (3×4M-row huge + 40×375k medium, same
generators), median of 5 defaults runs + targeted variants, every run zero-loss
checksummed.

| Configuration | wall (s) | aggregate heap MB/s |
|---|---|---|
| **pgcopydb defaults** (`--table-jobs 4`) | **40–41** | **~175** |
| pgcopydb `--table-jobs 8` | 44 | ~160 (4 already saturates) |
| sluice defaults (4 tables × 8 chunks = 32 streams) | 62–65 | ~110 |
| sluice `--table-parallelism=8 --bulk-parallelism=1` (8 single streams, **best**) | 53–56 | ~130 |
| sluice 8 × 2 | 56 | ~127 |
| sluice `--bulk-parallelism=1` (4 single streams) | 80 | ~88 |
| sluice `--raw-copy-format=binary` (4×8) | 62 | text ≈ binary |

What the matrix says:

1. **Defaults-vs-defaults the RAM gap is 1.63×; best-vs-best it is 1.33×** —
   the disk was hiding roughly a third of the measured 1.42× at 110 GB and
   compressing the rest.
2. **Stream *shape* is a real lever in RAM, and sluice's default is disk-tuned.**
   Eight single-table streams beat thirty-two chunk-streams (53 s vs 62 s): the
   per-chunk session setup + budget contention that pays for itself against
   disk latency is overhead at RAM speed. The knobs already exist; the default
   stays disk-tuned because real targets have disks.
3. **The per-stream frontier was found and closed (2026-06-11).** At the time
   of this matrix, pgcopydb's per-stream pipe was roughly 2× leaner even
   against sluice's raw passthrough lane. Profiling the lane found the whole
   gap in one place: PostgreSQL emits one CopyData message *per row* on
   `COPY TO STDOUT`, and the unbuffered pipe between `CopyTo` and `CopyFrom`
   shipped each as its own ~265-byte socket write — one syscall per row
   (81.8% of single-stream CPU), where libpq buffers to 8 KiB internally. A
   64 KiB buffer on the exporter side of the pipe (31 lines) measured
   **4.9× single-stream** (14 → ~73 MB/s on a 4M-row / 1 GB table,
   checksum-identical). The matrix above predates the fix; a re-measure on
   the RAM rig would show materially different stream-shape economics — at
   ~73 MB/s/stream, sluice's per-stream rate now exceeds the ~44 MB/s/stream
   pgcopydb showed at its 4-stream saturation point here.

## Where pgcopydb is strictly better

- **Per-stream COPY throughput** (~2.5×, above). The binary-protocol path is lean.
- **Leanness for the PG-only case.** No IR, no engine registry, no CDC machinery —
  it does exactly one thing.
- **`pgcopydb clone` ergonomics for whole-database snapshot+follow** (it has a
  `--follow` logical-decoding mode; sluice's continuous story is its own CDC, a
  different design, compared elsewhere).
- **Maturity on the PG → PG path specifically.**

## Where sluice is strictly better

- **Cross-engine.** pgcopydb is PG → PG only. sluice does MySQL ↔ PG, Vitess /
  PlanetScale → PG, and PG → MySQL/Vitess (see the cross-engine throughput matrix in
  [`comparison-pgloader.md`](comparison-pgloader.md)). This is the entire reason to
  reach for sluice over pgcopydb.
- **Out-of-box single-large-table copy** (no need to know the split flags).
- **Resumable, checkpointed copy** (`--resume`, per-batch `table_progress` cursor).
- **Continuous CDC after the copy** as a first-class mode, cross-engine.

---

## When NOT to use sluice here (and when pgcopydb wins by default)

If your job is *"snapshot this PG database into that PG database, once, as fast as
possible, and I will tune the flags"* — pgcopydb is the specialist and its tuned
and single-stream numbers say so. sluice earns its place the moment a **second
engine**, a **resumable migration**, or **ongoing sync** enters the picture.

---

## Methodology & caveats (read before quoting these)

- **Hardware/host:** single Windows + Rancher-Desktop Docker host, 8-CPU containers.
  Absolute seconds are host-specific; the *ratios* are the portable part.
- **Images:** `ghcr.io/sluicesync/sluice:0.99.13`, `ghcr.io/dimitri/pgcopydb:latest`
  (pgcopydb 0.17.34, compiled vs PG 16), `postgres:16`. Both tools containerized on
  one user-defined Docker network.
- **Wall-clock includes container start** (~0.5–1 s, similar for both). pgcopydb's
  own internal report put its split-4 "total wall clock" at ~3.4 s vs our measured
  4.86 s, i.e. ~1.4 s is container+connect overhead on its side; sluice's is
  comparable. The gap *between* tools is what matters and is overhead-symmetric.
- **Single-table shape** deliberately exercises **within-table** parallel COPY (the
  pgcopydb tactic sluice borrowed). It is the *least* favorable shape for pgcopydb's
  default (cross-table) parallelism — a multi-table corpus would move the out-of-box
  number toward pgcopydb. Stated plainly so nobody over-reads the default-vs-default
  row.
- **3 repeats, median reported**; best also shown. Variance was low (<5%) for every
  config except the noisy MySQL-target ones (covered in the pgloader companion).
- **Not measured here:** very-wide tables, TOAST-heavy/bytea payloads, `--follow` /
  CDC, multi-table snapshot consistency. Those are separate questions.

See also the cross-engine companion, [`comparison-pgloader.md`](comparison-pgloader.md)
(sluice vs. pgloader for MySQL → PG, plus the Vitess and MySQL throughput matrix).
