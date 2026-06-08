# sluice vs. pgcopydb — the open-source PG → PG bulk-copy comparison

pgcopydb is the fast-path reference for Postgres → Postgres copy. It's a single
static binary, BSD-licensed, snapshot-only (no CDC), and it's the tool whose
tactics — parallel `COPY`, deferred index/constraint creation, snapshot-based
consistency — directly inspired sluice's bulk-copy implementation (`CLAUDE.md`,
`docs/dev/notes/pgcopydb-planetscale-fork-review.md`). So the honest question an
evaluator asks is: *"for the PG → PG initial copy, how close did sluice actually
get to the thing it was modeled on?"*

**Short answer.** Close, and out-of-the-box sluice is actually a touch faster on a
single large table — but pgcopydb still has a meaningfully faster *per-stream* COPY,
and it remains the right tool when you're PG → PG forever and want nothing but the
copy. sluice's pitch over pgcopydb is **coverage** (cross-engine + CDC), not raw
single-table COPY throughput.

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

## Multi-table: where pgcopydb's default pulls ahead (and how sluice catches up)

The single-table table above is the *least* favorable shape for pgcopydb's default.
The opposite shape — **many tables** — is where pgcopydb's cross-table parallelism
shines and sluice's serial-per-table copy shows its limit. Same 1.5M total rows, now
spread across **30 tables × 50k rows** (each below sluice's 80k within-table-split
threshold, so this isolates the *cross-table* axis). Median of 3, sluice 0.99.17:

| Configuration | Median | Read-out |
|---|---|---|
| **pgcopydb — default (4 table-jobs)** | **6.13 s** | copies 4 tables concurrently |
| pgcopydb — table-jobs 8 | 6.47 s | flattens past 4 (slight contention) |
| **sluice — default** | **16.12 s** | **copies tables serially** (within-table parallel only) |
| sluice — `--bulk-parallel-min-rows 10000` | 6.97 s | forces 8-way within-table split per table |

Two honest read-outs:

1. **Out-of-the-box on many tables, pgcopydb is ~2.6× faster** (6.13 vs 16.12). This
   *reverses* the single-table default result. The cause is architectural:
   **sluice's `migrate` has no cross-table concurrency** — the per-table loop is
   serial (`internal/pipeline/migrate.go`), and `--bulk-parallelism` only splits
   *within* a table. pgcopydb's `--table-jobs` (default 4) copies multiple tables at
   once. On a many-table schema, sluice leaves cores idle between tables.

2. **The gap is mostly *tunable*, not fundamental.** Lowering
   `--bulk-parallel-min-rows` so each 50k table gets split into 8 PK-range chunks
   brings sluice to **6.97 s — within ~14% of pgcopydb.** 8-way *within*-table ≈ 4-way
   *across*-table for total throughput; the default only loses because 50k < the 80k
   threshold leaves each table single-streamed *and* serial. The residual ~14% is the
   serial-table scheduling overhead pgcopydb doesn't pay.

**Roadmap implication (worth a real issue):** sluice would benefit from either
cross-table copy concurrency, or an adaptive `--bulk-parallel-min-rows` that drops as
the table count rises (so many-medium-table schemas auto-engage within-table
parallelism instead of single-streaming serially). Today the operator has to know to
lower the threshold by hand.

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
