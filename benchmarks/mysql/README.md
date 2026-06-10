# bench-mysql — MySQL→MySQL index-build-overlap benchmark (ADR-0080 / roadmap 3c)

Measures how much the MySQL-target **index-build overlap** (roadmap item 3c,
shipped **v0.99.30**, [ADR-0080](../../docs/adr/adr-0080-mysql-index-build-overlap.md))
actually helps versus the pre-3c **serial post-copy index phase** (v0.99.29), on a
realistic many-indexed corpus. ADR-0080 deliberately published *no* throughput
number ("no number until an at-scale bench measures it") because the analogous
Postgres overlap (ADR-0077) was a **−60 s regression** when disk-bound. This
harness produces the honest measured number for MySQL.

Mirrors the structure of [`benchmarks/pgcopydb/`](../pgcopydb): a seed generator,
a verify/checksum, a reusable-volume up-script, and a retry-hardened detached+polled
run-script. `img/` (cross-compiled binaries) and `results/` (per-run logs) are
gitignored.

## What it measures

- **MySQL→MySQL `sluice migrate`**, both the sluice container and two tuned
  `mysql:8.0` containers on the `benchnet` Docker network. Same-engine isolates the
  index-overlap signal — no cross-engine value-translation noise.
- **Two binaries**, identical except for the 3c feature:
  - **WITH overlap = v0.99.30** — the MySQL `SchemaWriter` implements
    `ir.IncrementalIndexBuilder`, so the orchestrator routes it through
    `runOverlappedCopyAndIndexPhase`: each table's secondary indexes are built (via
    `ALTER TABLE … ADD INDEX`, one job per index, bounded pool of N=4) as that
    table's copy lands, concurrently with the still-copying tables.
  - **WITHOUT overlap = v0.99.29** — pre-3c. Full bulk-copy phase, then a serial
    whole-schema `CreateIndexes`.
  - Built by cross-compiling each tag (`GOOS=linux GOARCH=amd64 CGO_ENABLED=0
    go build -ldflags "-X main.version=vX.Y.Z" -o img/sluice-vX.Y.Z ./cmd/sluice`)
    from a worktree at that tag, then `docker build --build-arg BIN=…`. Each image's
    `--version` is asserted to report the right tag.

## Corpus

30 tables × 1.5 M rows (≈45 M rows total), each:

- `id BIGINT PRIMARY KEY` (so the reader can chunk + keyset-paginate),
- a mix of columns (`user_id`, `DECIMAL`, `VARCHAR`, `JSON`, `DATETIME`,
  `TINYINT(1)`, fat `TEXT` filler),
- **4 SECONDARY indexes** (`idx_user_id`, `idx_created_at`, `idx_event_type`,
  `idx_active_amt` composite) — so per-table index-build work is a *meaningful
  fraction* of the copy, which is the whole point.

Realized size: **≈10.7 GiB total = 7.55 GiB data + 3.13 GiB index** (index is ~29 %
of the corpus — a large, measurable index-build fraction).

MySQL has no `generate_series`, so rows come from a **tally table** (a 0–9 digit
table cross-joined 7× → 10 M candidate rows, `LIMIT nrows`); `gen_table()` is then a
server-side `INSERT … SELECT`. The source tables carry the 4 secondary indexes
inline so sluice's reader sees them and recreates them on the target.

## Config

Both `mysql:8.0` containers: `--local-infile=ON` (sluice's `LOAD DATA LOCAL INFILE`
fast loader needs it on the target — it probes `@@local_infile` and falls back to
batched INSERT if OFF), `--innodb-buffer-pool-size=3G`,
`--innodb-redo-log-capacity=4G`, `--innodb-flush-log-at-trx-commit=2`,
`--max-connections=300`, `--skip-log-bin`. Host ports **3326 (src) / 3327 (dst)** to
avoid the local rig's 3316/3317.

## Running

```bash
# 1. Boot + seed once (≈30 min seed; persists in the benchmysqlsrc volume).
bash bench-mysql-up.sh            # re-running later is a ~10s reattach, seed skipped

# 2. Run each binary (≥2× each). Detached + polled; survives Hyper-V socket blips.
SLUICE_IMG=sluice-bench-mysql:v0.99.29 bash bench-mysql.sh run1   # WITHOUT overlap
SLUICE_IMG=sluice-bench-mysql:v0.99.30 bash bench-mysql.sh run1   # WITH overlap
```

Each run resets the target DB, runs the migrate detached, captures **total wall**
from `docker inspect` StartedAt/FinishedAt (immune to control-plane blips), extracts
the **phase split** from sluice's structured slog (`phase=bulk_copy` vs
`phase=indexes` "phase complete" timestamps), then verifies **zero-loss** (a content
checksum: per-table count + sum(id) + sum(amount) + sum(len(event_type)) +
true-count, md5'd) **and** that all 4 secondary indexes are present on every target
table (`information_schema.statistics`).

**Phase-split caveat (read the numbers correctly).** In the **serial** path
(v0.99.29) `bulk_copy` completes, then the separate `indexes` phase completes later
— so `index` is the *genuine* post-copy tail. In the **overlap** path (v0.99.30) the
orchestrator logs `bulk_copy` and `indexes` "phase complete" back-to-back after the
*combined* phase returns, so the reported `index` is ~0 and `bulk_copy` already
*includes* the overlapped builds. The honest cross-binary comparison is therefore
**total wall**.

## Results

Host: Windows 11 + Rancher Desktop, local Docker, both DBs containerized on one
host (shared disk/buffer-pool — a disk-bound regime, the realistic worst case for
overlap, same as ADR-0077's PG bench). Corpus 10.7 GiB, 30×1.5 M, 4 indexes/table.

| binary | run | total wall | bulk_copy | post-copy index tail |
|--------|-----|-----------:|----------:|---------------------:|
| **v0.99.29** (serial)  | run1  | 2680.9 s | 1177.4 s | **1500.3 s** |
| **v0.99.29** (serial)  | run2  | 2207.2 s | 773.3 s  | **1432.5 s** |
| **v0.99.30** (overlap) | run1  | **2111.0 s** | 2109.4 s (combined copy+index) | ~0 (folded in) |
| **v0.99.30** (overlap) | run2b | **2132.5 s** | 2131.1 s (combined copy+index) | ~0 (folded in) |

(One v0.99.30 attempt — "run2" — aborted mid-copy on a `mysql: rows iteration:
invalid connection` host-socket blip, NOT a sluice fault: both DBs stayed up 3 h,
corpus intact, max_used_connections 65 ≪ 300. Re-run as "run2b". This is the exact
Rancher Hyper-V flap the harness retry-wraps; a single dropped DB connection mid-read
isn't recoverable inside one migrate, so the harness reported it failed and we re-ran
— the documented flakiness, surfaced loudly, not silently.)

**Representative (median of the 2 good runs each):**

- WITHOUT overlap (v0.99.29): **~2444 s** total wall (copy + a **~1432–1500 s serial
  index tail** — the index tail is *longer than the copy itself*, because 3.13 GiB of
  secondary index over 45 M rows is a lot of `ALTER … ADD INDEX` work).
- WITH overlap (v0.99.30): **~2122 s** total wall (copy + index folded into one
  combined phase).
- **Overlap delta: ≈ −322 s (−13.2 %) on the medians; −570 s (−21.3 %) run1-vs-run1.**

All four good runs **ZERO-LOSS-OK** (source/target content checksums match,
`0375a14fb115d8f0843757a1923ab3a9`) and **ALL-INDEXES-OK** (30 tables × 4 secondary
indexes present on the target).

**On the variance.** The serial baseline's *copy* phase varied a lot run-to-run
(1177 s → 773 s — buffer-pool warmth / less host contention on the 2nd run), which
widens the wall-delta band (13 %–21 %). The **stable, directly-comparable signal is
the index work**: the serial post-copy `ALTER … ADD INDEX` tail is **~1432–1500 s and
rock-steady across runs**, and the overlap collapses essentially all of it into the
copy window (the overlap runs have *no* separate tail and land at ~2110–2133 s wall,
also steady). So the win is real and repeatable regardless of which baseline copy
time you anchor to; the median (−13 %) is the conservative number to publish, the
run1-vs-run1 (−21 %) the best case.

### Honest read-out: a WIN at this scale (not neutral/negative)

Unlike ADR-0077's Postgres overlap (−60 s, a regression on saturated disk), the
MySQL overlap is a **clear, repeatable win here: ~13 % off the median total wall (up
to ~21 %)**. Why the difference:

1. **The index tail is huge and steady.** With 4 secondary indexes over 45 M rows /
   3.13 GiB, the serial post-copy `ALTER … ADD INDEX` phase (~1432–1500 s, stable
   across both baseline runs) is *larger than the copy phase itself* (~773–1177 s).
   There is a lot of tail to collapse, so even partial overlap recovers hundreds of
   seconds. (ADR-0077's PG corpus had only 3 indexes and a copy-dominated profile, so
   there was little tail to win back and disk contention dominated.)
2. **The overlap is real and concurrent.** Live `SHOW PROCESSLIST` during a
   v0.99.30 run shows `ALTER TABLE bench_N ADD INDEX` running *concurrently* with
   `LOAD DATA LOCAL INFILE` on other tables — the early-copied tables' indexes build
   while later tables are still loading, instead of waiting for the whole copy.

### Bottleneck observations

- **Disk / buffer-pool bound, but not saturated enough to erase the win.** Both DBs
  share one host's disk + a 3 GiB buffer pool each; the index builds and copies
  compete. Yet the structural overlap (do index work *during* otherwise
  copy-/IO-bound time) still nets out positive, because the serial baseline leaves
  the CPU/IO under-utilized during its long pure-index tail.
- **Within-table MDL serialization (a real, named limit on the per-index-job
  design).** ADR-0080 builds *one index per job* so indexes parallelize across
  workers. But InnoDB takes a table **metadata lock** per `ALTER`, so the 4
  `ADD INDEX` jobs *for the same table* cannot run simultaneously — `SHOW
  PROCESSLIST` shows one "altering table" + siblings "Waiting for table metadata
  lock". So the parallelism the design buys is **cross-table**, not within-table.
  This does **not** hurt at 30 tables (plenty of cross-table work to fill the N=4
  pool), and it confirms ADR-0080's deferred note that a **combined
  `ALTER … ADD INDEX a, ADD INDEX b` per table** (one table scan, all 4 indexes)
  is the right measured follow-up — it would both dodge the MDL ping-pong and share
  the table scan. At this scale the win is already large without it.

### What scale surfaces / amplifies the signal

The signal is already large here (index tail > copy). It is surfaced by **(a) many
indexes per table** (4 here; with 1–2 the tail would be a small fraction and the win
modest) and **(b) enough tables to keep the N=4 index pool busy during copy** (30
here). The win would **shrink** toward neutral if: indexes/table dropped to 1, the
corpus were copy-dominated with a tiny index tail, or storage were so saturated that
concurrent index builds purely stole copy bandwidth (the ADR-0077 PG regime). It
would **grow** with more indexes/table or wider rows.

## Files

- `gen_mysql.sql` — tally table + `gen_table()` procedure (seed generator).
- `verify_mysql.sql` — `bench_checksum()` content checksum (defined transiently per
  run so a full copy never carries it to the target).
- `bench-mysql-up.sh` — boot tuned src+dst, seed once into the `benchmysqlsrc`
  volume, reuse on re-run.
- `bench-mysql.sh` — run one migrate, time it (docker-inspect wall + slog phase
  split), verify zero-loss + index presence.
- `img/Dockerfile` — alpine runtime wrapping a cross-compiled `sluice` binary
  (`--build-arg BIN=…`). `img/` + `results/` gitignored.

### Combined-ALTER incremental (overlap per-index vs overlap+combined)

Measures the combined-`ALTER` follow-up (`591da65`) ON TOP of the per-index
index-build overlap (v0.99.30). Both images use the overlap code path, so the
phase split logs `bulk_copy`/`indexes` back-to-back (index≈0); **total wall** is
the comparison. Same corpus (30×1.5 M, 4 BTREE idx/table — all combinable, so
the combined `ALTER` fully engages: 4 `ADD INDEX` → 1 `ALTER`/table), same host.
Interleaved, 2 clean runs each.

| image | run | total wall | zero-loss | indexes |
|-------|-----|-----------:|-----------|---------|
| `sluice-bench-mysql:v0.99.30` (per-index overlap) | run1 | 2120.4 s | ZERO-LOSS-OK | 30×4 |
| `sluice-bench-mysql:main-combined` (overlap+combined) | run1 | **1801.3 s** | ZERO-LOSS-OK | 30×4 |
| `sluice-bench-mysql:v0.99.30` (per-index overlap) | run2 | 2233.9 s | ZERO-LOSS-OK | 30×4 |
| `sluice-bench-mysql:main-combined` (overlap+combined) | run2 | **1763.9 s** | ZERO-LOSS-OK* | 30×4 |

- **Per-index median 2177 s; combined median 1783 s → −18.1 % (−394 s).** Per-pair
  −15.0 % (run1) and −21.0 % (run2). A strong, consistent incremental win.
- **Why it's bigger than one might expect at 30 tables** (where cross-table
  parallelism already keeps the N=4 pool busy): the win is per-table scan-sharing,
  not parallelism — each table's 4 index builds collapse from 4 InnoDB clustered-index
  scans + 4 metadata-lock acquisitions (with the within-table MDL ping-pong the
  v0.99.30 bench observed) down to ONE scan + ONE MDL. That per-table saving
  compounds across all 30 tables regardless of how full the pool is.
- *run2's harness `checksum()` step flaked EMPTY on a Rancher Hyper-V socket blip
  (rc=0 migrate, both src+dst checksums returned empty → script's `-n` guard reported
  a false MISMATCH). Re-running the checksum out-of-band after the blips passed:
  src == dst == `0375a14fb115d8f0843757a1923ab3a9` — zero-loss confirmed. The migrate
  was clean; only the post-run verify CLI blipped. (`run-incremental.sh`'s retry only
  re-runs on rc≠0, so a flaked-but-rc=0 checksum slips through — a harness note, not a
  product issue.)

Net of both bench passes: serial post-copy tail → overlap (−13 % median, v0.99.30) →
overlap + combined-`ALTER` (a further −18 %).
