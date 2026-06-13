# sluice backup vs. the backup ecosystem — logical fair fights and physical positioning

sluice's `backup` verb is a **logical, row-level, cross-engine** backup with
CDC-based incrementals. That puts it in a specific lane of a crowded
ecosystem, and the honest comparison starts by naming the lanes:

| Lane | Tools | What they capture | sluice's relationship |
|---|---|---|---|
| **Logical row-level** | `pg_dump`/`pg_restore`, `mysqldump`, `mydumper`/`myloader` | rows + schema, replayable anywhere | **direct competitor** — measured head-to-head below |
| **Physical / WAL-shipping** | pgBackRest, WAL-G, `pg_basebackup`, XtraBackup | data-directory bytes + WAL, PITR, tied to engine major/arch | **deliberately not competing** — see scope note |
| **Managed/Vitess physical** | `vtbackup`, provider snapshots | shard data dirs inside the vendor's system | complement: sluice is the *operator-owned logical copy* of that data |

**The scope line is a design decision, not a gap** (Phase 3 design doc,
`docs/dev/design/logical-backups.md`): *"logical backups only, not
`pg_basebackup` / WAL-archive territory. `wal-g` and `pgbackrest` exist and
are excellent. Sluice's value is the cross-engine and operator-owned-storage
angle, not competing with WAL-shipping tools."* Physical backups are faster
at scale and give PITR — and they restore only onto the same engine,
major-version family, and (usually) architecture. A sluice backup restores
into **Postgres or MySQL, from either**, with redaction and encryption in
the pipeline, into storage the operator owns.

## TL;DR

- **Both structural gaps are now closed (measured below): sluice sits
  1.8× (backup) / 1.5× (restore) behind the `pg_dump`/`pg_restore -j8`
  specialists, with defaults.** The original measurements on this corpus
  were 10.2× and ~11.5× — first the *parallelism* share fell (ADR-0084
  cross-table pools: 2367 s → 881 s backup, projected ~3 h → 2810 s
  restore), then the *per-row* share (the v0.99.39 fast row codec, tasks
  #51/#52: profiling showed reflection-based JSON encode/decode was 49%
  of backup CPU and 69% of restore CPU) cut both legs roughly in half
  again: backup **435 s**, restore **1390 s**, zero-loss. What remains
  (~1.5–1.8×) is dominated by zstd compression of the bulkier
  self-describing JSONL format plus per-chunk SHA-256 — the direct price
  of cross-engine restorability and independent chunk verifiability.
- **Incrementals are the structural reason to pick sluice.** `pg_dump` has
  no incremental story — its only refresh is a full re-dump (232–273 s +
  16 GB every cycle, on our corpus). `sluice backup incremental` captured a
  3.6M-row-event delta in **104 s / 1.5 GB**, and the cost scales with the
  *delta*, not the database. At any realistic change rate, the chain wins
  from the second cycle onward — and it's restorable cross-engine.

---

## The measured corpus

Same corpus as the pgcopydb comparison (`benchmarks/pgcopydb/`): **107 GB
heap / 133 GB with indexes, 422M rows, 43 tables** (3×94M-row huge + 40×3.5M
medium, mixed types: bigint PK, varchar, jsonb, numeric, timestamptz,
boolean, text + 3 secondary indexes per table). Source `postgres:16`
(`wal_level=logical`), tools containerized on one Docker network, single
Windows/Rancher-Desktop host, shared NVMe. Both tools write **zstd**
(sluice's default codec since v0.67.0; `pg_dump --compress=zstd:3`), so the
sizes are codec-matched. Absolute seconds are host-specific; the ratios are
the portable part.

## Full backup (one-shot)

| Configuration | Wall | Output size | Notes |
|---|---|---|---|
| **pg_dump -Fd -j8 --compress=zstd:3** | **232 s** | 16 GB | 8 parallel workers |
| pg_dump -Fd -j1 --compress=zstd:3 | 798 s | 16 GB | single worker |
| sluice backup full — pre-ADR-0084 | 2367 s | 22 GB | sequential per table |
| sluice backup full (non-anchored fallback) | 2398 s | 22 GB | `wal_level=replica` source |
| sluice backup full — ADR-0084 defaults | 881 s | 22 GB | parallel sweep engaged (`table_parallelism=4`), snapshot-anchored, same exported snapshot across all readers; corpus-matched `pg_dump -j8` re-run: 273 s → 3.2× gap |
| **sluice backup full — v0.99.39 defaults (fast row codec #51 + O(1) sidecar checkpoints ADR-0086)** | **435 s** | 22 GB | same parallel sweep; corpus-matched `pg_dump -j8` re-run: 238 s → **1.83× gap** |

Read-outs:

1. **The 10.2× decomposes into ~3.4× parallelism × ~3.0× per-row cost.**
   pg_dump's own j1→j8 spread is 798→232 s (3.4×); sluice-vs-pg_dump at
   one worker each is 2367 vs 798 s (~3.0×). The parallelism axis is the
   same cross-table pool sluice's migrate path already has (ADR-0076,
   `--table-parallelism`) — the backup orchestrator simply predates it and
   is sequential per table (named in `internal/pipeline/backup.go`:
   *"Phase 2 will add parallel reads"*; now shipped — ADR-0084, measured
   in the table above). The ~3× per-row
   residual is the IR decode + JSONL encode + per-chunk SHA-256 — the price
   of cross-engine restorability and per-chunk verifiability that `pg_dump`'s
   engine-native COPY stream doesn't pay.
2. **Snapshot anchoring is free** (2367 vs 2398 s, within noise) — the
   chain-enabling consistent view costs one replication slot, not time.
3. **Sizes are comparable** (22 vs 16 GB, both zstd): JSONL-with-names is
   ~35% bulkier than COPY text — the self-describing format is what makes
   chunks independently verifiable and cross-engine replayable.

## Restore (one-shot)

| Configuration | Wall | Notes |
|---|---|---|
| **pg_restore -j8** | **917 s** | all 422M rows + all 172 indexes, verified |
| sluice restore — pre-ADR-0084 | **cut off at 5278 s** with 2/43 tables done | sequential; each 94M-row table took ~41 min; projected ~3 h+ |
| sluice restore — ADR-0084 defaults | 2810 s | parallel apply engaged (`table_parallelism=4`); all 43 tables + 172 indexes verified; corpus-matched `pg_restore -j8` re-run: 896 s → 3.1× gap |
| **sluice restore — v0.99.39 defaults (fast row codec #52)** | **1390 s** | same parallel apply; all 43 tables verified; corpus-matched `pg_restore -j8` re-run: 918 s → **1.51× gap** |

Restore matters *more* than backup speed — restore time is your recovery
time objective. Pre-ADR-0084 the projection was ~11.5× behind
`pg_restore -j8`; the cross-table writer pool (engine-generic — it engages
for MySQL targets too, since parallel *writers* need no shared snapshot)
brings it to ~3.1× with defaults.

### The before/after read-out

The gap closed in two measured stages, each confirming its decomposition:

1. **ADR-0084 (parallelism):** both legs landed at ~3.1–3.2× of their
   PostgreSQL-native counterpart — almost exactly the ~3.0× per-row
   residual the j1 decomposition below predicted.
2. **v0.99.39 (the per-row residual itself):** CPU profiles on this
   corpus showed the residual was mostly *codec*, not IR: the
   reflection-based `encoding/json` round trip of each row map was 49%
   of backup CPU and 69% of restore CPU. The fast row codec (tasks
   #51/#52 — same wire bytes, direct buffer append/parse, legacy path
   kept as the semantic oracle) plus the O(1) sidecar checkpoints
   (ADR-0086, task #54) cut both legs ~51%: backup 881→435 s, restore
   2810→1390 s. The remaining 1.5–1.8× is **read throughput and the
   bulkier self-describing JSONL format**, not codec choice.
3. **zstd tuning was measured and rejected as a lever (2026-06-12).**
   Although zstd is ~24% of backup CPU, the full backup runs at under
   2 of 8 cores — it is PG-read-bound, not compression-bound — so
   dropping to `SpeedFastest` moved the zstd CPU share 24%→17% but
   bought only −4% backup wall at +0.7% artifact size and +2% restore
   (a wash), while `SpeedBetterCompression` cost +76% wall for −2% size
   and lowering encoder concurrency only serialized the long-pole table.
   `SpeedDefault` (klauspost default concurrency = GOMAXPROCS) is the
   right operating point; no `--compression-level` knob is warranted
   (every non-default setting measured was a net loss). The frontier
   that remains is read throughput, not codec CPU.

(All after-numbers measured on the post-burst corpus: 136 GB / 431M
rows, ~2 % larger than the original 133 GB / 422M — comparators re-run
corpus-matched the same day on the same host.)

## Incremental — the structural win

`pg_dump` cannot do this row. Its only "incremental" is dumping everything
again: **232 s + 16 GB per cycle, forever**, with no point-in-between
recoverability. sluice chains CDC windows off the full's snapshot anchor:

| Scenario (3.6M row events ≈ 1 GB heap delta: 3.0M INSERT + 0.5M UPDATE + 0.1M DELETE) | Wall | Output |
|---|---|---|
| **sluice backup incremental** | **104 s** | 1.5 GB (190 MB change chunks + parent store) |
| pg_dump full re-dump (the only comparator) | 232 s | 16 GB **per cycle** |

The 104 s covers decoding 3,600,021 events through the logical slot,
writing 37 zstd change chunks with per-chunk SHA-256, and committing the
chain-linked manifest. The cost scales with the **delta**: at a tenth the
change rate the incremental is ~10 s against the same 232 s re-dump. Storage
grows by the delta instead of the full size, chains compact (ADR-0046), and
the chain restores cross-engine.

## What the benchmark caught (fixed before this doc shipped)

Running the chain flow end-to-end on a fresh source surfaced a real
operator trap, worth stating because it validates the "validate end-to-end"
tenet: the chain expected a standing replication slot + publication created
*before* the full, nothing provisioned them, and the natural recovery
(create the slot afterwards) made the next incremental **silently** skip
every write in between — PostgreSQL fast-forwards `START_REPLICATION` to the
slot's `confirmed_flush_lsn` without complaint. Both are closed by
ADR-0083: `backup full --chain-slot` provisions slot + publication at the
snapshot anchor (zero-gap chain by construction), and a chain-resume
preflight refuses loudly when a slot cannot serve the parent position. A
second find — spurious `alter_table` schema deltas from non-deterministic
index ordering in catalog reads — is tracked separately (task #41).

## Physical positioning (pgBackRest / WAL-G / vtbackup)

Honest framing for evaluators choosing a *primary* backup strategy:

- **If you need PITR, multi-TB scale, and fastest possible same-engine
  restore:** use a physical tool (pgBackRest, WAL-G; XtraBackup/vtbackup on
  the MySQL/Vitess side). They copy data-directory bytes and archive WAL —
  no per-row work at all — and that lane is theirs by design.
- **What they cannot do, and sluice can:** restore into a *different*
  engine (PG↔MySQL), restore into a different major version cleanly,
  redact PII at capture time, or give a managed-database customer
  (PlanetScale et al.) a logical copy in storage *they* own, outside the
  vendor's system. Vitess's `vtbackup` runs inside the vendor's
  infrastructure; sluice's VStream-based backup is the operator-owned
  logical copy of the same keyspace.
- Many production setups reasonably run **both**: physical for DR of the
  primary, sluice chains for the cross-engine/off-vendor/compliance copy.

### MySQL-side fair fight (measured): sluice vs `mysqldump` vs `mydumper`

A single MySQL → MySQL logical dump+reload of a 35.9 GB corpus (12 tables /
6.72 M rows — 4 large ~8 GB + 8 medium; bigint PK, varchar, `decimal(18,6)`,
`datetime(6)`, JSON, a 1–4 KB high-entropy TEXT payload, 3 secondary indexes
per table). Source InnoDB buffer pool 18 GB (< corpus, so reads are
disk-bound — production-realistic, not RAM-cached). Median of 2 timed backup
runs after a discarded warm-up; restore into a freshly-dropped database. All
three tools **zero-loss verified** (per-table rowcounts src==dst on all 12
tables, `CHECKSUM TABLE … EXTENDED` byte-identical, a BIT_XOR-of-row-MD5 value
digest matched). `voc-m-8c-64gb` (lax), container-to-container.

| Tool | Backup | Restore | Artifact | Ratio |
|---|---|---|---|---|
| **sluice** `backup full` → `restore` | 250.9 s | **145.0 s** | **8.42 GB** (zstd) | **4.26:1** |
| `mysqldump --single-transaction` | 129.1 s | 460.6 s | 16.67 GB (.sql) | 2.15:1 |
| `mydumper --threads 8` / `myloader -j8` | **18.4 s** | 95.6 s | 16.82 GB (raw) | 2.14:1 |

**The honest read:**

- **mydumper wins raw dump+reload, decisively** (≈18 s dump vs sluice's ≈251 s).
  The cause is structural and visible in sluice's own log: `cross-table parallel
  reads not engaged; sweeping tables serially — source snapshot is not shareable
  (per-session)`. MySQL's snapshot is per-session (unlike Postgres's shareable
  snapshot — which is why the PG→PG `--table-parallelism` path above *does*
  overlap table reads), so sluice's MySQL backup sweep is **serial**, and it also
  pays zstd on every chunk. mydumper does neither — parallel uncompressed
  per-table files. On the one job a purpose-built parallel dumper exists for, it
  should win, and it does.
- **sluice's restore is 3.2× faster than `mysqldump`'s** (145 s vs 461 s —
  parallel apply, `table_parallelism=4`); single-threaded SQL replay is
  `mysqldump`'s weak point. sluice's artifact is also **~2× smaller** (always-on
  zstd) — and 4.26:1 here is *conservative*: the synthetic payload is
  high-entropy, so real data compresses further, widening sluice's size lead over
  the uncompressed dumpers.
- **The capability axes are the real differentiator, not the drag race.** On a
  single MySQL→MySQL full dump, mydumper is the faster way to copy bytes; sluice's
  value is the axes the single-purpose dumpers don't have at all: **incremental
  backup chains** (rolling stream + compaction vs re-dump-everything),
  **cross-engine restore** (MySQL backup → Postgres target via the IR), **PII
  redaction**, **envelope encryption** (AWS/GCP/Azure KMS), **off-vendor object
  storage** (S3/GCS/Azure/MinIO/R2/B2), and chunk-level integrity verification.
  mysqldump/mydumper are faster byte-copiers; sluice is a backup *system*.

## Methodology & caveats

- Single host, shared NVMe (~100–120 MB/s effective under contention);
  containerized peers on one Docker network; `postgres:16` source tuned
  `shared_buffers=2GB`, `max_wal_size=16GB`, `wal_level=logical`.
- sluice = dev build at the v0.99.34 tag commit for the ADR-0084 rows;
  the v0.99.39 rows = main at `597fad4` (the v0.99.39 content).
  `pg_dump`/`pg_restore` 16.x.
- The incremental was measured on a chain rooted in a *scoped* full
  (7 tables, 143 s) because the original chain's anchor predated the
  publication (the historic-catalog trap described above — the measurement
  that found the bug). The incremental's cost depends on the delta, not the
  full's size. The standing slot was created/advanced to the anchor point
  before the burst, mirroring the healthy `--chain-slot` state.
- Restore verification: pg_restore's 422M rows + 172 indexes confirmed by
  catalog queries; sluice's partial restore confirmed 188,005,343 rows
  loaded at cutoff. Backup-side outputs were not separately
  checksum-verified against the source in this round (the migrate-path
  benchmarks and the chain integration suite carry the zero-loss pins).
- The MySQL fair-fight section above (sluice vs mysqldump vs mydumper, phase 2)
  was measured on a separate `voc-m-8c-64gb` lax box (8 vCPU / 64 GB / NVMe,
  container-to-container, 35.9 GB corpus, source pool 18 GB < corpus); its
  absolute seconds are box-specific — the durable signal is the tool ratios.
- Not measured here: `--follow`-style continuous sync (different surface,
  see `comparison-bucardo.md`), encrypted-chain overhead, S3-target
  throughput, and the physical tools (pgBackRest / WAL-G / vtbackup — phases
  3/4) which are positioned qualitatively above rather than raced (different
  category — physical bytes, not logical rows).

See also: [`comparison-pgcopydb.md`](comparison-pgcopydb.md) (the bulk-copy
fair fight on the same corpus), [`backup-format-versioning.md`](backup-format-versioning.md)
(manifest/chunk format guarantees).
